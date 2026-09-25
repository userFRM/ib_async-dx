//! ib_async's `IB`: the session, its settings and its methods.

mod account;
mod extras;
mod market_data;
mod orders;
mod reference;

use std::fmt;
use std::future::Future;
use std::ops::{BitAnd, BitOr, BitXor, Deref, Not};
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};

use jiff::tz::TimeZone;
use jiff::{SignedDuration, Timestamp, Zoned};

use crate::contract::Contract;
use crate::engine::EClientConfig;
use crate::error::{Error, Result};
use crate::event::{Event, lock, on_owner};
use crate::live::Live;
use crate::objects::{
    AccountValue, CommissionReport, Execution, Fill, IBDefaults, NewsBulletin, NewsTick, PnL,
    PnLSingle, PortfolioItem, Position, ScanDataList,
};
use crate::order::{Order, Trade};
use crate::owner::{self, Attempt, Class, Entry, LOG_IB, Shared, SyncOpts, Via};
use crate::pending::{Pending, Registration, Unpark};
use crate::requests::fresh_token;
use crate::session::{self, Logon};
use crate::ticker::{Tick, Ticker};
use crate::timer::{Clock, Sleep, TimerHandle, Timers};
use crate::util::{TimeT, block_on, global_error_event};

pub use crate::state::Bars;
pub(crate) use orders::clear_volatility;

/// The timeouts a method takes when its `timeout` is `None`.
pub mod defaults {
    use std::time::Duration;

    pub use super::extras::{CORPORATE_ACTIONS_TIMEOUT, SPREAD_SCAN_TIMEOUT};
    pub use super::reference::HISTORICAL_TIMEOUT;

    /// `set_timeout`'s timeout when none is given.
    pub const SET_TIMEOUT: Duration = Duration::from_secs(60);

    /// `Client::connect`'s timeout when none is given.
    pub const CLIENT_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
}

/// ib_async's timeout of 0, no limit, where its default is no limit too.
fn unlimited_at_zero(timeout: Option<Duration>) -> Option<Duration> {
    timeout.filter(|t| !t.is_zero())
}

/// A waker that wakes the threads waiting on an IB's progress.
struct Nudge(Weak<Shared>);

impl Wake for Nudge {
    fn wake(self: Arc<Self>) {
        if let Some(ib) = self.0.upgrade() {
            ib.nudge();
        }
    }
}

fn running() -> Error {
    Error::Value("This event loop is already running".to_owned())
}

/// The settings ib_async keeps as `IB` class attributes: `RequestTimeout`,
/// `RaiseRequestErrors`, `MaxSyncedSubAccounts` and `TimezoneTWS`. Read at
/// each use, so a change applies from the next call.
#[derive(Clone, Debug)]
pub struct IBConfig {
    /// How long a blocking call waits, `None` or zero without limit:
    /// `RequestTimeout`, whose 0 is no limit.
    pub request_timeout: Option<Duration>,
    /// Whether a request ended by an error fails with it, where a list
    /// result otherwise ends with what arrived: `RaiseRequestErrors`.
    pub raise_request_errors: bool,
    /// The most accounts whose updates `connect` asks for:
    /// `MaxSyncedSubAccounts`.
    pub max_synced_sub_accounts: usize,
    /// The zone a naive execution time is in, `None` the system's:
    /// `TimezoneTWS`.
    pub timezone_tws: Option<TimeZone>,
}

impl Default for IBConfig {
    fn default() -> Self {
        IBConfig {
            request_timeout: None,
            raise_request_errors: false,
            max_synced_sub_accounts: 50,
            timezone_tws: None,
        }
    }
}

/// What `connect` fetches at startup: ib_async's `StartupFetch` flags.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StartupFetch(u8);

impl StartupFetch {
    /// The positions.
    pub const POSITIONS: StartupFetch = StartupFetch(1);
    /// The open orders.
    pub const ORDERS_OPEN: StartupFetch = StartupFetch(2);
    /// The completed orders.
    pub const ORDERS_COMPLETE: StartupFetch = StartupFetch(4);
    /// The account's updates.
    pub const ACCOUNT_UPDATES: StartupFetch = StartupFetch(8);
    /// Each account's updates.
    pub const SUB_ACCOUNT_UPDATES: StartupFetch = StartupFetch(16);
    /// The executions.
    pub const EXECUTIONS: StartupFetch = StartupFetch(32);
    /// Everything: `StartupFetchALL`.
    pub const ALL: StartupFetch = StartupFetch(63);
    /// Nothing: `StartupFetchNONE`.
    pub const NONE: StartupFetch = StartupFetch(0);

    /// Whether every flag of `other` is set.
    pub fn contains(self, other: StartupFetch) -> bool {
        self.0 & other.0 == other.0
    }
}

impl BitOr for StartupFetch {
    type Output = StartupFetch;
    fn bitor(self, rhs: Self) -> Self {
        StartupFetch(self.0 | rhs.0)
    }
}

impl BitAnd for StartupFetch {
    type Output = StartupFetch;
    fn bitand(self, rhs: Self) -> Self {
        StartupFetch(self.0 & rhs.0)
    }
}

impl BitXor for StartupFetch {
    type Output = StartupFetch;
    fn bitxor(self, rhs: Self) -> Self {
        StartupFetch(self.0 ^ rhs.0)
    }
}

impl Not for StartupFetch {
    type Output = StartupFetch;
    fn not(self) -> Self {
        StartupFetch(!self.0 & Self::ALL.0)
    }
}

/// What `connect` takes: ib_async's `connect` arguments, with the engine's
/// settings in place of a gateway's host and port.
pub struct ConnectOptions {
    /// The engine's settings: the login, and `readonly`. `Default` sets
    /// `paper`.
    pub config: EClientConfig,
    /// `clientId`: a 32-bit integer.
    pub client_id: i64,
    /// Bounds the wait for the account's working orders, ib_async's
    /// handshake, and each startup request: `timeout`. `Some(ZERO)` is none.
    pub timeout: Option<Duration>,
    /// Bounds the engine's login alone, which no ib_async timeout bounds.
    /// `None` is the engine's own bounds, a person's second factor included.
    pub logon_timeout: Option<Duration>,
    /// The account whose updates are fetched, `""` for a lone one:
    /// `account`.
    pub account: String,
    /// Whether a startup request that times out fails the connect:
    /// `raiseSyncErrors`.
    pub raise_sync_errors: bool,
    /// What is fetched at startup: `fetchFields`.
    pub fetch_fields: StartupFetch,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        ConnectOptions {
            config: EClientConfig {
                paper: true,
                ..EClientConfig::default()
            },
            client_id: 1,
            timeout: Some(Duration::from_secs(4)),
            logon_timeout: None,
            account: String::new(),
            raise_sync_errors: false,
            fetch_fields: StartupFetch::ALL,
        }
    }
}

/// What `qualify_contracts` finds for a contract; `Unknown` is ib_async's
/// `None`.
#[derive(Clone, Debug, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "ib_async gives the contract itself"
)]
pub enum Qualified {
    /// No contract matched.
    Unknown,
    /// The contract, filled in.
    One(Contract),
    /// Every contract that matched, with `return_all`.
    Ambiguous(Vec<Contract>),
}

/// An item of `loop_until`: its condition's value once it holds.
#[derive(Clone, Debug, PartialEq)]
pub enum LoopItem<T> {
    /// Not yet: ib_async yields the falsy value.
    Pending,
    /// The condition's value.
    Done(T),
    /// The timeout passed first: ib_async yields `False`.
    TimedOut,
}

/// One session: ib_async's `IB`. Dropping it closes the session and
/// returns once it has logged out. Its methods are [`IBHandle`]'s.
pub struct IB {
    handle: IBHandle,
}

/// A handle to an [`IB`]'s methods that does not keep its session open: what
/// a handler captures.
#[derive(Clone)]
pub struct IBHandle {
    pub(crate) shared: Arc<Shared>,
}

impl Deref for IB {
    type Target = IBHandle;
    fn deref(&self) -> &IBHandle {
        &self.handle
    }
}

impl IB {
    /// ib_async's event names, as `IB.events` lists them.
    pub const EVENTS: [&'static str; 25] = [
        "connectedEvent",
        "disconnectedEvent",
        "updateEvent",
        "pendingTickersEvent",
        "barUpdateEvent",
        "newOrderEvent",
        "orderModifyEvent",
        "cancelOrderEvent",
        "openOrderEvent",
        "orderStatusEvent",
        "execDetailsEvent",
        "commissionReportEvent",
        "updatePortfolioEvent",
        "positionEvent",
        "accountValueEvent",
        "accountSummaryEvent",
        "pnlEvent",
        "pnlSingleEvent",
        "scannerDataEvent",
        "tickNewsEvent",
        "newsBulletinEvent",
        "wshMetaEvent",
        "wshEvent",
        "errorEvent",
        "timeoutEvent",
    ];

    /// A new IB, not connected: `IB()`. The thread that runs every IB is
    /// started if none runs.
    pub fn new() -> Result<IB> {
        IB::with(IBDefaults::default(), IBConfig::default())
    }

    /// A new IB with `defaults` and `config`: `IB(defaults)`.
    pub fn with(defaults: IBDefaults, config: IBConfig) -> Result<IB> {
        IB::build(defaults, config, Clock::system())
    }

    fn build(defaults: IBDefaults, config: IBConfig, clock: Clock) -> Result<IB> {
        let shared = Shared::new(defaults, config, clock);
        shared.connect_internal_slots();
        owner::register(shared.clone())?;
        Ok(IB {
            handle: IBHandle { shared },
        })
    }

    /// An IB whose session is `client`, published as a connect with `opts`
    /// would publish it, on `clock`.
    #[cfg(test)]
    pub(crate) fn attach(
        client: crate::engine::EClient,
        opts: ConnectOptions,
        clock: Clock,
    ) -> Result<IB> {
        let ib = IB::build(IBDefaults::default(), IBConfig::default(), clock)?;
        let via = Via::Test(Some(Arc::new(client)));
        let (p, _logon) = ib.begin_connect(opts, true, Some(via))?;
        owner::park_on(p)?;
        Ok(ib)
    }

    /// A handle that does not keep the session open.
    pub fn handle(&self) -> IBHandle {
        self.handle.clone()
    }

    /// Runs `callback` on the thread that runs every IB at `time`: ib_async's
    /// `schedule`. A time past runs it at once.
    pub fn schedule<F: FnOnce() + Send + 'static>(
        time: impl Into<TimeT>,
        callback: F,
    ) -> Result<TimerHandle> {
        let h = owner::owner()?;
        let at = deadline_of(time.into())?;
        Ok(h.timers.schedule(at, callback))
    }

    /// Waits `secs` while every IB goes on: ib_async's `sleep`. A peer or
    /// internal close of any IB ends it with that close's error.
    pub fn sleep(secs: Duration) -> Result<bool> {
        wait_for(Instant::now().checked_add(secs))
    }

    /// Waits until the instant `t`: ib_async's `waitUntil`.
    pub fn wait_until(t: impl Into<TimeT>) -> Result<bool> {
        if on_owner() {
            return Err(running());
        }
        wait_for(deadline_of(t.into())?)
    }

    /// `wait_until`'s async form, on the owner's timers: ib_async's
    /// `waitUntilAsync`.
    pub fn wait_until_async(t: impl Into<TimeT>) -> impl Future<Output = Result<bool>> + Send {
        let t = t.into();
        async move {
            let h = owner::owner()?;
            let at = deadline_of(t)?;
            h.timers.sleep(at).await;
            Ok(true)
        }
    }

    /// Each time from `start` to `end` by `step`, reached in turn and given
    /// once it is: ib_async's `timeRange`. Times already past are skipped.
    pub fn time_range(
        start: impl Into<TimeT>,
        end: impl Into<TimeT>,
        step: Duration,
    ) -> Result<impl Iterator<Item = Result<Zoned>>> {
        let (t, end, step) = range(start.into(), end.into(), step)?;
        if on_owner() {
            return Err(running());
        }
        Ok(TimeRange { t, end, step })
    }

    /// `time_range`'s async form, each step an async sleep on the owner's
    /// timers: ib_async's `timeRangeAsync`.
    pub fn time_range_async(
        start: impl Into<TimeT>,
        end: impl Into<TimeT>,
        step: Duration,
    ) -> Result<impl futures_core::Stream<Item = Zoned> + Send> {
        let (t, end, step) = range(start.into(), end.into(), step)?;
        let timers = owner::owner()?.timers.clone();
        Ok(TimeStream {
            timers,
            t,
            end,
            step,
            sleep: None,
        })
    }
}

impl Drop for IB {
    /// Closes the session, runs everything admitted before the drop, takes
    /// back a logon in flight, ends every event and waiter, and returns once
    /// the engine has logged out. On the thread that runs every IB it returns
    /// at once, and the session closes after the current step.
    fn drop(&mut self) {
        let ib = &self.handle.shared;
        ib.queue.latch();
        owner::wake_owner();
        if on_owner() {
            log::warn!(
                target: LOG_IB,
                "an IB was dropped inside a handler: it closes after the current step"
            );
            return;
        }
        ib.wait_progress(None, |p| p.closed);
    }
}

impl fmt::Debug for IB {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.handle, f)
    }
}

impl fmt::Debug for IBHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.shared.connected() {
            Some((_, client)) => write!(
                f,
                "<IB connected to {} clientId={}>",
                client.account_id,
                self.client_id()
            ),
            None => f.write_str("<IB not connected>"),
        }
    }
}

/// The deadline at the instant `t` names.
fn deadline_of(t: TimeT) -> Result<Option<Instant>> {
    let now = Timestamp::now();
    let at = t.to_zoned(now, &TimeZone::system())?;
    Ok(Clock::system().instant_at(at.timestamp()))
}

/// Blocks until `deadline`, `None` never, listening to `global_error_event`.
fn wait_for(deadline: Option<Instant>) -> Result<bool> {
    let left = deadline.map(|d| d.saturating_duration_since(Instant::now()));
    match block_on(std::future::pending::<()>(), left) {
        Ok(()) | Err(Error::Timeout) => Ok(true),
        Err(e) => Err(e),
    }
}

/// A time range's first time not yet past, its end, and its step.
fn range(
    start: TimeT,
    end: TimeT,
    step: Duration,
) -> Result<(Option<Zoned>, Zoned, SignedDuration)> {
    if step.is_zero() {
        return Err(Error::Value("the step must be positive".into()));
    }
    let now = Timestamp::now();
    let tz = TimeZone::system();
    let mut t = Some(start.to_zoned(now, &tz)?);
    let end = end.to_zoned(now, &tz)?;
    let step = SignedDuration::try_from(step).map_err(|e| Error::Value(e.to_string()))?;
    while let Some(at) = t.as_ref().filter(|t| t.timestamp() < now) {
        t = at.checked_add(step).ok();
    }
    Ok((t, end, step))
}

struct TimeRange {
    t: Option<Zoned>,
    end: Zoned,
    step: SignedDuration,
}

impl Iterator for TimeRange {
    type Item = Result<Zoned>;

    fn next(&mut self) -> Option<Result<Zoned>> {
        let t = self.t.take()?;
        if t.timestamp() > self.end.timestamp() {
            return None;
        }
        if let Err(e) = wait_for(Clock::system().instant_at(t.timestamp())) {
            return Some(Err(e));
        }
        self.t = t.checked_add(self.step).ok();
        Some(Ok(t))
    }
}

struct TimeStream {
    timers: Timers,
    t: Option<Zoned>,
    end: Zoned,
    step: SignedDuration,
    sleep: Option<Sleep>,
}

impl futures_core::Stream for TimeStream {
    type Item = Zoned;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Zoned>> {
        let this = self.get_mut();
        let Some(t) = this.t.clone() else {
            return Poll::Ready(None);
        };
        if t.timestamp() > this.end.timestamp() {
            this.t = None;
            return Poll::Ready(None);
        }
        let sleep = this
            .sleep
            .get_or_insert_with(|| this.timers.sleep(Clock::system().instant_at(t.timestamp())));
        if Pin::new(sleep).poll(cx).is_pending() {
            return Poll::Pending;
        }
        this.sleep = None;
        this.t = t.checked_add(this.step).ok();
        Poll::Ready(Some(t))
    }
}

/// Takes a connect back when its future is dropped before it ends.
struct TakeBack(Option<Logon>);

impl Drop for TakeBack {
    fn drop(&mut self) {
        if let Some(l) = self.0.take() {
            l.take_back();
        }
    }
}

macro_rules! events {
    ($($(#[$doc:meta])* $name:ident: $ty:ty,)*) => {
        impl IBHandle {
            $(
                $(#[$doc])*
                pub fn $name(&self) -> &Event<$ty> {
                    &self.shared.events.$name
                }
            )*
        }
    };
}

events! {
    /// The end of `connect`: `connectedEvent`.
    connected_event: (),
    /// A session's end: `disconnectedEvent`.
    disconnected_event: (),
    /// The end of each network read: `updateEvent`.
    update_event: (),
    /// The tickers a read changed, at its end: `pendingTickersEvent`.
    pending_tickers_event: Vec<Live<Ticker>>,
    /// A bar list's new or changed bar: `barUpdateEvent`.
    bar_update_event: (Bars, bool),
    /// An order placed: `newOrderEvent`.
    new_order_event: Live<Trade>,
    /// An order modified: `orderModifyEvent`.
    order_modify_event: Live<Trade>,
    /// An order cancelled from here: `cancelOrderEvent`.
    cancel_order_event: Live<Trade>,
    /// An order reported that no request asked for: `openOrderEvent`.
    open_order_event: Live<Trade>,
    /// A trade's status changed: `orderStatusEvent`.
    order_status_event: Live<Trade>,
    /// A trade's fill: `execDetailsEvent`.
    exec_details_event: (Live<Trade>, Fill),
    /// A fill's commission: `commissionReportEvent`.
    commission_report_event: (Live<Trade>, Fill, Live<CommissionReport>),
    /// A portfolio item changed: `updatePortfolioEvent`.
    update_portfolio_event: PortfolioItem,
    /// A position changed: `positionEvent`.
    position_event: Position,
    /// An account value changed: `accountValueEvent`.
    account_value_event: AccountValue,
    /// An account summary value changed: `accountSummaryEvent`.
    account_summary_event: AccountValue,
    /// A P&L changed: `pnlEvent`.
    pnl_event: Live<PnL>,
    /// A position's P&L changed: `pnlSingleEvent`.
    pnl_single_event: Live<PnLSingle>,
    /// A scan's results: `scannerDataEvent`.
    scanner_data_event: Live<ScanDataList>,
    /// A news headline: `tickNewsEvent`.
    tick_news_event: NewsTick,
    /// An IB news bulletin: `newsBulletinEvent`.
    news_bulletin_event: NewsBulletin,
    /// Wall Street Horizon metadata: `wshMetaEvent`.
    wsh_meta_event: String,
    /// Wall Street Horizon event data: `wshEvent`.
    wsh_event: String,
    /// A warning or error, as (reqId, code, message, contract):
    /// `errorEvent`.
    error_event: (i64, i64, String, Option<Contract>),
    /// No data for the time `set_timeout` set, in seconds: `timeoutEvent`.
    timeout_event: f64,
    /// Each tick record, as it is appended to its ticker.
    tick_event: (Live<Ticker>, Tick),
}

impl IBHandle {
    /// The settings in force: the class attributes ib_async's `IB` has.
    pub fn config(&self) -> IBConfig {
        self.shared.config()
    }

    /// Changes the settings, which apply from each one's next use.
    pub fn set_config(&self, config: IBConfig) {
        self.shared.set_config(config);
    }

    /// What bounds a blocking call: `IBConfig.request_timeout`, zero being
    /// no limit, as ib_async's `RequestTimeout` of 0 is.
    fn request_timeout(&self) -> Option<Duration> {
        unlimited_at_zero(self.config().request_timeout)
    }

    /// `Client.clientId`: -1 before any connect, then the last connect's.
    pub(crate) fn client_id(&self) -> i64 {
        self.shared
            .client_id
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    // -- Connection and loop -------------------------------------------------

    /// A connect's waiter and its take-back switch; its command is admitted
    /// at the waiter's first poll or wait.
    pub(crate) fn begin_connect(
        &self,
        opts: ConnectOptions,
        sync: bool,
        via: Option<Via>,
    ) -> Result<(Pending<()>, Logon)> {
        let ConnectOptions {
            config,
            client_id,
            timeout,
            logon_timeout,
            account,
            raise_sync_errors,
            fetch_fields,
        } = opts;
        let client_id = session::client_id(client_id)?;
        let logon = Logon::new(config.cancel.clone());
        let sync = sync.then(|| SyncOpts {
            account: account.clone(),
            raise_sync_errors,
            fetch: fetch_fields,
            readonly: config.readonly,
        });
        let named = if account.is_empty() {
            config.username.clone()
        } else {
            account
        };
        let via = via.unwrap_or_else(|| Via::Engine(Box::new(config)));
        let token = fresh_token();
        let (mut p, reply) = Pending::new(Some(Registration::new(self.shared.abandoner(), token)));
        let a = Attempt::new(
            token,
            reply,
            logon.clone(),
            via,
            client_id,
            named,
            timeout.filter(|t| !t.is_zero()),
            logon_timeout,
            sync,
        );
        let entry = Entry::step(Class::Request, move |ib| ib.connect_step(a));
        self.shared.hold(&mut p, entry);
        Ok((p, logon))
    }

    /// Opens the session and synchronizes with it: ib_async's `connect`.
    ///
    /// The engine logs in, waits for the account's working orders, then the
    /// positions, orders, account updates and executions `opts` names are
    /// fetched. Bounded by `IBConfig.request_timeout`, and the login alone by
    /// `opts.logon_timeout`. On a connected IB it closes the session first,
    /// and this fails with `Socket disconnect`, as ib_async's does.
    pub fn connect(&self, opts: ConnectOptions) -> Result<()> {
        if on_owner() {
            return Err(running());
        }
        let now = Instant::now();
        let login = opts.logon_timeout.and_then(|t| now.checked_add(t));
        let request = self.request_timeout().and_then(|t| now.checked_add(t));
        let (p, logon) = self.begin_connect(opts, true, None)?;
        wait_connect(p, &logon, request, login)?;
        self.warn_competing();
        Ok(())
    }

    /// `connect`'s async form: ib_async's `connectAsync`. Dropping the future
    /// takes the connect back.
    pub async fn connect_async(&self, opts: ConnectOptions) -> Result<()> {
        let (p, logon) = self.begin_connect(opts, true, None)?;
        let mut guard = TakeBack(Some(logon));
        let r = p.await;
        guard.0 = None;
        r?;
        self.warn_competing();
        Ok(())
    }

    /// Says at WARN that another session held the account when this one
    /// connected.
    fn warn_competing(&self) {
        if let Ok(Some(s)) = self.competing_session() {
            let holding = if s.read_only {
                ", holding the account, so this session may only read"
            } else {
                ""
            };
            log::warn!(
                target: LOG_IB,
                "Another session was logged in on this account when this one connected: \
                 from {}, logged in at {} UTC{holding}",
                s.origin,
                s.logged_in_at.strftime("%Y%m%d-%H:%M:%S"),
            );
        }
    }

    /// Closes the session and gives ib_async's status line, `None` when no
    /// session is open: ib_async's `disconnect`. Every call ends every
    /// `run()`.
    pub fn disconnect(&self) -> Option<String> {
        self.shared
            .step(Class::Control, |ib| Ok(ib.disconnect()))
            .unwrap_or(None)
    }

    /// Whether the session is up: ib_async's `isConnected`. A loss the
    /// engine is still recovering counts as up.
    pub fn is_connected(&self) -> bool {
        self.shared.is_ready()
    }

    /// Waits for the next network read of this IB: ib_async's
    /// `waitOnUpdate`, `None` or zero waiting without limit. `Ok(false)` at
    /// the timeout; a peer or internal close of any IB fails it with that
    /// close's error once its teardown has run.
    pub fn wait_on_update(&self, timeout: Option<Duration>) -> Result<bool> {
        if on_owner() {
            return Err(running());
        }
        let deadline = unlimited_at_zero(timeout).and_then(|t| Instant::now().checked_add(t));
        // The close's error decides this gate, published when the owner's
        // unit ends, as `block_on`'s is; its publication nudges this wait.
        let (mut gate, reply) = Pending::<()>::new(None);
        let reply = Mutex::new(Some(reply));
        let event = global_error_event();
        let id = event.connect(move |e| {
            let reply = lock(&reply).take();
            if let Some(reply) = reply {
                reply.send(Err(e.clone()));
            }
        });
        let waker = Waker::from(Arc::new(Nudge(Arc::downgrade(&self.shared))));
        let mut cx = Context::from_waker(&waker);
        let mut failed = None;
        let start = self.shared.progress(|p| p.passes);
        let passed = self.shared.wait_progress(deadline, |p| {
            if failed.is_none()
                && let Poll::Ready(Err(e)) = Pin::new(&mut gate).poll(&mut cx)
            {
                failed = Some(e);
            }
            failed.is_some() || p.passes != start || p.closed
        });
        drop(gate);
        event.disconnect(id);
        if let Some(e) = failed {
            return Err(e);
        }
        if self.shared.is_closed() {
            return Err(Error::NotConnected);
        }
        Ok(passed)
    }

    /// Checks `condition` after every network read until it gives a value
    /// or `timeout` passes, `None` or zero being no limit: ib_async's
    /// `loopUntil`.
    pub fn loop_until<'a, T, F: FnMut() -> Option<T> + 'a>(
        &'a self,
        condition: F,
        timeout: Option<Duration>,
    ) -> Result<impl Iterator<Item = Result<LoopItem<T>>> + 'a> {
        if on_owner() {
            return Err(running());
        }
        Ok(LoopUntil {
            ib: self,
            condition,
            end: unlimited_at_zero(timeout).map(|t| Instant::now().checked_add(t)),
            waiting: false,
            done: false,
        })
    }

    /// Emits `timeout_event` once no data has arrived for `timeout`, `None`
    /// being 60 seconds and zero disarming it: ib_async's `setTimeout`.
    pub fn set_timeout(&self, timeout: Option<Duration>) {
        let t = timeout.unwrap_or(defaults::SET_TIMEOUT);
        self.shared.control(move |ib| ib.set_timeout(t));
    }

    /// Blocks until the next `disconnect()` of this IB: ib_async's `run()`,
    /// whose loop runs until stopped. On the thread that runs every IB it
    /// returns at once.
    pub fn run(&self) -> Result<()> {
        if on_owner() {
            return Ok(());
        }
        let start = self.shared.progress(|p| p.stops);
        self.shared
            .wait_progress(None, |p| p.stops != start || p.closed);
        Ok(())
    }

    /// Runs `f` to its end, bounded by `timeout`, `None` or zero being no
    /// limit: ib_async's `run(awaitable)`. A peer or internal close of any
    /// IB fails it.
    pub fn run_until<F: IntoFuture>(&self, f: F, timeout: Option<Duration>) -> Result<F::Output> {
        block_on(f, unlimited_at_zero(timeout))
    }

    // -- State reads ---------------------------------------------------------

    fn read<R>(&self, f: impl FnOnce(&crate::state::State) -> R) -> R {
        f(&self.shared.core().state)
    }

    /// The accounts this login holds: `managedAccounts`.
    pub fn managed_accounts(&self) -> Vec<String> {
        self.read(|s| s.accounts.clone())
    }

    /// The account values of `account`, `""` for every account:
    /// `accountValues`.
    pub fn account_values(&self, account: &str) -> Vec<AccountValue> {
        self.read(|s| {
            s.account_values
                .values()
                .filter(|v| account.is_empty() || v.account == account)
                .cloned()
                .collect()
        })
    }

    /// The account summary of `account`, `""` for every account, asked for
    /// first when none has arrived: `accountSummary`.
    pub fn account_summary(&self, account: &str) -> Result<Vec<AccountValue>> {
        let timeout = self.request_timeout();
        block_on(self.account_summary_async(account), timeout)?
    }

    /// `account_summary`'s async form: `accountSummaryAsync`.
    pub async fn account_summary_async(&self, account: &str) -> Result<Vec<AccountValue>> {
        let empty = self.read(|s| s.acct_summary.is_empty());
        if empty {
            self.shared.account_summary_request().await?;
        }
        Ok(self.read(|s| {
            s.acct_summary
                .values()
                .filter(|v| account.is_empty() || v.account == account)
                .cloned()
                .collect()
        }))
    }

    /// The portfolio of `account`, `""` for every account: `portfolio`.
    pub fn portfolio(&self, account: &str) -> Vec<PortfolioItem> {
        self.read(|s| {
            s.portfolio
                .iter()
                .filter(|(a, _)| account.is_empty() || a.as_str() == account)
                .flat_map(|(_, items)| items.values().cloned())
                .collect()
        })
    }

    /// The positions of `account`, `""` for every account: `positions`.
    pub fn positions(&self, account: &str) -> Vec<Position> {
        self.read(|s| {
            s.positions
                .iter()
                .filter(|(a, _)| account.is_empty() || a.as_str() == account)
                .flat_map(|(_, items)| items.values().cloned())
                .collect()
        })
    }

    /// The subscribed P&Ls, by account and model code, `""` for any: `pnl`.
    pub fn pnl(&self, account: &str, model_code: &str) -> Vec<Live<PnL>> {
        self.read(|s| {
            s.req_id_to_pnl
                .values()
                .filter(|p| {
                    let v = p.read();
                    (account.is_empty() || v.account == account)
                        && (model_code.is_empty() || v.model_code == model_code)
                })
                .cloned()
                .collect()
        })
    }

    /// The subscribed position P&Ls, by account, model code and contract,
    /// `""` or 0 for any: `pnlSingle`.
    pub fn pnl_single(&self, account: &str, model_code: &str, con_id: i64) -> Vec<Live<PnLSingle>> {
        self.read(|s| {
            s.req_id_to_pnl_single
                .values()
                .filter(|p| {
                    let v = p.read();
                    (account.is_empty() || v.account == account)
                        && (model_code.is_empty() || v.model_code == model_code)
                        && (con_id == 0 || v.con_id == con_id)
                })
                .cloned()
                .collect()
        })
    }

    /// Every trade of the session: `trades`.
    pub fn trades(&self) -> Vec<Live<Trade>> {
        self.read(|s| s.trades.values().cloned().collect())
    }

    /// The trades not yet done: `openTrades`.
    pub fn open_trades(&self) -> Vec<Live<Trade>> {
        self.read(|s| {
            s.trades
                .values()
                .filter(|t| !t.read().is_done())
                .cloned()
                .collect()
        })
    }

    /// Every trade's order, the handle it was placed with: `orders`.
    pub fn orders(&self) -> Vec<Live<Order>> {
        self.read(|s| s.trades.values().map(|t| t.read().order.clone()).collect())
    }

    /// The orders of the trades not yet done: `openOrders`.
    pub fn open_orders(&self) -> Vec<Live<Order>> {
        self.read(|s| {
            s.trades
                .values()
                .map(|t| t.read())
                .filter(|t| !t.is_done())
                .map(|t| t.order.clone())
                .collect()
        })
    }

    /// Every fill of the session: `fills`.
    pub fn fills(&self) -> Vec<Fill> {
        self.read(|s| s.fills.values().cloned().collect())
    }

    /// Every execution of the session: `executions`.
    pub fn executions(&self) -> Vec<Execution> {
        self.read(|s| s.fills.values().map(|f| f.execution.clone()).collect())
    }

    /// The ticker of `contract`, if one was requested: `ticker`. A contract
    /// that cannot be hashed is `Err(Value)`.
    pub fn ticker(&self, contract: &Contract) -> Result<Option<Live<Ticker>>> {
        let key = contract.ticker_key()?;
        Ok(self.read(|s| s.tickers.get(&key).cloned()))
    }

    /// Every ticker: `tickers`.
    pub fn tickers(&self) -> Vec<Live<Ticker>> {
        self.read(|s| s.tickers.values().cloned().collect())
    }

    /// The tickers the last read changed: `pendingTickers`.
    pub fn pending_tickers(&self) -> Vec<Live<Ticker>> {
        self.read(|s| s.pending_tickers.iter().cloned().collect())
    }

    /// Every list a subscription keeps up to date: `realtimeBars`.
    pub fn realtime_bars(&self) -> Vec<Bars> {
        self.read(|s| s.req_id_to_subscriber.values().cloned().collect())
    }

    /// The news headlines, a copy: `newsTicks`.
    pub fn news_ticks(&self) -> Vec<NewsTick> {
        self.read(|s| s.news_ticks.clone())
    }

    /// Changes the news headlines in place, as a program trims the list
    /// ib_async's `newsTicks` returns.
    pub fn edit_news_ticks(
        &self,
        f: impl FnOnce(&mut Vec<NewsTick>) + Send + 'static,
    ) -> Result<()> {
        self.shared.step(Class::Control, move |ib| {
            let mut ticks = std::mem::take(&mut ib.core().state.news_ticks);
            f(&mut ticks);
            ib.core().state.news_ticks = ticks;
            Ok(())
        })
    }

    /// The IB news bulletins, one per message id: `newsBulletins`.
    pub fn news_bulletins(&self) -> Vec<NewsBulletin> {
        self.read(|s| s.msg_id_to_news_bulletin.values().cloned().collect())
    }
}

/// The blocking connect's wait: until the connect ends, a close of any IB
/// emits `global_error_event`, the request timeout passes, or the login
/// deadline passes while the login still runs. Leaving takes the connect
/// back.
fn wait_connect(
    mut p: Pending<()>,
    logon: &Logon,
    request: Option<Instant>,
    login: Option<Instant>,
) -> Result<()> {
    let (mut gate, reply) = Pending::<()>::new(None);
    let reply = Mutex::new(Some(reply));
    let event = global_error_event();
    let id = event.connect(move |e| {
        let reply = lock(&reply).take();
        if let Some(reply) = reply {
            reply.send(Err(e.clone()));
        }
    });
    let waker = Waker::from(Arc::new(Unpark(thread::current())));
    let mut cx = Context::from_waker(&waker);
    let r = loop {
        if let Poll::Ready(Err(e)) = Pin::new(&mut gate).poll(&mut cx) {
            logon.take_back();
            break Err(e);
        }
        if let Poll::Ready(r) = Pin::new(&mut p).poll(&mut cx) {
            break r;
        }
        let login_due = login.filter(|_| logon.logging_in());
        let next = [request, login_due].into_iter().flatten().min();
        match next.map(|d| d.checked_duration_since(Instant::now())) {
            None => thread::park(),
            Some(Some(left)) if !left.is_zero() => thread::park_timeout(left),
            Some(_) => {
                let now = Instant::now();
                if login_due.is_some_and(|d| d <= now) && logon.expire() {
                    break Err(Error::Timeout);
                }
                if request.is_some_and(|d| d <= now) {
                    if p.expire() {
                        logon.take_back();
                        break Err(Error::Timeout);
                    }
                    // Decided: its publication wakes this thread.
                    thread::park();
                }
            }
        }
    };
    drop(p);
    drop(gate);
    event.disconnect(id);
    r
}

struct LoopUntil<'a, F> {
    ib: &'a IBHandle,
    condition: F,
    /// `Some(None)`: a timeout past what an `Instant` holds, never reached.
    end: Option<Option<Instant>>,
    waiting: bool,
    done: bool,
}

impl<T, F: FnMut() -> Option<T>> Iterator for LoopUntil<'_, F> {
    type Item = Result<LoopItem<T>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        if std::mem::take(&mut self.waiting) {
            let left = self.end.map(|e| {
                e.map_or(Duration::MAX, |e| {
                    e.saturating_duration_since(Instant::now())
                })
            });
            // Past the end, ib_async's wait times out at once: no wait.
            if left != Some(Duration::ZERO)
                && let Err(e) = self.ib.wait_on_update(left)
            {
                self.done = true;
                return Some(Err(e));
            }
        }
        if let Some(v) = (self.condition)() {
            self.done = true;
            return Some(Ok(LoopItem::Done(v)));
        }
        if self
            .end
            .is_some_and(|e| e.is_some_and(|e| Instant::now() > e))
        {
            self.done = true;
            return Some(Ok(LoopItem::TimedOut));
        }
        self.waiting = true;
        Some(Ok(LoopItem::Pending))
    }
}

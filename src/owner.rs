//! The thread that owns every `IB` of the process and applies what arrives.
//!
//! One owner thread serves every IB. It is the only writer of each IB's
//! state, requests and ids, and it runs every emission of every IB's events.
//! Another thread reaches an IB through the IB's command queue, one FIFO for
//! every entry; the owner takes those entries in turns with its reads of the
//! engine, so neither starves the other. Each piece of the owner's work is a
//! unit, and what a unit decides for a waiter becomes visible when the unit
//! ends.

use std::any::Any;
use std::collections::VecDeque;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError, RwLock, Weak};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};

use jiff::Zoned;

use crate::contract::Contract;
use crate::engine::{self as e, EClient};
use crate::error::{Error, Result};
use crate::event::{Event, Tasks, lock, on_owner, panic_message, set_on_owner, set_owner_wake};
use crate::ib::{IBConfig, StartupFetch};
use crate::live::{Control, Holder, Live};
use crate::objects::{
    AccountValue, CommissionReport, ExecutionFilter, Fill, IBDefaults, NewsBulletin, NewsTick, PnL,
    PnLSingle, PortfolioItem, Position, ScanDataList,
};
use crate::order::Trade;
use crate::pending::{self, Abandon, Pending, Registration, Reply, Token, Unadmitted, Unpark};
use crate::record::{Callback, Capture};
use crate::requests::{
    Ask, Cleanup, Exec, IbId, IdSpace, Origin, ReqKey, Requests, Waiter, fresh_token,
};
use crate::session::{self, Cause, Conn, EngineEnd, Logon, Post};
use crate::state::{self, Bars, Books, Call, Emit, Sink, State};
use crate::ticker::{Tick, Ticker};
use crate::timer::{Clock, DeadlineHeap, DeadlineId, IdleCheck, OwnerTimer, Timers, idle_check};
use crate::util::{format_si, global_error_event};

pub(crate) const LOG_IB: &str = "ib_async.ib";
pub(crate) const LOG_CLIENT: &str = "ib_async.client";

/// Entries an IB's queue gives up between two of its reads.
pub(crate) const CMD_GROUP: usize = 64;
/// Requests queued here, exchanges unsent in lanes, and the engine's unsent
/// work, per IB: the depth the engine gives its own control channel.
pub(crate) const UNSENT: usize = 64;
/// Controls queued, per IB.
pub(crate) const CONTROLS: usize = 64;
/// The longest the owner waits when nothing signals it.
const WAKE_BOUND: Duration = Duration::from_millis(10);

/// What ib_async's `reqAccountSummaryAsync` asks for (ib:2248-2259).
pub(crate) const ACCOUNT_SUMMARY_TAGS: &str = "AccountType,NetLiquidation,TotalCashValue,SettledCash,\
AccruedCash,BuyingPower,EquityWithLoanValue,PreviousDayEquityWithLoanValue,GrossPositionValue,\
RegTEquity,RegTMargin,SMA,InitMarginReq,MaintMarginReq,AvailableFunds,ExcessLiquidity,Cushion,\
FullInitMarginReq,FullMaintMarginReq,FullAvailableFunds,FullExcessLiquidity,LookAheadNextChange,\
LookAheadInitMarginReq,LookAheadMaintMarginReq,LookAheadAvailableFunds,LookAheadExcessLiquidity,\
HighestSeverity,DayTradesRemaining,DayTradesRemainingT+1,DayTradesRemainingT+2,\
DayTradesRemainingT+3,DayTradesRemainingT+4,Leverage,$LEDGER:ALL";

// ---------------------------------------------------------------------------
// The process's owner.

/// The owner's wait: set by every push, every engine's wake hook, a timer or
/// a task's waker, and taken by the owner's next wait.
#[derive(Default)]
struct Latch {
    woken: Mutex<bool>,
    cv: Condvar,
}

impl Latch {
    fn set(&self) {
        *lock(&self.woken) = true;
        self.cv.notify_one();
    }

    fn wait(&self, bound: Duration) {
        let mut woken = lock(&self.woken);
        if !*woken {
            woken = self
                .cv
                .wait_timeout(woken, bound)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
        *woken = false;
    }
}

/// The process's owner, as the rest of the process reaches it.
pub(crate) struct OwnerHandle {
    /// IBs registered and not yet in the owner's own list.
    incoming: Mutex<Vec<Arc<Shared>>>,
    /// `schedule` callbacks and async sleeps.
    pub(crate) timers: Timers,
}

impl OwnerHandle {
    fn register(&self, ib: Arc<Shared>) {
        lock(&self.incoming).push(ib);
        wake_owner();
    }
}

static OWNER: Mutex<Option<Arc<OwnerHandle>>> = Mutex::new(None);
static LATCH: OnceLock<Arc<Latch>> = OnceLock::new();

/// Wakes the owner from its wait.
pub(crate) fn wake_owner() {
    if let Some(l) = LATCH.get() {
        l.set();
    }
}

/// The process's owner, started if none runs. It never exits, as ib_async's
/// loop lives for the program. A thread that cannot be spawned leaves
/// nothing behind, and the next call tries again.
pub(crate) fn owner() -> Result<Arc<OwnerHandle>> {
    let mut slot = lock(&OWNER);
    if let Some(h) = slot.as_ref() {
        return Ok(h.clone());
    }
    let latch = LATCH.get_or_init(Arc::default).clone();
    let h = Arc::new(OwnerHandle {
        incoming: Mutex::default(),
        timers: Timers::new(wake_owner),
    });
    let served = h.clone();
    thread::Builder::new()
        .name("ib_async_dx-owner".into())
        .spawn(move || serve(&served, &latch))?;
    set_owner_wake(wake_owner);
    *slot = Some(h.clone());
    Ok(h)
}

/// Registers `ib` with the owner, starting it if none runs.
pub(crate) fn register(ib: Arc<Shared>) -> Result<()> {
    owner()?.register(ib);
    Ok(())
}

fn internal_error(p: &(dyn Any + Send)) -> Error {
    Error::Connection(format!("internal error: {}", panic_message(p)))
}

/// One unit of the owner's work that belongs to no IB.
fn owner_unit(f: impl FnOnce()) {
    if let Err(p) = pending::unit(f, internal_error) {
        log::error!(target: LOG_IB, "{}", panic_message(&*p));
    }
}

fn serve(h: &OwnerHandle, latch: &Latch) {
    set_on_owner(true);
    let mut ibs: Vec<(Arc<Shared>, Capture)> = Vec::new();
    let mut heap = DeadlineHeap::<OwnerTimer>::default();
    let mut tasks = Tasks::default();
    loop {
        let lapped = catch_unwind(AssertUnwindSafe(|| {
            lap(h, &mut ibs, &mut heap, &mut tasks);
        }));
        if let Err(p) = lapped {
            log::error!(target: LOG_IB, "{}", panic_message(&*p));
        }
        let now = Instant::now();
        let mut next = heap.next_due().map(|at| at.saturating_duration_since(now));
        for (ib, _) in &ibs {
            if let Some(d) = ib.next_due() {
                next = Some(next.map_or(d, |n| n.min(d)));
            }
        }
        latch.wait(next.map_or(WAKE_BOUND, |d| d.min(WAKE_BOUND)));
    }
}

fn lap(
    h: &OwnerHandle,
    ibs: &mut Vec<(Arc<Shared>, Capture)>,
    heap: &mut DeadlineHeap<OwnerTimer>,
    tasks: &mut Tasks,
) {
    let incoming = std::mem::take(&mut *lock(&h.incoming));
    ibs.extend(incoming.into_iter().map(|ib| {
        let capture = Capture::new(ib.defaults.timezone.clone());
        (ib, capture)
    }));
    tasks.take_spawned();
    let now = Instant::now();
    owner_unit(|| heap.take_timers(h.timers.take()));
    for due in heap.take_due(now) {
        owner_unit(|| due.fire());
    }
    for (ib, capture) in ibs.iter_mut() {
        ib.lap(capture);
    }
    ibs.retain(|(ib, _)| !ib.is_closed());
    for task in tasks.take_woken() {
        owner_unit(|| task.poll());
    }
}

/// A waker that wakes the owner.
struct OwnerWake;

impl Wake for OwnerWake {
    fn wake(self: Arc<Self>) {
        wake_owner();
    }
}

// ---------------------------------------------------------------------------
// The command queue.

/// What an entry is, for admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Class {
    /// A step that sends the engine new work.
    Request,
    /// Anything else a user thread calls.
    Control,
    /// The logon and closer threads' posts: at most two per IB at once.
    Post,
}

enum Work {
    Step(Due),
    Control(Control),
}

/// One entry of an IB's queue, run by the owner in admission order.
pub(crate) struct Entry {
    work: Work,
    class: Class,
}

impl Entry {
    pub(crate) fn step(class: Class, f: impl FnOnce(&Arc<Shared>) + Send + 'static) -> Self {
        Entry {
            work: Work::Step(Box::new(f)),
            class,
        }
    }

    fn run(self, ib: &Arc<Shared>) {
        match self.work {
            Work::Step(f) => f(ib),
            Work::Control(f) => f(),
        }
    }

    fn into_control(self) -> Control {
        match self.work {
            Work::Control(c) => c,
            Work::Step(_) => Box::new(|| {}),
        }
    }
}

/// Why a push came back.
pub(crate) enum Refused {
    Closed(Entry),
    /// No room, by the deadline or at once.
    Full(Entry),
}

#[derive(Default)]
struct QueueState {
    entries: VecDeque<Entry>,
    requests: usize,
    controls: usize,
    /// Question exchanges waiting unsent in a lane.
    lane_unsent: usize,
    /// The engine's `backlog()` as the owner last read it, and what it has
    /// sent since; zero outside `Connected`.
    engine_unsent: usize,
    /// One per waiting future, by its key.
    space_wakers: Vec<(u64, Waker)>,
    /// Entries taken since the IB began.
    taken: u64,
    /// The drop's latch: taken once `taken` reaches it.
    latch: Option<u64>,
    closed: bool,
}

impl QueueState {
    fn room(&self, class: Class) -> bool {
        match class {
            Class::Request => self.requests + self.lane_unsent + self.engine_unsent < UNSENT,
            Class::Control => self.controls < CONTROLS,
            Class::Post => true,
        }
    }

    fn admit(&mut self, e: Entry) {
        match e.class {
            Class::Request => self.requests += 1,
            Class::Control => self.controls += 1,
            Class::Post => {}
        }
        self.entries.push_back(e);
    }
}

/// An IB's FIFO of entries from other threads, with its admission bounds.
#[derive(Default)]
pub(crate) struct CommandQueue {
    q: Mutex<QueueState>,
    space: Condvar,
}

impl CommandQueue {
    /// Admits `e`, waiting for room until `deadline`, `None` without limit.
    pub(crate) fn push(&self, e: Entry, deadline: Option<Instant>) -> Result<(), Refused> {
        let mut st = lock(&self.q);
        loop {
            if st.closed {
                return Err(Refused::Closed(e));
            }
            if st.room(e.class) {
                st.admit(e);
                return Ok(());
            }
            st = match deadline {
                None => self.space.wait(st).unwrap_or_else(PoisonError::into_inner),
                Some(d) => match d.checked_duration_since(Instant::now()) {
                    Some(left) if !left.is_zero() => {
                        self.space
                            .wait_timeout(st, left)
                            .unwrap_or_else(PoisonError::into_inner)
                            .0
                    }
                    _ => return Err(Refused::Full(e)),
                },
            };
        }
    }

    /// Admits `e` if there is room now.
    pub(crate) fn try_push(&self, e: Entry) -> Result<(), Refused> {
        let mut st = lock(&self.q);
        if st.closed {
            Err(Refused::Closed(e))
        } else if st.room(e.class) {
            st.admit(e);
            Ok(())
        } else {
            Err(Refused::Full(e))
        }
    }

    /// A waiting future's try: admits `e`, or keeps `waker` under `key` in
    /// place of the one kept before and gives `e` back. A closed queue drops
    /// `e`, after the lock.
    fn poll_push(&self, e: Entry, key: u64, waker: &Waker) -> Result<(), Option<Entry>> {
        let fresh = waker.clone();
        let (r, displaced, dropped) = {
            let mut st = lock(&self.q);
            if st.closed {
                (Err(None), None, Some(e))
            } else if st.room(e.class) {
                st.admit(e);
                (Ok(()), None, None)
            } else {
                let displaced = match st.space_wakers.iter_mut().find(|w| w.0 == key) {
                    Some(w) => Some(std::mem::replace(&mut w.1, fresh)),
                    None => {
                        st.space_wakers.push((key, fresh));
                        None
                    }
                };
                (Err(Some(e)), displaced, None)
            }
        };
        drop(displaced);
        drop(dropped);
        r
    }

    /// Removes the waker kept under `key`, dropped after the lock.
    fn forget(&self, key: u64) {
        let removed = {
            let mut st = lock(&self.q);
            let i = st.space_wakers.iter().position(|w| w.0 == key);
            i.map(|i| st.space_wakers.swap_remove(i))
        };
        drop(removed);
    }

    /// Changes the counts under the lock; when room may have appeared, the
    /// waiting threads and futures are woken after it.
    fn update(&self, f: impl FnOnce(&mut QueueState) -> bool) {
        let wakers = {
            let mut st = lock(&self.q);
            if f(&mut st) {
                std::mem::take(&mut st.space_wakers)
            } else {
                Vec::new()
            }
        };
        self.space.notify_all();
        for (_, w) in wakers {
            w.wake();
        }
    }

    /// Up to `n` entries, in admission order. A request's count moves to the
    /// engine's unsent work, which the next read sets from the engine.
    pub(crate) fn take_group(&self, n: usize) -> Vec<Entry> {
        let mut taken = Vec::new();
        self.update(|st| {
            let k = n.min(st.entries.len());
            taken.extend(st.entries.drain(..k));
            for e in &taken {
                match e.class {
                    Class::Request => {
                        st.requests -= 1;
                        st.engine_unsent += 1;
                    }
                    Class::Control => st.controls -= 1,
                    Class::Post => {}
                }
            }
            st.taken += k as u64;
            k > 0
        });
        taken
    }

    /// The engine's unsent work, as the owner reads it.
    pub(crate) fn set_engine_unsent(&self, n: usize) {
        self.update(|st| std::mem::replace(&mut st.engine_unsent, n) > n);
    }

    /// Work the owner has just handed the engine.
    pub(crate) fn sent(&self, n: usize) {
        self.update(|st| {
            st.engine_unsent += n;
            false
        });
    }

    /// The exchanges waiting unsent in the lanes.
    pub(crate) fn set_lane_unsent(&self, n: usize) {
        self.update(|st| std::mem::replace(&mut st.lane_unsent, n) > n);
    }

    /// The IB's drop: the exit steps run once every entry admitted before it
    /// has been taken.
    pub(crate) fn latch(&self) {
        let mut st = lock(&self.q);
        if st.latch.is_none() {
            st.latch = Some(st.taken + st.entries.len() as u64);
        }
    }

    pub(crate) fn latched(&self) -> bool {
        lock(&self.q).latch.is_some()
    }

    fn exit_due(&self) -> bool {
        let st = lock(&self.q);
        st.latch.is_some_and(|m| st.taken >= m)
    }

    /// Closes the queue: a later push comes back, and every thread and
    /// future waiting for room is woken. Gives the entries still queued, to
    /// drop outside the lock.
    fn close(&self) -> Vec<Entry> {
        let mut drained = Vec::new();
        self.update(|st| {
            st.closed = true;
            drained.extend(st.entries.drain(..));
            st.requests = 0;
            st.controls = 0;
            true
        });
        drained
    }

    pub(crate) fn is_closed(&self) -> bool {
        lock(&self.q).closed
    }
}

/// A command waiting for room, held by the `Pending` it answers: admitted at
/// its first poll or wait.
struct Held {
    ib: Arc<Shared>,
    entry: Option<Entry>,
    key: u64,
}

impl Held {
    fn new(ib: Arc<Shared>, entry: Entry) -> Box<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        Box::new(Held {
            ib,
            entry: Some(entry),
            key: NEXT.fetch_add(1, Ordering::Relaxed),
        })
    }
}

impl Unadmitted for Held {
    fn poll_admit(mut self: Box<Self>, waker: &Waker) -> Option<Box<dyn Unadmitted>> {
        let e = self.entry.take()?;
        match self.ib.queue.poll_push(e, self.key, waker) {
            Ok(()) => {
                wake_owner();
                None
            }
            Err(None) => None,
            Err(Some(e)) => {
                self.entry = Some(e);
                Some(self)
            }
        }
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        self.ib.queue.forget(self.key);
    }
}

/// Blocks this thread on `p` without listening to `global_error_event`: the
/// wait of a method that is not one of ib_async's `_run` faces.
pub(crate) fn park_on<T>(mut p: Pending<T>) -> Result<T> {
    let waker = Waker::from(Arc::new(Unpark(thread::current())));
    let mut cx = Context::from_waker(&waker);
    loop {
        if let Poll::Ready(r) = Pin::new(&mut p).poll(&mut cx) {
            return r;
        }
        thread::park();
    }
}

// ---------------------------------------------------------------------------
// An IB as the owner holds it.

macro_rules! ib_events {
    ($($field:ident: $ty:ty = $name:literal,)*) => {
        /// An IB's events, ib_async's names, and its `Client`'s three.
        pub(crate) struct IbEvents {
            $(pub(crate) $field: Event<$ty>,)*
        }

        impl IbEvents {
            fn new(holder: &Weak<dyn Holder>) -> Self {
                IbEvents { $($field: Event::with_holder($name, holder.clone()),)* }
            }

            fn set_done(&self) {
                $(self.$field.set_done();)*
            }

            fn clear(&self) {
                $(self.$field.clear();)*
            }
        }
    };
}

ib_events! {
    connected_event: () = "connectedEvent",
    disconnected_event: () = "disconnectedEvent",
    update_event: () = "updateEvent",
    pending_tickers_event: Vec<Live<Ticker>> = "pendingTickersEvent",
    bar_update_event: (Bars, bool) = "barUpdateEvent",
    new_order_event: Live<Trade> = "newOrderEvent",
    order_modify_event: Live<Trade> = "orderModifyEvent",
    cancel_order_event: Live<Trade> = "cancelOrderEvent",
    open_order_event: Live<Trade> = "openOrderEvent",
    order_status_event: Live<Trade> = "orderStatusEvent",
    exec_details_event: (Live<Trade>, Fill) = "execDetailsEvent",
    commission_report_event: (Live<Trade>, Fill, Live<CommissionReport>) = "commissionReportEvent",
    update_portfolio_event: PortfolioItem = "updatePortfolioEvent",
    position_event: Position = "positionEvent",
    account_value_event: AccountValue = "accountValueEvent",
    account_summary_event: AccountValue = "accountSummaryEvent",
    pnl_event: Live<PnL> = "pnlEvent",
    pnl_single_event: Live<PnLSingle> = "pnlSingleEvent",
    scanner_data_event: Live<ScanDataList> = "scannerDataEvent",
    tick_news_event: NewsTick = "tickNewsEvent",
    news_bulletin_event: NewsBulletin = "newsBulletinEvent",
    wsh_meta_event: String = "wshMetaEvent",
    wsh_event: String = "wshEvent",
    error_event: (i64, i64, String, Option<Contract>) = "errorEvent",
    timeout_event: f64 = "timeoutEvent",
    tick_event: (Live<Ticker>, Tick) = "tickEvent",
    api_start: () = "apiStart",
    api_end: () = "apiEnd",
    api_error: String = "apiError",
}

/// How often the IB has run a pass and been told to stop, and whether its
/// exit is done. Changed only under its mutex, and notified after it.
#[derive(Default)]
pub(crate) struct Progress {
    pub(crate) passes: u64,
    pub(crate) stops: u64,
    pub(crate) closed: bool,
}

/// What an attempt's logon runs on.
pub(crate) enum Via {
    /// The engine's own login.
    Engine(Box<e::EClientConfig>),
    /// A session already built: posted as logged on at once. `None` posts
    /// nothing; the test posts in the logon thread's place.
    #[cfg(test)]
    Test(Option<Arc<EClient>>),
}

/// What `connect` fetches at startup, and how it reports.
pub(crate) struct SyncOpts {
    pub(crate) account: String,
    pub(crate) raise_sync_errors: bool,
    pub(crate) fetch: StartupFetch,
    pub(crate) readonly: bool,
}

/// One `connect`: its waiter, its logon and its startup sync.
pub(crate) struct Attempt {
    token: Token,
    reply: Option<Reply<()>>,
    logon: Logon,
    via: Option<Via>,
    client_id: i64,
    account: String,
    /// The replay wait's bound, and each startup request's.
    timeout: Option<Duration>,
    logon_timeout: Option<Duration>,
    /// `None` for `Client::connect`, which runs no startup sync.
    sync: Option<SyncOpts>,
    g: u64,
    /// `api_error` was emitted for it.
    reported: bool,
}

impl Attempt {
    #[expect(clippy::too_many_arguments, reason = "one per connect argument")]
    pub(crate) fn new(
        token: Token,
        reply: Reply<()>,
        logon: Logon,
        via: Via,
        client_id: i64,
        account: String,
        timeout: Option<Duration>,
        logon_timeout: Option<Duration>,
        sync: Option<SyncOpts>,
    ) -> Self {
        Attempt {
            token,
            reply: Some(reply),
            logon,
            via: Some(via),
            client_id,
            account,
            timeout,
            logon_timeout,
            sync,
            g: 0,
            reported: false,
        }
    }

    fn fail(mut self, e: Error) {
        if let Some(r) = self.reply.take() {
            r.send(Err(e));
        }
    }
}

enum Stage {
    Requests,
    Executions,
}

/// The startup sync of a published generation, waiting on its requests.
struct Startup {
    g: u64,
    waiting: Vec<(String, Pending<()>)>,
    errors: Vec<String>,
    stage: Stage,
    fetch: StartupFetch,
    raise: bool,
    timeout: Option<Duration>,
    failed: Option<Error>,
}

/// `set_timeout`'s idle check, bound to the generation it was set in.
struct Idle {
    generation: u64,
    timeout: Duration,
    deadline: DeadlineId,
}

/// What an IB's deadline does when it fires, on the owner.
pub(crate) type Due = Box<dyn FnOnce(&Arc<Shared>) + Send>;

/// What the owner writes of an IB. Held only for a step, never across user
/// code or an engine call.
pub(crate) struct Core {
    pub(crate) conn: Conn,
    /// The last generation a connect started.
    generation: u64,
    /// A teardown step failed: the next publication starts from a fresh
    /// `State`.
    rebuild: bool,
    pub(crate) state: State,
    pub(crate) requests: Requests,
    pub(crate) ids: IdSpace,
    /// Request deadlines, the login deadline, the idle check, closer
    /// retries.
    pub(crate) heap: DeadlineHeap<Due>,
    attempt: Option<Attempt>,
    parked: VecDeque<Attempt>,
    sync: Option<Startup>,
    /// A logon thread has not yet posted.
    logon_live: bool,
    /// Closers started and not yet posted.
    closers: usize,
    idle: Option<Idle>,
    /// The last pass's arrival, or `set_timeout`'s call.
    last_activity: Instant,
    /// When the generation was published: the connection statistics' clock.
    pub(crate) started: Instant,
}

/// One IB, as the owner, its handles, its objects and its waiters share it.
pub(crate) struct Shared {
    config: RwLock<IBConfig>,
    pub(crate) defaults: IBDefaults,
    pub(crate) clock: Clock,
    core: Mutex<Core>,
    pub(crate) queue: CommandQueue,
    abandoned: Mutex<Vec<Token>>,
    pub(crate) events: IbEvents,
    progress: Mutex<Progress>,
    progressed: Condvar,
    /// `Client.clientId`: -1, then each connect's; no reset clears it.
    pub(crate) client_id: AtomicI64,
    me: Weak<Shared>,
}

impl Holder for Shared {
    fn is_active(&self) -> bool {
        !self.queue.is_closed()
    }

    fn push_control(&self, control: Control) -> Result<(), Control> {
        let e = Entry {
            work: Work::Control(control),
            class: Class::Control,
        };
        match self.queue.push(e, None) {
            Ok(()) => {
                wake_owner();
                Ok(())
            }
            Err(Refused::Closed(e) | Refused::Full(e)) => Err(e.into_control()),
        }
    }
}

impl Abandon for Shared {
    fn abandon(&self, token: Token) {
        lock(&self.abandoned).push(token);
        wake_owner();
    }
}

/// A waiter that takes whatever its request answers as done.
struct Ignore(Reply<()>);

impl Waiter for Ignore {
    fn finish(self: Box<Self>, r: Result<Box<dyn Any + Send>>) -> bool {
        self.0.send(r.map(drop))
    }
}

/// Arms `x`'s method deadline at `at`: when it passes before the answer, `x`
/// leaves its request, and `expired` completes it.
pub(crate) fn arm(
    heap: &mut DeadlineHeap<Due>,
    x: &mut Exec,
    at: Instant,
    expired: impl FnOnce(&Arc<Shared>, Exec) + Send + 'static,
) {
    let token = x.token;
    x.deadline = Some(heap.insert(
        at,
        Box::new(move |ib: &Arc<Shared>| {
            let x = ib.core().requests.take(token);
            if let Some(mut x) = x {
                x.deadline = None;
                ib.settle(&mut x);
                expired(ib, x);
            }
        }),
    ));
}

fn timed_out(_: &Arc<Shared>, x: Exec) {
    x.finish(Err(Error::Timeout));
}

/// Python's `str` of a list of strings.
fn py_list(items: &[String]) -> String {
    let quoted: Vec<String> = items.iter().map(|s| format!("'{s}'")).collect();
    format!("[{}]", quoted.join(", "))
}

impl Shared {
    pub(crate) fn new(defaults: IBDefaults, config: IBConfig, clock: Clock) -> Arc<Shared> {
        Arc::new_cyclic(|me: &Weak<Shared>| {
            let holder: Weak<dyn Holder> = me.clone();
            let id = IbId::new();
            let now = clock.wall().to_zoned(defaults.timezone.clone());
            let at = clock.now();
            Shared {
                config: RwLock::new(config),
                defaults: defaults.clone(),
                core: Mutex::new(Core {
                    conn: Conn::Disconnected {
                        engine: EngineEnd::Closed,
                    },
                    generation: 0,
                    rebuild: false,
                    state: State::new(defaults, holder.clone(), now),
                    requests: Requests::new(id),
                    ids: IdSpace::new(),
                    heap: DeadlineHeap::default(),
                    attempt: None,
                    parked: VecDeque::new(),
                    sync: None,
                    logon_live: false,
                    closers: 0,
                    idle: None,
                    last_activity: at,
                    started: at,
                }),
                clock,
                queue: CommandQueue::default(),
                abandoned: Mutex::default(),
                events: IbEvents::new(&holder),
                progress: Mutex::default(),
                progressed: Condvar::new(),
                client_id: AtomicI64::new(-1),
                me: me.clone(),
            }
        })
    }

    /// Connects ib_async's internal first slots: `_onError`, which on 1102
    /// asks for the account summary again, and `apiEnd`'s forwarding to
    /// `disconnected_event` (ib:280-281).
    pub(crate) fn connect_internal_slots(&self) {
        let me = self.me.clone();
        self.events.error_event.connect(move |e| {
            if e.1 == 1102
                && let Some(ib) = me.upgrade()
            {
                drop(ib.account_summary_request());
            }
        });
        let me = self.me.clone();
        self.events.api_end.connect(move |()| {
            if let Some(ib) = me.upgrade() {
                ib.events.disconnected_event.emit(&());
            }
        });
    }

    pub(crate) fn core(&self) -> MutexGuard<'_, Core> {
        lock(&self.core)
    }

    pub(crate) fn config(&self) -> IBConfig {
        self.config
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn set_config(&self, config: IBConfig) {
        let old = std::mem::replace(
            &mut *self.config.write().unwrap_or_else(PoisonError::into_inner),
            config,
        );
        drop(old);
    }

    fn holder(&self) -> Weak<dyn Holder> {
        self.me.clone()
    }

    pub(crate) fn abandoner(&self) -> Weak<dyn Abandon> {
        self.me.clone()
    }

    /// Now, in the zone ib_async stamps times in.
    pub(crate) fn wall_now(&self) -> Zoned {
        self.clock.wall().to_zoned(self.defaults.timezone.clone())
    }

    /// The generation `g` still stands.
    pub(crate) fn is_generation(&self, g: u64) -> bool {
        matches!(&self.core().conn, Conn::Connected { g: cg, .. } if *cg == g)
    }

    /// The published generation and its engine session.
    pub(crate) fn connected(&self) -> Option<(u64, Arc<EClient>)> {
        match &self.core().conn {
            Conn::Connected { g, client } => Some((*g, client.clone())),
            _ => None,
        }
    }

    fn client_of(&self, g: u64) -> Option<Arc<EClient>> {
        self.connected().filter(|c| c.0 == g).map(|c| c.1)
    }

    /// ib_async's `isReady`: published, and the engine has not given the
    /// session up; a loss it is still recovering (1100) counts as up.
    pub(crate) fn is_ready(&self) -> bool {
        self.connected().is_some_and(|(_, c)| !c.session_over())
    }

    pub(crate) fn is_closed(&self) -> bool {
        lock(&self.progress).closed
    }

    pub(crate) fn progress<R>(&self, f: impl FnOnce(&Progress) -> R) -> R {
        f(&lock(&self.progress))
    }

    fn progressed_by(&self, f: impl FnOnce(&mut Progress)) {
        f(&mut lock(&self.progress));
        self.progressed.notify_all();
    }

    /// Wakes every thread waiting on this IB's progress.
    pub(crate) fn nudge(&self) {
        self.progressed_by(|_| {});
    }

    /// Waits until `done` holds of the progress or `deadline` passes, giving
    /// whether `done` held.
    pub(crate) fn wait_progress(
        &self,
        deadline: Option<Instant>,
        mut done: impl FnMut(&Progress) -> bool,
    ) -> bool {
        let mut p = lock(&self.progress);
        loop {
            if done(&p) {
                return true;
            }
            p = match deadline {
                None => self
                    .progressed
                    .wait(p)
                    .unwrap_or_else(PoisonError::into_inner),
                Some(d) => match d.checked_duration_since(Instant::now()) {
                    Some(left) if !left.is_zero() => {
                        self.progressed
                            .wait_timeout(p, left)
                            .unwrap_or_else(PoisonError::into_inner)
                            .0
                    }
                    _ => return false,
                },
            };
        }
    }

    fn next_due(&self) -> Option<Duration> {
        if !lock(&self.queue.q).entries.is_empty() {
            // More than a group was queued: the rest go next lap.
            return Some(Duration::ZERO);
        }
        let at = self.core().heap.next_due()?;
        Some(at.saturating_duration_since(self.clock.now()))
    }

    // -- Steps and commands ------------------------------------------------

    /// Runs `f` as an owner step and gives its result: inline on the owner,
    /// otherwise as a command of `class` this thread waits for, with no
    /// deadline. The reply comes after the step's emissions.
    pub(crate) fn step<R: Send + 'static>(
        self: &Arc<Self>,
        class: Class,
        f: impl FnOnce(&Arc<Shared>) -> Result<R> + Send + 'static,
    ) -> Result<R> {
        if on_owner() {
            return f(self);
        }
        let (p, reply) = Pending::new(None);
        let e = Entry::step(class, move |ib| {
            reply.send(f(ib));
        });
        match self.queue.push(e, None) {
            Ok(()) => wake_owner(),
            Err(_) => return Err(Error::NotConnected),
        }
        park_on(p)
    }

    /// A request's waiter: `f` runs as an owner step with the execution's
    /// token and the waiter's reply. Inline on the owner; otherwise admitted
    /// now if there is room, and else at the waiter's first poll or wait. A
    /// waiter that left before its command ran is retired once it has.
    pub(crate) fn request<T: Send + 'static>(
        self: &Arc<Self>,
        f: impl FnOnce(&Arc<Shared>, Token, Reply<T>) + Send + 'static,
    ) -> Pending<T> {
        let token = fresh_token();
        let (mut p, reply) = Pending::new(Some(Registration::new(self.abandoner(), token)));
        let e = Entry::step(Class::Request, move |ib| {
            let here = reply.start();
            f(ib, token, reply);
            if !here {
                ib.retire(vec![token]);
            }
        });
        if on_owner() {
            e.run(self);
            return p;
        }
        match self.queue.try_push(e) {
            Ok(()) => wake_owner(),
            Err(Refused::Full(e)) => p.hold(Held::new(self.clone(), e)),
            Err(Refused::Closed(e)) => drop(e),
        }
        p
    }

    /// A control that returns once admitted: `f` runs as an owner step,
    /// inline on the owner.
    pub(crate) fn control(self: &Arc<Self>, f: impl FnOnce(&Arc<Shared>) + Send + 'static) {
        if on_owner() {
            return f(self);
        }
        if self
            .queue
            .push(Entry::step(Class::Control, f), None)
            .is_ok()
        {
            wake_owner();
        }
    }

    /// Holds `entry` in `p` until its first poll or wait admits it, or runs
    /// it at once on the owner.
    pub(crate) fn hold<T: Send + 'static>(self: &Arc<Self>, p: &mut Pending<T>, entry: Entry) {
        if on_owner() {
            entry.run(self);
        } else {
            p.hold(Held::new(self.clone(), entry));
        }
    }

    /// A logon or closer thread's post. It comes back once the queue is
    /// closed.
    pub(crate) fn post(self: &Arc<Self>, post: Post) -> Result<(), Post> {
        let slot = Arc::new(Mutex::new(Some(post)));
        let taken = slot.clone();
        let e = Entry::step(Class::Post, move |ib| {
            let post = lock(&taken).take();
            if let Some(post) = post {
                ib.on_post(post);
            }
        });
        match self.queue.push(e, None) {
            Ok(()) => {
                wake_owner();
                Ok(())
            }
            Err(_) => lock(&slot).take().map_or(Ok(()), Err),
        }
    }

    // -- The lap -------------------------------------------------------------

    /// One unit of this IB's work, under `catch_unwind`: what it decided is
    /// published when it ends, whether it returned or panicked. A panic in
    /// the owner's own work closes the generation as Internal.
    pub(crate) fn unit<R>(self: &Arc<Self>, f: impl FnOnce() -> R) -> Option<R> {
        let r = pending::unit(f, internal_error);
        let lanes = self.core().requests.unsent();
        self.queue.set_lane_unsent(lanes);
        match r {
            Ok(r) => Some(r),
            Err(p) => {
                let why = panic_message(&*p);
                log::error!(target: LOG_IB, "{why}");
                self.internal(why);
                None
            }
        }
    }

    fn internal(self: &Arc<Self>, why: String) {
        self.core.clear_poison();
        let g = match &self.core().conn {
            Conn::Connected { g, .. } => *g,
            _ => return,
        };
        let _ = pending::unit(|| self.close(g, Cause::Internal(why)), internal_error);
    }

    /// The IB's turn in the owner's lap: waiters gone since the last lap,
    /// then a group of queued entries, the deadlines due, one read, the
    /// startup sync, and the exit steps once dropped.
    pub(crate) fn lap(self: &Arc<Self>, capture: &mut Capture) {
        let gone = std::mem::take(&mut *lock(&self.abandoned));
        if !gone.is_empty() {
            self.unit(|| self.retire(gone));
        }
        for e in self.queue.take_group(CMD_GROUP) {
            self.unit(|| e.run(self));
        }
        let due = self.core().heap.take_due(self.clock.now());
        for d in due {
            self.unit(|| d(self));
        }
        match self.connected() {
            Some((g, client)) => {
                if let Some((passed, closed)) = self.unit(|| self.read(g, &client, capture)) {
                    if passed {
                        self.progressed_by(|p| p.passes += 1);
                    }
                    if closed {
                        self.unit(|| self.close(g, Cause::Peer));
                    }
                }
            }
            // Outside a session nothing reached the engine: what the group
            // took is no one's unsent work.
            None => self.queue.set_engine_unsent(0),
        }
        if self.core().sync.is_some() {
            self.unit(|| self.sync_step());
        }
        if self.queue.exit_due() && !self.is_closed() {
            self.unit(|| self.exit_step());
        }
    }

    /// One read of the engine, applied as one pass.
    fn read(self: &Arc<Self>, g: u64, client: &EClient, capture: &mut Capture) -> (bool, bool) {
        capture.timezone_tws = self.config().timezone_tws;
        // What a read that panicked recorded belongs to the generation that
        // panic ended.
        drop(capture.take());
        client.process_msgs(capture);
        let r = self.apply_read(g, capture.take());
        if self.is_generation(g) {
            self.queue.set_engine_unsent(client.backlog());
        }
        r
    }

    /// Applies one read's callbacks as generation `g`'s pass. Gives whether
    /// it was a pass, and whether it held `connection_closed`.
    pub(crate) fn apply_read(self: &Arc<Self>, g: u64, callbacks: Vec<Callback>) -> (bool, bool) {
        let passed = callbacks
            .iter()
            .any(|c| !matches!(c, Callback::QuestionRetired(_)));
        if passed {
            self.core().last_activity = self.clock.now();
        }
        let arrived = self.wall_now();
        let closed = state::pass(&mut PassSink { ib: self, g }, callbacks, arrived);
        (passed, closed)
    }

    /// Retires the executions whose waiters are gone.
    pub(crate) fn retire(self: &Arc<Self>, tokens: Vec<Token>) {
        for t in tokens {
            if self.attempt_left(t) {
                continue;
            }
            let (origin, cleanup) = {
                let mut c = self.core();
                let c = &mut *c;
                let origin = c.requests.origin(t);
                (origin, c.requests.retire(t, &mut c.heap))
            };
            if let (Some(o), Some(cl)) = (origin, cleanup) {
                self.clean_up(o, cl);
            }
        }
    }

    // -- Emissions and engine calls ---------------------------------------

    /// Runs an emission of this IB's, or of one of its objects.
    pub(crate) fn emit(&self, e: Emit) {
        let ev = &self.events;
        match e {
            Emit::AccountValue(v) => ev.account_value_event.emit(&v),
            Emit::AccountSummary(v) => ev.account_summary_event.emit(&v),
            Emit::UpdatePortfolio(v) => ev.update_portfolio_event.emit(&v),
            Emit::Position(v) => ev.position_event.emit(&v),
            Emit::Pnl(v) => ev.pnl_event.emit(&v),
            Emit::PnlSingle(v) => ev.pnl_single_event.emit(&v),
            Emit::OpenOrder(t) => ev.open_order_event.emit(&t),
            Emit::OrderStatus(t) => ev.order_status_event.emit(&t),
            Emit::ExecDetails(t, f) => ev.exec_details_event.emit(&(t, f)),
            Emit::CommissionReport(t, f, r) => ev.commission_report_event.emit(&(t, f, r)),
            Emit::BarUpdate(b, new) => ev.bar_update_event.emit(&(b, new)),
            Emit::ScannerData(l) => ev.scanner_data_event.emit(&l),
            Emit::TickNews(n) => ev.tick_news_event.emit(&n),
            Emit::NewsBulletin(n) => ev.news_bulletin_event.emit(&n),
            Emit::WshMeta(s) => ev.wsh_meta_event.emit(&s),
            Emit::Wsh(s) => ev.wsh_event.emit(&s),
            Emit::Error(id, code, msg, c) => ev.error_event.emit(&(id, code, msg, c)),
            Emit::Update => ev.update_event.emit(&()),
            Emit::PendingTickers(v) => ev.pending_tickers_event.emit(&v),
            Emit::Tick(t, tick) => ev.tick_event.emit(&(t, tick)),
            Emit::TradeStatus(t) => t.status_event().emit(&t),
            Emit::TradeFill(t, f) => t.fill_event().emit(&(t.clone(), f)),
            Emit::TradeCommissionReport(t, f, r) => {
                t.commission_report_event().emit(&(t.clone(), f, r));
            }
            Emit::TradeFilled(t) => t.filled_event().emit(&t),
            Emit::TradeCancelled(t) => t.cancelled_event().emit(&t),
            Emit::TickerUpdate(t) => t.update_event().emit(&t),
            Emit::BarsUpdate(Bars::Historical(l), new) => l.update_event().emit(&(l.clone(), new)),
            Emit::BarsUpdate(Bars::RealTime(l), new) => l.update_event().emit(&(l.clone(), new)),
            Emit::BarsUpdate(Bars::Scan(l), _) | Emit::ScanUpdate(l) => l.update_event().emit(&l),
        }
    }

    /// Sends a question's exchange.
    pub(crate) fn send_ask(&self, client: &EClient, ask: &Ask) {
        match ask {
            Ask::OpenOrders => client.req_open_orders(),
            Ask::AllOpenOrders => client.req_all_open_orders(),
            Ask::CompletedOrders { api_only } => client.req_completed_orders(*api_only),
            Ask::Positions => client.req_positions(),
            Ask::AccountUpdates { account } => client.req_account_updates(true, account),
            Ask::CurrentTime => client.req_current_time(),
            Ask::CurrentTimeInMillis => client.req_current_time_in_millis(),
            Ask::NewsProviders => client.req_news_providers(),
            Ask::MarketRule(id) => client.req_market_rule(*id),
            Ask::MktDepthExchanges => client.req_mkt_depth_exchanges(),
            Ask::ScannerParameters => client.req_scanner_parameters(),
            Ask::Fa(t) => client.request_fa(*t),
        }
        self.queue.sent(1);
    }

    fn call(&self, g: u64, c: Call) {
        let Some(client) = self.client_of(g) else {
            return;
        };
        match c {
            Call::Ask(ask) => self.send_ask(&client, &ask),
            Call::CancelRealTimeBars(id) => client.cancel_real_time_bars(id),
            Call::ReqRealTimeBars(list) => {
                let l = list.read();
                let contract = e::Contract::from(&l.contract);
                client.req_real_time_bars(
                    l.req_id,
                    &contract,
                    l.bar_size,
                    &l.what_to_show,
                    l.use_rth,
                );
                self.queue.sent(1);
            }
            Call::CancelHistoricalData(id) => client.cancel_historical_data(id),
            Call::ReqHistoricalData(list) => {
                let l = list.read();
                let contract = e::Contract::from(&l.contract);
                match crate::util::format_ib_datetime(l.end_date_time.clone()) {
                    Ok(end) => {
                        client.req_historical_data(
                            l.req_id,
                            &contract,
                            &end,
                            &l.duration_str,
                            &l.bar_size_setting,
                            &l.what_to_show,
                            l.use_rth,
                            l.format_date,
                            l.keep_up_to_date,
                        );
                        self.queue.sent(1);
                    }
                    Err(why) => log::error!(target: "ib_async.wrapper", "{why}"),
                }
            }
        }
    }

    /// An execution leaves: its deadline is removed and its cleanup run.
    pub(crate) fn settle(&self, x: &mut Exec) {
        let cleanup = x.settle(&mut self.core().heap);
        if let Some(c) = cleanup {
            self.clean_up(x.origin, c);
        }
    }

    /// Runs the cleanup of an execution started under `origin`, only while
    /// that origin is this IB's current generation.
    pub(crate) fn clean_up(&self, origin: Origin, c: Cleanup) {
        if !self.core().requests.owns(&origin) {
            return;
        }
        let Some(client) = self.client_of(origin.generation) else {
            return;
        };
        match c {
            Cleanup::CancelImpliedVolatility => {
                client.cancel_calculate_implied_volatility(origin.req_id);
            }
            Cleanup::CancelOptionPrice => client.cancel_calculate_option_price(origin.req_id),
            Cleanup::EndSnapshot => {
                let mut c = self.core();
                let ticker = c.state.req_id_to_ticker.get(&origin.req_id).cloned();
                if let Some(t) = ticker {
                    c.state.end_ticker(&t, "snapshot");
                }
            }
        }
    }

    /// The account summary, asked again: ib_async's `reqAccountSummaryAsync`
    /// (ib:2244-2261), whose subscription nothing cancels.
    pub(crate) fn account_summary_request(self: &Arc<Self>) -> Pending<()> {
        self.request(|ib, token, reply: Reply<()>| {
            let Some((_, client)) = ib.connected() else {
                return;
            };
            let floor = client.order_id_floor();
            let allocated = ib.core().ids.allocate(floor, 1);
            let id = match allocated {
                Ok(id) => id,
                Err(e) => {
                    reply.send(Err(e));
                    return;
                }
            };
            let mut c = ib.core();
            let mut x = c.requests.exec_as(ReqKey::Id(id), token);
            x.waiter = Some(Box::new(reply));
            c.requests.insert(x);
            drop(c);
            client.req_account_summary(id, "All", ACCOUNT_SUMMARY_TAGS);
            ib.queue.sent(1);
        })
    }

    // -- Connect -------------------------------------------------------------

    /// A connect's owner step: it starts a logon, waits behind a session
    /// still closing, or closes a connected one first.
    pub(crate) fn connect_step(self: &Arc<Self>, a: Attempt) {
        if !a.reply.as_ref().is_some_and(Reply::start) {
            // Its waiter left before the command ran: nothing is started.
            return;
        }
        if a.logon.taken_back() || self.queue.latched() {
            return a.fail(Error::NotConnected);
        }
        let mut c = self.core();
        let replaced = match &c.conn {
            Conn::Disconnected {
                engine: EngineEnd::Closed,
            } => {
                drop(c);
                return self.start_logon(a);
            }
            Conn::Disconnected {
                engine: EngineEnd::Unconfirmed(why),
            } => {
                let e = Error::Connection(format!(
                    "the previous engine session did not confirm its shutdown: {why}"
                ));
                drop(c);
                return a.fail(e);
            }
            Conn::Closing { .. } => None,
            Conn::Connecting { logon, .. } => {
                logon.take_back();
                None
            }
            Conn::Connected { g, .. } => Some(*g),
        };
        c.parked.push_back(a);
        drop(c);
        if let Some(g) = replaced {
            self.close(g, Cause::Replaced);
        }
    }

    fn start_logon(self: &Arc<Self>, mut a: Attempt) {
        let mut c = self.core();
        let Some(g) = c.generation.checked_add(1) else {
            drop(c);
            return a.fail(Error::Value("generations exhausted".into()));
        };
        c.generation = g;
        a.g = g;
        self.client_id.store(a.client_id, Ordering::Relaxed);
        log::info!(
            target: LOG_CLIENT,
            "Connecting to {} with clientId {}...",
            a.account,
            a.client_id
        );
        if let Some(at) = a
            .logon_timeout
            .and_then(|t| self.clock.now().checked_add(t))
        {
            c.heap.insert(at, Box::new(move |ib| ib.login_deadline(g)));
        }
        let via = a.via.take();
        let (logon, timeout) = (a.logon.clone(), a.timeout);
        c.conn = Conn::Connecting {
            g,
            logon: logon.clone(),
        };
        c.logon_live = true;
        c.attempt = Some(a);
        drop(c);
        match via {
            Some(Via::Engine(config)) => {
                let config = session::logon_config(*config, &logon);
                if let Err(e) = session::spawn_logon(self.me.clone(), g, config, timeout, logon) {
                    let a = {
                        let mut c = self.core();
                        c.conn = Conn::Disconnected {
                            engine: EngineEnd::Closed,
                        };
                        c.logon_live = false;
                        c.attempt.take()
                    };
                    if let Some(a) = a {
                        a.fail(Error::from(e));
                    }
                }
            }
            #[cfg(test)]
            Some(Via::Test(Some(client))) => self.on_post(Post::LoggedOn { g, client }),
            #[cfg(test)]
            Some(Via::Test(None)) => {}
            None => {}
        }
    }

    fn login_deadline(self: &Arc<Self>, g: u64) {
        let reply = {
            let mut c = self.core();
            let c = &mut *c;
            match (&c.conn, c.attempt.as_mut()) {
                (Conn::Connecting { g: cg, logon }, Some(a))
                    if *cg == g && a.g == g && logon.expire() =>
                {
                    a.reported = true;
                    a.reply.take()
                }
                _ => return,
            }
        };
        if let Some(r) = reply {
            r.send(Err(Error::Timeout));
        }
        self.report("TimeoutError()");
    }

    /// `api_error` with a failed connect's message, logged as ib_async's
    /// `connectAsync` logs it (cl:229-233).
    fn report(&self, why: &str) {
        let msg = format!("API connection failed: {why}");
        log::error!(target: LOG_CLIENT, "{msg}");
        self.events.api_error.emit(&msg);
    }

    /// A connect's waiter left: a parked or logging-on attempt is dropped and
    /// reported, a published one's generation closed. Gives whether `t` was
    /// a connect's.
    fn attempt_left(self: &Arc<Self>, t: Token) -> bool {
        enum Left {
            Report(&'static str),
            Close(u64),
            Quiet,
        }
        let left = {
            let mut c = self.core();
            let c = &mut *c;
            if let Some(i) = c.parked.iter().position(|a| a.token == t) {
                let why = left_why(&c.parked[i].logon);
                c.parked.remove(i);
                Left::Report(why)
            } else if let Some(a) = c.attempt.as_mut().filter(|a| a.token == t) {
                match &c.conn {
                    Conn::Connecting { g, logon } if *g == a.g => {
                        logon.take_back();
                        if std::mem::replace(&mut a.reported, true) {
                            Left::Quiet
                        } else {
                            Left::Report(left_why(logon))
                        }
                    }
                    Conn::Connected { g, .. } if *g == a.g => Left::Close(*g),
                    _ => Left::Quiet,
                }
            } else {
                return false;
            }
        };
        match left {
            Left::Report(why) => self.report(why),
            Left::Close(g) => self.close(g, Cause::User),
            Left::Quiet => {}
        }
        true
    }

    pub(crate) fn on_post(self: &Arc<Self>, post: Post) {
        match post {
            Post::LoggedOn { g, client } => self.logged_on(g, client),
            Post::LogonFailed { g, error, engine } => self.logon_failed(g, error, engine),
            Post::EngineClosed { g, engine } => self.engine_closed(g, engine),
        }
    }

    fn logged_on(self: &Arc<Self>, g: u64, client: Arc<EClient>) {
        let mut c = self.core();
        let live = match &c.conn {
            Conn::Connecting { g: cg, logon } if *cg == g => !logon.taken_back(),
            _ => {
                // No generation waits for it: it only logs out.
                c.closers += 1;
                drop(c);
                return self.close_unclaimed(g, client);
            }
        };
        c.logon_live = false;
        if live && !self.queue.latched() {
            drop(c);
            return self.publish(g, client);
        }
        c.conn = Conn::Closing {
            g,
            client: Some(client),
        };
        let a = c.attempt.take();
        drop(c);
        if let Some(a) = a {
            a.fail(Error::Connection("the connect was taken back".into()));
        }
        self.start_closer(g);
    }

    /// Hands a session no generation claims to a closer, retried each
    /// second as `start_closer` is, so its logout never runs on the owner.
    /// It is counted in `closers` until the closer posts.
    fn close_unclaimed(self: &Arc<Self>, g: u64, client: Arc<EClient>) {
        if let Err(e) = session::spawn_closer(self.me.clone(), g, client.clone()) {
            log::error!(target: LOG_IB, "a closer could not start: {e}");
            if let Some(at) = self.clock.now().checked_add(Duration::from_secs(1)) {
                self.core()
                    .heap
                    .insert(at, Box::new(move |ib| ib.close_unclaimed(g, client)));
            }
        }
    }

    fn logon_failed(self: &Arc<Self>, g: u64, error: Error, engine: EngineEnd) {
        let a = {
            let mut c = self.core();
            if !matches!(&c.conn, Conn::Connecting { g: cg, .. } if *cg == g) {
                return;
            }
            c.conn = Conn::Disconnected { engine };
            c.logon_live = false;
            c.attempt.take()
        };
        if let Some(mut a) = a {
            if let Some(r) = a.reply.take() {
                r.send(Err(error.clone()));
            }
            if !a.reported {
                let why = match &error {
                    Error::Timeout => "TimeoutError()".to_owned(),
                    e => e.to_string(),
                };
                self.report(&why);
            }
        }
        self.resume_parked();
    }

    fn engine_closed(self: &Arc<Self>, g: u64, engine: EngineEnd) {
        let resume = {
            let mut c = self.core();
            c.closers = c.closers.saturating_sub(1);
            let closing = matches!(&c.conn, Conn::Closing { g: cg, .. } if *cg == g);
            if closing {
                c.conn = Conn::Disconnected { engine };
            }
            closing
        };
        if resume {
            self.resume_parked();
        }
    }

    fn resume_parked(self: &Arc<Self>) {
        loop {
            let a = {
                let mut c = self.core();
                if !matches!(c.conn, Conn::Disconnected { .. }) {
                    return;
                }
                c.parked.pop_front()
            };
            let Some(a) = a else {
                return;
            };
            if self.queue.latched() {
                a.fail(Error::NotConnected);
            } else if a.reply.as_ref().is_some_and(Reply::start) {
                self.connect_step(a);
            } else {
                // Its waiter left while it was parked.
                self.report(left_why(&a.logon));
            }
        }
    }

    /// Publishes generation `g`: a reset or fresh `State`, `api_start`, then
    /// the startup sync while `g` stands.
    fn publish(self: &Arc<Self>, g: u64, client: Arc<EClient>) {
        let now = self.wall_now();
        let (sync, old) = {
            let mut c = self.core();
            let c = &mut *c;
            let old = if std::mem::take(&mut c.rebuild) {
                let fresh = State::new(self.defaults.clone(), self.holder(), now);
                std::mem::replace(&mut c.state, fresh)
            } else {
                c.state.reset(now)
            };
            if let Some(i) = c.idle.take() {
                c.heap.cancel(i.deadline);
            }
            let (sync, client_id, timeout) = match c.attempt.as_mut() {
                Some(a) => (a.sync.take(), a.client_id, a.timeout),
                None => (None, self.client_id.load(Ordering::Relaxed), None),
            };
            c.state.client_id = client_id;
            c.state.accounts = client.accounts.clone();
            c.requests.begin(g);
            c.ids = IdSpace::new();
            c.last_activity = self.clock.now();
            c.started = self.clock.now();
            c.conn = Conn::Connected {
                g,
                client: client.clone(),
            };
            (sync.map(|s| (s, client_id, timeout)), old)
        };
        drop(old);
        self.queue.set_engine_unsent(0);
        client.on_data(Some(Arc::new(wake_owner)));
        self.events.api_start.emit(&());
        if !self.is_generation(g) {
            // Closed inside `api_start`: ib_async's next step reads the
            // accounts, which raises (cl:175-179).
            return self.finish_connect(g, Err(Error::NotConnected), false);
        }
        log::info!(target: LOG_CLIENT, "API connection ready");
        match sync {
            None => self.finish_connect(g, Ok(()), false),
            Some((s, client_id, timeout)) => self.start_sync(g, &client, client_id, s, timeout),
        }
    }

    /// Decides `g`'s connect: `synced` for `IB.connect`, which then logs and
    /// emits `connected_event`. A waiter gone, or an error, closes `g` as
    /// User, as `connectAsync`'s `except: disconnect()` does.
    fn finish_connect(self: &Arc<Self>, g: u64, r: Result<()>, synced: bool) {
        let reply = {
            let mut c = self.core();
            match c.attempt.take() {
                Some(mut a) if a.g == g => a.reply.take(),
                other => {
                    c.attempt = other;
                    None
                }
            }
        };
        let ok = r.is_ok();
        let there = match reply {
            Some(reply) => reply.send(r),
            None => false,
        };
        if ok && there {
            if synced {
                log::info!(target: LOG_IB, "Synchronization complete");
                self.events.connected_event.emit(&());
            }
        } else if self.is_generation(g) {
            let cause = if synced {
                Cause::User
            } else {
                Cause::ClientUser
            };
            self.close(g, cause);
        }
    }

    // -- The startup sync (ib:2038-2106) ---------------------------------

    fn start_sync(
        self: &Arc<Self>,
        g: u64,
        client: &Arc<EClient>,
        client_id: i64,
        s: SyncOpts,
        timeout: Option<Duration>,
    ) {
        if client_id == 0 {
            client.req_auto_open_orders(true);
        }
        let accounts = client.accounts.clone();
        let account = match accounts.as_slice() {
            [only] if s.account.is_empty() => only.clone(),
            _ => s.account.clone(),
        };
        let at = timeout.and_then(|t| self.clock.now().checked_add(t));
        let f = s.fetch;
        let mut waiting = Vec::new();
        if f.contains(StartupFetch::POSITIONS) {
            waiting.push((
                "positions".into(),
                self.sync_ask(client, Ask::Positions, at),
            ));
        }
        if !s.readonly {
            if f.contains(StartupFetch::ORDERS_OPEN) {
                waiting.push((
                    "open orders".into(),
                    self.sync_ask(client, Ask::OpenOrders, at),
                ));
            }
            if f.contains(StartupFetch::ORDERS_COMPLETE) {
                let ask = Ask::CompletedOrders { api_only: false };
                waiting.push(("completed orders".into(), self.sync_ask(client, ask, at)));
            }
        }
        if !account.is_empty() && f.contains(StartupFetch::ACCOUNT_UPDATES) {
            let ask = Ask::AccountUpdates { account };
            waiting.push(("account updates".into(), self.sync_ask(client, ask, at)));
        }
        if accounts.len() <= self.config().max_synced_sub_accounts
            && f.contains(StartupFetch::SUB_ACCOUNT_UPDATES)
        {
            for acc in &accounts {
                let p = self.sync_numbered(client, at, |client, id| {
                    client.req_account_updates_multi(id, acc, "", false);
                });
                waiting.push((format!("account updates for {acc}"), p));
            }
        }
        self.core().sync = Some(Startup {
            g,
            waiting,
            errors: Vec::new(),
            stage: Stage::Requests,
            fetch: f,
            raise: s.raise_sync_errors,
            timeout,
            failed: None,
        });
    }

    fn sync_ask(self: &Arc<Self>, client: &EClient, ask: Ask, at: Option<Instant>) -> Pending<()> {
        let (p, reply) = Pending::new(None);
        let send = {
            let mut c = self.core();
            let c = &mut *c;
            let mut x = c.requests.exec(ask.key());
            x.waiter = Some(Box::new(Ignore(reply)));
            if let Some(at) = at {
                arm(&mut c.heap, &mut x, at, timed_out);
            }
            c.requests.ask(ask, Some(x))
        };
        if let Some(ask) = send {
            self.send_ask(client, &ask);
        }
        p
    }

    fn sync_numbered(
        self: &Arc<Self>,
        client: &EClient,
        at: Option<Instant>,
        send: impl FnOnce(&EClient, i64),
    ) -> Pending<()> {
        let floor = client.order_id_floor();
        let (p, reply) = Pending::new(None);
        let id = {
            let mut c = self.core();
            let c = &mut *c;
            match c.ids.allocate(floor, 1) {
                Ok(id) => {
                    let mut x = c.requests.exec(ReqKey::Id(id));
                    x.waiter = Some(Box::new(Ignore(reply)));
                    if let Some(at) = at {
                        arm(&mut c.heap, &mut x, at, timed_out);
                    }
                    c.requests.insert(x);
                    id
                }
                Err(e) => return Pending::failed(e),
            }
        };
        send(client, id);
        self.queue.sent(1);
        p
    }

    fn sync_step(self: &Arc<Self>) {
        let Some(mut s) = self.core().sync.take() else {
            return;
        };
        let waker = Waker::from(Arc::new(OwnerWake));
        let mut cx = Context::from_waker(&waker);
        let mut still = Vec::new();
        for (name, mut p) in s.waiting.drain(..) {
            match Pin::new(&mut p).poll(&mut cx) {
                Poll::Pending => still.push((name, p)),
                Poll::Ready(Err(Error::Timeout)) => {
                    let msg = format!("{name} request timed out");
                    log::error!(target: LOG_IB, "{msg}");
                    s.errors.push(msg);
                }
                Poll::Ready(Err(e)) if matches!(s.stage, Stage::Executions) => s.failed = Some(e),
                Poll::Ready(_) => {}
            }
        }
        s.waiting = still;
        if !s.waiting.is_empty() {
            self.core().sync = Some(s);
            return;
        }
        if matches!(s.stage, Stage::Requests) && s.fetch.contains(StartupFetch::EXECUTIONS) {
            // The executions request must come after every order is in.
            let Some(client) = self.client_of(s.g) else {
                return self.finish_connect(s.g, Err(Error::NotConnected), true);
            };
            let at = s.timeout.and_then(|t| self.clock.now().checked_add(t));
            let filter = e::ExecutionFilter::from(&ExecutionFilter::default());
            let p =
                self.sync_numbered(&client, at, |client, id| client.req_executions(id, &filter));
            s.stage = Stage::Executions;
            s.waiting.push(("executions".into(), p));
            self.core().sync = Some(s);
            return;
        }
        let r = if let Some(e) = s.failed {
            Err(e)
        } else if s.raise && !s.errors.is_empty() {
            Err(Error::Connection(py_list(&s.errors)))
        } else if !(self.is_generation(s.g) && self.is_ready()) {
            Err(Error::Connection(
                "Socket connection broken while connecting".into(),
            ))
        } else {
            Ok(())
        };
        self.finish_connect(s.g, r, true);
    }

    // -- Teardown ------------------------------------------------------------

    /// `disconnect()`: a User close of the published generation, with
    /// ib_async's status line. Every call ends every `run()`.
    pub(crate) fn disconnect(self: &Arc<Self>) -> Option<String> {
        let status = self.connected().map(|(g, client)| {
            let t = client.traffic();
            let secs = self
                .clock
                .now()
                .saturating_duration_since(self.core().started)
                .as_secs_f64();
            let si = |n: f64| format_si(n).unwrap_or_default();
            let status = format!(
                "Disconnecting from {}, {}B sent in {} messages, {}B received in {} messages, \
                 session time {}s.",
                client.account_id,
                si(t.bytes_sent as f64),
                t.messages_sent,
                si(t.bytes_received as f64),
                t.messages_received,
                si(secs),
            );
            log::info!(target: LOG_IB, "{status}");
            self.close(g, Cause::User);
            status
        });
        self.progressed_by(|p| p.stops += 1);
        status
    }

    /// `Client::disconnect()`: a ClientUser close, which keeps `State`.
    pub(crate) fn client_disconnect(self: &Arc<Self>) {
        if let Some((g, _)) = self.connected() {
            self.close(g, Cause::ClientUser);
        }
        self.progressed_by(|p| p.stops += 1);
    }

    /// Runs a teardown step: a panic is logged, and the next publication
    /// starts from a fresh `State`.
    fn t(&self, f: impl FnOnce()) {
        if let Err(p) = catch_unwind(AssertUnwindSafe(f)) {
            log::error!(target: LOG_IB, "{}", panic_message(&*p));
            self.core().rebuild = true;
        }
    }

    /// Teardown T of generation `g`, in ib_async's order for its cause; then
    /// its engine session goes to a closer.
    pub(crate) fn close(self: &Arc<Self>, g: u64, cause: Cause) {
        let client = {
            let mut c = self.core();
            let client = match &c.conn {
                Conn::Connected { g: cg, client } if *cg == g => client.clone(),
                _ => return,
            };
            c.conn = Conn::Closing {
                g,
                client: Some(client.clone()),
            };
            if let Some(i) = c.idle.take() {
                c.heap.cancel(i.deadline);
            }
            client
        };
        client.on_data(None);
        drop(client);
        self.queue.set_engine_unsent(0);
        let err = match &cause {
            Cause::User | Cause::ClientUser => Error::NotConnected,
            Cause::Replaced | Cause::Peer => Error::Connection("Socket disconnect".into()),
            Cause::Internal(why) => Error::Connection(format!("internal error: {why}")),
        };
        match &cause {
            Cause::User => {
                self.t(|| log::info!(target: LOG_CLIENT, "Disconnecting"));
                self.t(|| self.events.disconnected_event.emit(&()));
                self.t(|| self.set_events_done());
                self.t(|| self.reset_state());
                self.t(|| self.fail_all(&err));
                self.t(|| log::info!(target: LOG_CLIENT, "Disconnected."));
            }
            Cause::ClientUser => {
                self.t(|| log::info!(target: LOG_CLIENT, "Disconnecting"));
                self.t(|| self.set_events_done());
                self.t(|| self.fail_all(&err));
                self.t(|| log::info!(target: LOG_CLIENT, "Disconnected."));
            }
            Cause::Replaced => {
                self.t(|| log::info!(target: LOG_CLIENT, "Disconnected."));
                self.peer_tail(&err);
            }
            Cause::Peer | Cause::Internal(_) => {
                let msg = match &cause {
                    Cause::Internal(why) => format!("internal error: {why}"),
                    _ => "Peer closed connection.".to_owned(),
                };
                self.t(|| log::error!(target: LOG_CLIENT, "{msg}"));
                self.t(|| self.events.api_error.emit(&msg));
                self.peer_tail(&err);
            }
        }
        if let Cause::Internal(_) = cause {
            self.core().rebuild = true;
        }
        self.start_closer(g);
    }

    fn peer_tail(&self, err: &Error) {
        self.t(|| self.set_events_done());
        self.t(|| self.fail_all(err));
        self.t(|| global_error_event().emit(err));
        self.t(|| self.reset_state());
        self.t(|| self.events.api_end.emit(&()));
    }

    fn reset_state(&self) {
        let now = self.wall_now();
        let old = self.core().state.reset(now);
        drop(old);
    }

    /// ib_async's `setEventsDone` (wr:344-359): every ticker's, subscribed
    /// list's and trade's events.
    fn set_events_done(&self) {
        let (tickers, lists, trades) = {
            let c = self.core();
            let s = &c.state;
            (
                s.tickers.values().cloned().collect::<Vec<_>>(),
                s.req_id_to_subscriber.values().cloned().collect::<Vec<_>>(),
                s.trades.values().cloned().collect::<Vec<_>>(),
            )
        };
        for t in tickers {
            t.update_event().set_done();
        }
        for l in lists {
            match l {
                Bars::Historical(l) => l.update_event().set_done(),
                Bars::RealTime(l) => l.update_event().set_done(),
                Bars::Scan(l) => l.update_event().set_done(),
            }
        }
        for t in trades {
            t.status_event().set_done();
            t.modify_event().set_done();
            t.fill_event().set_done();
            t.filled_event().set_done();
            t.commission_report_event().set_done();
            t.cancel_event().set_done();
            t.cancelled_event().set_done();
        }
    }

    /// Fails every registration with `err`: the whole map is taken first,
    /// and each waiter completed under its own guard.
    fn fail_all(&self, err: &Error) {
        let execs = {
            let mut c = self.core();
            let c = &mut *c;
            let mut xs = c.requests.take_all();
            for x in &mut xs {
                if let Some(d) = x.deadline.take() {
                    c.heap.cancel(d);
                }
            }
            xs
        };
        for x in execs {
            let e = err.clone();
            if let Err(p) = catch_unwind(AssertUnwindSafe(|| x.finish(Err(e)))) {
                log::error!(target: LOG_IB, "{}", panic_message(&*p));
            }
        }
    }

    fn start_closer(self: &Arc<Self>, g: u64) {
        let client = match &self.core().conn {
            Conn::Closing {
                g: cg,
                client: Some(c),
                ..
            } if *cg == g => c.clone(),
            _ => return,
        };
        client.on_data(None);
        match session::spawn_closer(self.me.clone(), g, client) {
            Ok(()) => {
                let held = {
                    let mut c = self.core();
                    c.closers += 1;
                    match &mut c.conn {
                        Conn::Closing { g: cg, client, .. } if *cg == g => client.take(),
                        _ => None,
                    }
                };
                drop(held);
            }
            Err(e) => {
                log::error!(target: LOG_IB, "a closer could not start: {e}");
                let at = self.clock.now().checked_add(Duration::from_secs(1));
                if let Some(at) = at {
                    self.core()
                        .heap
                        .insert(at, Box::new(move |ib| ib.start_closer(g)));
                }
            }
        }
    }

    // -- set_timeout (wr:440-467) ---------------------------------------

    pub(crate) fn set_timeout(self: &Arc<Self>, timeout: Duration) {
        let (now, wall) = (self.clock.now(), self.wall_now());
        let mut c = self.core();
        let c = &mut *c;
        c.last_activity = now;
        c.state.last_time = wall;
        if let Some(i) = c.idle.take() {
            c.heap.cancel(i.deadline);
        }
        if !timeout.is_zero() {
            arm_idle(c, timeout, now.checked_add(timeout));
        }
    }

    fn idle_due(self: &Arc<Self>) {
        let (now, wall) = (self.clock.now(), self.wall_now());
        let idle = {
            let mut c = self.core();
            let c = &mut *c;
            let Some(i) = c.idle.take() else {
                return;
            };
            if i.generation != c.generation {
                return;
            }
            match idle_check(c.last_activity, now, i.timeout) {
                IdleCheck::Idle(d) => {
                    c.last_activity = now;
                    c.state.last_time = wall;
                    d
                }
                IdleCheck::Rearm(at) => return arm_idle(c, i.timeout, at),
            }
        };
        log::debug!(target: "ib_async.wrapper", "Timeout");
        self.events.timeout_event.emit(&idle.as_secs_f64());
    }

    // -- The IB's exit, once it is dropped ------------------------------

    fn exit_step(self: &Arc<Self>) {
        let g = {
            let c = self.core();
            match &c.conn {
                Conn::Connected { g, .. } => Some(*g),
                Conn::Connecting { logon, .. } => {
                    logon.take_back();
                    None
                }
                _ => None,
            }
        };
        if let Some(g) = g {
            self.close(g, Cause::User);
        }
        let parked: Vec<Attempt> = self.core().parked.drain(..).collect();
        for a in parked {
            a.fail(Error::NotConnected);
        }
        {
            let c = self.core();
            if c.logon_live
                || c.closers > 0
                || matches!(c.conn, Conn::Connecting { .. } | Conn::Closing { .. })
            {
                return;
            }
        }
        drop(self.queue.close());
        self.set_events_done();
        self.t(|| self.events.set_done());
        self.fail_all(&Error::NotConnected);
        let attempt = self.core().attempt.take();
        if let Some(a) = attempt {
            a.fail(Error::NotConnected);
        }
        self.events.clear();
        let heap = std::mem::take(&mut self.core().heap);
        drop(heap);
        self.reset_state();
        self.progressed_by(|p| p.closed = true);
    }
}

/// What `api_error` says of a connect whose waiter left: the login
/// deadline's `TimeoutError()` when that is why, whichever side reached it.
fn left_why(logon: &Logon) -> &'static str {
    if logon.expired() {
        "TimeoutError()"
    } else {
        "CancelledError()"
    }
}

fn arm_idle(c: &mut Core, timeout: Duration, at: Option<Instant>) {
    if let Some(at) = at {
        let deadline = c
            .heap
            .insert(at, Box::new(|ib: &Arc<Shared>| ib.idle_due()));
        c.idle = Some(Idle {
            generation: c.generation,
            timeout,
            deadline,
        });
    }
}

/// Where a pass's effects go: the IB's books, events and engine session.
struct PassSink<'a> {
    ib: &'a Arc<Shared>,
    g: u64,
}

impl Sink for PassSink<'_> {
    fn books<R>(&mut self, f: impl FnOnce(&mut Books<'_>) -> R) -> R {
        let mut c = self.ib.core();
        let c = &mut *c;
        f(&mut Books {
            state: &mut c.state,
            requests: &mut c.requests,
            ids: &mut c.ids,
        })
    }

    fn emit(&mut self, e: Emit) {
        self.ib.emit(e);
    }

    fn call(&mut self, c: Call) {
        self.ib.call(self.g, c);
    }

    fn settle(&mut self, x: &mut Exec) {
        self.ib.settle(x);
    }

    fn connected(&self) -> bool {
        self.ib.is_generation(self.g)
    }

    fn raise_request_errors(&self) -> bool {
        self.ib.config().raise_request_errors
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::mpsc;

    use jiff::Timestamp;
    use jiff::tz::TimeZone;

    use super::*;
    use crate::engine::{ControlCommand, ErrorOrigin, SharedState, Wrapper};
    use crate::event::RecvError;
    use crate::ib::{ConnectOptions, IB, IBHandle};
    use crate::objects::{BarDataList, RealTimeBarList, ScanDataList};
    use crate::state::OrderKey;
    use crate::tests::GLOBAL_ERRORS;

    type Log = Arc<Mutex<Vec<String>>>;

    fn note(log: &Log, s: impl Into<String>) {
        lock(log).push(s.into());
    }

    fn seen(log: &Log) -> Vec<String> {
        lock(log).clone()
    }

    /// Marks this test's thread as the owner while it lives.
    struct AsOwner;

    impl AsOwner {
        fn new() -> Self {
            set_on_owner(true);
            AsOwner
        }
    }

    impl Drop for AsOwner {
        fn drop(&mut self) {
            set_on_owner(false);
        }
    }

    /// An engine session on no venue whose loop never runs, and the channel
    /// its commands arrive on.
    fn engine() -> (EClient, mpsc::Receiver<ControlCommand>) {
        engine_on(thread::spawn(|| {}))
    }

    /// As `engine`, with `thread` as the engine's: its `disconnect` returns
    /// once `thread` has ended.
    fn engine_on(thread: thread::JoinHandle<()>) -> (EClient, mpsc::Receiver<ControlCommand>) {
        let (tx, rx) = mpsc::channel();
        let shared = Arc::new(SharedState::new());
        let client = EClient::from_parts(shared, tx, thread, "DU123".into());
        (client, rx)
    }

    /// An IB no owner serves: the test drives its laps.
    fn ib() -> Arc<Shared> {
        let ib = Shared::new(
            IBDefaults::default(),
            IBConfig::default(),
            Clock::manual(Timestamp::UNIX_EPOCH),
        );
        ib.connect_internal_slots();
        ib
    }

    fn opts() -> ConnectOptions {
        ConnectOptions {
            fetch_fields: StartupFetch::NONE,
            ..ConnectOptions::default()
        }
    }

    fn handle(ib: &Arc<Shared>) -> IBHandle {
        IBHandle { shared: ib.clone() }
    }

    fn capture() -> Capture {
        Capture::new(TimeZone::UTC)
    }

    /// A connect whose logon is `via`, made on this thread.
    fn connect(ib: &Arc<Shared>, via: Via, o: ConnectOptions) -> (Pending<()>, Logon) {
        handle(ib).begin_connect(o, true, Some(via)).unwrap()
    }

    /// An IB connected on `client`, driven here as its owner.
    fn connected(client: EClient) -> (Arc<Shared>, Capture) {
        let (ib, mut capture) = (ib(), capture());
        let (mut p, _) = connect(&ib, Via::Test(Some(Arc::new(client))), opts());
        ib.lap(&mut capture);
        assert!(matches!(published(&mut p), Some(Ok(()))));
        (ib, capture)
    }

    fn published<T>(p: &mut Pending<T>) -> Option<Result<T>> {
        match Pin::new(p).poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(r) => Some(r),
            Poll::Pending => None,
        }
    }

    /// Laps `ib` until `done`, for at most two seconds.
    fn lap_until(ib: &Arc<Shared>, capture: &mut Capture, mut done: impl FnMut() -> bool) -> bool {
        let end = Instant::now() + Duration::from_secs(2);
        while Instant::now() < end {
            ib.lap(capture);
            if done() {
                return true;
            }
            thread::sleep(Duration::from_millis(1));
        }
        false
    }

    /// Waits for `done` on another thread's progress, for at most five
    /// seconds.
    fn within(mut done: impl FnMut() -> bool) -> bool {
        let end = Instant::now() + Duration::from_secs(5);
        while Instant::now() < end {
            if done() {
                return true;
            }
            thread::sleep(Duration::from_millis(1));
        }
        false
    }

    fn error(code: i64) -> Callback {
        Callback::Error {
            origin: ErrorOrigin::Session,
            code,
            message: "m".into(),
            advanced_order_reject_json: String::new(),
        }
    }

    /// A registration whose waiter notes how it ended.
    struct Noted(Log);

    impl Waiter for Noted {
        fn finish(self: Box<Self>, r: Result<Box<dyn Any + Send>>) -> bool {
            let e = r.err().map(|e| e.to_string()).unwrap_or_default();
            note(&self.0, format!("waiter {e}"));
            true
        }
    }

    /// Registers `waiter` under request `id`, giving its token.
    fn register(ib: &Arc<Shared>, id: i64, waiter: impl Waiter + 'static) -> Token {
        let mut c = ib.core();
        let mut x = c.requests.exec(ReqKey::Id(id));
        let token = x.token;
        x.waiter = Some(Box::new(waiter));
        c.requests.insert(x);
        token
    }

    /// The published generation's engine session.
    fn session(ib: &Arc<Shared>) -> Arc<EClient> {
        ib.connected().unwrap().1
    }

    /// Notes whether its IB's state and queue locks are free: when it is
    /// dropped, and, as a waker, when it is woken.
    struct Probe {
        ib: Weak<Shared>,
        log: Log,
        name: &'static str,
    }

    impl Probe {
        fn note(&self, what: &str) {
            let free = self
                .ib
                .upgrade()
                .is_some_and(|ib| ib.core.try_lock().is_ok() && ib.queue.q.try_lock().is_ok());
            note(&self.log, format!("{} {what}, free {free}", self.name));
        }
    }

    impl Drop for Probe {
        fn drop(&mut self) {
            self.note("dropped");
        }
    }

    impl Wake for Probe {
        fn wake(self: Arc<Self>) {
            self.note("woken");
        }
    }

    fn probe(ib: &Arc<Shared>, log: &Log, name: &'static str) -> Probe {
        Probe {
            ib: Arc::downgrade(ib),
            log: log.clone(),
            name,
        }
    }

    #[test]
    fn teardown_runs_in_ib_asyncs_order_for_each_cause() {
        let _g = lock(&GLOBAL_ERRORS);
        let _o = AsOwner::new();
        let peer = |first: &[&str]| {
            let rest = [
                "trade done",
                "waiter Socket disconnect",
                "global Socket disconnect",
                "disconnected",
                "apiEnd",
            ];
            first
                .iter()
                .chain(&rest)
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        };
        let cases = [
            (
                Cause::User,
                vec!["disconnected", "trade done", "waiter Not connected"],
                false,
            ),
            (
                Cause::ClientUser,
                vec!["trade done", "waiter Not connected"],
                true,
            ),
            (Cause::Peer, vec![], false),
            (Cause::Replaced, vec![], false),
        ];
        for (cause, want, kept) in cases {
            let want: Vec<String> = match cause {
                Cause::Peer => peer(&["apiError Peer closed connection."]),
                Cause::Replaced => peer(&[]),
                _ => want.iter().map(|s| s.to_string()).collect(),
            };
            let (client, _rx) = engine();
            let (ib, _) = connected(client);
            let log = Log::default();
            let l = log.clone();
            ib.events
                .disconnected_event
                .connect(move |()| note(&l, "disconnected"));
            let l = log.clone();
            ib.events
                .api_error
                .connect(move |m| note(&l, format!("apiError {m}")));
            let l = log.clone();
            ib.events.api_end.connect(move |()| note(&l, "apiEnd"));
            let l = log.clone();
            let global = global_error_event().connect(move |e| note(&l, format!("global {e}")));
            let trade = Live::new(Trade::default());
            let l = log.clone();
            trade
                .status_event()
                .done_event()
                .unwrap()
                .connect(move |()| note(&l, "trade done"));
            let g = {
                let mut c = ib.core();
                c.state.trades.insert(OrderKey::Perm(1), trade);
                let mut x = c.requests.exec(ReqKey::Id(7));
                x.waiter = Some(Box::new(Noted(log.clone())));
                c.requests.insert(x);
                c.generation
            };
            ib.close(g, cause.clone());
            global_error_event().disconnect(global);
            assert_eq!(seen(&log), want, "{cause:?}");
            assert_eq!(ib.core().state.trades.is_empty(), !kept, "{cause:?}");
            assert!(matches!(ib.core().conn, Conn::Closing { .. }), "{cause:?}");
        }
    }

    #[test]
    fn a_connect_on_a_connected_ib_replaces_it_and_a_failed_logon_decides_first() {
        let _g = lock(&GLOBAL_ERRORS);
        let _o = AsOwner::new();
        let (client, _rx) = engine();
        let (ib, mut capture) = connected(client);
        let log = Log::default();
        let l = log.clone();
        ib.events
            .disconnected_event
            .connect(move |()| note(&l, "disconnected"));

        // The connected session is closed, and the connect waits for the
        // engine's end before it starts a logon.
        let (second, _) = connect(&ib, Via::Test(None), opts());
        assert!(matches!(ib.core().conn, Conn::Closing { g: 1, .. }));
        assert_eq!(seen(&log), ["disconnected"]);
        assert!(lap_until(&ib, &mut capture, || matches!(
            ib.core().conn,
            Conn::Connecting { g: 2, .. }
        )));

        // A failed logon decides its waiter, then emits api_error.
        let second = Arc::new(Mutex::new(second));
        let (s, l) = (second.clone(), log.clone());
        ib.events.api_error.connect(move |m| {
            let decided = !lock(&s).expire();
            note(&l, format!("{m} decided={decided}"));
        });
        ib.on_post(Post::LogonFailed {
            g: 2,
            error: Error::Connection("refused".into()),
            engine: EngineEnd::Unconfirmed("stuck".into()),
        });
        assert_eq!(
            seen(&log)[1..],
            ["API connection failed: refused decided=true"]
        );
        let r = published(&mut lock(&second));
        assert!(matches!(r, Some(Err(Error::Connection(m))) if m == "refused"));

        // An engine not known to have ended: no session is started.
        let (mut third, _) = connect(&ib, Via::Test(None), opts());
        let r = published(&mut third);
        assert!(matches!(r, Some(Err(Error::Connection(m))) if m.contains("did not confirm")));
        assert!(matches!(ib.core().conn, Conn::Disconnected { .. }));
    }

    #[test]
    fn a_handler_that_disconnects_in_api_start_fails_the_connect() {
        let _o = AsOwner::new();
        let (ib, (client, _rx)) = (ib(), engine());
        let h = handle(&ib);
        ib.events.api_start.connect(move |()| {
            h.disconnect();
        });
        let log = Log::default();
        let l = log.clone();
        ib.events
            .connected_event
            .connect(move |()| note(&l, "connected"));
        let (mut p, _) = connect(&ib, Via::Test(Some(Arc::new(client))), opts());
        ib.lap(&mut capture());
        assert!(matches!(published(&mut p), Some(Err(Error::NotConnected))));
        assert!(seen(&log).is_empty());
    }

    #[test]
    fn a_disconnect_inside_a_read_drops_its_later_callbacks_and_the_pass_ends() {
        let _o = AsOwner::new();
        let (client, _rx) = engine();
        let (ib, _) = connected(client);
        let log = Log::default();
        let (l, h) = (log.clone(), handle(&ib));
        ib.events.error_event.connect(move |e| {
            note(&l, format!("error {}", e.1));
            h.disconnect();
        });
        let l = log.clone();
        ib.events
            .error_event
            .connect(move |e| note(&l, format!("second slot {}", e.1)));
        let l = log.clone();
        ib.events.update_event.connect(move |()| note(&l, "update"));
        let (passed, closed) = ib.apply_read(1, vec![error(2104), error(2106)]);
        assert!(passed && !closed);
        assert_eq!(seen(&log), ["error 2104", "second slot 2104", "update"]);
    }

    #[test]
    fn error_1102_asks_for_the_account_summary_until_error_event_is_cleared() {
        let _o = AsOwner::new();
        let (client, rx) = engine();
        let (ib, _) = connected(client);
        let refreshes = |rx: &mpsc::Receiver<ControlCommand>| {
            rx.try_iter()
                .filter(|c| matches!(c, ControlCommand::RefreshAccount { .. }))
                .count()
        };
        refreshes(&rx);
        ib.apply_read(1, vec![error(1102)]);
        assert_eq!(refreshes(&rx), 1);
        ib.events.error_event.clear();
        ib.apply_read(1, vec![error(1102)]);
        assert_eq!(refreshes(&rx), 0);
    }

    #[test]
    fn api_end_forwards_to_disconnected_event_until_cleared() {
        let _g = lock(&GLOBAL_ERRORS);
        let _o = AsOwner::new();
        for clear in [false, true] {
            let (client, _rx) = engine();
            let (ib, _) = connected(client);
            let log = Log::default();
            let l = log.clone();
            ib.events
                .disconnected_event
                .connect(move |()| note(&l, "disconnected"));
            if clear {
                ib.events.api_end.clear();
            }
            ib.close(1, Cause::Peer);
            assert_eq!(seen(&log).len(), usize::from(!clear));
        }
    }

    struct Count(AtomicUsize);

    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn counted() -> (Arc<Count>, Waker) {
        let c = Arc::new(Count(AtomicUsize::new(0)));
        (c.clone(), Waker::from(c))
    }

    #[test]
    fn admission_holds_requests_and_controls_at_their_bounds_and_lets_posts_pass() {
        let ib = ib();
        let q = &ib.queue;
        let entry = |class| Entry::step(class, |_| {});
        for _ in 0..UNSENT {
            assert!(q.try_push(entry(Class::Request)).is_ok());
        }
        assert!(matches!(
            q.try_push(entry(Class::Request)),
            Err(Refused::Full(_))
        ));
        for _ in 0..CONTROLS {
            assert!(q.try_push(entry(Class::Control)).is_ok());
        }
        assert!(matches!(
            q.try_push(entry(Class::Control)),
            Err(Refused::Full(_))
        ));
        assert!(q.try_push(entry(Class::Post)).is_ok());

        // A request waiting for room keeps one waker, the latest poll's.
        let mut p = ib.request(|_, _, _: Reply<()>| {});
        let ((first, w1), (second, w2)) = (counted(), counted());
        assert!(
            Pin::new(&mut p)
                .poll(&mut Context::from_waker(&w1))
                .is_pending()
        );
        assert!(
            Pin::new(&mut p)
                .poll(&mut Context::from_waker(&w2))
                .is_pending()
        );
        assert_eq!(lock(&q.q).space_wakers.len(), 1);

        // Taken, a request counts as the engine's unsent work until a read
        // says the engine has it; then there is room, and the waiter is woken.
        assert_eq!(q.take_group(CMD_GROUP).len(), CMD_GROUP);
        assert!(matches!(
            q.try_push(entry(Class::Request)),
            Err(Refused::Full(_))
        ));
        q.set_engine_unsent(0);
        assert_eq!(first.0.load(Ordering::SeqCst), 0);
        assert!(second.0.load(Ordering::SeqCst) >= 1);
        let queued = lock(&q.q).entries.len();
        assert!(
            Pin::new(&mut p)
                .poll(&mut Context::from_waker(&w2))
                .is_pending()
        );
        assert_eq!(lock(&q.q).entries.len(), queued + 1);

        // A waiter dropped before it found room sends nothing.
        q.sent(UNSENT);
        let mut dropped = ib.request(|_, _, _: Reply<()>| {});
        assert!(
            Pin::new(&mut dropped)
                .poll(&mut Context::from_waker(&w1))
                .is_pending()
        );
        drop(dropped);
        assert_eq!(lock(&q.q).entries.len(), queued + 1);
        assert!(lock(&q.q).space_wakers.is_empty());

        // Closing wakes every waiter and hands back what was queued.
        let mut late = ib.request(|_, _, _: Reply<()>| {});
        assert!(
            Pin::new(&mut late)
                .poll(&mut Context::from_waker(&w1))
                .is_pending()
        );
        assert_eq!(q.close().len(), queued + 1);
        assert!(first.0.load(Ordering::SeqCst) >= 1);
        assert!(matches!(
            published(&mut late),
            Some(Err(Error::NotConnected))
        ));
    }

    #[test]
    fn the_login_deadline_takes_the_logon_back_once_and_spares_a_finished_login() {
        let _o = AsOwner::new();
        let o = || ConnectOptions {
            logon_timeout: Some(Duration::from_secs(5)),
            ..opts()
        };
        let ib = ib();
        let mut capture = capture();
        let log = Log::default();
        let l = log.clone();
        ib.events.api_error.connect(move |m| note(&l, m.clone()));
        let (mut p, logon) = connect(&ib, Via::Test(None), o());
        ib.clock.advance(Duration::from_secs(4));
        ib.lap(&mut capture);
        assert!(published(&mut p).is_none());
        ib.clock.advance(Duration::from_secs(1));
        ib.lap(&mut capture);
        assert!(matches!(published(&mut p), Some(Err(Error::Timeout))));
        assert!(logon.taken_back());
        // The logon thread's end, taken back, reports nothing more.
        ib.on_post(Post::LogonFailed {
            g: 1,
            error: Error::Timeout,
            engine: EngineEnd::Closed,
        });
        assert_eq!(seen(&log), ["API connection failed: TimeoutError()"]);

        // A login done in time is not timed out by it.
        let (client, _rx) = engine();
        let (mut p, _) = connect(&ib, Via::Test(Some(Arc::new(client))), o());
        ib.clock.advance(Duration::from_secs(6));
        ib.lap(&mut capture);
        assert!(matches!(published(&mut p), Some(Ok(()))));
        assert!(ib.is_ready());
    }

    #[test]
    fn a_connect_whose_waiter_leaves_is_taken_back_and_one_left_unrun_starts_nothing() {
        let (ib, mut capture) = (ib(), capture());
        let log = Log::default();
        let l = log.clone();
        ib.events.api_error.connect(move |m| note(&l, m.clone()));
        // Gone while its logon runs: the owner takes the logon back at its
        // next lap and reports it once.
        let (p, logon) = {
            let _o = AsOwner::new();
            connect(&ib, Via::Test(None), opts())
        };
        drop(p);
        {
            let _o = AsOwner::new();
            ib.lap(&mut capture);
            assert!(logon.taken_back());
            ib.on_post(Post::LogonFailed {
                g: 1,
                error: Error::Connection("taken back".into()),
                engine: EngineEnd::Closed,
            });
        }
        assert_eq!(seen(&log), ["API connection failed: CancelledError()"]);
        // Gone before its command ran: no logon starts.
        let (mut p, _) = connect(&ib, Via::Test(None), opts());
        assert!(published(&mut p).is_none());
        drop(p);
        let _o = AsOwner::new();
        ib.lap(&mut capture);
        assert!(matches!(ib.core().conn, Conn::Disconnected { .. }));
        assert_eq!(seen(&log).len(), 1);

        // Gone at its own login deadline: reported as the owner's deadline
        // would report it.
        let (p, logon) = connect(&ib, Via::Test(None), opts());
        assert!(logon.expire());
        drop(p);
        ib.lap(&mut capture);
        assert_eq!(seen(&log)[1..], ["API connection failed: TimeoutError()"]);
    }

    #[test]
    fn a_parked_connect_whose_waiter_left_is_reported_whenever_it_resumes() {
        let _g = lock(&GLOBAL_ERRORS);
        let _o = AsOwner::new();
        let (client, _rx) = engine();
        let (ib, mut capture) = connected(client);
        let log = Log::default();
        let l = log.clone();
        ib.events.api_error.connect(move |m| note(&l, m.clone()));
        // Parked behind the close of the session it replaces, then left;
        // the engine's end resumes it before its leaving is retired.
        let (p, _) = connect(&ib, Via::Test(None), opts());
        drop(p);
        ib.on_post(Post::EngineClosed {
            g: 1,
            engine: EngineEnd::Closed,
        });
        ib.lap(&mut capture);
        assert_eq!(seen(&log), ["API connection failed: CancelledError()"]);
        assert!(matches!(ib.core().conn, Conn::Disconnected { .. }));
    }

    #[test]
    fn a_startup_request_that_times_out_fails_the_connect_when_sync_errors_raise() {
        crate::tests::capture_logs();
        let _o = AsOwner::new();
        let (ib, mut capture) = (ib(), capture());
        let (client, rx) = engine();
        let log = Log::default();
        let l = log.clone();
        ib.events
            .disconnected_event
            .connect(move |()| note(&l, "disconnected"));
        let l = log.clone();
        ib.events
            .connected_event
            .connect(move |()| note(&l, "connected"));
        let o = ConnectOptions {
            fetch_fields: StartupFetch::POSITIONS,
            raise_sync_errors: true,
            timeout: Some(Duration::from_secs(1)),
            ..opts()
        };
        let (mut p, _) = connect(&ib, Via::Test(Some(Arc::new(client))), o);
        ib.lap(&mut capture);
        assert!(published(&mut p).is_none());
        assert!(ib.is_ready(), "connected through the sync, as isReady() is");
        assert!(rx.try_iter().any(|c| matches!(c, ControlCommand::Ask(_))));
        ib.clock.advance(Duration::from_secs(1));
        ib.lap(&mut capture);
        let r = published(&mut p);
        assert!(
            matches!(&r, Some(Err(Error::Connection(m))) if m == "['positions request timed out']"),
            "{r:?}"
        );
        assert_eq!(seen(&log), ["disconnected"]);
        let logged = crate::tests::errors_here();
        assert!(logged.contains(&(LOG_IB.into(), "positions request timed out".into())));
    }

    #[test]
    fn the_exit_runs_what_was_admitted_first_then_ends_every_event() {
        let _g = lock(&GLOBAL_ERRORS);
        let (client, _rx) = engine();
        let (ib, mut capture) = {
            let _o = AsOwner::new();
            connected(client)
        };
        let log = Log::default();
        let l = log.clone();
        ib.events
            .disconnected_event
            .connect(move |()| note(&l, "disconnected"));
        // A connect that closed the session it replaces, parked behind that
        // close when the IB is dropped.
        let (mut waiter, _) = connect(&ib, Via::Test(None), opts());
        assert!(published(&mut waiter).is_none());
        {
            let _o = AsOwner::new();
            ib.lap(&mut capture);
        }
        assert_eq!(ib.core().parked.len(), 1);
        for i in 0..3 {
            let l = log.clone();
            ib.control(move |_| note(&l, format!("control {i}")));
        }
        let mut updates = ib.events.update_event.subscribe();
        ib.queue.latch();
        {
            let _o = AsOwner::new();
            assert!(lap_until(&ib, &mut capture, || ib.is_closed()));
        }
        assert_eq!(
            seen(&log),
            ["disconnected", "control 0", "control 1", "control 2"]
        );
        assert!(matches!(
            published(&mut waiter),
            Some(Err(Error::NotConnected))
        ));
        assert!(matches!(updates.try_recv(), Err(RecvError::Done)));
        assert!(matches!(
            ib.queue.try_push(Entry::step(Class::Control, |_| {})),
            Err(Refused::Closed(_))
        ));
    }

    #[test]
    fn requests_run_outside_a_session_leave_no_charge() {
        // A reconnect loop's failed connects are requests: were they charged,
        // the 65th would never be admitted.
        let (ib, mut capture) = (ib(), capture());
        for _ in 0..UNSENT {
            assert!(
                ib.queue
                    .try_push(Entry::step(Class::Request, |_| {}))
                    .is_ok()
            );
        }
        ib.lap(&mut capture);
        assert!(
            ib.queue
                .try_push(Entry::step(Class::Request, |_| {}))
                .is_ok()
        );
    }

    #[test]
    fn the_state_a_teardown_resets_is_dropped_after_the_lock() {
        let _o = AsOwner::new();
        let (client, _rx) = engine();
        let (ib, _) = connected(client);
        let log = Log::default();
        let probe = probe(&ib, &log, "state");
        let trade = Live::new(Trade::default());
        trade.status_event().connect(move |_| {
            let _ = &probe;
        });
        let g = {
            let mut c = ib.core();
            c.state.trades.insert(OrderKey::Perm(1), trade);
            c.generation
        };
        ib.close(g, Cause::User);
        assert_eq!(seen(&log), ["state dropped, free true"]);
    }

    #[test]
    fn what_a_panicked_read_recorded_is_not_the_next_reads() {
        let _g = lock(&GLOBAL_ERRORS);
        let _o = AsOwner::new();
        let (client, _rx) = engine();
        let (ib, mut capture) = connected(client);
        // A read that panicked leaves what it recorded behind.
        capture.connection_closed();
        ib.lap(&mut capture);
        assert!(ib.is_generation(1));
    }

    #[test]
    fn a_read_comes_between_every_group_of_queued_entries() {
        let _o = AsOwner::new();
        let (client, _rx) = engine();
        let (ib, mut capture) = connected(client);
        let log = Log::default();
        let l = log.clone();
        ib.events.error_event.connect(move |_| note(&l, "read"));
        for class in [Class::Control, Class::Request] {
            for _ in 0..CMD_GROUP {
                let l = log.clone();
                let e = Entry::step(class, move |_| note(&l, "entry"));
                assert!(ib.queue.try_push(e).is_ok());
            }
        }
        let mut want = Vec::new();
        for _ in 0..2 {
            session(&ib).refuse(ErrorOrigin::Session, 2104, "farm ok");
            ib.lap(&mut capture);
            want.extend(["entry"; CMD_GROUP]);
            want.push("read");
        }
        assert_eq!(seen(&log), want);
    }

    #[test]
    fn a_panic_in_the_owners_own_work_ends_its_generation_and_answers_every_waiter() {
        struct PanicsWhenAnswered;
        impl Waiter for PanicsWhenAnswered {
            fn finish(self: Box<Self>, _: Result<Box<dyn Any + Send>>) -> bool {
                panic!("answered")
            }
        }
        struct PanicsWhenDropped;
        impl Waiter for PanicsWhenDropped {
            fn finish(self: Box<Self>, _: Result<Box<dyn Any + Send>>) -> bool {
                true
            }
        }
        impl Drop for PanicsWhenDropped {
            fn drop(&mut self) {
                panic!("dropped")
            }
        }
        let _g = lock(&GLOBAL_ERRORS);
        let _o = AsOwner::new();
        for place in ["a pass", "a retirement", "a command"] {
            let (client, _rx) = engine();
            let (ib, mut capture) = connected(client);
            let log = Log::default();
            let l = log.clone();
            ib.events
                .api_error
                .connect(move |m| note(&l, format!("apiError {m}")));
            // A registration only the teardown answers.
            register(&ib, 9, Noted(log.clone()));
            let mut replies = Vec::new();
            let why = match place {
                "a pass" => {
                    register(&ib, 7, PanicsWhenAnswered);
                    let ends = ErrorOrigin::Request { id: 7, ends: true };
                    session(&ib).refuse(ends, 200, "no");
                    "answered"
                }
                "a retirement" => {
                    let token = register(&ib, 7, PanicsWhenDropped);
                    ib.abandon(token);
                    "dropped"
                }
                _ => {
                    // One command decides its reply and then panics; the
                    // next panics before it replies.
                    let (p, reply) = Pending::new(None);
                    let e = Entry::step(Class::Control, move |_| {
                        reply.send(Ok(()));
                        panic!("decided")
                    });
                    assert!(ib.queue.try_push(e).is_ok());
                    replies.push(p);
                    let (p, reply) = Pending::<()>::new(None);
                    let e = Entry::step(Class::Control, move |_| {
                        let _unsent = reply;
                        panic!("undecided")
                    });
                    assert!(ib.queue.try_push(e).is_ok());
                    replies.push(p);
                    "decided"
                }
            };
            ib.lap(&mut capture);
            let internal = format!("internal error: {why}");
            assert_eq!(
                seen(&log),
                [format!("apiError {internal}"), format!("waiter {internal}")],
                "{place}"
            );
            if let [decided, undecided] = &mut replies[..] {
                assert!(matches!(published(decided), Some(Ok(()))));
                let r = published(undecided);
                assert!(
                    matches!(&r, Some(Err(Error::Connection(m))) if m == "internal error: undecided"),
                    "{r:?}"
                );
            }
            // The owner goes on, and the next generation starts.
            assert!(lap_until(&ib, &mut capture, || matches!(
                ib.core().conn,
                Conn::Disconnected { .. }
            )));
            let (client, _rx) = engine();
            let (mut p, _) = connect(&ib, Via::Test(Some(Arc::new(client))), opts());
            ib.lap(&mut capture);
            assert!(matches!(published(&mut p), Some(Ok(()))), "{place}");
        }
    }

    #[test]
    fn a_teardown_step_that_panics_leaves_the_rest_to_run() {
        struct Boom;
        impl Drop for Boom {
            fn drop(&mut self) {
                panic!("a dropped value panicked")
            }
        }
        let _g = lock(&GLOBAL_ERRORS);
        let _o = AsOwner::new();
        let (client, _rx) = engine();
        let (ib, mut capture) = connected(client);
        let log = Log::default();
        let l = log.clone();
        ib.events
            .api_error
            .connect(move |m| note(&l, format!("apiError {m}")));
        let l = log.clone();
        ib.events
            .disconnected_event
            .connect(move |()| note(&l, "disconnected"));
        register(&ib, 7, Noted(log.clone()));
        // A trade only the state holds, whose handler's value panics when
        // the reset drops it.
        let trade = Live::new(Trade::default());
        let boom = Boom;
        trade.status_event().connect(move |_| {
            let _ = &boom;
        });
        ib.core().state.trades.insert(OrderKey::Perm(1), trade);
        session(&ib).shared_state().push_closed();
        ib.lap(&mut capture);
        assert_eq!(
            seen(&log),
            [
                "apiError Peer closed connection.",
                "waiter Socket disconnect",
                "disconnected"
            ]
        );
        assert!(lap_until(&ib, &mut capture, || matches!(
            ib.core().conn,
            Conn::Disconnected { .. }
        )));
        let (client, _rx) = engine();
        let (mut p, _) = connect(&ib, Via::Test(Some(Arc::new(client))), opts());
        ib.lap(&mut capture);
        assert!(matches!(published(&mut p), Some(Ok(()))));
    }

    #[test]
    fn a_connect_whose_waiter_left_as_its_sync_ended_emits_no_connected_event() {
        let _o = AsOwner::new();
        let (ib, mut capture) = (ib(), capture());
        let log = Log::default();
        let l = log.clone();
        ib.events
            .connected_event
            .connect(move |()| note(&l, "connected"));
        let l = log.clone();
        ib.events
            .disconnected_event
            .connect(move |()| note(&l, "disconnected"));
        let (client, _rx) = engine();
        let o = ConnectOptions {
            fetch_fields: StartupFetch::POSITIONS,
            ..opts()
        };
        let (p, _) = connect(&ib, Via::Test(Some(Arc::new(client))), o);
        ib.lap(&mut capture);
        // The caller's deadline, reached before the sync's last answer.
        assert!(p.expire());
        ib.apply_read(1, vec![Callback::PositionEnd]);
        ib.lap(&mut capture);
        assert_eq!(seen(&log), ["disconnected"]);
    }

    #[test]
    fn the_read_that_closes_is_applied_whole_before_the_teardown() {
        let _g = lock(&GLOBAL_ERRORS);
        let _o = AsOwner::new();
        let (client, _rx) = engine();
        let (ib, mut capture) = connected(client);
        let log = Log::default();
        let l = log.clone();
        ib.events
            .error_event
            .connect(move |e| note(&l, format!("error {}", e.1)));
        let l = log.clone();
        ib.events.update_event.connect(move |()| note(&l, "update"));
        let l = log.clone();
        ib.events
            .api_error
            .connect(move |m| note(&l, format!("apiError {m}")));
        let l = log.clone();
        ib.events
            .disconnected_event
            .connect(move |()| note(&l, "disconnected"));
        register(&ib, 7, Noted(log.clone()));
        // A record, then the engine's last, taken by one read.
        let client = session(&ib);
        client.refuse(ErrorOrigin::Session, 2104, "farm ok");
        client.shared_state().push_closed();
        drop(client);
        ib.lap(&mut capture);
        assert_eq!(
            seen(&log),
            [
                "error 2104",
                "update",
                "apiError Peer closed connection.",
                "waiter Socket disconnect",
                "disconnected"
            ]
        );
    }

    #[test]
    fn a_blocking_connects_own_deadlines_hold_while_the_owner_is_stalled() {
        let _g = lock(&GLOBAL_ERRORS);
        let limit = Some(Duration::from_millis(100));
        for (login, request) in [(limit, None), (None, limit)] {
            // No owner serves this IB: its connect is never run.
            let ib = ib();
            ib.set_config(IBConfig {
                request_timeout: request,
                ..IBConfig::default()
            });
            let cancel = Arc::new(AtomicBool::new(false));
            let o = ConnectOptions {
                config: e::EClientConfig {
                    cancel: Some(cancel.clone()),
                    ..e::EClientConfig::default()
                },
                logon_timeout: login,
                ..opts()
            };
            let start = Instant::now();
            assert!(matches!(handle(&ib).connect(o), Err(Error::Timeout)));
            assert!(start.elapsed() < Duration::from_secs(2));
            assert!(cancel.load(Ordering::Acquire), "the logon is taken back");
        }
    }

    #[test]
    fn objects_a_client_close_keeps_are_freed_once_the_next_session_starts() {
        let _o = AsOwner::new();
        let (client, _rx) = engine();
        let (ib, mut capture) = connected(client);
        let trade = Live::new(Trade::default());
        let ticker = Live::new(Ticker::new(None, IBDefaults::default()));
        let bars = Live::new(BarDataList::default());
        let realtime = Live::new(RealTimeBarList::default());
        let scan = Live::new(ScanDataList::default());
        {
            let mut c = ib.core();
            let s = &mut c.state;
            s.trades.insert(OrderKey::Perm(1), trade.clone());
            s.req_id_to_ticker.insert(1, ticker.clone());
            s.req_id_to_subscriber
                .insert(2, Bars::Historical(bars.clone()));
            s.req_id_to_subscriber
                .insert(3, Bars::RealTime(realtime.clone()));
            s.req_id_to_subscriber.insert(4, Bars::Scan(scan.clone()));
        }
        for e in [
            Emit::TradeStatus(trade.clone()),
            Emit::TradeFilled(trade.clone()),
            Emit::TradeCancelled(trade.clone()),
            Emit::TickerUpdate(ticker.clone()),
            Emit::BarsUpdate(Bars::Historical(bars.clone()), true),
            Emit::BarsUpdate(Bars::RealTime(realtime.clone()), true),
            Emit::ScanUpdate(scan.clone()),
        ] {
            ib.emit(e);
        }
        let weak = (
            trade.downgrade(),
            ticker.downgrade(),
            bars.downgrade(),
            realtime.downgrade(),
            scan.downgrade(),
        );
        drop((trade, ticker, bars, realtime, scan));
        let alive = || {
            [
                weak.0.upgrade().is_some(),
                weak.1.upgrade().is_some(),
                weak.2.upgrade().is_some(),
                weak.3.upgrade().is_some(),
                weak.4.upgrade().is_some(),
            ]
        };
        // `Client::disconnect` keeps them readable until the next session.
        ib.close(1, Cause::ClientUser);
        assert_eq!(alive(), [true; 5]);
        assert!(lap_until(&ib, &mut capture, || matches!(
            ib.core().conn,
            Conn::Disconnected { .. }
        )));
        let (client, _rx) = engine();
        let (mut p, _) = connect(&ib, Via::Test(Some(Arc::new(client))), opts());
        ib.lap(&mut capture);
        assert!(matches!(published(&mut p), Some(Ok(()))));
        assert_eq!(alive(), [false; 5]);
    }

    #[test]
    fn what_the_ibs_exit_drops_may_call_back_into_it() {
        struct Answered {
            _probe: Probe,
        }
        impl Waiter for Answered {
            fn finish(self: Box<Self>, _: Result<Box<dyn Any + Send>>) -> bool {
                true
            }
        }
        let _o = AsOwner::new();
        let (client, _rx) = engine();
        let (ib, mut capture) = connected(client);
        let log = Log::default();
        // A waiter the close fails, a deadline dropped with the heap, and a
        // handler the exit clears.
        let _probe = probe(&ib, &log, "waiter");
        register(&ib, 7, Answered { _probe });
        let p = probe(&ib, &log, "deadline");
        let at = ib.clock.now() + Duration::from_secs(3600);
        ib.core()
            .heap
            .insert(at, Box::new(move |_: &Arc<Shared>| drop(p)));
        let p = probe(&ib, &log, "handler");
        ib.events.error_event.connect(move |_| {
            let _ = &p;
        });
        ib.queue.latch();
        ib.lap(&mut capture);
        // The closer's post, then a full group of controls ahead of an
        // entry the queue's close drains.
        assert!(within(|| !lock(&ib.queue.q).entries.is_empty()));
        for _ in 1..CONTROLS {
            assert!(
                ib.queue
                    .try_push(Entry::step(Class::Control, |_| {}))
                    .is_ok()
            );
        }
        let p = probe(&ib, &log, "entry");
        let e = Entry::step(Class::Control, move |_| drop(p));
        assert!(ib.queue.try_push(e).is_ok());
        ib.lap(&mut capture);
        assert!(ib.is_closed());
        assert_eq!(
            seen(&log),
            [
                "waiter dropped, free true",
                "entry dropped, free true",
                "handler dropped, free true",
                "deadline dropped, free true"
            ]
        );
    }

    #[test]
    fn a_request_waiting_for_room_has_its_waker_woken_and_dropped_with_no_lock_held() {
        let ib = ib();
        for _ in 0..UNSENT {
            assert!(
                ib.queue
                    .try_push(Entry::step(Class::Request, |_| {}))
                    .is_ok()
            );
        }
        let log = Log::default();
        // Each waker's only other holder is the queue once it is polled.
        let poll = |p: &mut Pending<()>, name| {
            let w = Waker::from(Arc::new(probe(&ib, &log, name)));
            assert!(Pin::new(p).poll(&mut Context::from_waker(&w)).is_pending());
        };
        let mut kept = ib.request(|_, _, _: Reply<()>| {});
        poll(&mut kept, "replaced");
        poll(&mut kept, "kept");
        let mut gone = ib.request(|_, _, _: Reply<()>| {});
        poll(&mut gone, "forgotten");
        drop(gone);
        ib.queue.take_group(CMD_GROUP);
        assert_eq!(
            seen(&log),
            [
                "replaced dropped, free true",
                "forgotten dropped, free true",
                "kept woken, free true",
                "kept dropped, free true"
            ]
        );
    }

    // -- On the owner thread, with the engine harness ----------------------

    #[test]
    fn an_attached_session_is_read_and_dropping_the_ib_logs_it_out() {
        let _g = lock(&GLOBAL_ERRORS);
        let (client, _rx) = engine();
        let ib = IB::attach(client, opts(), Clock::system()).unwrap();
        assert!(ib.is_connected());
        assert_eq!(format!("{ib:?}"), "<IB connected to DU123 clientId=1>");
        let log = Log::default();
        let l = log.clone();
        ib.error_event()
            .connect(move |e| note(&l, format!("{} {}", e.1, e.2)));
        let (_, client) = ib.shared.connected().unwrap();
        client.refuse(ErrorOrigin::Session, 2104, "farm ok");
        assert!(within(|| seen(&log) == ["2104 farm ok"]));

        // A read that arrives while a thread waits for one ends its wait.
        let pusher = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            client.refuse(ErrorOrigin::Session, 2106, "hmds ok");
        });
        assert_eq!(
            ib.wait_on_update(Some(Duration::from_secs(5))).ok(),
            Some(true)
        );
        pusher.join().unwrap();

        let h = ib.handle();
        drop(ib);
        assert!(h.shared.is_closed());
        assert!(!h.is_connected());
        assert_eq!(h.disconnect(), None);
    }

    #[test]
    fn a_peer_close_fails_blocking_waits_once_its_teardown_has_run() {
        let _g = lock(&GLOBAL_ERRORS);
        let (client, _rx) = engine();
        let ib = IB::attach(client, opts(), Clock::system()).unwrap();
        let (other, _orx) = engine();
        let other = IB::attach(other, opts(), Clock::system()).unwrap();
        let log = Log::default();
        let l = log.clone();
        ib.disconnected_event().connect(move |()| {
            // Slow, so a wait woken inside the teardown is seen first.
            thread::sleep(Duration::from_millis(50));
            note(&l, "disconnected");
        });
        let listening = global_error_event().len();
        let l = log.clone();
        let sleeper = thread::spawn(move || {
            let r = IB::sleep(Duration::from_secs(30)).map(drop);
            note(&l, "sleep");
            r
        });
        // Any IB's close ends a wait on another, as ib_async's run does.
        let (l, h) = (log.clone(), other.handle());
        let updater = thread::spawn(move || {
            let r = h.wait_on_update(Some(Duration::from_secs(30))).map(drop);
            note(&l, "wait_on_update");
            r
        });
        assert!(within(|| global_error_event().len() >= listening + 2));
        let (_, client) = ib.shared.connected().unwrap();
        client.disconnect();
        for r in [sleeper.join().unwrap(), updater.join().unwrap()] {
            assert!(matches!(r, Err(Error::Connection(m)) if m == "Socket disconnect"));
        }
        assert_eq!(seen(&log)[0], "disconnected");
        assert!(!ib.is_connected());
    }

    #[test]
    fn blocking_faces_fail_at_once_on_the_owner_thread_and_run_returns() {
        let (client, _rx) = engine();
        let ib = IB::attach(client, opts(), Clock::system()).unwrap();
        let log = Log::default();
        let (l, h) = (log.clone(), ib.handle());
        ib.error_event().connect(move |_| {
            let value = |r: Result<()>| matches!(r, Err(Error::Value(_)));
            note(
                &l,
                format!("wait_on_update {}", value(h.wait_on_update(None).map(drop))),
            );
            note(
                &l,
                format!("sleep {}", value(IB::sleep(Duration::ZERO).map(drop))),
            );
            note(&l, format!("connect {}", value(h.connect(opts()))));
            note(&l, format!("run {}", h.run().is_ok()));
        });
        let (_, client) = ib.shared.connected().unwrap();
        client.refuse(ErrorOrigin::Session, 2104, "farm ok");
        assert!(within(|| seen(&log).len() == 4));
        assert_eq!(
            seen(&log),
            [
                "wait_on_update true",
                "sleep true",
                "connect true",
                "run true"
            ]
        );
    }

    /// A request that hands the engine one command, as each of the IB's
    /// requests does, noting its turn; answered once sent.
    fn send_one(ib: &Arc<Shared>, log: &Log, n: usize) -> Pending<()> {
        let log = log.clone();
        ib.request(move |ib, _, reply: Reply<()>| {
            if let Some((_, client)) = ib.connected() {
                client.req_positions();
                ib.queue.sent(1);
            }
            note(&log, format!("sent {n}"));
            reply.send(Ok(()));
        })
    }

    #[test]
    fn a_stalled_engine_holds_user_requests_at_unsent_while_the_owner_goes_on() {
        let _g = lock(&GLOBAL_ERRORS);
        let (client, rx) = engine();
        let ib = IB::attach(client, opts(), Clock::system()).unwrap();
        let client = session(&ib.shared);
        let log = Log::default();
        // The engine's loop never runs: whatever it is handed stays unsent.
        for n in 0..UNSENT {
            park_on(send_one(&ib.shared, &log, n)).unwrap();
        }
        // A blocking face times out with nothing admitted, so nothing sent.
        let r = send_one(&ib.shared, &log, UNSENT).wait(Some(Duration::from_millis(100)));
        assert!(matches!(r, Err(Error::Timeout)), "{r:?}");
        let (shared, l) = (ib.shared.clone(), log.clone());
        let waiting = thread::spawn(move || park_on(send_one(&shared, &l, UNSENT + 1)));
        // The owner still reads, takes controls, fires deadlines, and sends a
        // request made on it.
        let (shared, l) = (ib.shared.clone(), log.clone());
        ib.error_event()
            .connect(move |_| drop(send_one(&shared, &l, 99)));
        let l = log.clone();
        ib.timeout_event().connect(move |_| note(&l, "timeout"));
        ib.set_timeout(Some(Duration::from_millis(50)));
        client.refuse(ErrorOrigin::Session, 2104, "farm ok");
        assert!(within(|| {
            let seen = seen(&log);
            seen.contains(&"sent 99".into()) && seen.contains(&"timeout".into())
        }));
        assert!(!waiting.is_finished());
        assert_eq!(rx.try_iter().count(), UNSENT + 1);
        // Once the engine takes its work, the waiting request goes in turn.
        client
            .shared_state()
            .publish_finished(u64::try_from(UNSENT).unwrap() + 1);
        waiting.join().unwrap().unwrap();
        assert_eq!(rx.try_iter().count(), 1);
        let sent: Vec<String> = seen(&log)
            .into_iter()
            .filter(|s| s.starts_with("sent"))
            .collect();
        let want: Vec<String> = (0..UNSENT)
            .chain([99, UNSENT + 1])
            .map(|n| format!("sent {n}"))
            .collect();
        assert_eq!(sent, want);
    }

    #[test]
    fn handlers_of_two_ibs_call_each_other_inline() {
        let (a, _arx) = engine();
        let a = IB::attach(a, opts(), Clock::system()).unwrap();
        let (b, _brx) = engine();
        let b = IB::attach(b, opts(), Clock::system()).unwrap();
        let log = Log::default();
        let (l, hb) = (log.clone(), b.handle());
        a.error_event().connect(move |e| {
            note(&l, format!("a error {}", e.1));
            note(&l, format!("b closed {}", hb.disconnect().is_some()));
        });
        let (l, ha) = (log.clone(), a.handle());
        b.disconnected_event().connect(move |()| {
            let edited = ha.edit_news_ticks(Vec::clear).is_ok();
            note(&l, format!("a edited {edited}"));
        });
        session(&a.shared).refuse(ErrorOrigin::Session, 2104, "farm ok");
        assert!(within(|| seen(&log).len() == 3));
        assert_eq!(
            seen(&log),
            ["a error 2104", "a edited true", "b closed true"]
        );
        // Dropping one leaves the other served.
        drop(b);
        session(&a.shared).refuse(ErrorOrigin::Session, 2106, "hmds ok");
        assert!(within(|| seen(&log).len() == 5));
        assert_eq!(seen(&log)[3..], ["a error 2106", "b closed false"]);
    }

    #[test]
    fn dropping_an_ib_mid_close_returns_once_its_engine_has_ended() {
        let _g = lock(&GLOBAL_ERRORS);
        let (gate, held) = mpsc::channel::<()>();
        let (client, _rx) = engine_on(thread::spawn(move || {
            let _ = held.recv();
        }));
        let ib = IB::attach(client, opts(), Clock::system()).unwrap();
        let mut updates = ib.update_event().subscribe();
        // The peer closes; the closer's `disconnect` waits on the engine's
        // thread.
        session(&ib.shared).shared_state().push_closed();
        assert!(within(|| matches!(
            ib.shared.core().conn,
            Conn::Closing { client: None, .. }
        )));
        let dropper = thread::spawn(move || drop(ib));
        thread::sleep(Duration::from_millis(100));
        assert!(!dropper.is_finished());
        while updates.try_recv().is_ok() {}
        assert!(matches!(updates.try_recv(), Err(RecvError::Empty)));
        gate.send(()).unwrap();
        dropper.join().unwrap();
        assert!(matches!(updates.try_recv(), Err(RecvError::Done)));
    }

    #[test]
    fn dropping_an_ib_takes_its_logon_back_and_waits_for_the_logon_to_end() {
        let ib = IB::new().unwrap();
        let log = Log::default();
        let l = log.clone();
        ib.shared
            .events
            .api_start
            .connect(move |()| note(&l, "api_start"));
        // A logon in flight: this test posts in the logon thread's place.
        let (mut first, logon) = ib
            .begin_connect(opts(), true, Some(Via::Test(None)))
            .unwrap();
        assert!(published(&mut first).is_none());
        assert!(within(|| matches!(
            ib.shared.core().conn,
            Conn::Connecting { g: 1, .. }
        )));
        let h = ib.handle();
        let dropper = thread::spawn(move || drop(ib));
        assert!(within(|| logon.taken_back()));
        // A connect admitted after the drop starts no logon.
        let (mut late, _) = h
            .begin_connect(opts(), true, Some(Via::Test(None)))
            .unwrap();
        let mut r = None;
        assert!(within(|| {
            r = published(&mut late);
            r.is_some()
        }));
        assert!(matches!(r, Some(Err(Error::NotConnected))), "{r:?}");
        assert!(!dropper.is_finished());
        assert!(matches!(
            h.shared.core().conn,
            Conn::Connecting { g: 1, .. }
        ));
        // The logon ends with a session: a closer logs it out, and only then
        // does the drop return.
        let (client, rx) = engine();
        let post = Post::LoggedOn {
            g: 1,
            client: Arc::new(client),
        };
        assert!(h.shared.post(post).is_ok());
        dropper.join().unwrap();
        assert!(rx.try_iter().any(|c| matches!(c, ControlCommand::Shutdown)));
        assert!(matches!(
            published(&mut first),
            Some(Err(Error::Connection(_)))
        ));
        assert!(seen(&log).is_empty());
        assert!(!h.is_connected());
    }

    #[test]
    fn what_a_teardown_decides_is_seen_only_once_it_ends() {
        let _g = lock(&GLOBAL_ERRORS);
        let (client, _rx) = engine();
        let ib = IB::attach(client, opts(), Clock::system()).unwrap();
        let log = Log::default();
        // Two registrations, one polled and one waited on.
        let register_as = |id| {
            ib.shared.request(move |ib, token, reply: Reply<()>| {
                let mut c = ib.core();
                let mut x = c.requests.exec_as(ReqKey::Id(id), token);
                x.waiter = Some(Box::new(reply));
                c.requests.insert(x);
            })
        };
        let (mut polled, waited) = (register_as(7), register_as(8));
        assert!(within(|| {
            let c = ib.shared.core();
            c.requests.is_request(7) && c.requests.is_request(8)
        }));
        let (gate, held) = mpsc::channel::<()>();
        let (held, l) = (Mutex::new(held), log.clone());
        ib.disconnected_event().connect(move |()| {
            note(&l, "held");
            let _ = lock(&held).recv_timeout(Duration::from_secs(5));
        });
        let poller = thread::spawn(move || {
            loop {
                if let Some(r) = published(&mut polled) {
                    return r;
                }
                thread::sleep(Duration::from_millis(1));
            }
        });
        let waiter = thread::spawn(move || waited.wait(None));
        session(&ib.shared).shared_state().push_closed();
        assert!(within(|| seen(&log) == ["held"]));
        thread::sleep(Duration::from_millis(100));
        assert!(!poller.is_finished() && !waiter.is_finished());
        gate.send(()).unwrap();
        for r in [poller.join().unwrap(), waiter.join().unwrap()] {
            assert!(
                matches!(&r, Err(Error::Connection(m)) if m == "Socket disconnect"),
                "{r:?}"
            );
        }
    }

    #[test]
    fn run_returns_at_disconnect_and_not_at_a_peer_close() {
        let _g = lock(&GLOBAL_ERRORS);
        let (client, _rx) = engine();
        let ib = IB::attach(client, opts(), Clock::system()).unwrap();
        let (started, running) = mpsc::channel();
        let h = ib.handle();
        let runner = thread::spawn(move || {
            started.send(()).unwrap();
            h.run()
        });
        running.recv().unwrap();
        // A peer close, then a reconnect.
        session(&ib.shared).shared_state().push_closed();
        assert!(within(|| matches!(
            ib.shared.core().conn,
            Conn::Disconnected { .. }
        )));
        let (client, _rx) = engine();
        let via = Via::Test(Some(Arc::new(client)));
        let (p, _) = ib.begin_connect(opts(), true, Some(via)).unwrap();
        park_on(p).unwrap();
        assert!(ib.is_connected());
        assert!(!runner.is_finished());
        assert!(ib.disconnect().is_some());
        runner.join().unwrap().unwrap();
    }
}

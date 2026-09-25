//! `IB`'s methods beyond ib_async's API, on what the engine offers.
//!
//! Three kinds. `req_mkt_data_ex`, `req_current_time_in_millis` and
//! `req_ping` are sent by the owner, as every request is. The corporate
//! actions and the spread scan are numbered requests whose answer no callback
//! carries: the owner reads it from the session every `POLL`, from the IB's
//! deadline heap, until it arrives, an error under the request's number ends
//! it, the session ends, or its timeout passes. The rest are reads of what
//! the session holds, made on the calling thread; none of them waits.

use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::{Duration, Instant};

use jiff::Zoned;
use jiff::civil::DateTime;
use jiff::tz::TimeZone;

use super::IBHandle;
use crate::contract::{Contract, TagValue};
use crate::convert::unstated_as_nan;
use crate::engine::{self as e, EClient, HeldElsewhere, ScannedStrategy, SpreadScan};
use crate::error::{Error, Result};
use crate::event::panic_message;
use crate::live::Live;
use crate::objects::{
    AccountValue, CompetingSession, CorporateAction, OptionModel, OrderPreset, PositionElsewhere,
    TickerExtras,
};
use crate::owner::{Class, LOG_IB, Shared};
use crate::pending::{Pending, Reply, Token};
use crate::requests::{Ask, Cleanup, Exec, ReqKey};
use crate::session::Conn;
use crate::ticker::Ticker;
use crate::util::block_on;

/// `req_corporate_actions`' timeout when none is given: the engine's own.
pub const CORPORATE_ACTIONS_TIMEOUT: Duration = Duration::from_secs(15);

/// `req_spread_scan`'s timeout when none is given.
pub const SPREAD_SCAN_TIMEOUT: Duration = Duration::from_secs(10);

/// How often the owner reads an answer no callback carries.
const POLL: Duration = Duration::from_millis(50);

/// `timeout` as a method whose default is `default` takes it: `None` is the
/// default, zero no limit.
fn limit(timeout: Option<Duration>, default: Duration) -> Option<Duration> {
    match timeout {
        None => Some(default),
        Some(t) if t.is_zero() => None,
        t => t,
    }
}

/// A request id on the published session, as ib_async's `getReqId` gives
/// one: `NotConnected` unless the session is up.
fn next_id(ib: &Shared) -> Result<(i64, Arc<EClient>)> {
    let Some((_, client)) = ib.connected().filter(|(_, c)| !c.session_over()) else {
        return Err(Error::NotConnected);
    };
    let floor = client.order_id_floor();
    let id = ib.core().ids.allocate(floor, 1)?;
    Ok((id, client))
}

/// `f`, with the engine's panic logged and read as the empty value: the
/// engine does not recover its own locks, and a read never panics in its
/// caller.
fn guarded<R: Default>(f: impl FnOnce() -> R) -> R {
    catch_unwind(AssertUnwindSafe(f)).unwrap_or_else(|p| {
        log::error!(
            target: LOG_IB,
            "the session's state could not be read: {}",
            panic_message(&*p)
        );
        R::default()
    })
}

/// A read of the published session's state, on the calling thread; the
/// empty value without one.
fn session_read<R: Default>(ib: &Shared, f: impl FnOnce(&EClient) -> R) -> R {
    match ib.connected() {
        Some((_, client)) => guarded(|| f(&client)),
        None => R::default(),
    }
}

/// A read by the request that feeds `t` as `mktData` in the current session,
/// so a ticker of an ended session reads empty, never another request's.
fn ticker_read<R: Default>(ib: &Shared, t: &Live<Ticker>, f: impl FnOnce(&EClient, i64) -> R) -> R {
    let found = {
        let c = ib.core();
        match &c.conn {
            Conn::Connected { client, .. } => c
                .state
                .ticker_to_req_id
                .get("mktData")
                .and_then(|m| m.get(t))
                .map(|id| (client.clone(), *id)),
            _ => None,
        }
    };
    match found {
        Some((client, id)) => guarded(|| f(&client, id)),
        None => R::default(),
    }
}

/// Each of `series`, with what `f` reads for it.
fn by_series<V>(series: Vec<u32>, mut f: impl FnMut(u32) -> V) -> BTreeMap<u32, V> {
    series.into_iter().map(|s| (s, f(s))).collect()
}

/// The venue's GMT stamp `yyyyMMdd-HH:mm:ss`, in UTC.
fn gmt(since: &str) -> Result<Zoned> {
    DateTime::strptime("%Y%m%d-%H:%M:%S", since)
        .and_then(|t| t.to_zoned(TimeZone::UTC))
        .map_err(|_| {
            Error::Value(format!(
                "time data '{since}' does not match format '%Y%m%d-%H:%M:%S'"
            ))
        })
}

/// A numbered request whose answer the owner reads from the session.
trait Polled: Copy + Send + 'static {
    type Item: Send + 'static;
    /// What is run once it is over, unless the venue refused it or its
    /// answer ends it there.
    const WITHDRAW: Cleanup;
    /// Whether its answer ends it at the venue, leaving nothing to withdraw.
    const ANSWER_ENDS: bool;
    /// The answer, once it has arrived.
    fn answer(self, client: &EClient, id: i64) -> Option<Vec<Self::Item>>;
    /// What it gives when `limit` passes first.
    fn expired(self, id: i64, limit: Duration) -> Result<Vec<Self::Item>>;
}

#[derive(Clone, Copy)]
struct Adjustments;

impl Polled for Adjustments {
    type Item = CorporateAction;
    // The venue serves the query until it is withdrawn.
    const WITHDRAW: Cleanup = Cleanup::WithdrawAdjustments;
    const ANSWER_ENDS: bool = true;

    fn answer(self, client: &EClient, id: i64) -> Option<Vec<CorporateAction>> {
        client
            .adjustments_for(id)
            .map(|a| a.iter().map(CorporateAction::from).collect())
    }

    fn expired(self, id: i64, limit: Duration) -> Result<Vec<CorporateAction>> {
        Err(Error::Request {
            req_id: id,
            code: -1,
            message: format!("no answer within {limit:?}"),
        })
    }
}

#[derive(Clone, Copy)]
struct Scan;

impl Polled for Scan {
    type Item = ScannedStrategy;
    const WITHDRAW: Cleanup = Cleanup::WithdrawScan;
    const ANSWER_ENDS: bool = false;

    fn answer(self, client: &EClient, id: i64) -> Option<Vec<ScannedStrategy>> {
        Some(client.scanned_strategies(id)).filter(|s| !s.is_empty())
    }

    fn expired(self, _: i64, _: Duration) -> Result<Vec<ScannedStrategy>> {
        Ok(Vec::new())
    }
}

/// Sends a polled request with `send` under a fresh id, its waiter `reply`,
/// ending at `limit` after now. Given up however it is, it is withdrawn.
fn polled<P: Polled>(
    ib: &Arc<Shared>,
    token: Token,
    reply: Reply<Vec<P::Item>>,
    p: P,
    limit: Option<Duration>,
    send: impl FnOnce(&Arc<Shared>, &EClient, i64) -> Result<()>,
) {
    let sent = next_id(ib).and_then(|(id, client)| send(ib, &client, id).map(|()| id));
    let id = match sent {
        Ok(id) => id,
        Err(e) => {
            reply.send(Err(e));
            return;
        }
    };
    let deadline = limit.and_then(|t| Some((ib.clock.now().checked_add(t)?, t)));
    let mut x = ib.core().requests.exec_as(ReqKey::Id(id), token);
    x.waiter = Some(Box::new(reply));
    // An error under its id ends it with what arrived, nothing, unless
    // request errors are raised.
    x.acc = Some(Box::new(Vec::<P::Item>::new()));
    x.guard = Some(P::WITHDRAW);
    watch(ib, x, p, deadline);
    ib.queue.sent(1);
}

/// Registers `x`, to be read at the next poll or at its deadline. Not
/// `owner::arm`, which would run its withdrawal at each poll.
fn watch<P: Polled>(ib: &Arc<Shared>, mut x: Exec, p: P, deadline: Option<(Instant, Duration)>) {
    let now = ib.clock.now();
    let at = [now.checked_add(POLL), deadline.map(|d| d.0)]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(now);
    let token = x.token;
    let mut c = ib.core();
    let c = &mut *c;
    x.deadline = Some(c.heap.insert(
        at,
        Box::new(move |ib: &Arc<Shared>| {
            let x = ib.core().requests.take(token);
            if let Some(mut x) = x {
                x.deadline = None;
                poll(ib, x, p, deadline);
            }
        }),
    ));
    c.requests.insert(x);
}

/// One read of `x`'s answer: it ends with the answer, or with its expiry
/// once its deadline has come, or it is read again.
fn poll<P: Polled>(ib: &Arc<Shared>, mut x: Exec, p: P, deadline: Option<(Instant, Duration)>) {
    let id = x.origin.req_id;
    let client = ib
        .connected()
        .filter(|(g, _)| *g == x.origin.generation)
        .map(|(_, c)| c);
    let Some(client) = client else {
        x.finish(Err(Error::NotConnected));
        return;
    };
    let r = match (p.answer(&client, id), deadline) {
        (Some(v), _) => {
            if P::ANSWER_ENDS {
                x.guard = None;
            }
            Ok(v)
        }
        (None, Some((at, limit))) if ib.clock.now() >= at => p.expired(id, limit),
        (None, _) => return watch(ib, x, p, deadline),
    };
    ib.settle(&mut x);
    x.finish(r.map(|v| Box::new(v) as Box<dyn Any + Send>));
}

impl IBHandle {
    /// `req_mkt_data` with a market data type for this request alone:
    /// `reqMktDataEx`. `market_data_type` is numbered as in
    /// `req_market_data_type`: 1 live, 2 frozen, 3 delayed, 4 delayed frozen;
    /// `None` keeps the session's, and then `mkt_data_options` is taken and
    /// not sent. Any other number is `Err(Value)`. A contract holds one
    /// subscription, so cancel between two types.
    pub fn req_mkt_data_ex(
        &self,
        c: &Contract,
        generic_tick_list: &str,
        snapshot: bool,
        regulatory_snapshot: bool,
        mkt_data_options: &[TagValue],
        market_data_type: Option<i32>,
    ) -> Result<Live<Ticker>> {
        // The engine numbers them 0 live, 1 delayed, 2 frozen, 3 delayed frozen.
        let mode = market_data_type
            .map(|t| match t {
                1 => Ok(0),
                2 => Ok(2),
                3 => Ok(1),
                4 => Ok(3),
                t => Err(Error::Value(format!(
                    "marketDataType={t}: 1 live, 2 frozen, 3 delayed or 4 delayed frozen"
                ))),
            })
            .transpose()?;
        let contract = c.clone();
        let generic_tick_list = generic_tick_list.to_owned();
        let options: Vec<e::TagValue> = mkt_data_options.iter().map(e::TagValue::from).collect();
        self.shared.step(Class::Request, move |ib| {
            let (id, client) = next_id(ib)?;
            let ticker = ib.core().state.start_ticker(id, &contract, "mktData")?;
            let ec = e::Contract::from(&contract);
            match mode {
                None => {
                    client.req_mkt_data(id, &ec, &generic_tick_list, snapshot, regulatory_snapshot)
                }
                Some(mode) => client.req_mkt_data_ex(
                    id,
                    &ec,
                    &generic_tick_list,
                    snapshot,
                    regulatory_snapshot,
                    mode,
                    &options,
                ),
            }
            ib.queue.sent(1);
            Ok(ticker)
        })
    }

    /// The venue's clock in milliseconds since the epoch:
    /// `reqCurrentTimeInMillis`. The local clock corrected by the venue's, so
    /// given to the millisecond and accurate to about a second.
    pub fn req_current_time_in_millis(&self) -> Result<i64> {
        let timeout = self.request_timeout();
        self.req_current_time_in_millis_async().wait(timeout)
    }

    /// `req_current_time_in_millis`' async form:
    /// `reqCurrentTimeInMillisAsync`.
    pub fn req_current_time_in_millis_async(&self) -> Pending<i64> {
        self.shared
            .request_connected(|ib, token, reply: Reply<i64>| {
                let Some((_, client)) = ib.connected() else {
                    reply.send(Err(Error::NotConnected));
                    return;
                };
                let ask = Ask::CurrentTimeInMillis;
                let mut c = ib.core();
                let mut x = c.requests.exec_as(ask.key(), token);
                x.waiter = Some(Box::new(reply));
                let send = c.requests.ask(ask, Some(x));
                drop(c);
                if let Some(ask) = send {
                    ib.send_ask(&client, &ask);
                }
            })
    }

    /// A contract's corporate actions from `start_date` to `end_date`, days
    /// as `YYYYMMDD`: `reqCorporateActions`. The contract is named by its
    /// `con_id`. `timeout` `None` is 15 seconds, zero no limit; unanswered by
    /// then, the query is withdrawn and this fails with code -1.
    pub fn req_corporate_actions(
        &self,
        c: &Contract,
        start_date: &str,
        end_date: &str,
        timeout: Option<Duration>,
    ) -> Result<Vec<CorporateAction>> {
        let wait = self.request_timeout();
        block_on(
            self.req_corporate_actions_async(c, start_date, end_date, timeout),
            wait,
        )?
    }

    /// `req_corporate_actions`' async form: `reqCorporateActionsAsync`.
    pub async fn req_corporate_actions_async(
        &self,
        c: &Contract,
        start_date: &str,
        end_date: &str,
        timeout: Option<Duration>,
    ) -> Result<Vec<CorporateAction>> {
        let limit = limit(timeout, CORPORATE_ACTIONS_TIMEOUT);
        let (con_id, sec_type, exchange) = (c.con_id, c.sec_type.clone(), c.exchange.clone());
        let (start, end) = (start_date.to_owned(), end_date.to_owned());
        self.shared
            .request_connected(move |ib, token, reply| {
                polled(ib, token, reply, Adjustments, limit, |_, client, id| {
                    client.req_adjustments(id, con_id, &sec_type, &exchange, &start, &end);
                    Ok(())
                });
            })
            .await
    }

    /// The strategies the venue finds scanning an underlying, named by its
    /// `con_id`: `reqSpreadScan`. It subscribes, takes the first answer and
    /// cancels. `timeout` `None` is 10 seconds, zero no limit; unanswered by
    /// then, it cancels and finds nothing.
    pub fn req_spread_scan(
        &self,
        c: &Contract,
        scan: &SpreadScan,
        timeout: Option<Duration>,
    ) -> Result<Vec<ScannedStrategy>> {
        let wait = self.request_timeout();
        block_on(self.req_spread_scan_async(c, scan, timeout), wait)?
    }

    /// `req_spread_scan`'s async form: `reqSpreadScanAsync`.
    pub async fn req_spread_scan_async(
        &self,
        c: &Contract,
        scan: &SpreadScan,
        timeout: Option<Duration>,
    ) -> Result<Vec<ScannedStrategy>> {
        let limit = limit(timeout, SPREAD_SCAN_TIMEOUT);
        let (under, contract) = (c.clone(), e::Contract::from(c));
        let scan = scan.clone();
        self.shared
            .request_connected(move |ib, token, reply| {
                polled(ib, token, reply, Scan, limit, |ib, client, id| {
                    // Its quotes reach the underlying's ticker meanwhile.
                    ib.core().state.start_ticker(id, &under, "spreadScan")?;
                    client.req_spread_scan(id, &contract, &scan);
                    Ok(())
                });
            })
            .await
    }

    /// What the venue has stated for `t`'s market data request beyond
    /// `Ticker`'s fields, read now: `tickerExtras`. A series is asked for by
    /// its number in the generic tick list. Empty for a ticker no request of
    /// the current session feeds.
    pub fn ticker_extras(&self, t: &Live<Ticker>) -> TickerExtras {
        ticker_read(&self.shared, t, |c, id| {
            let (shares_outstanding, open_a_year_ago) =
                c.contract_figures(id).map_or((f64::NAN, f64::NAN), |f| {
                    (f.shares_outstanding, f.open_a_year_ago)
                });
            unstated_as_nan(TickerExtras {
                shares_outstanding,
                open_a_year_ago,
                short_sale_restricted: c.short_sale_restricted(id),
                stated_figures: by_series(c.stated_figures_series(id), |s| c.stated_figures(id, s)),
                numbered_figures: by_series(c.numbered_figures_series(id), |s| {
                    (
                        c.numbered_figures(id, s, false).into_iter().collect(),
                        c.numbered_figures(id, s, true).into_iter().collect(),
                    )
                }),
                paired_figures: by_series(c.paired_figures_series(id), |s| c.paired_figures(id, s)),
                stated_rows: by_series(c.stated_rows_series(id), |s| c.stated_rows(id, s)),
            })
        })
    }

    /// The venue's model of `t`'s option, or `None` until it states one:
    /// `optionModel`.
    pub fn option_model(&self, t: &Live<Ticker>) -> Option<OptionModel> {
        ticker_read(&self.shared, t, |c, id| {
            c.option_model(id).map(|m| OptionModel::from(&m))
        })
    }

    /// The venue's model of `t`'s option as it closed, or `None` until it
    /// states one: `closingOptionModel`.
    pub fn closing_option_model(&self, t: &Live<Ticker>) -> Option<OptionModel> {
        ticker_read(&self.shared, t, |c, id| {
            c.closing_option_model(id).map(|m| OptionModel::from(&m))
        })
    }

    /// What the venue states about `c`'s company or terms, by series, as the
    /// pairs it wrote: `companyData`. Held by the contract's `con_id` and
    /// kept after a cancel. Empty means not entitled or nothing stated.
    pub fn company_data(&self, c: &Contract) -> BTreeMap<u32, Vec<(String, String)>> {
        let Ok(con_id) = u32::try_from(c.con_id) else {
            return BTreeMap::new();
        };
        session_read(&self.shared, |e| {
            by_series(e.company_data_series(con_id), |s| e.company_data(con_id, s))
        })
    }

    /// The capability tokens the venue enabled for this account:
    /// `enabledFeatures`.
    pub fn enabled_features(&self) -> Vec<String> {
        session_read(&self.shared, EClient::enabled_features)
    }

    /// The order types the venue permits, by security type:
    /// `orderPermissions`.
    pub fn order_permissions(&self) -> HashMap<String, Vec<String>> {
        session_read(&self.shared, EClient::order_permissions)
    }

    /// The order types permitted for `sec_type`, `None` when it is not
    /// permitted: `permittedOrderTypes`. An order the account may not place
    /// comes back Inactive with no text.
    pub fn permitted_order_types(&self, sec_type: &str) -> Option<Vec<String>> {
        session_read(&self.shared, |c| c.permitted_order_types(sec_type))
    }

    /// The algorithms the venue offers, keyed `PROVIDER/SECTYPE`:
    /// `algorithms`.
    pub fn algorithms(&self) -> HashMap<String, Vec<String>> {
        session_read(&self.shared, EClient::algorithms)
    }

    /// The algorithms offered for `sec_type`, across every provider:
    /// `algorithmsFor`.
    pub fn algorithms_for(&self, sec_type: &str) -> Vec<String> {
        session_read(&self.shared, |c| c.algorithms_for(sec_type))
    }

    /// The sets of order defaults this account holds, without their values:
    /// `orderPresets`.
    pub fn order_presets(&self) -> Vec<OrderPreset> {
        session_read(&self.shared, |c| {
            c.order_presets()
                .into_iter()
                .map(|(key, version, last_changed)| OrderPreset {
                    key,
                    version,
                    last_changed,
                })
                .collect()
        })
    }

    /// Holdings the venue reports that this broker does not hold itself,
    /// kept out of `positions()`: `positionsElsewhere`.
    pub fn positions_elsewhere(&self) -> Vec<PositionElsewhere> {
        session_read(&self.shared, |c| {
            c.positions_elsewhere()
                .iter()
                .map(PositionElsewhere::from)
                .collect()
        })
    }

    /// The account figures of one of the sets `positions_elsewhere` names,
    /// for the session's account with no model code; kept out of
    /// `account_values()` and `account_value_event`:
    /// `accountValuesElsewhere`.
    pub fn account_values_elsewhere(&self, held: HeldElsewhere) -> Vec<AccountValue> {
        session_read(&self.shared, |c| {
            c.values_elsewhere(held)
                .into_iter()
                .map(|(tag, value, currency)| AccountValue {
                    account: c.account_id.clone(),
                    tag,
                    value,
                    currency,
                    model_code: String::new(),
                })
                .collect()
        })
    }

    /// Another session that held this account when this one connected:
    /// `competingSession`. Its logon time is in UTC; a stamp the venue wrote
    /// in another form is `Err(Value)`.
    pub fn competing_session(&self) -> Result<Option<CompetingSession>> {
        let Some((origin, since, read_only)) =
            session_read(&self.shared, EClient::competing_session)
        else {
            return Ok(None);
        };
        Ok(Some(CompetingSession {
            origin,
            logged_in_at: gmt(&since)?,
            read_only,
        }))
    }

    /// Measures the round trip to the venue, which `last_rtt` then reads:
    /// `reqPing`.
    pub fn req_ping(&self) -> Result<()> {
        self.shared.step(Class::Request, |ib| {
            let (_, client) = ib.connected().ok_or(Error::NotConnected)?;
            client.req_ping();
            ib.queue.sent(1);
            Ok(())
        })
    }

    /// The latest round trip to the venue, `None` before any: `lastRtt`.
    pub fn last_rtt(&self) -> Option<Duration> {
        session_read(&self.shared, EClient::last_rtt)
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::mpsc;
    use std::task::{Context, Poll, Waker};
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    use jiff::Timestamp;

    use super::*;
    use crate::engine::{Adjustment, AdjustmentKind, ControlCommand, ErrorOrigin, SharedState};
    use crate::event::set_on_owner;
    use crate::ib::{ConnectOptions, IBConfig, StartupFetch};
    use crate::objects::IBDefaults;
    use crate::owner::Via;
    use crate::record::{Callback, Capture};
    use crate::timer::Clock;

    /// A session on an engine whose loop never runs, driven by this thread
    /// as its owner: the test laps it on a manual clock, and the engine's
    /// commands arrive on `sent`.
    struct Harness {
        ib: Arc<Shared>,
        capture: Capture,
        engine: Arc<SharedState>,
        sent: mpsc::Receiver<ControlCommand>,
    }

    impl Harness {
        fn new() -> Self {
            set_on_owner(true);
            let (tx, sent) = mpsc::channel();
            let engine = Arc::new(SharedState::new());
            let client =
                EClient::from_parts(engine.clone(), tx, thread::spawn(|| {}), "DU123".into());
            let mut h = Harness {
                ib: Shared::new(
                    IBDefaults::default(),
                    IBConfig::default(),
                    Clock::manual(Timestamp::UNIX_EPOCH),
                ),
                capture: Capture::new(TimeZone::UTC),
                engine,
                sent,
            };
            let opts = ConnectOptions {
                fetch_fields: StartupFetch::NONE,
                ..ConnectOptions::default()
            };
            let via = Via::Test(Some(Arc::new(client)));
            let (mut p, _) = h.handle().begin_connect(opts, true, Some(via)).unwrap();
            h.lap();
            assert!(matches!(ready(&mut p), Some(Ok(()))));
            h.sent();
            h
        }

        fn handle(&self) -> IBHandle {
            IBHandle {
                shared: self.ib.clone(),
            }
        }

        fn lap(&mut self) {
            self.ib.lap(&mut self.capture);
        }

        /// Moves the IB's clock on by `by`, then laps.
        fn after(&mut self, by: Duration) {
            self.ib.clock.advance(by);
            self.lap();
        }

        /// What the engine has been sent since last asked.
        fn sent(&self) -> Vec<ControlCommand> {
            self.sent.try_iter().collect()
        }

        fn session(&self) -> Arc<EClient> {
            self.ib.connected().unwrap().1
        }
    }

    impl Drop for Harness {
        fn drop(&mut self) {
            set_on_owner(false);
        }
    }

    fn ready<F: Future + Unpin>(f: &mut F) -> Option<F::Output> {
        match Pin::new(f).poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(r) => Some(r),
            Poll::Pending => None,
        }
    }

    fn stock(symbol: &str, con_id: i64) -> Contract {
        Contract {
            con_id,
            ..Contract::stock(symbol, "SMART", "USD")
        }
    }

    #[test]
    fn req_mkt_data_ex_asks_under_the_type_it_names() {
        let h = Harness::new();
        let ib = h.handle();
        let spy = stock("SPY", 756733);
        // The session's own type, frozen, is what a request naming none
        // asks under.
        h.session().req_market_data_type(2);
        // ib_async's 1 live, 2 frozen, 3 delayed and 4 delayed frozen are
        // the engine's 0, 2, 1 and 3.
        for (named, mode) in [
            (Some(1), 0),
            (Some(2), 2),
            (Some(3), 1),
            (Some(4), 3),
            (None, 2),
        ] {
            let t = ib
                .req_mkt_data_ex(&spy, "", false, false, &[], named)
                .unwrap();
            let sent = h.sent();
            let [
                ControlCommand::Subscribe {
                    req_id, mode_9887, ..
                },
            ] = sent.as_slice()
            else {
                panic!("{named:?}: {sent:?}");
            };
            assert_eq!(*mode_9887, mode, "{named:?}");
            // Fed as the contract's `mktData`, where `cancel_mkt_data` and
            // the ticker's extras find it.
            let fed = h.ib.core().state.ticker_to_req_id["mktData"]
                .get(&t)
                .copied();
            assert_eq!(fed, Some(*req_id), "{named:?}");
        }
        let other = ib.req_mkt_data_ex(&spy, "", false, false, &[], Some(5));
        assert!(matches!(other, Err(Error::Value(_))), "{other:?}");
        assert!(h.sent().is_empty());
    }

    #[test]
    fn corporate_actions_end_on_their_answer_their_refusal_or_their_timeout() {
        let mut h = Harness::new();
        let ib = h.handle();
        let aapl = stock("AAPL", 265598);
        let fetched = |sent: Vec<ControlCommand>| match sent.as_slice() {
            [ControlCommand::FetchAdjustments { req_id, .. }] => *req_id,
            other => panic!("{other:?}"),
        };

        // The owner reads the answer the engine files, a poll at a time.
        let mut answered =
            Box::pin(ib.req_corporate_actions_async(&aapl, "20200101", "20201231", None));
        assert!(ready(&mut answered).is_none());
        let id = fetched(h.sent());
        h.after(POLL);
        assert!(ready(&mut answered).is_none());
        let split = Adjustment {
            kind: Some(AdjustmentKind::Split),
            date: "20200831".into(),
            value: "4".into(),
            ..Adjustment::default()
        };
        let reference = &h.engine.reference;
        reference.expect_adjustments(id);
        let mut about = reference
            .adjustments_for("")
            .map(|a| a.0)
            .unwrap_or_default();
        about.con_id = "265598".into();
        reference.note_adjustments(about, vec![split], id);
        h.after(POLL);
        let actions = ready(&mut answered).unwrap().unwrap();
        assert_eq!(
            actions
                .iter()
                .map(|a| (a.kind.as_str(), a.date.as_str(), a.value.as_str()))
                .collect::<Vec<_>>(),
            [("SS", "20200831", "4")]
        );
        assert!(h.sent().is_empty());

        // Unanswered at the default 15 s: the query is withdrawn, and no
        // answer came.
        let mut unanswered =
            Box::pin(ib.req_corporate_actions_async(&aapl, "20210101", "20211231", None));
        assert!(ready(&mut unanswered).is_none());
        let id = fetched(h.sent());
        h.after(CORPORATE_ACTIONS_TIMEOUT - POLL);
        assert!(ready(&mut unanswered).is_none());
        h.after(POLL);
        assert!(matches!(
            h.sent().as_slice(),
            [ControlCommand::CancelCorporateActions { req_id }] if *req_id == id
        ));
        match ready(&mut unanswered) {
            Some(Err(Error::Request {
                req_id,
                code: -1,
                message,
            })) => {
                assert_eq!(req_id, i64::from(id));
                assert_eq!(message, "no answer within 15s");
            }
            other => panic!("{other:?}"),
        }

        // A contract without its id is refused under the request's number,
        // which ends it with what arrived; nothing is withdrawn afterwards.
        let unnamed = Contract::stock("AAPL", "SMART", "USD");
        let mut refused = Box::pin(ib.req_corporate_actions_async(&unnamed, "", "", None));
        assert!(ready(&mut refused).is_none());
        h.lap();
        assert!(matches!(ready(&mut refused), Some(Ok(v)) if v.is_empty()));
        h.after(CORPORATE_ACTIONS_TIMEOUT);
        assert!(h.sent().is_empty());

        // Given up before its answer, as a blocking face's timeout gives it
        // up too: the query is withdrawn.
        let mut dropped =
            Box::pin(ib.req_corporate_actions_async(&aapl, "20220101", "20221231", None));
        assert!(ready(&mut dropped).is_none());
        let id = fetched(h.sent());
        drop(dropped);
        h.lap();
        assert!(matches!(
            h.sent().as_slice(),
            [ControlCommand::CancelCorporateActions { req_id }] if *req_id == id
        ));
    }

    #[test]
    fn a_spread_scan_feeds_its_ticker_and_is_cancelled_however_it_ends_unless_refused() {
        let mut h = Harness::new();
        let (ib, spy, all) = (h.handle(), stock("SPY", 756733), SpreadScan::default());
        let g = h.ib.connected().unwrap().0;
        let subscribed = |sent: Vec<ControlCommand>| match sent.as_slice() {
            [
                ControlCommand::Subscribe {
                    req_id,
                    spread_scan: Some(_),
                    ..
                },
            ] => *req_id,
            other => panic!("{other:?}"),
        };
        let cancelled = |sent: Vec<ControlCommand>, id| {
            let [ControlCommand::CancelMktData { req_id }] = sent.as_slice() else {
                return false;
            };
            *req_id == id
        };
        let mut scan = Box::pin(ib.req_spread_scan_async(&spy, &all, None));
        assert!(ready(&mut scan).is_none());
        let id = subscribed(h.sent());

        // The subscription's quotes reach the underlying's ticker.
        let last = Callback::TickPrice {
            req_id: id,
            tick_type: 4,
            price: 1.5,
        };
        h.ib.apply_read(g, vec![last]);
        assert_eq!(ib.ticker(&spy).unwrap().unwrap().read().last, 1.5);

        // Unanswered at its timeout: cancelled, and it found nothing.
        h.after(SPREAD_SCAN_TIMEOUT - POLL);
        assert!(ready(&mut scan).is_none());
        h.after(POLL);
        assert!(cancelled(h.sent(), id));
        assert!(matches!(ready(&mut scan), Some(Ok(v)) if v.is_empty()));

        // Given up before its answer: cancelled.
        let mut scan = Box::pin(ib.req_spread_scan_async(&spy, &all, None));
        assert!(ready(&mut scan).is_none());
        let id = subscribed(h.sent());
        drop(scan);
        h.lap();
        assert!(cancelled(h.sent(), id));

        // Refused: there is nothing to cancel.
        let mut scan = Box::pin(ib.req_spread_scan_async(&spy, &all, None));
        assert!(ready(&mut scan).is_none());
        let id = subscribed(h.sent());
        let refused = Callback::Error {
            origin: ErrorOrigin::Request { id, ends: true },
            code: 321,
            message: "refused".into(),
            advanced_order_reject_json: String::new(),
        };
        h.ib.apply_read(g, vec![refused]);
        assert!(matches!(ready(&mut scan), Some(Ok(v)) if v.is_empty()));
        h.after(SPREAD_SCAN_TIMEOUT);
        assert!(h.sent().is_empty());
    }

    #[test]
    fn req_current_time_in_millis_answers_the_venues_clock_in_milliseconds() {
        let mut h = Harness::new();
        let mut asked = h.handle().req_current_time_in_millis_async();
        h.lap();
        let ms = ready(&mut asked).unwrap().unwrap();
        let now = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap();
        assert!((now - 5_000..=now).contains(&ms), "{ms} against {now}");
    }

    #[test]
    fn session_reads_state_what_the_engine_holds_as_the_extras_type_it() {
        let h = Harness::new();
        let ib = h.handle();

        // Another session's logon, stamped in GMT as the venue writes it.
        let reference = &h.engine.reference;
        reference.set_competing_session(Some((
            "10.0.0.1".into(),
            "20260925-13:14:15".into(),
            true,
        )));
        let other = ib.competing_session().unwrap().unwrap();
        let utc: Zoned = "2026-09-25T13:14:15+00:00[UTC]".parse().unwrap();
        assert_eq!(other.logged_in_at, utc);
        assert_eq!((other.origin.as_str(), other.read_only), ("10.0.0.1", true));
        reference.set_competing_session(Some(("10.0.0.1".into(), "2026-09-25".into(), false)));
        assert!(matches!(ib.competing_session(), Err(Error::Value(_))));

        // A figure of holdings held elsewhere is the session account's,
        // under no model.
        h.engine.portfolio.set_value_elsewhere(
            HeldElsewhere::Away,
            "NetLiquidation".into(),
            "1000".into(),
            "USD".into(),
        );
        assert_eq!(
            ib.account_values_elsewhere(HeldElsewhere::Away),
            [AccountValue {
                account: "DU123".into(),
                tag: "NetLiquidation".into(),
                value: "1000".into(),
                currency: "USD".into(),
                model_code: String::new(),
            }]
        );

        // Company data is held by the venue's 32-bit id: a wider one names
        // no contract, not the one its low bits do.
        reference.note_company_data(5, 700, vec![("k".into(), "v".into())]);
        let data = BTreeMap::from([(700, vec![("k".to_owned(), "v".to_owned())])]);
        assert_eq!(ib.company_data(&stock("X", 5)), data);
        assert!(ib.company_data(&stock("X", (1 << 32) + 5)).is_empty());
    }
}

//! `IB`'s market data methods.

use std::any::Any;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::task::Poll;
use std::time::Duration;

use super::{IBHandle, unlimited_at_zero};
use crate::contract::{Contract, TagValue};
use crate::engine::{self as e, EClient};
use crate::error::{Error, Result};
use crate::live::{Holder, Live};
use crate::objects::{
    DepthMktDataDescription, OptionComputation, PriceIncrement, RealTimeBarList, SmartComponent,
};
use crate::owner::{Class, Entry, LOG_IB, Shared, arm};
use crate::pending::{Pending, Registration, Reply, Token};
use crate::requests::{Ask, Cleanup, Exec, ReqKey, Route, Waiter, fresh_token};
use crate::state::Bars;
use crate::ticker::Ticker;
use crate::util::block_on;

/// How long `calculate_implied_volatility` and `calculate_option_price` wait
/// for their answer (ib:2512, 2533).
const CALCULATION_TIMEOUT: Duration = Duration::from_secs(4);
/// How long `req_market_rule` waits for its answer (ib:2319).
const MARKET_RULE_TIMEOUT: Duration = Duration::from_secs(1);

/// The session as ib_async's `getReqId` and `send` find it ready, or
/// `NotConnected` as they raise.
fn ready(ib: &Shared) -> Result<Arc<EClient>> {
    match ib.connected() {
        Some((_, client)) if !client.session_over() => Ok(client),
        _ => Err(Error::NotConnected),
    }
}

/// A new request id: ib_async's `getReqId`, clear of the engine's floor.
fn next_id(ib: &Shared, client: &EClient) -> Result<i64> {
    let floor = client.order_id_floor();
    ib.core().ids.allocate(floor, 1)
}

/// What a method's own deadline completes its waiter with.
struct TimedOut;

/// The waiter of a method that gives `None` when its own deadline passes,
/// as ib_async's `wait_for` there returns `None` on its timeout.
struct Within<T: Send + 'static> {
    reply: Reply<Option<T>>,
    /// The request whose contract is forgotten at its end, as `_endReq`
    /// forgets it (wr:390).
    forget: Option<(Weak<Shared>, i64)>,
}

impl<T: Send + 'static> Waiter for Within<T> {
    fn finish(self: Box<Self>, r: Result<Box<dyn Any + Send>>) -> bool {
        let Within { reply, forget } = *self;
        let r = match r {
            Ok(v) if v.is::<TimedOut>() => return reply.send(Ok(None)),
            Ok(v) => v.downcast::<T>().map(|v| Some(*v)).map_err(|_| {
                Error::Value(format!(
                    "an answer that is not {}",
                    std::any::type_name::<T>()
                ))
            }),
            Err(e) => Err(e),
        };
        if let Some((ib, id)) = forget
            && let Some(ib) = ib.upgrade()
        {
            ib.core().state.req_id_to_contract.remove(&id);
        }
        reply.send(r)
    }
}

/// Asks the question `ask` for the execution `token`, with `waiter`, and
/// sends it now if its lane is free. `within` is the method's own
/// deadline, and the name its timeout is logged under.
fn question(
    ib: &Arc<Shared>,
    client: &EClient,
    ask: Ask,
    token: Token,
    waiter: Box<dyn Waiter>,
    within: Option<(Duration, &'static str)>,
) {
    let at = within.and_then(|(d, what)| Some((ib.clock.now().checked_add(d)?, what)));
    let mut core = ib.core();
    let c = &mut *core;
    let mut x = c.requests.exec_as(ask.key(), token);
    x.waiter = Some(waiter);
    if let Some((at, what)) = at {
        arm(&mut c.heap, &mut x, at, move |_, x| timed_out(what, x));
    }
    let now = c.requests.ask(ask, Some(x));
    drop(core);
    if let Some(a) = now {
        ib.send_ask(client, &a);
    }
}

/// A method's own deadline passed first: ib_async logs it and gives `None`.
fn timed_out(what: &str, x: Exec) {
    log::error!(target: LOG_IB, "{what}: Timeout");
    x.finish(Ok(Box::new(TimedOut)));
}

/// Waits for every part, failing at the first that fails, as
/// `asyncio.gather` does.
async fn all<T>(mut parts: Vec<Pending<T>>) -> Result<Vec<T>>
where
    T: Send + 'static,
{
    let mut done: Vec<Option<T>> = parts.iter().map(|_| None).collect();
    poll_fn(|cx| {
        let mut waiting = false;
        for (p, d) in parts.iter_mut().zip(done.iter_mut()) {
            if d.is_some() {
                continue;
            }
            match Pin::new(p).poll(cx) {
                Poll::Ready(Ok(v)) => *d = Some(v),
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => waiting = true,
            }
        }
        if waiting {
            Poll::Pending
        } else {
            Poll::Ready(Ok(done.iter_mut().filter_map(Option::take).collect()))
        }
    })
    .await
}

/// `c`'s ticker, and the id that fed it under `tick_type`, now ended:
/// ib_async's `endTicker` of `ticker(contract)`, 0 when none did. An
/// unhashable contract is `Err(Value)`, as `hash(contract)` raises.
fn end_ticker(ib: &Shared, c: &Contract, tick_type: &str) -> Result<(Option<Live<Ticker>>, i64)> {
    let key = c.ticker_key()?;
    let mut core = ib.core();
    let s = &mut core.state;
    Ok(match s.tickers.get(&key).cloned() {
        Some(t) => {
            let id = s.end_ticker(&t, tick_type);
            (Some(t), id)
        }
        None => (None, 0),
    })
}

fn clear_dom(ticker: &Live<Ticker>) {
    ticker.update(|t| {
        t.dom_bids.clear();
        t.dom_asks.clear();
        t.dom_bids_dict.clear();
        t.dom_asks_dict.clear();
    });
}

/// `req_tickers`' owner step: one snapshot per contract, in order, each
/// its own request, ending its ticker however it ends (ib:2182-2202).
fn snapshots(
    ib: &Arc<Shared>,
    contracts: Vec<Contract>,
    parts: Vec<(Token, Reply<Live<Ticker>>)>,
    regulatory_snapshot: bool,
) {
    let mut gone = Vec::new();
    for (c, (token, reply)) in contracts.iter().zip(parts) {
        if !reply.start() {
            // Its waiter left before this ran: sent as ib_async sent it,
            // then retired.
            gone.push(token);
        }
        let started = ready(ib).and_then(|client| {
            let id = next_id(ib, &client)?;
            let ticker = ib.core().state.start_ticker(id, c, "snapshot")?;
            Ok((client, id, ticker))
        });
        let (client, id, ticker) = match started {
            Ok(s) => s,
            Err(e) => {
                reply.send(Err(e));
                break;
            }
        };
        {
            let mut core = ib.core();
            let mut x = core.requests.exec_as(ReqKey::Id(id), token);
            x.waiter = Some(Box::new(reply));
            // Its answer is the ticker, so an error ends it with the ticker
            // unless errors raise.
            x.acc = Some(Box::new(ticker));
            x.route = Route::Ticker;
            x.guard = Some(Cleanup::EndSnapshot);
            core.requests.insert(x);
        }
        client.req_mkt_data(id, &e::Contract::from(c), "", true, regulatory_snapshot);
        ib.queue.sent(1);
    }
    ib.retire(gone);
}

/// A calculation of the implied volatility, or else of the option's
/// price, under its own 4 second deadline, cancelled however it ends
/// (ib:2499-2539).
fn calculate(
    ib: &IBHandle,
    c: &Contract,
    volatility: bool,
    given: f64,
    under_price: f64,
) -> Pending<Option<OptionComputation>> {
    let c = c.clone();
    ib.shared
        .request(move |ib, token, reply: Reply<Option<OptionComputation>>| {
            let started = ready(ib).and_then(|client| Ok((next_id(ib, &client)?, client)));
            let (id, client) = match started {
                Ok(s) => s,
                Err(e) => {
                    reply.send(Err(e));
                    return;
                }
            };
            let (guard, what) = if volatility {
                (
                    Cleanup::CancelImpliedVolatility,
                    "calculateImpliedVolatilityAsync",
                )
            } else {
                (Cleanup::CancelOptionPrice, "calculateOptionPriceAsync")
            };
            let at = ib.clock.now().checked_add(CALCULATION_TIMEOUT);
            {
                let mut core = ib.core();
                let core = &mut *core;
                core.state.req_id_to_contract.insert(id, c.clone());
                let mut x = core.requests.exec_as(ReqKey::Id(id), token);
                x.waiter = Some(Box::new(Within {
                    reply,
                    forget: Some((Arc::downgrade(ib), id)),
                }));
                x.guard = Some(guard);
                if let Some(at) = at {
                    arm(&mut core.heap, &mut x, at, move |_, x| timed_out(what, x));
                }
                core.requests.insert(x);
            }
            let contract = e::Contract::from(&c);
            if volatility {
                client.calculate_implied_volatility(id, &contract, given, under_price);
            } else {
                client.calculate_option_price(id, &contract, given, under_price);
            }
            ib.queue.sent(1);
        })
}

/// The blocking faces' bound: `IBConfig.request_timeout`, zero being none.
fn bound(ib: &IBHandle) -> Option<Duration> {
    unlimited_at_zero(ib.config().request_timeout)
}

impl IBHandle {
    /// Subscribes to `c`'s ticks, or asks for one snapshot of them, and
    /// gives the ticker they fill: ib_async's `reqMktData`.
    ///
    /// `mkt_data_options` is taken and not sent: the TWS API reserves it.
    /// An unhashable contract is `Err(Value)`.
    pub fn req_mkt_data(
        &self,
        c: &Contract,
        generic_tick_list: &str,
        snapshot: bool,
        regulatory_snapshot: bool,
        mkt_data_options: &[TagValue],
    ) -> Result<Live<Ticker>> {
        let _ = mkt_data_options;
        let (c, ticks) = (c.clone(), generic_tick_list.to_owned());
        self.shared.step(Class::Request, move |ib| {
            let client = ready(ib)?;
            let id = next_id(ib, &client)?;
            let ticker = ib.core().state.start_ticker(id, &c, "mktData")?;
            client.req_mkt_data(
                id,
                &e::Contract::from(&c),
                &ticks,
                snapshot,
                regulatory_snapshot,
            );
            ib.queue.sent(1);
            Ok(ticker)
        })
    }

    /// Ends the subscription `req_mkt_data` made for `c`: ib_async's
    /// `cancelMktData`. `false`, logged, when there is none.
    pub fn cancel_mkt_data(&self, c: &Contract) -> Result<bool> {
        let c = c.clone();
        self.shared.step(Class::Control, move |ib| {
            let (_, id) = end_ticker(ib, &c, "mktData")?;
            if id == 0 {
                log::error!(target: LOG_IB, "cancelMktData: No reqId found for contract {c:?}");
                return Ok(false);
            }
            ready(ib)?.cancel_mkt_data(id);
            Ok(true)
        })
    }

    /// A snapshot of each contract, given once every one has ended:
    /// ib_async's `reqTickers`. Each ticker stops being a snapshot's
    /// however its request ends, an error's included. An error fails the
    /// call only when `IBConfig.raise_request_errors` is set.
    pub fn req_tickers(
        &self,
        contracts: &[Contract],
        regulatory_snapshot: bool,
    ) -> Result<Vec<Live<Ticker>>> {
        let timeout = bound(self);
        block_on(
            self.req_tickers_async(contracts, regulatory_snapshot),
            timeout,
        )?
    }

    /// `req_tickers`' async form: ib_async's `reqTickersAsync`.
    pub async fn req_tickers_async(
        &self,
        contracts: &[Contract],
        regulatory_snapshot: bool,
    ) -> Result<Vec<Live<Ticker>>> {
        if contracts.is_empty() {
            return Ok(Vec::new());
        }
        let mut waiters = Vec::with_capacity(contracts.len());
        let mut parts = Vec::with_capacity(contracts.len());
        for _ in contracts {
            let token = fresh_token();
            let reg = Registration::new(self.shared.abandoner(), token);
            let (p, reply) = Pending::new(Some(reg));
            waiters.push(p);
            parts.push((token, reply));
        }
        let contracts = contracts.to_vec();
        let entry = Entry::step(Class::Request, move |ib| {
            snapshots(ib, contracts, parts, regulatory_snapshot);
        });
        if let Some(first) = waiters.first_mut() {
            self.shared.hold(first, entry);
        }
        all(waiters).await
    }

    /// Subscribes to `c`'s tick-by-tick data of `tick_type` (`"Last"`,
    /// `"AllLast"`, `"BidAsk"` or `"MidPoint"`) and gives the ticker whose
    /// `tick_by_ticks` it fills: ib_async's `reqTickByTickData`.
    pub fn req_tick_by_tick_data(
        &self,
        c: &Contract,
        tick_type: &str,
        number_of_ticks: i32,
        ignore_size: bool,
    ) -> Result<Live<Ticker>> {
        let (c, tick_type) = (c.clone(), tick_type.to_owned());
        self.shared.step(Class::Request, move |ib| {
            let client = ready(ib)?;
            let id = next_id(ib, &client)?;
            let ticker = ib.core().state.start_ticker(id, &c, &tick_type)?;
            client.req_tick_by_tick_data(
                id,
                &e::Contract::from(&c),
                &tick_type,
                number_of_ticks,
                ignore_size,
            );
            ib.queue.sent(1);
            Ok(ticker)
        })
    }

    /// Ends `c`'s tick-by-tick subscription of `tick_type`: ib_async's
    /// `cancelTickByTickData`. `false`, logged, when there is none.
    pub fn cancel_tick_by_tick_data(&self, c: &Contract, tick_type: &str) -> Result<bool> {
        let (c, tick_type) = (c.clone(), tick_type.to_owned());
        self.shared.step(Class::Control, move |ib| {
            let (_, id) = end_ticker(ib, &c, &tick_type)?;
            if id == 0 {
                // ib_async's own text (ib:1495).
                log::error!(target: LOG_IB, "cancelMktData: No reqId found for contract {c:?}");
                return Ok(false);
            }
            ready(ib)?.cancel_tick_by_tick_data(id);
            Ok(true)
        })
    }

    /// Subscribes to `c`'s order book and gives the ticker whose `dom_bids`,
    /// `dom_asks` and `dom_ticks` it fills, its book cleared first:
    /// ib_async's `reqMktDepth`. `mkt_depth_options` is taken and not sent:
    /// the TWS API reserves it.
    pub fn req_mkt_depth(
        &self,
        c: &Contract,
        num_rows: i32,
        is_smart_depth: bool,
        mkt_depth_options: &[TagValue],
    ) -> Result<Live<Ticker>> {
        let _ = mkt_depth_options;
        let c = c.clone();
        self.shared.step(Class::Request, move |ib| {
            let client = ready(ib)?;
            let id = next_id(ib, &client)?;
            let ticker = ib.core().state.start_ticker(id, &c, "mktDepth")?;
            clear_dom(&ticker);
            client.req_mkt_depth(id, &e::Contract::from(&c), num_rows, is_smart_depth);
            ib.queue.sent(1);
            Ok(ticker)
        })
    }

    /// Ends `c`'s order book subscription and clears the ticker's book:
    /// ib_async's `cancelMktDepth`; logged when there is none. The session
    /// knows which kind of book it opened, so `is_smart_depth` is taken and
    /// not needed.
    pub fn cancel_mkt_depth(&self, c: &Contract, is_smart_depth: bool) -> Result<()> {
        let _ = is_smart_depth;
        let c = c.clone();
        self.shared.step(Class::Control, move |ib| {
            match end_ticker(ib, &c, "mktDepth")? {
                (Some(ticker), id) if id != 0 => {
                    ready(ib)?.cancel_mkt_depth(id);
                    clear_dom(&ticker);
                }
                _ => {
                    log::error!(target: LOG_IB, "cancelMktDepth: No reqId found for contract {c:?}")
                }
            }
            Ok(())
        })
    }

    /// The exchanges whose books name their market makers: ib_async's
    /// `reqMktDepthExchanges`.
    pub fn req_mkt_depth_exchanges(&self) -> Result<Vec<DepthMktDataDescription>> {
        self.req_mkt_depth_exchanges_async().wait(bound(self))
    }

    /// `req_mkt_depth_exchanges`' async form: ib_async's
    /// `reqMktDepthExchangesAsync`.
    pub fn req_mkt_depth_exchanges_async(&self) -> Pending<Vec<DepthMktDataDescription>> {
        self.shared
            .request(
                |ib, token, reply: Reply<Vec<DepthMktDataDescription>>| match ready(ib) {
                    Ok(client) => question(
                        ib,
                        &client,
                        Ask::MktDepthExchanges,
                        token,
                        Box::new(reply),
                        None,
                    ),
                    Err(e) => {
                        reply.send(Err(e));
                    }
                },
            )
    }

    /// What each one-letter exchange code of `bbo_exchange`'s quotes names:
    /// ib_async's `reqSmartComponents`.
    pub fn req_smart_components(&self, bbo_exchange: &str) -> Result<Vec<SmartComponent>> {
        self.req_smart_components_async(bbo_exchange)
            .wait(bound(self))
    }

    /// `req_smart_components`' async form: ib_async's
    /// `reqSmartComponentsAsync`.
    pub fn req_smart_components_async(&self, bbo_exchange: &str) -> Pending<Vec<SmartComponent>> {
        let bbo = bbo_exchange.to_owned();
        self.shared
            .request(move |ib, token, reply: Reply<Vec<SmartComponent>>| {
                let started = ready(ib).and_then(|client| Ok((next_id(ib, &client)?, client)));
                let (id, client) = match started {
                    Ok(s) => s,
                    Err(e) => {
                        reply.send(Err(e));
                        return;
                    }
                };
                {
                    let mut core = ib.core();
                    let mut x = core.requests.exec_as(ReqKey::Id(id), token);
                    x.waiter = Some(Box::new(reply));
                    x.acc = Some(Box::new(Vec::<SmartComponent>::new()));
                    core.requests.insert(x);
                }
                client.req_smart_components(id, &bbo);
                ib.queue.sent(1);
            })
    }

    /// Subscribes to `c`'s five-second bars and gives the list they fill:
    /// ib_async's `reqRealTimeBars`. Every bar is five seconds, whatever
    /// `bar_size` says, as on a gateway; `real_time_bars_options` is kept
    /// on the list and not sent: the TWS API reserves it.
    pub fn req_real_time_bars(
        &self,
        c: &Contract,
        bar_size: i32,
        what_to_show: &str,
        use_rth: bool,
        real_time_bars_options: &[TagValue],
    ) -> Result<Live<RealTimeBarList>> {
        let (c, what) = (c.clone(), what_to_show.to_owned());
        let options = real_time_bars_options.to_vec();
        self.shared.step(Class::Request, move |ib| {
            let client = ready(ib)?;
            let id = next_id(ib, &client)?;
            let list = Live::new(RealTimeBarList {
                req_id: id,
                contract: c.clone(),
                bar_size,
                what_to_show: what.clone(),
                use_rth,
                real_time_bars_options: options,
                ..RealTimeBarList::default()
            });
            let ib_weak: Weak<Shared> = Arc::downgrade(ib);
            list.bind(ib_weak as Weak<dyn Holder>);
            {
                let mut core = ib.core();
                core.state.req_id_to_contract.insert(id, c.clone());
                core.state
                    .req_id_to_subscriber
                    .insert(id, Bars::RealTime(list.clone()));
            }
            client.req_real_time_bars(id, &e::Contract::from(&c), bar_size, &what, use_rth);
            ib.queue.sent(1);
            Ok(list)
        })
    }

    /// Ends the subscription that fills `bars`: ib_async's
    /// `cancelRealTimeBars`. A list this IB no longer keeps up to date, an
    /// earlier session's or another IB's, is left alone.
    pub fn cancel_real_time_bars(&self, bars: &Live<RealTimeBarList>) -> Result<()> {
        let bars = bars.clone();
        self.shared.step(Class::Control, move |ib| {
            let client = ready(ib)?;
            let id = bars.read().req_id;
            let mut core = ib.core();
            let s = &mut core.state;
            if !matches!(s.req_id_to_subscriber.get(&id), Some(Bars::RealTime(l)) if Live::ptr_eq(l, &bars))
            {
                return Ok(());
            }
            s.req_id_to_contract.remove(&id);
            let ended = s.req_id_to_subscriber.shift_remove(&id);
            drop(core);
            drop(ended);
            client.cancel_real_time_bars(id);
            Ok(())
        })
    }

    /// The volatility `option_price` implies for the option `c` with its
    /// underlying at `under_price`: ib_async's
    /// `calculateImpliedVolatility`. `None` when no answer comes within 4
    /// seconds. The session computes it from the venue's own model for `c`,
    /// and `impl_vol_options` is taken and not sent.
    pub fn calculate_implied_volatility(
        &self,
        c: &Contract,
        option_price: f64,
        under_price: f64,
        impl_vol_options: &[TagValue],
    ) -> Result<Option<OptionComputation>> {
        let timeout = bound(self);
        let f =
            self.calculate_implied_volatility_async(c, option_price, under_price, impl_vol_options);
        block_on(f, timeout)?
    }

    /// `calculate_implied_volatility`'s async form: ib_async's
    /// `calculateImpliedVolatilityAsync`. The calculation is cancelled
    /// however it ends, dropping the future included.
    pub async fn calculate_implied_volatility_async(
        &self,
        c: &Contract,
        option_price: f64,
        under_price: f64,
        impl_vol_options: &[TagValue],
    ) -> Result<Option<OptionComputation>> {
        let _ = impl_vol_options;
        calculate(self, c, true, option_price, under_price).await
    }

    /// The price `volatility` implies for the option `c` with its
    /// underlying at `under_price`: ib_async's `calculateOptionPrice`.
    /// `None` when no answer comes within 4 seconds. The session computes
    /// it from the venue's own model for `c`, and `opt_prc_options` is
    /// taken and not sent.
    pub fn calculate_option_price(
        &self,
        c: &Contract,
        volatility: f64,
        under_price: f64,
        opt_prc_options: &[TagValue],
    ) -> Result<Option<OptionComputation>> {
        let timeout = bound(self);
        let f = self.calculate_option_price_async(c, volatility, under_price, opt_prc_options);
        block_on(f, timeout)?
    }

    /// `calculate_option_price`'s async form: ib_async's
    /// `calculateOptionPriceAsync`. The calculation is cancelled however it
    /// ends, dropping the future included.
    pub async fn calculate_option_price_async(
        &self,
        c: &Contract,
        volatility: f64,
        under_price: f64,
        opt_prc_options: &[TagValue],
    ) -> Result<Option<OptionComputation>> {
        let _ = opt_prc_options;
        calculate(self, c, false, volatility, under_price).await
    }

    /// The price increments of the market rule `market_rule_id`: ib_async's
    /// `reqMarketRule`. `None` when no answer comes within a second: a rule
    /// the session has not seen with a contract's details is refused on
    /// `error_event`, and then gives `None`.
    pub fn req_market_rule(&self, market_rule_id: i32) -> Result<Option<Vec<PriceIncrement>>> {
        let timeout = bound(self);
        block_on(self.req_market_rule_async(market_rule_id), timeout)?
    }

    /// `req_market_rule`'s async form: ib_async's `reqMarketRuleAsync`.
    pub async fn req_market_rule_async(
        &self,
        market_rule_id: i32,
    ) -> Result<Option<Vec<PriceIncrement>>> {
        self.shared
            .request(
                move |ib, token, reply: Reply<Option<Vec<PriceIncrement>>>| match ready(ib) {
                    Ok(client) => {
                        let waiter = Box::new(Within {
                            reply,
                            forget: None,
                        });
                        let within = Some((MARKET_RULE_TIMEOUT, "reqMarketRuleAsync"));
                        question(
                            ib,
                            &client,
                            Ask::MarketRule(market_rule_id),
                            token,
                            waiter,
                            within,
                        );
                    }
                    Err(e) => {
                        reply.send(Err(e));
                    }
                },
            )
            .await
    }

    /// The feed the subscriptions made after it ask for: 1 live, 2 frozen,
    /// 3 delayed, 4 delayed frozen: ib_async's `reqMarketDataType`.
    pub fn req_market_data_type(&self, market_data_type: i32) -> Result<()> {
        self.shared.step(Class::Request, move |ib| {
            ready(ib)?.req_market_data_type(market_data_type);
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::sync::Mutex;
    use std::sync::mpsc;
    use std::task::{Context, Waker};
    use std::thread;

    use jiff::Timestamp;
    use jiff::tz::TimeZone;

    use super::*;
    use crate::engine::{ControlCommand, ErrorOrigin, SharedState};
    use crate::event::{lock, set_on_owner};
    use crate::ib::{ConnectOptions, IB, IBConfig, StartupFetch};
    use crate::objects::IBDefaults;
    use crate::owner::Via;
    use crate::record::{Callback, Capture};
    use crate::tests::{GLOBAL_ERRORS, capture_logs, errors_here};
    use crate::timer::Clock;

    /// Marks this test's thread as the owner while it lives: methods run
    /// inline, and the test drives the IB's laps.
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
        let (tx, rx) = mpsc::channel();
        let shared = Arc::new(SharedState::new());
        let client = EClient::from_parts(shared, tx, thread::spawn(|| {}), "DU123".into());
        (client, rx)
    }

    fn opts() -> ConnectOptions {
        ConnectOptions {
            fetch_fields: StartupFetch::NONE,
            ..ConnectOptions::default()
        }
    }

    struct Session {
        ib: IBHandle,
        capture: RefCell<Capture>,
        rx: mpsc::Receiver<ControlCommand>,
    }

    impl Session {
        /// An IB on a manual clock connected to `engine()`, driven by this
        /// thread as its owner.
        fn new() -> Self {
            let (client, rx) = engine();
            let shared = Shared::new(
                IBDefaults::default(),
                IBConfig::default(),
                Clock::manual(Timestamp::UNIX_EPOCH),
            );
            shared.connect_internal_slots();
            let ib = IBHandle { shared };
            let via = Via::Test(Some(Arc::new(client)));
            let (mut p, _) = ib.begin_connect(opts(), true, Some(via)).unwrap();
            let mut capture = Capture::new(TimeZone::UTC);
            ib.shared.lap(&mut capture);
            assert!(matches!(poll(&mut p), Some(Ok(()))));
            Session {
                ib,
                capture: RefCell::new(capture),
                rx,
            }
        }

        fn client(&self) -> Arc<EClient> {
            self.ib.shared.connected().unwrap().1
        }

        fn lap(&self) {
            self.ib.shared.lap(&mut self.capture.borrow_mut());
        }

        /// Applies `callbacks` as one read of the session.
        fn read(&self, callbacks: Vec<Callback>) {
            let g = self.ib.shared.connected().unwrap().0;
            self.ib
                .shared
                .unit(|| self.ib.shared.apply_read(g, callbacks));
        }

        fn sent(&self) -> Vec<ControlCommand> {
            self.rx.try_iter().collect()
        }

        fn raise_request_errors(&self, raise: bool) {
            self.ib.set_config(IBConfig {
                raise_request_errors: raise,
                ..IBConfig::default()
            });
        }
    }

    /// What `f` gives when polled once.
    fn poll<F: Future + Unpin>(f: &mut F) -> Option<F::Output> {
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

    fn error(id: i64, ends: bool, code: i64) -> Callback {
        Callback::Error {
            origin: ErrorOrigin::Request { id, ends },
            code,
            message: "m".into(),
            advanced_order_reject_json: String::new(),
        }
    }

    /// The ids of the market data requests among `sent`.
    fn quotes(sent: &[ControlCommand]) -> Vec<i64> {
        sent.iter()
            .filter_map(|c| match c {
                ControlCommand::Subscribe { req_id, .. } => Some(*req_id),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn req_tickers_ends_every_snapshot_however_it_ends() {
        #[derive(Clone, Copy, Debug)]
        enum End {
            Answered,
            Refused { raise: bool },
            Dropped,
        }
        let _o = AsOwner::new();
        let (aapl, msft) = (stock("AAPL", 265598), stock("MSFT", 272093));
        for end in [
            End::Answered,
            End::Refused { raise: false },
            End::Refused { raise: true },
            End::Dropped,
        ] {
            let s = Session::new();
            let notices = Arc::new(Mutex::new(Vec::new()));
            let n = notices.clone();
            s.ib.error_event()
                .connect(move |e| lock(&n).push((e.0, e.3.is_some())));
            if let End::Refused { raise } = end {
                s.raise_request_errors(raise);
            }
            let contracts = [aapl.clone(), msft.clone()];
            let mut f = Box::pin(s.ib.req_tickers_async(&contracts, false));
            assert!(poll(&mut f).is_none(), "{end:?}");
            let ids = quotes(&s.sent());
            assert_eq!(ids.len(), 2, "{end:?}: one snapshot per contract");
            let r = match end {
                End::Answered => {
                    s.read(vec![Callback::TickSnapshotEnd(ids[0])]);
                    assert!(poll(&mut f).is_none(), "one snapshot has not ended");
                    s.read(vec![Callback::TickSnapshotEnd(ids[1])]);
                    poll(&mut f)
                }
                End::Refused { .. } => {
                    s.read(vec![
                        error(ids[0], true, 200),
                        Callback::TickSnapshotEnd(ids[1]),
                    ]);
                    poll(&mut f)
                }
                End::Dropped => None,
            };
            let tickers = [&aapl, &msft].map(|c| s.ib.ticker(c).unwrap().unwrap());
            match (end, r) {
                (End::Answered | End::Refused { raise: false }, Some(Ok(got))) => {
                    assert_eq!(got, tickers, "{end:?}");
                }
                (End::Refused { raise: true }, Some(Err(Error::Request { code: 200, .. })))
                | (End::Dropped, None) => {}
                (end, r) => panic!("{end:?}: {r:?}"),
            }
            drop(f);
            s.lap();
            // A notice under a snapshot's id names its contract only while
            // the ticker is still that snapshot's (wr:416-419, 1626-1628).
            lock(&notices).clear();
            s.read(ids.iter().map(|&id| error(id, false, 10167)).collect());
            assert_eq!(
                *lock(&notices),
                [(ids[0], false), (ids[1], false)],
                "{end:?}"
            );
        }
    }

    #[test]
    fn req_tickers_blocks_a_user_thread_until_every_snapshot_has_ended() {
        let _g = lock(&GLOBAL_ERRORS);
        let (client, rx) = engine();
        let ib = IB::attach(client, opts(), Clock::system()).unwrap();
        let session = ib.shared.connected().unwrap().1;
        // The engine refuses each snapshot as it is asked for.
        let refuser = thread::spawn(move || {
            for id in rx.iter().take(2).flat_map(|c| quotes(&[c])) {
                let origin = ErrorOrigin::Request { id, ends: true };
                session.refuse(origin, 200, "No security definition has been found");
            }
        });
        let contracts = [stock("AAPL", 265598), stock("MSFT", 272093)];
        let got = ib.req_tickers(&contracts, false).unwrap();
        refuser.join().unwrap();
        let want = contracts.map(|c| ib.ticker(&c).unwrap().unwrap());
        assert_eq!(got, want);
    }

    #[test]
    fn calculations_are_cancelled_however_they_end_and_give_none_after_four_seconds() {
        #[derive(Clone, Copy, Debug)]
        enum End {
            Answered,
            Refused,
            TimedOut,
            Dropped,
        }
        capture_logs();
        let _o = AsOwner::new();
        let option = Contract {
            con_id: 1,
            ..Contract::option("AAPL", "20261218", 200.0, "C", "SMART")
        };
        let computed = OptionComputation {
            tick_attrib: 0,
            implied_vol: Some(0.25),
            delta: None,
            opt_price: Some(10.0),
            pv_dividend: None,
            gamma: None,
            vega: None,
            theta: None,
            und_price: Some(200.0),
        };
        let cancelled = |sent: &[ControlCommand]| -> Vec<i64> {
            sent.iter()
                .filter_map(|c| match c {
                    ControlCommand::CancelCalculation { req_id } => Some(*req_id),
                    _ => None,
                })
                .collect()
        };
        for (end, volatility) in [
            (End::Answered, true),
            (End::Refused, true),
            (End::TimedOut, false),
            (End::Dropped, true),
        ] {
            let s = Session::new();
            let mut f: Pin<Box<dyn Future<Output = Result<Option<OptionComputation>>>>> =
                if volatility {
                    Box::pin(s.ib.calculate_implied_volatility_async(&option, 10.0, 200.0, &[]))
                } else {
                    Box::pin(s.ib.calculate_option_price_async(&option, 0.25, 200.0, &[]))
                };
            assert!(poll(&mut f).is_none(), "{end:?}");
            let id = quotes(&s.sent())[0];
            let r = match end {
                End::Answered => {
                    s.read(vec![Callback::TickOptionComputation {
                        req_id: id,
                        tick_type: 53,
                        computation: computed,
                    }]);
                    poll(&mut f)
                }
                End::Refused => {
                    s.read(vec![error(id, true, 200)]);
                    poll(&mut f)
                }
                End::TimedOut => {
                    s.ib.shared.clock.advance(Duration::from_millis(3999));
                    s.lap();
                    assert!(poll(&mut f).is_none(), "{end:?} before its deadline");
                    assert!(cancelled(&s.sent()).is_empty());
                    s.ib.shared.clock.advance(Duration::from_millis(1));
                    s.lap();
                    poll(&mut f)
                }
                End::Dropped => {
                    drop(f);
                    s.lap();
                    None
                }
            };
            match (end, r) {
                (End::Answered, Some(Ok(Some(c)))) => assert_eq!(c, computed),
                (End::Refused, Some(Err(Error::Request { code: 200, .. })))
                | (End::TimedOut, Some(Ok(None)))
                | (End::Dropped, None) => {}
                (end, r) => panic!("{end:?}: {r:?}"),
            }
            assert_eq!(cancelled(&s.sent()), [id], "{end:?}");
        }
        let logged = (
            "ib_async.ib".to_owned(),
            "calculateOptionPriceAsync: Timeout".to_owned(),
        );
        assert_eq!(errors_here().iter().filter(|l| **l == logged).count(), 1);
    }

    #[test]
    fn an_unknown_market_rule_is_refused_on_error_event_then_gives_none_after_a_second() {
        capture_logs();
        let _o = AsOwner::new();
        let s = Session::new();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let n = seen.clone();
        s.ib.error_event()
            .connect(move |e| lock(&n).push((e.0, e.2.clone(), e.3.is_some())));
        let mut f = Box::pin(s.ib.req_market_rule_async(26));
        assert!(poll(&mut f).is_none());
        s.lap();
        let seen = lock(&seen).clone();
        assert_eq!(seen.len(), 1);
        assert_eq!((seen[0].0, seen[0].2), (-1, false));
        assert!(seen[0].1.contains("market rule 26"), "{}", seen[0].1);
        s.ib.shared.clock.advance(Duration::from_millis(999));
        s.lap();
        assert!(poll(&mut f).is_none(), "the refusal completes nothing");
        s.ib.shared.clock.advance(Duration::from_millis(1));
        s.lap();
        assert!(matches!(poll(&mut f), Some(Ok(None))));
        let logged = (
            "ib_async.ib".to_owned(),
            "reqMarketRuleAsync: Timeout".to_owned(),
        );
        assert!(errors_here().contains(&logged));

        // A rule the session knows is answered with its increments.
        let s = Session::new();
        let mut f = Box::pin(s.ib.req_market_rule_async(26));
        assert!(poll(&mut f).is_none());
        let increments = vec![PriceIncrement {
            low_edge: 0.0,
            increment: 0.01,
        }];
        s.read(vec![Callback::MarketRule {
            market_rule_id: 26,
            price_increments: increments.clone(),
        }]);
        assert_eq!(poll(&mut f).unwrap().unwrap(), Some(increments));
    }

    #[test]
    fn each_cancel_ends_its_own_subscription_of_the_contracts_ticker() {
        capture_logs();
        let _o = AsOwner::new();
        let s = Session::new();
        let c = stock("AAPL", 265598);
        let quote = s.ib.req_mkt_data(&c, "", false, false, &[]).unwrap();
        let ticks = s.ib.req_tick_by_tick_data(&c, "AllLast", 0, false).unwrap();
        let book = s.ib.req_mkt_depth(&c, 5, false, &[]).unwrap();
        assert!(Live::ptr_eq(&quote, &ticks) && Live::ptr_eq(&quote, &book));
        let ids: Vec<i64> = s
            .sent()
            .iter()
            .map(|cmd| match cmd {
                ControlCommand::Subscribe { req_id, .. }
                | ControlCommand::SubscribeTbt { req_id, .. } => *req_id,
                ControlCommand::SubscribeDepth { req_id, .. } => i64::from(*req_id),
                other => panic!("{other:?}"),
            })
            .collect();
        let [quote_id, ticks_id, book_id] = ids[..] else {
            panic!("{ids:?}");
        };
        s.read(vec![Callback::UpdateMktDepth {
            req_id: book_id,
            position: 0,
            operation: 0,
            side: 1,
            price: 150.0,
            size: 100.0,
        }]);
        assert_eq!(book.read().dom_bids.len(), 1);

        assert_eq!(
            s.ib.cancel_tick_by_tick_data(&c, "AllLast").ok(),
            Some(true)
        );
        assert!(
            matches!(s.sent()[..], [ControlCommand::UnsubscribeTbt { req_id }] if req_id == ticks_id)
        );
        assert_eq!(
            s.ib.cancel_tick_by_tick_data(&c, "AllLast").ok(),
            Some(false)
        );
        assert!(s.sent().is_empty());
        let logged = format!("cancelMktData: No reqId found for contract {c:?}");
        assert!(errors_here().contains(&("ib_async.ib".to_owned(), logged)));

        s.ib.cancel_mkt_depth(&c, false).unwrap();
        assert!(
            matches!(s.sent()[..], [ControlCommand::UnsubscribeDepth { req_id }] if i64::from(req_id) == book_id)
        );
        assert!(book.read().dom_bids.is_empty());

        assert_eq!(s.ib.cancel_mkt_data(&c).ok(), Some(true));
        assert!(
            matches!(s.sent()[..], [ControlCommand::CancelMktData { req_id }] if req_id == quote_id)
        );
    }

    #[test]
    fn a_bar_list_is_cancelled_only_through_the_ib_that_keeps_it() {
        let _o = AsOwner::new();
        let (a, b) = (Session::new(), Session::new());
        let c = stock("AAPL", 265598);
        let theirs =
            a.ib.req_real_time_bars(&c, 5, "TRADES", false, &[])
                .unwrap();
        let mine =
            b.ib.req_real_time_bars(&c, 5, "TRADES", false, &[])
                .unwrap();
        // Both IBs number their first request alike.
        assert_eq!(theirs.read().req_id, mine.read().req_id);
        let _ = (a.sent(), b.sent());

        b.ib.cancel_real_time_bars(&theirs).unwrap();
        assert!(b.sent().is_empty());
        assert_eq!(b.ib.realtime_bars(), [Bars::RealTime(mine.clone())]);

        b.ib.cancel_real_time_bars(&mine).unwrap();
        let id = mine.read().req_id;
        assert!(
            matches!(b.sent()[..], [ControlCommand::CancelRealTimeBar { req_id }] if i64::from(req_id) == id)
        );
        assert!(b.ib.realtime_bars().is_empty());
        assert_eq!(a.ib.realtime_bars(), [Bars::RealTime(theirs)]);
    }

    #[test]
    fn smart_components_and_depth_exchanges_reach_their_callers() {
        let _o = AsOwner::new();
        // Answered.
        let s = Session::new();
        // A session's first request is numbered at the engine's floor.
        let id = s.client().order_id_floor();
        let mut p = s.ib.req_smart_components_async("SMART");
        let components = vec![SmartComponent {
            bit_number: 1,
            exchange: "NYSE".into(),
            exchange_letter: "N".into(),
        }];
        s.read(vec![Callback::SmartComponents {
            req_id: id,
            components: components.clone(),
        }]);
        assert_eq!(poll(&mut p).unwrap().unwrap(), components);
        // Refused, as the engine refuses an exchange no subscription named:
        // what arrived, nothing, unless errors raise.
        for raise in [false, true] {
            let s = Session::new();
            s.raise_request_errors(raise);
            let mut p = s.ib.req_smart_components_async("SMART");
            s.lap();
            match (raise, poll(&mut p)) {
                (false, Some(Ok(v))) => assert!(v.is_empty()),
                (true, Some(Err(Error::Request { .. }))) => {}
                (raise, r) => panic!("{raise}: {r:?}"),
            }
        }
        // A question's answer.
        let s = Session::new();
        let mut p = s.ib.req_mkt_depth_exchanges_async();
        assert!(matches!(
            s.sent()[..],
            [ControlCommand::FetchMktDepthExchanges]
        ));
        let exchanges = vec![DepthMktDataDescription {
            exchange: "ISLAND".into(),
            sec_type: "STK".into(),
            ..DepthMktDataDescription::default()
        }];
        s.read(vec![Callback::MktDepthExchanges(exchanges.clone())]);
        assert_eq!(poll(&mut p).unwrap().unwrap(), exchanges);
    }
}

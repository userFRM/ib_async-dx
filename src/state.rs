//! The state ib_async's `Wrapper` keeps, and the callbacks that change it.
//!
//! `State` holds the `Wrapper`'s fields. [`apply`] runs one callback as
//! ib_async's wrapper method of the same name does, in its source order:
//! each change to the state, then each emission, in place. The owner holds
//! its books only while a change runs and releases them before every
//! emission, so each handler sees the state its emission follows and the
//! next change starts from whatever the handlers did. [`pass`] is one read:
//! ib_async's `tcpDataArrived`, the read's callbacks, and `tcpDataProcessed`.
//!
//! What ib_async keeps as `_futures` and `_results` is the requests' own
//! bookkeeping (`Requests`); what a request's answer is, and how an error
//! ends it, is decided here.

#![expect(dead_code, reason = "the owner applies each read through it")]

use std::any::Any;
use std::collections::HashMap;
use std::sync::Weak;

use indexmap::{IndexMap, IndexSet};
use jiff::{Timestamp, Zoned};

use crate::contract::{Contract, TickerKey};
use crate::engine::{ErrorOrigin, OrderOp, Question};
use crate::error::{Error, Result};
use crate::live::{Holder, Live, Observed};
use crate::objects::{
    AccountValue, BarDataList, CommissionReport, DOMLevel, Dividends, Fill, IBDefaults,
    MktDepthData, NewsBulletin, NewsTick, PnL, PnLSingle, PortfolioItem, Position, RealTimeBarList,
    ScanDataList, TickByTickAllLast, TickByTickBidAsk, TickByTickMidPoint, TickData, TradeLogEntry,
};
use crate::order::{OrderStatus, Trade};
use crate::record::Callback;
use crate::requests::{Ask, Exec, IdSpace, Refused, ReqKey, Requests};
use crate::ticker::{Tick, TickByTick, Ticker};
use crate::util::{BarDate, parse_ib_datetime, py_float, py_int, py_number};

const LOG: &str = "ib_async.wrapper";

/// What `Wrapper.trades` is keyed by: `(clientId, orderId)`, or the
/// `permId` of an order placed outside the API (`orderKey`, wr:431-438).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum OrderKey {
    /// An order an API client numbered.
    Order { client_id: i64, order_id: i64 },
    /// A manual order, or a completed one: its `permId`.
    Perm(i64),
}

/// ib_async's `orderKey`.
pub(crate) fn order_key(client_id: i64, order_id: i64, perm_id: i64) -> OrderKey {
    if order_id <= 0 {
        OrderKey::Perm(perm_id)
    } else {
        OrderKey::Order {
            client_id,
            order_id,
        }
    }
}

/// A list a subscription keeps up to date: ib_async's `reqId2Subscriber`
/// values, which `realtime_bars()` lists.
#[derive(Clone, Debug, PartialEq)]
pub enum Bars {
    /// Historical bars kept up to date: a `BarDataList`.
    Historical(Live<BarDataList>),
    /// Five-second bars: a `RealTimeBarList`.
    RealTime(Live<RealTimeBarList>),
    /// A scanner subscription's results: a `ScanDataList`.
    Scan(Live<ScanDataList>),
}

/// The fields of ib_async's `Wrapper` (wr:218-311), less `_futures` and
/// `_results`, which are `Requests`, and the idle timer, which is the
/// owner's.
pub(crate) struct State {
    /// `IBDefaults`: the empty price and size, and the zone times are
    /// stamped in.
    pub(crate) defaults: IBDefaults,
    /// The IB the objects made here are bound to.
    holder: Weak<dyn Holder>,
    /// `(account, tag, currency, modelCode)` → value.
    pub(crate) account_values: IndexMap<(String, String, String, String), AccountValue>,
    /// `(account, tag, currency)` → value.
    pub(crate) acct_summary: IndexMap<(String, String, String), AccountValue>,
    /// account → conId → item.
    pub(crate) portfolio: IndexMap<String, IndexMap<i64, PortfolioItem>>,
    /// account → conId → position.
    pub(crate) positions: IndexMap<String, IndexMap<i64, Position>>,
    pub(crate) trades: IndexMap<OrderKey, Live<Trade>>,
    pub(crate) perm_id_to_trade: HashMap<i64, Live<Trade>>,
    /// execId → fill.
    pub(crate) fills: IndexMap<String, Fill>,
    pub(crate) news_ticks: Vec<NewsTick>,
    pub(crate) msg_id_to_news_bulletin: IndexMap<i64, NewsBulletin>,
    /// `hash(contract)` → ticker.
    pub(crate) tickers: IndexMap<TickerKey, Live<Ticker>>,
    /// In the order they were added; ib_async's is a set.
    pub(crate) pending_tickers: IndexSet<Live<Ticker>>,
    pub(crate) req_id_to_ticker: HashMap<i64, Live<Ticker>>,
    /// tick type (`mktData`, `snapshot`, `mktDepth`, or a tick-by-tick
    /// type) → ticker → reqId.
    pub(crate) ticker_to_req_id: HashMap<String, IndexMap<Live<Ticker>, i64>>,
    pub(crate) req_id_to_subscriber: IndexMap<i64, Bars>,
    pub(crate) req_id_to_pnl: IndexMap<i64, Live<PnL>>,
    pub(crate) req_id_to_pnl_single: IndexMap<i64, Live<PnLSingle>>,
    /// `(account, modelCode)` → reqId.
    pub(crate) pnl_key_to_req_id: HashMap<(String, String), i64>,
    /// `(account, modelCode, conId)` → reqId.
    pub(crate) pnl_single_key_to_req_id: HashMap<(String, String, i64), i64>,
    /// When the last read arrived, in `defaults.timezone`: `lastTime`.
    pub(crate) last_time: Zoned,
    /// That arrival in seconds since the epoch; -1 before any: `time`.
    pub(crate) time: f64,
    pub(crate) accounts: Vec<String>,
    /// The session's client id; -1 while there is none.
    pub(crate) client_id: i64,
    pub(crate) wsh_meta_req_id: i64,
    pub(crate) wsh_event_req_id: i64,
    /// `_reqId2Contract`: the contract each request is for.
    pub(crate) req_id_to_contract: HashMap<i64, Contract>,
    /// The prices of this read that wait for the size stated beside them,
    /// by (reqId, price tick type).
    priced: IndexMap<(i64, i32), f64>,
}

impl State {
    /// A fresh wrapper's state, `now` being the reset's `lastTime`.
    pub(crate) fn new(defaults: IBDefaults, holder: Weak<dyn Holder>, now: Zoned) -> Self {
        State {
            defaults,
            holder,
            account_values: IndexMap::new(),
            acct_summary: IndexMap::new(),
            portfolio: IndexMap::new(),
            positions: IndexMap::new(),
            trades: IndexMap::new(),
            perm_id_to_trade: HashMap::new(),
            fills: IndexMap::new(),
            news_ticks: Vec::new(),
            msg_id_to_news_bulletin: IndexMap::new(),
            tickers: IndexMap::new(),
            pending_tickers: IndexSet::new(),
            req_id_to_ticker: HashMap::new(),
            ticker_to_req_id: HashMap::new(),
            req_id_to_subscriber: IndexMap::new(),
            req_id_to_pnl: IndexMap::new(),
            req_id_to_pnl_single: IndexMap::new(),
            pnl_key_to_req_id: HashMap::new(),
            pnl_single_key_to_req_id: HashMap::new(),
            last_time: now,
            time: -1.0,
            accounts: Vec::new(),
            client_id: -1,
            wsh_meta_req_id: 0,
            wsh_event_req_id: 0,
            req_id_to_contract: HashMap::new(),
            priced: IndexMap::new(),
        }
    }

    /// ib_async's `reset` (wr:313-342), whose `setTimeout(0)` leaves
    /// `lastTime` at `now`.
    pub(crate) fn reset(&mut self, now: Zoned) {
        let holder = self.holder.clone();
        *self = State::new(self.defaults.clone(), holder, now);
    }

    /// `v`, bound to this IB, so a program's edit of it goes through the
    /// owner.
    fn bind<T: Observed>(&self, v: T) -> Live<T> {
        let live = Live::new(v);
        live.bind(self.holder.clone());
        live
    }

    /// ib_async's `startTicker` (wr:401-414): the contract's ticker, made on
    /// first use, now fed by `req_id` under `tick_type`. An unhashable
    /// contract fails, as `hash(contract)` raises.
    pub(crate) fn start_ticker(
        &mut self,
        req_id: i64,
        contract: &Contract,
        tick_type: &str,
    ) -> Result<Live<Ticker>> {
        let key = contract.ticker_key()?;
        let ticker = match self.tickers.get(&key) {
            Some(t) => t.clone(),
            None => {
                let t = self.bind(Ticker::new(Some(contract.clone()), self.defaults.clone()));
                self.tickers.insert(key, t.clone());
                t
            }
        };
        self.req_id_to_ticker.insert(req_id, ticker.clone());
        self.req_id_to_contract.insert(req_id, contract.clone());
        self.ticker_to_req_id
            .entry(tick_type.to_owned())
            .or_default()
            .insert(ticker.clone(), req_id);
        Ok(ticker)
    }

    /// ib_async's `endTicker` (wr:416-419): the id that fed `ticker` under
    /// `tick_type`, 0 when none did.
    pub(crate) fn end_ticker(&mut self, ticker: &Live<Ticker>, tick_type: &str) -> i64 {
        let req_id = self
            .ticker_to_req_id
            .get_mut(tick_type)
            .and_then(|m| m.shift_remove(ticker))
            .unwrap_or(0);
        self.req_id_to_contract.remove(&req_id);
        req_id
    }

    /// The trade `(clientId, orderId)` of this session names.
    fn own_trade(&self, order_id: i64) -> Option<Live<Trade>> {
        let key = OrderKey::Order {
            client_id: self.client_id,
            order_id,
        };
        self.trades.get(&key).cloned()
    }

    fn pending(&mut self, ticker: &Live<Ticker>) {
        self.pending_tickers.insert(ticker.clone());
    }
}

/// The owner's parts a callback changes: held only while a change runs.
pub(crate) struct Books<'a> {
    pub(crate) state: &'a mut State,
    pub(crate) requests: &'a mut Requests,
    pub(crate) ids: &'a mut IdSpace,
}

/// An emission a callback makes, in its place: an IB event or one of a live
/// object's own. The payloads are ib_async's.
#[derive(Clone, Debug)]
pub(crate) enum Emit {
    AccountValue(AccountValue),
    AccountSummary(AccountValue),
    UpdatePortfolio(PortfolioItem),
    Position(Position),
    Pnl(Live<PnL>),
    PnlSingle(Live<PnLSingle>),
    OpenOrder(Live<Trade>),
    OrderStatus(Live<Trade>),
    ExecDetails(Live<Trade>, Fill),
    CommissionReport(Live<Trade>, Fill, Live<CommissionReport>),
    BarUpdate(Bars, bool),
    ScannerData(Live<ScanDataList>),
    TickNews(NewsTick),
    NewsBulletin(NewsBulletin),
    WshMeta(String),
    Wsh(String),
    /// `error_event`: reqId, code, message and the request's contract.
    Error(i64, i64, String, Option<Contract>),
    /// `update_event`, at the pass's end.
    Update,
    PendingTickers(Vec<Live<Ticker>>),
    /// `tick_event`: a record appended to a ticker's `ticks`,
    /// `tick_by_ticks` or `dom_ticks`, at the append.
    Tick(Live<Ticker>, Tick),
    /// `Trade.statusEvent`.
    TradeStatus(Live<Trade>),
    /// `Trade.fillEvent`.
    TradeFill(Live<Trade>, Fill),
    /// `Trade.commissionReportEvent`.
    TradeCommissionReport(Live<Trade>, Fill, Live<CommissionReport>),
    /// `Trade.filledEvent`.
    TradeFilled(Live<Trade>),
    /// `Trade.cancelledEvent`.
    TradeCancelled(Live<Trade>),
    /// `Ticker.updateEvent`.
    TickerUpdate(Live<Ticker>),
    /// A bar list's `updateEvent`, with `hasNewBar`.
    BarsUpdate(Bars, bool),
    /// `ScanDataList.updateEvent`.
    ScanUpdate(Live<ScanDataList>),
}

/// An engine call a callback makes.
#[derive(Clone, Debug)]
pub(crate) enum Call {
    /// A question's next exchange, its lane now free.
    Ask(Ask),
    /// 10225 on real-time bars: the cancel, then the same request again
    /// under the same id (wr:1698-1707).
    CancelRealTimeBars(i64),
    ReqRealTimeBars(Live<RealTimeBarList>),
    /// 10225 on historical bars: the cancel, then the list's request again
    /// under the same id, its end written by `format_ib_datetime`, where
    /// ib_async sends the end as it was given (wr:1708-1721).
    CancelHistoricalData(i64),
    ReqHistoricalData(Live<BarDataList>),
}

/// Where a callback's effects go besides the books.
///
/// An execution a callback ends leaves through [`Sink::settle`], and its
/// waiter is then decided with the callback's own result: the value
/// ib_async's `_endReq` is given, or what was collected. A method whose
/// result is an `Option` wraps it.
pub(crate) trait Sink {
    /// Runs `f` on the books, which the owner holds only while it runs.
    fn books<R>(&mut self, f: impl FnOnce(&mut Books<'_>) -> R) -> R;
    /// Runs an emission, with no crate lock held.
    fn emit(&mut self, e: Emit);
    /// Makes an engine call.
    fn call(&mut self, c: Call);
    /// An execution leaves: its deadline is removed and its cleanup run.
    fn settle(&mut self, x: &mut Exec);
    /// Whether the generation still stands: a handler that closes it ends
    /// the read.
    fn connected(&self) -> bool;
    /// `IBConfig.raise_request_errors`, read at each use.
    fn raise_request_errors(&self) -> bool;
}

/// One read: each question's retirement in its place, and, when the
/// read carries anything a gateway would send, the pass around it. Gives
/// whether the read held `connection_closed`.
pub(crate) fn pass<S: Sink>(sink: &mut S, callbacks: Vec<Callback>, arrived: Zoned) -> bool {
    if callbacks
        .iter()
        .all(|c| matches!(c, Callback::QuestionRetired(_)))
    {
        for c in callbacks {
            apply(sink, c);
        }
        return false;
    }
    let mut closed = false;
    tcp_data_arrived(sink, arrived);
    for c in callbacks {
        if !sink.connected() {
            break;
        }
        closed |= matches!(c, Callback::ConnectionClosed);
        apply(sink, c);
    }
    // The prices no size followed, each with its side's size standing.
    let priced: Vec<_> = sink.books(|b| b.state.priced.drain(..).collect());
    for ((req_id, tick_type), price) in priced {
        price_alone(sink, req_id, tick_type, price);
    }
    tcp_data_processed(sink);
    closed
}

/// `tcpDataArrived` (wr:1725-1733).
fn tcp_data_arrived<S: Sink>(sink: &mut S, arrived: Zoned) {
    sink.books(|b| {
        let s = &mut *b.state;
        let t = arrived.timestamp();
        s.time = t.as_second() as f64 + f64::from(t.subsec_nanosecond()) / 1e9;
        s.last_time = arrived;
        for t in s.pending_tickers.drain(..) {
            t.update(|t| {
                t.ticks.clear();
                t.tick_by_ticks.clear();
                t.dom_ticks.clear();
            });
        }
    });
}

/// `tcpDataProcessed` (wr:1735-1743): `update_event`, then each pending
/// ticker stamped just before its own event.
fn tcp_data_processed<S: Sink>(sink: &mut S) {
    sink.emit(Emit::Update);
    let pending: Vec<_> = sink.books(|b| b.state.pending_tickers.iter().cloned().collect());
    if pending.is_empty() {
        return;
    }
    for t in &pending {
        sink.books(|b| {
            let (time, stamp) = (b.state.last_time.clone(), b.state.time);
            t.update(|t| {
                t.time = Some(time);
                t.timestamp = Some(stamp);
            });
        });
        sink.emit(Emit::TickerUpdate(t.clone()));
    }
    sink.emit(Emit::PendingTickers(pending));
}

/// A request's end: its execution settled and its waiter decided with
/// `value`, or with what was collected (`_endReq`, wr:384-399).
fn end_request<S: Sink>(sink: &mut S, req_id: i64, value: Option<Box<dyn Any + Send>>) {
    if let Some(x) = sink.books(|b| b.requests.end(req_id)) {
        finish(sink, x, value);
    }
}

fn finish<S: Sink>(sink: &mut S, mut x: Exec, value: Option<Box<dyn Any + Send>>) {
    sink.settle(&mut x);
    let r = value
        .or_else(|| x.acc.take())
        .unwrap_or_else(|| Box::new(()));
    x.finish(Ok(r));
}

/// A question's terminal callback: each call it answers gets `value`, or
/// what was collected for it, and the lane's next exchange is sent.
fn answer<S: Sink, T: Clone + Send + 'static>(sink: &mut S, q: Question, value: Option<T>) {
    let (execs, next) = sink.books(|b| b.requests.answered(q));
    for x in execs {
        let v = value.clone().map(|v| Box::new(v) as Box<dyn Any + Send>);
        finish(sink, x, v);
    }
    if let Some(ask) = next {
        sink.call(Call::Ask(ask));
    }
}

/// Adds `v` to what the numbered request `req_id` collects.
fn collect<S: Sink, T: Send + 'static>(sink: &mut S, req_id: i64, v: T) {
    let mut v = Some(v);
    sink.books(|b| {
        b.requests
            .accumulate::<Vec<T>>(ReqKey::Id(req_id), |acc| acc.extend(v.take()));
    });
}

/// Applies one callback as ib_async's wrapper method of its name does.
pub(crate) fn apply<S: Sink>(sink: &mut S, cb: Callback) {
    match cb {
        // The owner closes the generation after the pass that held it.
        Callback::ConnectionClosed => {}
        Callback::ManagedAccounts(accounts) => sink.books(|b| b.state.accounts = accounts),
        Callback::Error {
            origin,
            code,
            message,
            advanced_order_reject_json,
        } => error(sink, origin, code, message, advanced_order_reject_json),
        Callback::CurrentTime(t) => answer(sink, Question::CurrentTime, Some(t)),
        Callback::CurrentTimeInMillis(ms) => answer(sink, Question::CurrentTimeInMillis, Some(ms)),
        Callback::QuestionRetired(q) => {
            if let Some(ask) = sink.books(|b| b.requests.retired(q)) {
                sink.call(Call::Ask(ask));
            }
        }

        Callback::TickPrice {
            req_id,
            tick_type,
            price,
        } => tick_price(sink, req_id, tick_type, price),
        Callback::TickSize {
            req_id,
            tick_type,
            size,
        } => {
            let paired = price_of(tick_type).and_then(|p| {
                sink.books(|b| b.state.priced.shift_remove(&(req_id, p)))
                    .map(|price| (p, price))
            });
            match paired {
                Some((p, price)) => price_size_tick(sink, req_id, p, price, size),
                None => tick_size(sink, req_id, tick_type, size),
            }
        }
        Callback::TickString {
            req_id,
            tick_type,
            value,
        } => tick_string(sink, req_id, tick_type, &value),
        Callback::TickGeneric {
            req_id,
            tick_type,
            value,
        } => tick_generic(sink, req_id, tick_type, value),
        Callback::TickSnapshotEnd(req_id) => {
            // A snapshot's prices reach the ticker before its end.
            let priced: Vec<_> = sink.books(|b| {
                let mine: Vec<_> = b
                    .state
                    .priced
                    .keys()
                    .filter(|k| k.0 == req_id)
                    .copied()
                    .collect();
                mine.into_iter()
                    .filter_map(|k| b.state.priced.shift_remove(&k).map(|p| (k.1, p)))
                    .collect()
            });
            for (tick_type, price) in priced {
                price_alone(sink, req_id, tick_type, price);
            }
            end_request(sink, req_id, None);
        }
        Callback::MarketDataType {
            req_id,
            market_data_type,
        } => sink.books(|b| {
            if let Some(t) = b.state.req_id_to_ticker.get(&req_id) {
                t.update(|t| t.market_data_type = market_data_type);
            }
        }),
        Callback::TickReqParams {
            req_id,
            min_tick,
            bbo_exchange,
            snapshot_permissions,
        } => sink.books(|b| {
            let Some(t) = b.state.req_id_to_ticker.get(&req_id) else {
                return;
            };
            let Ok(permissions) = i32::try_from(snapshot_permissions) else {
                log::error!(target: "ib_async.Decoder", "Error for tickReqParams: snapshotPermissions {snapshot_permissions} is out of range");
                return;
            };
            t.update(|t| {
                t.min_tick = min_tick;
                t.bbo_exchange = bbo_exchange;
                t.snapshot_permissions = permissions;
            });
        }),
        Callback::TickOptionComputation {
            req_id,
            tick_type,
            computation,
        } => {
            enum Found {
                Ticker,
                Request,
                Nothing,
            }
            let found = sink.books(|b| {
                if let Some(t) = b.state.req_id_to_ticker.get(&req_id).cloned() {
                    let set = |t: &mut Ticker| -> Option<()> {
                        let field = match tick_type {
                            10 | 80 => &mut t.bid_greeks,
                            11 | 81 => &mut t.ask_greeks,
                            12 | 82 => &mut t.last_greeks,
                            13 | 83 => &mut t.model_greeks,
                            53 => &mut t.cust_greeks,
                            _ => return None,
                        };
                        *field = Some(computation);
                        Some(())
                    };
                    if t.update(set).is_none() {
                        log::error!(target: LOG, "Received tick tickType={tick_type} but we don't have an attribute mapping for it");
                        return Found::Nothing;
                    }
                    b.state.pending(&t);
                    Found::Ticker
                } else if b.requests.is_request(req_id) {
                    Found::Request
                } else {
                    log::error!(target: LOG, "tickOptionComputation: Unknown reqId: {req_id}");
                    Found::Nothing
                }
            });
            if let Found::Request = found {
                end_request(sink, req_id, Some(Box::new(computation)));
            }
        }
        Callback::TickEfp {
            req_id,
            tick_type,
            efp,
        } => sink.books(|b| {
            let Some(t) = b.state.req_id_to_ticker.get(&req_id).cloned() else {
                return;
            };
            let set = |t: &mut Ticker| -> bool {
                let field = match tick_type {
                    38 => &mut t.bid_efp,
                    39 => &mut t.ask_efp,
                    40 => &mut t.last_efp,
                    41 => &mut t.open_efp,
                    42 => &mut t.high_efp,
                    43 => &mut t.low_efp,
                    44 => &mut t.close_efp,
                    _ => return false,
                };
                *field = Some(efp);
                true
            };
            if t.update(set) {
                b.state.pending(&t);
            }
        }),
        Callback::TickNews { news, .. } => {
            sink.books(|b| b.state.news_ticks.push(news.clone()));
            sink.emit(Emit::TickNews(news));
        }
        Callback::TickByTickAllLast {
            req_id,
            tick_type,
            price,
            size,
            attrib,
            exchange,
            special_conditions,
            ..
        } => {
            let tick = sink.books(|b| {
                let Some(t) = b.state.req_id_to_ticker.get(&req_id).cloned() else {
                    log::error!(target: LOG, "tickByTickAllLast: Unknown reqId: {req_id}");
                    return None;
                };
                let d = &b.state.defaults;
                let (price, size) = if price == -1.0 && size == 0.0 {
                    (d.empty_price, d.empty_size)
                } else {
                    (price, size)
                };
                let tick = TickByTickAllLast {
                    tick_type,
                    time: b.state.last_time.clone(),
                    price,
                    size,
                    tick_attrib_last: attrib,
                    exchange,
                    special_conditions,
                };
                t.update(|t| {
                    t.prev_last = t.last;
                    t.prev_last_size = t.last_size;
                    t.last = price;
                    t.last_size = size;
                    t.tick_by_ticks.push(TickByTick::AllLast(tick.clone()));
                });
                b.state.pending(&t);
                Some((t, Tick::AllLast(tick)))
            });
            if let Some((t, tick)) = tick {
                sink.emit(Emit::Tick(t, tick));
            }
        }
        Callback::TickByTickBidAsk {
            req_id,
            bid_price,
            ask_price,
            bid_size,
            ask_size,
            attrib,
            ..
        } => {
            let tick = sink.books(|b| {
                let Some(t) = b.state.req_id_to_ticker.get(&req_id).cloned() else {
                    log::error!(target: LOG, "tickByTickBidAsk: Unknown reqId: {req_id}");
                    return None;
                };
                let (price, empty) = (b.state.defaults.empty_price, b.state.defaults.empty_size);
                let tick = TickByTickBidAsk {
                    time: b.state.last_time.clone(),
                    bid_price,
                    ask_price,
                    bid_size,
                    ask_size,
                    tick_attrib_bid_ask: attrib,
                };
                let or = |v: f64, d: f64| if v > 0.0 { v } else { d };
                t.update(|t| {
                    if bid_price != t.bid {
                        t.prev_bid = t.bid;
                        t.bid = or(bid_price, price);
                    }
                    if bid_size != t.bid_size {
                        t.prev_bid_size = t.bid_size;
                        t.bid_size = or(bid_size, empty);
                    }
                    if ask_price != t.ask {
                        t.prev_ask = t.ask;
                        t.ask = or(ask_price, price);
                    }
                    if ask_size != t.ask_size {
                        t.prev_ask_size = t.ask_size;
                        t.ask_size = or(ask_size, empty);
                    }
                    t.tick_by_ticks.push(TickByTick::BidAsk(tick.clone()));
                });
                b.state.pending(&t);
                Some((t, Tick::BidAsk(tick)))
            });
            if let Some((t, tick)) = tick {
                sink.emit(Emit::Tick(t, tick));
            }
        }
        Callback::TickByTickMidPoint {
            req_id, mid_point, ..
        } => {
            let tick = sink.books(|b| {
                let Some(t) = b.state.req_id_to_ticker.get(&req_id).cloned() else {
                    log::error!(target: LOG, "tickByTickMidPoint: Unknown reqId: {req_id}");
                    return None;
                };
                let tick = TickByTickMidPoint {
                    time: b.state.last_time.clone(),
                    mid_point,
                };
                t.update(|t| t.tick_by_ticks.push(TickByTick::MidPoint(tick.clone())));
                b.state.pending(&t);
                Some((t, Tick::MidPoint(tick)))
            });
            if let Some((t, tick)) = tick {
                sink.emit(Emit::Tick(t, tick));
            }
        }
        Callback::UpdateMktDepth {
            req_id,
            position,
            operation,
            side,
            price,
            size,
        } => depth(sink, req_id, position, String::new(), operation, side, price, size),
        Callback::UpdateMktDepthL2 {
            req_id,
            position,
            market_maker,
            operation,
            side,
            price,
            size,
            ..
        } => depth(sink, req_id, position, market_maker, operation, side, price, size),
        Callback::MktDepthExchanges(v) => answer(sink, Question::MktDepthExchanges, Some(v)),
        Callback::SmartComponents { req_id, components } => {
            end_request(sink, req_id, Some(Box::new(components)));
        }
        Callback::RealtimeBar { req_id, bar } => {
            let bars = sink.books(|b| match b.state.req_id_to_subscriber.get(&req_id) {
                Some(Bars::RealTime(list)) => {
                    list.update(|l| l.bars.push(bar));
                    Some(list.clone())
                }
                _ => None,
            });
            if let Some(list) = bars {
                sink.emit(Emit::BarUpdate(Bars::RealTime(list.clone()), true));
                sink.emit(Emit::BarsUpdate(Bars::RealTime(list), true));
            }
        }

        Callback::OrderStatus {
            order_id,
            status,
            filled,
            remaining,
            avg_fill_price,
            perm_id,
            parent_id,
            last_fill_price,
            client_id,
            why_held,
            mkt_cap_price,
        } => {
            let found = sink.books(|b| {
                let Some(trade) = b
                    .state
                    .trades
                    .get(&order_key(client_id, order_id, perm_id))
                    .cloned()
                else {
                    log::error!(target: LOG, "orderStatus: No order found for orderId {order_id} and clientId {client_id}");
                    return None;
                };
                let time = b.state.last_time.clone();
                let (logged, old) = trade.update(|t| {
                    let old = t.order_status.status.clone();
                    let new = OrderStatus {
                        order_id: t.order_status.order_id,
                        status: status.clone(),
                        filled,
                        remaining,
                        avg_fill_price,
                        perm_id,
                        parent_id,
                        last_fill_price,
                        client_id,
                        why_held,
                        mkt_cap_price,
                    };
                    let msg = if t.order_status != new {
                        t.order_status = new;
                        Some("")
                    } else if status == OrderStatus::SUBMITTED
                        && t.log.last().is_some_and(|e| e.message == "Modify")
                    {
                        // an acknowledged modification
                        Some("Modified")
                    } else {
                        None
                    };
                    if let Some(msg) = msg {
                        t.log.push(TradeLogEntry {
                            time,
                            status: status.clone(),
                            message: msg.into(),
                            error_code: 0,
                        });
                    }
                    (msg.is_some(), old)
                });
                if logged {
                    log::info!(target: LOG, "orderStatus: {trade:?}");
                }
                logged.then_some((trade, old))
            });
            if let Some((trade, old)) = found {
                sink.emit(Emit::OrderStatus(trade.clone()));
                sink.emit(Emit::TradeStatus(trade.clone()));
                if status != old {
                    if status == OrderStatus::FILLED {
                        sink.emit(Emit::TradeFilled(trade));
                    } else if status == OrderStatus::CANCELLED {
                        sink.emit(Emit::TradeCancelled(trade));
                    }
                }
            }
        }
        Callback::OpenOrder {
            order_id,
            contract,
            order,
            order_state,
        } => {
            if order.what_if {
                // A what-if's answer, which ends it once it states a margin
                // (wr:680-683).
                let Some(change) = py_float(&order_state.init_margin_change) else {
                    log::error!(target: LOG, "openOrder: could not convert initMarginChange to float: {:?}", order_state.init_margin_change);
                    return;
                };
                if change != f64::MAX {
                    end_request(sink, order.order_id, Some(Box::new(order_state)));
                }
            } else {
                let trade = sink.books(|b| {
                    let key = order_key(order.client_id, order.order_id, order.perm_id);
                    let perm_id = order.perm_id;
                    let trade = match b.state.trades.get(&key) {
                        Some(trade) => {
                            trade.read().order.update(|o| {
                                o.perm_id = order.perm_id;
                                o.total_quantity = order.total_quantity;
                                o.lmt_price = order.lmt_price;
                                o.aux_price = order.aux_price;
                                o.order_type = order.order_type;
                                o.order_ref = order.order_ref;
                            });
                            trade.clone()
                        }
                        None => {
                            // ib_async drops the decoder's "?"
                            // placeholders here (wr:696-698); the engine sends
                            // none, so nothing is dropped.
                            let status = OrderStatus {
                                order_id,
                                status: order_state.status,
                                ..OrderStatus::default()
                            };
                            let trade = Trade {
                                contract,
                                order: b.state.bind(order),
                                order_status: status,
                                ..Trade::default()
                            };
                            let trade = b.state.bind(trade);
                            b.state.trades.insert(key, trade.clone());
                            log::info!(target: LOG, "openOrder: {trade:?}");
                            trade
                        }
                    };
                    b.state
                        .perm_id_to_trade
                        .entry(perm_id)
                        .or_insert_with(|| trade.clone());
                    let q = ReqKey::Question(Question::OpenOrders);
                    if b.requests.records(Question::OpenOrders) {
                        // the answer to req_open_orders, not an event
                        b.requests
                            .accumulate::<Vec<Live<Trade>>>(q, |v| v.push(trade.clone()));
                        None
                    } else {
                        Some(trade)
                    }
                });
                if let Some(trade) = trade {
                    sink.emit(Emit::OpenOrder(trade));
                }
            }
            // ids above every order seen, another client's included
            sink.books(|b| b.ids.raise(order_id.saturating_add(1)));
        }
        Callback::OpenOrderEnd => answer::<S, ()>(sink, Question::OpenOrders, None),
        Callback::CompletedOrder {
            contract,
            order,
            order_state,
        } => sink.books(|b| {
            if !b.requests.records(Question::CompletedOrders) {
                // `_results["completedOrders"]` is only there while
                // req_completed_orders waits (wr:726)
                log::error!(target: LOG, "completedOrder: no completed-orders request is in flight");
                return;
            }
            let perm_id = order.perm_id;
            let status = OrderStatus {
                order_id: order.order_id,
                status: order_state.status,
                ..OrderStatus::default()
            };
            let trade = Trade {
                contract,
                order: b.state.bind(order),
                order_status: status,
                ..Trade::default()
            };
            let trade = b.state.bind(trade);
            b.requests.accumulate::<Vec<Live<Trade>>>(
                ReqKey::Question(Question::CompletedOrders),
                |v| v.push(trade.clone()),
            );
            if !b.state.perm_id_to_trade.contains_key(&perm_id) {
                b.state.trades.insert(OrderKey::Perm(perm_id), trade.clone());
                b.state.perm_id_to_trade.insert(perm_id, trade);
            }
        }),
        Callback::CompletedOrdersEnd => answer::<S, ()>(sink, Question::CompletedOrders, None),
        Callback::ExecDetails {
            req_id,
            contract,
            mut execution,
        } => {
            log::info!(target: LOG, "execDetails {execution:?}");
            if execution.order_id == i64::from(i32::MAX) {
                // executions of manual orders come with it unset
                execution.order_id = 0;
            }
            let (trade, fill, live, new) = sink.books(|b| {
                let s = &mut *b.state;
                let trade = s.perm_id_to_trade.get(&execution.perm_id).cloned().or_else(|| {
                    let key = order_key(execution.client_id, execution.order_id, execution.perm_id);
                    s.trades.get(&key).cloned()
                });
                let contract = match &trade {
                    Some(t) if t.read().contract == contract => t.read().contract.clone(),
                    _ => contract,
                };
                let live = !b.requests.is_request(req_id);
                let time = if live {
                    s.last_time.clone()
                } else {
                    execution.time.clone()
                };
                let fill = Fill {
                    contract,
                    execution,
                    commission_report: s.bind(CommissionReport::default()),
                    time,
                };
                let new = !s.fills.contains_key(&fill.execution.exec_id);
                if new {
                    s.fills
                        .insert(fill.execution.exec_id.clone(), fill.clone());
                    if let Some(t) = &trade {
                        let x = &fill.execution;
                        let message = format!("Fill {}@{}", py_repr(x.shares), py_repr(x.price));
                        t.update(|t| {
                            t.fills.push(fill.clone());
                            let status = t.order_status.status.clone();
                            t.log.push(TradeLogEntry {
                                time: fill.time.clone(),
                                status,
                                message,
                                error_code: 0,
                            });
                        });
                    }
                }
                (trade, fill, live, new)
            });
            if let (Some(trade), true, true) = (&trade, new, live) {
                log::info!(target: LOG, "execDetails: {fill:?}");
                sink.emit(Emit::ExecDetails(trade.clone(), fill.clone()));
                sink.emit(Emit::TradeFill(trade.clone(), fill.clone()));
            }
            if !live {
                collect(sink, req_id, fill);
            }
        }
        Callback::ExecDetailsEnd(req_id) => end_request(sink, req_id, None),
        Callback::CommissionReport(report) => {
            let found = sink.books(|b| {
                let fill = b.state.fills.get(&report.exec_id)?.clone();
                fill.commission_report.update(|r| *r = report);
                log::info!(target: LOG, "commissionReport: {:?}", fill.commission_report);
                let trade = b.state.perm_id_to_trade.get(&fill.execution.perm_id)?.clone();
                Some((trade, fill))
            });
            if let Some((trade, fill)) = found {
                let report = fill.commission_report.clone();
                sink.emit(Emit::CommissionReport(
                    trade.clone(),
                    fill.clone(),
                    report.clone(),
                ));
                sink.emit(Emit::TradeCommissionReport(trade, fill, report));
            }
        }

        Callback::UpdateAccountValue(v) => {
            sink.books(|b| {
                let key = (v.account.clone(), v.tag.clone(), v.currency.clone(), String::new());
                b.state.account_values.insert(key, v.clone());
            });
            sink.emit(Emit::AccountValue(v));
        }
        Callback::UpdatePortfolio(item) => {
            sink.books(|b| {
                let items = b.state.portfolio.entry(item.account.clone()).or_default();
                if item.position == 0.0 {
                    items.shift_remove(&item.contract.con_id);
                } else {
                    items.insert(item.contract.con_id, item.clone());
                }
            });
            log::info!(target: LOG, "updatePortfolio: {item:?}");
            sink.emit(Emit::UpdatePortfolio(item));
        }
        Callback::AccountDownloadEnd(_) => answer::<S, ()>(sink, Question::AccountUpdates, None),
        Callback::AccountSummary { value, .. } => {
            sink.books(|b| {
                let key = (value.account.clone(), value.tag.clone(), value.currency.clone());
                b.state.acct_summary.insert(key, value.clone());
            });
            sink.emit(Emit::AccountSummary(value));
        }
        Callback::AccountSummaryEnd(req_id) | Callback::AccountUpdateMultiEnd(req_id) => {
            end_request(sink, req_id, None);
        }
        Callback::AccountUpdateMulti { value, .. } => {
            sink.books(|b| {
                let key = (
                    value.account.clone(),
                    value.tag.clone(),
                    value.currency.clone(),
                    value.model_code.clone(),
                );
                b.state.account_values.insert(key, value.clone());
            });
            sink.emit(Emit::AccountValue(value));
        }
        Callback::Position(p) => {
            sink.books(|b| {
                let positions = b.state.positions.entry(p.account.clone()).or_default();
                if p.position == 0.0 {
                    positions.shift_remove(&p.contract.con_id);
                } else {
                    positions.insert(p.contract.con_id, p.clone());
                }
                log::info!(target: LOG, "position: {p:?}");
                b.requests
                    .accumulate::<Vec<Position>>(ReqKey::Question(Question::Positions), |v| {
                        v.push(p.clone());
                    });
            });
            sink.emit(Emit::Position(p));
        }
        Callback::PositionEnd => answer::<S, ()>(sink, Question::Positions, None),
        Callback::Pnl {
            req_id,
            daily_pnl,
            unrealized_pnl,
            realized_pnl,
        } => {
            let pnl = sink.books(|b| {
                let pnl = b.state.req_id_to_pnl.get(&req_id)?.clone();
                pnl.update(|p| {
                    p.daily_pnl = daily_pnl;
                    p.unrealized_pnl = unrealized_pnl;
                    p.realized_pnl = realized_pnl;
                });
                Some(pnl)
            });
            if let Some(pnl) = pnl {
                sink.emit(Emit::Pnl(pnl));
            }
        }
        Callback::PnlSingle {
            req_id,
            pos,
            daily_pnl,
            unrealized_pnl,
            realized_pnl,
            value,
        } => {
            let pnl = sink.books(|b| {
                let pnl = b.state.req_id_to_pnl_single.get(&req_id)?.clone();
                pnl.update(|p| {
                    p.position = pos;
                    p.daily_pnl = daily_pnl;
                    p.unrealized_pnl = unrealized_pnl;
                    p.realized_pnl = realized_pnl;
                    p.value = value;
                });
                Some(pnl)
            });
            if let Some(pnl) = pnl {
                sink.emit(Emit::PnlSingle(pnl));
            }
        }

        Callback::HistoricalData { req_id, bar } => sink.books(|b| {
            b.requests
                .accumulate::<Live<BarDataList>>(ReqKey::Id(req_id), |l| {
                    l.update(|l| l.bars.push(bar.clone()));
                });
        }),
        Callback::HistoricalDataEnd { req_id, .. }
        | Callback::ContractDetailsEnd(req_id)
        | Callback::SecurityDefinitionOptionParameterEnd(req_id)
        | Callback::HistoricalNewsEnd { req_id, .. } => end_request(sink, req_id, None),
        Callback::HistoricalDataUpdate { req_id, bar } => {
            let update = sink.books(|b| {
                let Some(Bars::Historical(list)) = b.state.req_id_to_subscriber.get(&req_id)
                else {
                    return None;
                };
                let list = list.clone();
                let new = list.update(|l| {
                    // an emptied list takes no update (wr:925-926)
                    let last = &l.bars.last()?.date;
                    match bar.date.partial_cmp(last) {
                        None => {
                            log::error!(target: LOG, "historicalDataUpdate: {:?} and {last:?} cannot be compared", bar.date);
                            None
                        }
                        Some(std::cmp::Ordering::Less) => None,
                        Some(std::cmp::Ordering::Greater) => {
                            l.bars.push(bar);
                            Some(true)
                        }
                        Some(std::cmp::Ordering::Equal) => {
                            let last = l.bars.last_mut()?;
                            if *last == bar {
                                None
                            } else {
                                *last = bar;
                                Some(false)
                            }
                        }
                    }
                })?;
                Some((list, new))
            });
            if let Some((list, new)) = update {
                sink.emit(Emit::BarUpdate(Bars::Historical(list.clone()), new));
                sink.emit(Emit::BarsUpdate(Bars::Historical(list), new));
            }
        }
        Callback::HeadTimestamp {
            req_id,
            head_timestamp,
        } => match parse_ib_datetime(&head_timestamp) {
            Ok(date) => end_request(sink, req_id, Some(Box::new(date))),
            Err(e) => {
                if let Some(mut x) = sink.books(|b| b.requests.end(req_id)) {
                    sink.settle(&mut x);
                    x.finish(Err(e));
                }
            }
        },
        Callback::HistoricalTicks {
            req_id,
            ticks,
            done,
        } => {
            sink.books(|b| {
                b.requests.accumulate::<Vec<_>>(ReqKey::Id(req_id), |v| {
                    v.extend(ticks.iter().cloned());
                });
            });
            if done {
                end_request(sink, req_id, None);
            }
        }
        Callback::HistoricalSchedule { req_id, schedule } => {
            end_request(sink, req_id, Some(Box::new(schedule)));
        }
        Callback::HistogramData { req_id, items } => {
            end_request(sink, req_id, Some(Box::new(items)));
        }
        Callback::ContractDetails { req_id, details } => collect(sink, req_id, details),
        Callback::SymbolSamples {
            req_id,
            descriptions,
        } => end_request(sink, req_id, Some(Box::new(descriptions))),
        Callback::SecurityDefinitionOptionParameter { req_id, chain } => {
            collect(sink, req_id, chain);
        }
        Callback::MarketRule {
            market_rule_id,
            price_increments,
        } => match i32::try_from(market_rule_id) {
            Ok(id) => answer(sink, Question::MarketRule(id), Some(price_increments)),
            Err(_) => {
                log::error!(target: "ib_async.Decoder", "Error for marketRule: id {market_rule_id} is out of range");
            }
        },
        Callback::FundamentalData { req_id, data } => {
            end_request(sink, req_id, Some(Box::new(data)));
        }
        Callback::ScannerParameters(xml) => answer(sink, Question::ScannerParameters, Some(xml)),
        Callback::ScannerData { req_id, data } => sink.books(|b| {
            let rank0 = data.rank == 0;
            let add = |l: &mut ScanDataList| {
                if rank0 {
                    l.data.clear();
                }
                l.data.push(data.clone());
            };
            match b.state.req_id_to_subscriber.get(&req_id) {
                Some(Bars::Scan(list)) => list.update(add),
                _ => b
                    .requests
                    .accumulate::<Live<ScanDataList>>(ReqKey::Id(req_id), |l| l.update(add)),
            }
        }),
        Callback::ScannerDataEnd(req_id) => {
            let (x, list) = sink.books(|b| {
                let mut x = b.requests.end(req_id);
                let collected = x
                    .as_mut()
                    .and_then(|x| x.acc.as_ref())
                    .and_then(|a| a.downcast_ref::<Live<ScanDataList>>())
                    .cloned();
                let list = collected.or_else(|| match b.state.req_id_to_subscriber.get(&req_id) {
                    Some(Bars::Scan(l)) => Some(l.clone()),
                    _ => None,
                });
                (x, list)
            });
            if let Some(x) = x {
                finish(sink, x, None);
            }
            if let Some(list) = list {
                sink.emit(Emit::ScannerData(list.clone()));
                sink.emit(Emit::ScanUpdate(list));
            }
        }

        Callback::NewsProviders(v) => answer(sink, Question::NewsProviders, Some(v)),
        Callback::NewsArticle { req_id, article } => {
            end_request(sink, req_id, Some(Box::new(article)));
        }
        Callback::HistoricalNews { req_id, news } => collect(sink, req_id, news),
        Callback::UpdateNewsBulletin(bulletin) => {
            sink.books(|b| {
                b.state
                    .msg_id_to_news_bulletin
                    .insert(bulletin.msg_id, bulletin.clone());
            });
            sink.emit(Emit::NewsBulletin(bulletin));
        }

        Callback::ReceiveFa { xml, .. } => answer(sink, Question::Fa, Some(xml)),
        Callback::WshMetaData { req_id, data_json } => {
            sink.emit(Emit::WshMeta(data_json.clone()));
            end_request(sink, req_id, Some(Box::new(data_json)));
        }
        Callback::WshEventData { req_id, data_json } => {
            sink.emit(Emit::Wsh(data_json.clone()));
            end_request(sink, req_id, Some(Box::new(data_json)));
        }
        // ib_async ends it with `[]`; the white-branding id is its answer
        // here (req_user_info).
        Callback::UserInfo {
            req_id,
            white_branding_id,
        } => end_request(sink, req_id, Some(Box::new(white_branding_id))),
    }
}

/// The paired size tick type of each price tick type a gateway sends with
/// its size (bid, ask, last, and their delayed forms).
fn size_of(price_type: i32) -> Option<i32> {
    match price_type {
        1 => Some(0),
        2 => Some(3),
        4 => Some(5),
        66 => Some(69),
        67 => Some(70),
        68 => Some(71),
        _ => None,
    }
}

fn price_of(size_type: i32) -> Option<i32> {
    [1, 2, 4, 66, 67, 68]
        .into_iter()
        .find(|p| size_of(*p) == Some(size_type))
}

/// A price as the engine states it. A gateway sends a price and its size
/// as one message, which ib_async's decoder hands over as one
/// `priceSizeTick`; the engine states them apart, so a price that has a
/// size waits for the size this read states beside it. A price of 0 is not
/// delivered, as ib_async's decoder drops it.
fn tick_price<S: Sink>(sink: &mut S, req_id: i64, tick_type: i32, price: f64) {
    if price == 0.0 {
        return;
    }
    if size_of(tick_type).is_some() {
        // a second price of the side before its size goes with the first's
        if let Some(first) = sink.books(|b| b.state.priced.insert((req_id, tick_type), price)) {
            price_alone(sink, req_id, tick_type, first);
        }
    } else {
        price_size_tick(sink, req_id, tick_type, price, 0.0);
    }
}

/// A price no size followed in its read: with its side's size standing.
fn price_alone<S: Sink>(sink: &mut S, req_id: i64, tick_type: i32, price: f64) {
    let size = sink.books(|b| {
        let t = b.state.req_id_to_ticker.get(&req_id)?.read();
        Some(match tick_type {
            1 | 66 => t.bid_size,
            2 | 67 => t.ask_size,
            _ => t.last_size,
        })
    });
    // An unknown id is logged by price_size_tick.
    price_size_tick(sink, req_id, tick_type, price, size.unwrap_or(0.0));
}

/// ib_async's `priceSizeTick` (wr:980-1049).
fn price_size_tick<S: Sink>(sink: &mut S, req_id: i64, tick_type: i32, price: f64, size: f64) {
    let tick = sink.books(|b| {
        let Some(t) = b.state.req_id_to_ticker.get(&req_id).cloned() else {
            log::error!(target: LOG, "priceSizeTick: Unknown reqId: {req_id}");
            return None;
        };
        let d = b.state.defaults.clone();
        let (mut price, mut size) = (price, size);
        let known = t.update(|t| {
            match tick_type {
                1 | 66 => {
                    if size == 0.0 {
                        (price, size) = (d.empty_price, d.empty_size);
                    }
                    t.prev_bid = t.bid;
                    t.prev_bid_size = t.bid_size;
                    t.bid = price;
                    t.bid_size = size;
                }
                2 | 67 => {
                    if size == 0.0 {
                        (price, size) = (d.empty_price, d.empty_size);
                    }
                    t.prev_ask = t.ask;
                    t.prev_ask_size = t.ask_size;
                    t.ask = price;
                    t.ask_size = size;
                }
                4 | 68 => {
                    // TICK-NYSE states -1 with no size and never a close
                    if price == -1.0 && size == 0.0 && t.close > 0.0 {
                        (price, size) = (d.empty_price, d.empty_size);
                    }
                    t.prev_last = t.last;
                    t.prev_last_size = t.last_size;
                    t.last = price;
                    t.last_size = size;
                }
                _ => match price_field(t, tick_type) {
                    Some(field) => *field = price,
                    None => return false,
                },
            }
            true
        });
        if !known {
            log::error!(target: LOG, "Received tick tickType={tick_type} price={price} but we don't have an attribute mapping for it");
            return None;
        }
        let tick = append_tick(b.state, &t, tick_type, price, size);
        b.state.pending(&t);
        tick.map(|tick| (t, tick))
    });
    if let Some((t, tick)) = tick {
        sink.emit(Emit::Tick(t, tick));
    }
}

/// Appends a `TickData` when it states a price or a size (`if price or
/// size`, which NaN passes as Python's truth does).
fn append_tick(s: &State, t: &Live<Ticker>, tick_type: i32, price: f64, size: f64) -> Option<Tick> {
    if price == 0.0 && size == 0.0 {
        return None;
    }
    let tick = TickData {
        time: s.last_time.clone(),
        tick_type,
        price,
        size,
    };
    t.update(|t| t.ticks.push(tick.clone()));
    Some(Tick::Data(tick))
}

/// ib_async's `tickSize` (wr:1051-1106).
fn tick_size<S: Sink>(sink: &mut S, req_id: i64, tick_type: i32, size: f64) {
    let tick = sink.books(|b| {
        let Some(t) = b.state.req_id_to_ticker.get(&req_id).cloned() else {
            log::error!(target: LOG, "tickSize: Unknown reqId: {req_id}");
            return None;
        };
        let d = b.state.defaults.clone();
        // None: nothing more to do; Some(price): the tick's price.
        let price = t.update(|t| match tick_type {
            0 | 69 => {
                if size == t.bid_size {
                    return None;
                }
                t.prev_bid_size = t.bid_size;
                if size == 0.0 {
                    t.bid = d.empty_price;
                    t.bid_size = d.empty_size;
                    Some(Some(d.empty_price))
                } else {
                    t.bid_size = size;
                    Some(Some(t.bid))
                }
            }
            3 | 70 => {
                if size == t.ask_size {
                    return None;
                }
                t.prev_ask_size = t.ask_size;
                if size == 0.0 {
                    t.ask = d.empty_price;
                    t.ask_size = d.empty_size;
                    Some(Some(d.empty_price))
                } else {
                    t.ask_size = size;
                    Some(Some(t.ask))
                }
            }
            5 | 71 => {
                let price = t.last;
                if t.is_unset(price) {
                    return None;
                }
                if size != t.last_size {
                    t.prev_last_size = t.last_size;
                    t.last_size = size;
                }
                Some(Some(price))
            }
            _ => match size_field(t, tick_type) {
                Some(field) => {
                    *field = size;
                    Some(Some(d.empty_price))
                }
                None => Some(None),
            },
        })?;
        let Some(price) = price else {
            log::error!(target: LOG, "Received tick tickType={tick_type} size={size} but we don't have an attribute mapping for it");
            return None;
        };
        let tick = append_tick(b.state, &t, tick_type, price, size);
        b.state.pending(&t);
        tick.map(|tick| (t, tick))
    });
    if let Some((t, tick)) = tick {
        sink.emit(Emit::Tick(t, tick));
    }
}

/// ib_async's `tickString` (wr:1197-1253).
fn tick_string<S: Sink>(sink: &mut S, req_id: i64, tick_type: i32, value: &str) {
    let tick = sink.books(|b| {
        let t = b.state.req_id_to_ticker.get(&req_id)?.clone();
        let tz = b.state.defaults.timezone.clone();
        let malformed = || {
            log::error!(target: LOG, "tickString with tickType {tick_type}: malformed value: {value:?}");
        };
        let mut tick = None;
        match tick_type {
            25 | 26 | 32 | 33 | 84 | 85 | 91 | 100 => t.update(|t| {
                let field = match tick_type {
                    25 => &mut t.option_bid_exch,
                    26 => &mut t.option_ask_exch,
                    32 => &mut t.bid_exchange,
                    33 => &mut t.ask_exchange,
                    84 => &mut t.last_exchange,
                    85 => &mut t.last_reg_time,
                    91 => &mut t.reuters_mutual_funds,
                    _ => &mut t.social_market_analytics,
                };
                *field = value.to_owned();
            }),
            45 | 88 => {
                let Some(stamp) = py_int(value) else {
                    malformed();
                    return None;
                };
                // "last trade: 20,000 days ago" is not reported
                if stamp != 0 {
                    let Ok(at) = Timestamp::from_second(stamp) else {
                        malformed();
                        return None;
                    };
                    let at = at.to_zoned(tz);
                    t.update(|t| {
                        if tick_type == 45 {
                            t.last_timestamp = Some(at);
                        } else {
                            t.delayed_last_timestamp = Some(at);
                        }
                    });
                }
            }
            47 => {
                let mut ratios = IndexMap::new();
                for part in value.split(';').filter(|p| !p.is_empty()) {
                    let kv: Vec<&str> = part.split('=').collect();
                    let [k, v] = kv[..] else {
                        malformed();
                        return None;
                    };
                    let v = if v == "-99999.99" { "nan" } else { v };
                    ratios.insert(k.to_owned(), py_number(v));
                }
                t.update(|t| t.fundamental_ratios = Some(ratios));
            }
            48 | 77 => {
                let parts: Vec<&str> = value.split(';').collect();
                let [price, size, rt_time, volume, vwap, _] = parts[..] else {
                    malformed();
                    return None;
                };
                if !volume.is_empty() {
                    let Some(volume) = py_float(volume) else {
                        malformed();
                        return None;
                    };
                    t.update(|t| {
                        if tick_type == 48 {
                            t.rt_volume = volume;
                        } else {
                            t.rt_trade_volume = volume;
                        }
                    });
                }
                if !vwap.is_empty() {
                    let Some(vwap) = py_float(vwap) else {
                        malformed();
                        return None;
                    };
                    t.update(|t| t.vwap = vwap);
                }
                if !rt_time.is_empty() {
                    let Some(at) = py_int(rt_time)
                        .and_then(|ms| Timestamp::from_millisecond(ms).ok())
                    else {
                        malformed();
                        return None;
                    };
                    t.update(|t| t.rt_time = Some(at.to_zoned(tz)));
                }
                if !price.is_empty() {
                    let (Some(price), Some(size)) = (py_float(price), py_float(size)) else {
                        malformed();
                        return None;
                    };
                    let data = TickData {
                        time: b.state.last_time.clone(),
                        tick_type,
                        price,
                        size,
                    };
                    t.update(|t| {
                        t.prev_last = t.last;
                        t.prev_last_size = t.last_size;
                        t.last = price;
                        t.last_size = size;
                        t.ticks.push(data.clone());
                    });
                    tick = Some(Tick::Data(data));
                }
            }
            59 => {
                let parts: Vec<&str> = value.split(',').collect();
                let [past, next, date, amount] = parts[..] else {
                    malformed();
                    return None;
                };
                let number = |s: &str| if s.is_empty() { Ok(None) } else { py_float(s).map(Some).ok_or(()) };
                let day = |s: &str| -> std::result::Result<_, ()> {
                    if s.is_empty() {
                        return Ok(None);
                    }
                    Ok(Some(match parse_ib_datetime(s).map_err(|_| ())? {
                        BarDate::Day(d) => d,
                        BarDate::At(z) => z.date(),
                        BarDate::Naive(t) => t.date(),
                    }))
                };
                let (Ok(past), Ok(next), Ok(date), Ok(amount)) =
                    (number(past), number(next), day(date), number(amount))
                else {
                    malformed();
                    return None;
                };
                t.update(|t| {
                    t.dividends = Some(Dividends {
                        past_12_months: past,
                        next_12_months: next,
                        next_date: date,
                        next_amount: amount,
                    });
                });
            }
            _ => {
                log::error!(target: LOG, "tickString with tickType {tick_type}: unhandled value: {value:?}");
            }
        }
        b.state.pending(&t);
        tick.map(|tick| (t, tick))
    });
    if let Some((t, tick)) = tick {
        sink.emit(Emit::Tick(t, tick));
    }
}

/// ib_async's `tickGeneric` (wr:1255-1277).
fn tick_generic<S: Sink>(sink: &mut S, req_id: i64, tick_type: i32, value: f64) {
    let tick = sink.books(|b| {
        let t = b.state.req_id_to_ticker.get(&req_id)?.clone();
        let value = if value > 0.0 {
            value
        } else {
            b.state.defaults.empty_size
        };
        let known = t.update(|t| {
            let field = match tick_type {
                23 => &mut t.hist_volatility,
                24 => &mut t.implied_volatility,
                31 => &mut t.index_future_premium,
                46 => &mut t.shortable,
                49 => &mut t.halted,
                54 => &mut t.trade_count,
                55 => &mut t.trade_rate,
                56 => &mut t.volume_rate,
                58 => &mut t.rt_hist_volatility,
                60 => &mut t.bond_factor_multiplier,
                90 => &mut t.delayed_halted,
                _ => return false,
            };
            *field = value;
            true
        });
        if !known {
            log::error!(target: LOG, "Received tick tickType={tick_type} value={value} but we don't have an attribute mapping for it");
            return None;
        }
        let tick = TickData {
            time: b.state.last_time.clone(),
            tick_type,
            price: value,
            size: 0.0,
        };
        t.update(|t| t.ticks.push(tick.clone()));
        b.state.pending(&t);
        Some((t, Tick::Data(tick)))
    });
    if let Some((t, tick)) = tick {
        sink.emit(Emit::Tick(t, tick));
    }
}

/// ib_async's `updateMktDepthL2` (wr:1309-1367); the book's sides list
/// their levels in position order, where ib_async lists them in the order
/// they were added.
#[expect(clippy::too_many_arguments, reason = "the callback's own arguments")]
fn depth<S: Sink>(
    sink: &mut S,
    req_id: i64,
    position: i32,
    market_maker: String,
    operation: i32,
    side: i32,
    price: f64,
    size: f64,
) {
    let tick = sink.books(|b| {
        let Some(t) = b.state.req_id_to_ticker.get(&req_id).cloned() else {
            log::error!(target: LOG, "updateMktDepthL2: Unknown reqId: {req_id}");
            return None;
        };
        let (mut price, mut size) = (price, size);
        t.update(|t| {
            let dom = if side != 0 {
                &mut t.dom_bids_dict
            } else {
                &mut t.dom_asks_dict
            };
            match operation {
                0 | 1 => {
                    dom.insert(
                        position,
                        DOMLevel {
                            price,
                            size,
                            market_maker: market_maker.clone(),
                        },
                    );
                }
                2 => {
                    size = 0.0;
                    if let Some(level) = dom.remove(&position) {
                        price = level.price;
                    }
                }
                _ => {}
            }
            let values = dom.values().cloned().collect();
            if side != 0 {
                t.dom_bids = values;
            } else {
                t.dom_asks = values;
            }
        });
        let tick = MktDepthData {
            time: b.state.last_time.clone(),
            position,
            market_maker,
            operation,
            side,
            price,
            size,
        };
        t.update(|t| t.dom_ticks.push(tick.clone()));
        b.state.pending(&t);
        Some((t, Tick::Depth(tick)))
    });
    if let Some((t, tick)) = tick {
        sink.emit(Emit::Tick(t, tick));
    }
}

/// The codes ib_async takes as warnings (wr:1609-1610).
fn is_warning_code(code: i64) -> bool {
    matches!(
        code,
        105 | 110 | 165 | 321 | 329 | 399 | 404 | 434 | 492 | 10167 | 2100..=2199
    )
}

/// ib_async's `error` (wr:1580-1723), by the origin the engine gives:
/// a refused modify is a warning, a refused placement or exercise an error,
/// a request's end is its origin's to state, and the rest is by code.
fn error<S: Sink>(sink: &mut S, origin: ErrorOrigin, code: i64, message: String, json: String) {
    let req_id = origin.id();
    let raise = sink.raise_request_errors();
    let (trade, contract, refused, warning) = sink.books(|b| {
        let registered = req_id != -1 && b.requests.is_request(req_id);
        let trade = if req_id != -1 {
            b.state.own_trade(req_id)
        } else {
            None
        };
        let mut warning = is_warning_code(code);
        if code == 110 && registered {
            // a what-if refused
            warning = false;
        }
        if code == 110
            && trade
                .as_ref()
                .is_some_and(|t| t.read().order_status.status == OrderStatus::PENDING_SUBMIT)
        {
            // an invalid price cancels a new order
            warning = false;
        }
        match origin {
            ErrorOrigin::Order {
                op: OrderOp::Modify,
                ..
            } => warning = true,
            ErrorOrigin::Order {
                op: OrderOp::Place | OrderOp::Exercise,
                ..
            } => warning = false,
            // A request's error ends it when the engine says so, and a
            // notice its answer follows ends nothing.
            ErrorOrigin::Request { ends, .. } if registered => warning = !ends,
            _ => {}
        }
        let refused = match origin {
            ErrorOrigin::Request { .. } | ErrorOrigin::Question { .. } => b.requests.error(origin),
            ErrorOrigin::Order { id, .. } if registered && !warning => b
                .requests
                .end(id)
                .map_or(Refused::Nothing, Refused::Request),
            _ => Refused::Nothing,
        };
        let contract = b.state.req_id_to_contract.get(&req_id).cloned();
        (trade, contract, refused, warning)
    });

    let mut msg = format!(
        "{} {code}, reqId {req_id}: {message}",
        if warning { "Warning" } else { "Error" }
    );
    if let Some(c) = &contract {
        msg.push_str(&format!(", contract: {c:?}"));
    }
    let error_code = i32::try_from(code).unwrap_or(i32::MAX);
    if let Refused::Question(Some(ask)) = &refused {
        sink.call(Call::Ask(ask.clone()));
    }
    let entry = |state: &State, status: &str| TradeLogEntry {
        time: state.last_time.clone(),
        status: status.to_owned(),
        message: msg.clone(),
        error_code,
    };
    if let Refused::Request(mut x) = refused {
        // only an error ends a request
        log::error!(target: LOG, "{msg}");
        sink.settle(&mut x);
        let e = Error::Request {
            req_id,
            code,
            message: message.clone(),
        };
        x.fail(e, raise);
    } else if warning {
        // the order is still live at the broker
        if let Some(trade) = &trade {
            sink.books(|b| {
                let entry = entry(b.state, OrderStatus::VALIDATION_ERROR);
                trade.update(|t| {
                    t.order_status.status = OrderStatus::VALIDATION_ERROR.into();
                    t.log.push(entry);
                });
            });
            log::warn!(target: LOG, "IBKR API validation warning: {trade:?}");
            sink.emit(Emit::OrderStatus(trade.clone()));
            sink.emit(Emit::TradeStatus(trade.clone()));
        } else {
            log::info!(target: LOG, "{msg}");
        }
    } else {
        // an order rejected, or ended by the broker: cancelled unless done
        log::error!(target: LOG, "{msg}");
        if let Some(trade) = &trade {
            let cancelled = sink.books(|b| {
                let entry = entry(b.state, OrderStatus::CANCELLED);
                trade.update(|t| {
                    if !json.is_empty() {
                        t.advanced_error = json.clone();
                    }
                    if t.is_done() {
                        return false;
                    }
                    t.order_status.status = OrderStatus::CANCELLED.into();
                    t.log.push(entry);
                    true
                })
            });
            if cancelled {
                log::warn!(target: LOG, "Canceled order: {trade:?}");
                sink.emit(Emit::OrderStatus(trade.clone()));
                sink.emit(Emit::TradeStatus(trade.clone()));
                sink.emit(Emit::TradeCancelled(trade.clone()));
            }
        }
    }

    match code {
        165 => {
            // the scan has no more matching results
            let list = sink.books(|b| match b.state.req_id_to_subscriber.get(&req_id) {
                Some(Bars::Scan(l)) if !l.read().data.is_empty() => {
                    l.update(|l| l.data.clear());
                    Some(l.clone())
                }
                _ => None,
            });
            if let Some(list) = list {
                sink.emit(Emit::ScanUpdate(list));
            }
        }
        317 => {
            // the book was reset: each level leaves as a deletion, the asks
            // first, each appended record reported as it is appended
            for side in [0, 1] {
                let ticks = sink.books(|b| {
                    let t = b.state.req_id_to_ticker.get(&req_id)?.clone();
                    let time = b.state.last_time.clone();
                    let r = t.read();
                    let levels = if side == 0 { &r.dom_asks } else { &r.dom_bids };
                    let gone: Vec<MktDepthData> = levels
                        .iter()
                        .map(|l| MktDepthData {
                            time: time.clone(),
                            position: 0,
                            market_maker: String::new(),
                            operation: 2,
                            side,
                            price: l.price,
                            size: 0.0,
                        })
                        .collect();
                    Some((t.clone(), gone))
                });
                let Some((t, gone)) = ticks else { break };
                for tick in gone {
                    sink.books(|_| t.update(|t| t.dom_ticks.push(tick.clone())));
                    sink.emit(Emit::Tick(t.clone(), Tick::Depth(tick)));
                }
            }
            sink.books(|b| {
                let t = b.state.req_id_to_ticker.get(&req_id)?.clone();
                t.update(|t| {
                    t.dom_asks.clear();
                    t.dom_bids.clear();
                    t.dom_bids_dict.clear();
                    t.dom_asks_dict.clear();
                });
                b.state.pending(&t);
                Some(())
            });
        }
        10225 => {
            // a bust: the subscription is resubscribed at once
            match sink.books(|b| b.state.req_id_to_subscriber.get(&req_id).cloned()) {
                Some(Bars::RealTime(list)) => {
                    sink.call(Call::CancelRealTimeBars(req_id));
                    sink.call(Call::ReqRealTimeBars(list));
                }
                Some(Bars::Historical(list)) => {
                    sink.call(Call::CancelHistoricalData(req_id));
                    sink.call(Call::ReqHistoricalData(list));
                }
                _ => {}
            }
        }
        _ => {}
    }
    sink.emit(Emit::Error(req_id, code, message, contract));
}

/// A price tick type's field in `PRICE_TICK_MAP` (wr:82-117).
fn price_field(t: &mut Ticker, tick_type: i32) -> Option<&mut f64> {
    Some(match tick_type {
        6 | 72 => &mut t.high,
        7 | 73 => &mut t.low,
        9 | 75 => &mut t.close,
        14 | 76 => &mut t.open,
        15 => &mut t.low_13_week,
        16 => &mut t.high_13_week,
        17 => &mut t.low_26_week,
        18 => &mut t.high_26_week,
        19 => &mut t.low_52_week,
        20 => &mut t.high_52_week,
        35 => &mut t.auction_price,
        37 => &mut t.mark_price,
        50 | 103 => &mut t.bid_yield,
        51 | 104 => &mut t.ask_yield,
        52 => &mut t.last_yield,
        57 => &mut t.last_rth_trade,
        78 => &mut t.creditman_mark_price,
        79 => &mut t.creditman_slow_mark_price,
        92 => &mut t.etf_nav_close,
        93 => &mut t.etf_nav_prior_close,
        94 => &mut t.etf_nav_bid,
        95 => &mut t.etf_nav_ask,
        96 => &mut t.etf_nav_last,
        97 => &mut t.etf_frozen_nav_last,
        98 => &mut t.etf_nav_high,
        99 => &mut t.etf_nav_low,
        101 => &mut t.estimated_ipo_midpoint,
        102 => &mut t.final_ipo_last,
        _ => return None,
    })
}

/// A size tick type's field in `SIZE_TICK_MAP` (wr:120-138).
fn size_field(t: &mut Ticker, tick_type: i32) -> Option<&mut f64> {
    Some(match tick_type {
        8 | 74 => &mut t.volume,
        63 => &mut t.volume_rate_3_min,
        64 => &mut t.volume_rate_5_min,
        65 => &mut t.volume_rate_10_min,
        21 => &mut t.av_volume,
        22 => &mut t.open_interest,
        27 => &mut t.call_open_interest,
        28 => &mut t.put_open_interest,
        29 => &mut t.call_volume,
        30 => &mut t.put_volume,
        34 => &mut t.auction_volume,
        36 => &mut t.auction_imbalance,
        61 => &mut t.regulatory_imbalance,
        86 => &mut t.futures_open_interest,
        87 => &mut t.av_option_volume,
        89 => &mut t.shortable_shares,
        _ => return None,
    })
}

/// Python's `repr` of a float, as an f-string writes it.
fn py_repr(v: f64) -> String {
    if v.is_nan() {
        return "nan".into();
    }
    let s = format!("{v:?}");
    match s.split_once('e') {
        Some((mantissa, exp)) => {
            let (sign, digits) = exp.strip_prefix('-').map_or(("+", exp), |d| ("-", d));
            format!("{mantissa}e{sign}{digits:0>2}")
        }
        None => s,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll, Waker};

    use super::*;
    use crate::live::tests::FakeIb;
    use crate::order::{Order, OrderState};
    use crate::pending::{Pending, Token};
    use crate::requests::IbId;

    /// A sink that keeps what a callback did: each emission noted as it
    /// runs, each engine call, and each execution settled.
    pub(crate) struct Recorder<N> {
        pub(crate) state: State,
        pub(crate) requests: Requests,
        pub(crate) ids: IdSpace,
        /// How an emission is noted, at the moment it runs.
        pub(crate) note: fn(&Emit) -> N,
        pub(crate) log: Vec<N>,
        pub(crate) calls: Vec<Call>,
        pub(crate) settled: Vec<Token>,
        pub(crate) raise: bool,
        /// Kept alive so the objects made here have a holder.
        _ib: Arc<FakeIb>,
    }

    impl<N> Recorder<N> {
        /// A connected session of client 1, its clock at `now`.
        pub(crate) fn new(note: fn(&Emit) -> N, now: Zoned) -> Self {
            let ib = FakeIb::new();
            let mut state = State::new(IBDefaults::default(), ib.holder(), now);
            state.client_id = 1;
            let mut requests = Requests::new(IbId::new());
            requests.begin(1);
            Recorder {
                state,
                requests,
                ids: IdSpace::new(),
                note,
                log: Vec::new(),
                calls: Vec::new(),
                settled: Vec::new(),
                raise: false,
                _ib: ib,
            }
        }
    }

    impl<N> Sink for Recorder<N> {
        fn books<R>(&mut self, f: impl FnOnce(&mut Books<'_>) -> R) -> R {
            f(&mut Books {
                state: &mut self.state,
                requests: &mut self.requests,
                ids: &mut self.ids,
            })
        }
        fn emit(&mut self, e: Emit) {
            self.log.push((self.note)(&e));
        }
        fn call(&mut self, c: Call) {
            self.calls.push(c);
        }
        fn settle(&mut self, x: &mut Exec) {
            self.settled.push(x.token);
        }
        fn connected(&self) -> bool {
            true
        }
        fn raise_request_errors(&self) -> bool {
            self.raise
        }
    }

    fn at(secs: i64) -> Zoned {
        Timestamp::from_second(secs)
            .expect("a time")
            .to_zoned(jiff::tz::TimeZone::UTC)
    }

    /// An emission's name, with the status of its trade as it ran.
    fn name(e: &Emit) -> String {
        let status = |t: &Live<Trade>| t.read().order_status.status.clone();
        match e {
            Emit::OrderStatus(t) => format!("order_status_event {}", status(t)),
            Emit::TradeStatus(t) => format!("status_event {}", status(t)),
            Emit::TradeCancelled(t) => format!("cancelled_event {}", status(t)),
            Emit::OpenOrder(_) => "open_order_event".into(),
            Emit::Error(id, code, ..) => format!("error_event {id} {code}"),
            Emit::Update => "update_event".into(),
            other => format!("{other:?}"),
        }
    }

    fn published<T>(p: &mut Pending<T>) -> Option<Result<T>> {
        match Pin::new(p).poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(r) => Some(r),
            Poll::Pending => None,
        }
    }

    /// A numbered request `id` whose result collects into `acc`, if any.
    fn request<T: Send + 'static>(r: &mut Recorder<String>, id: i64, acc: Option<T>) -> Pending<T> {
        let (p, reply) = Pending::new(None);
        let mut x = r.requests.exec(ReqKey::Id(id));
        x.waiter = Some(Box::new(reply));
        x.acc = acc.map(|a| Box::new(a) as Box<dyn Any + Send>);
        r.requests.insert(x);
        p
    }

    /// A trade of this session, numbered `id`, at `status`.
    fn trade(r: &mut Recorder<String>, id: i64, status: &str) -> Live<Trade> {
        let t = Live::new(Trade {
            order_status: OrderStatus {
                order_id: id,
                status: status.into(),
                ..OrderStatus::default()
            },
            ..Trade::default()
        });
        let key = OrderKey::Order {
            client_id: 1,
            order_id: id,
        };
        r.state.trades.insert(key, t.clone());
        t
    }

    fn error(origin: ErrorOrigin, code: i64) -> Callback {
        Callback::Error {
            origin,
            code,
            message: "m".into(),
            advanced_order_reject_json: String::new(),
        }
    }

    #[test]
    fn a_price_goes_with_the_size_its_read_states_beside_it() {
        let mut r = Recorder::new(
            |e| match e {
                Emit::Tick(_, Tick::Data(d)) => format!("{} {} {}", d.tick_type, d.price, d.size),
                Emit::Update => "update_event".into(),
                _ => String::new(),
            },
            at(0),
        );
        let stock = Contract {
            con_id: 1,
            ..Contract::default()
        };
        r.state
            .start_ticker(1, &stock, "mktData")
            .expect("hashable");
        let price = |tick_type, price| Callback::TickPrice {
            req_id: 1,
            tick_type,
            price,
        };
        let size = |tick_type, size| Callback::TickSize {
            req_id: 1,
            tick_type,
            size,
        };
        pass(
            &mut r,
            vec![
                price(1, 185.0),
                price(2, 186.0),
                price(9, 183.5),
                price(4, 0.0),
                size(0, 300.0),
                size(0, 400.0),
                size(3, 200.0),
            ],
            at(1),
        );
        // A price whose size this read does not state goes at its end, with
        // the size standing; one of 0 is not delivered.
        pass(&mut r, vec![price(2, 187.0), size(8, 5.0)], at(2));
        assert_eq!(
            r.log.iter().filter(|l| !l.is_empty()).collect::<Vec<_>>(),
            [
                "9 183.5 0",
                "1 185 300",
                "0 185 400",
                "2 186 200",
                "update_event",
                "8 -1 5",
                "2 187 200",
                "update_event",
            ]
        );
        let t = r.state.req_id_to_ticker[&1].read();
        assert_eq!(
            (t.prev_bid_size, t.bid_size, t.prev_ask, t.ask),
            (300.0, 400.0, 186.0, 187.0)
        );
    }

    #[test]
    fn a_pass_ends_with_update_event_then_each_ticker_stamped_before_its_own() {
        /// Runs each ticker's own event, as the owner does.
        struct Owner(Recorder<String>);
        impl Sink for Owner {
            fn books<R>(&mut self, f: impl FnOnce(&mut Books<'_>) -> R) -> R {
                self.0.books(f)
            }
            fn emit(&mut self, e: Emit) {
                if let Emit::TickerUpdate(t) = &e {
                    t.update_event().emit(t);
                }
                self.0.emit(e);
            }
            fn call(&mut self, c: Call) {
                self.0.call(c);
            }
            fn settle(&mut self, x: &mut Exec) {
                self.0.settle(x);
            }
            fn connected(&self) -> bool {
                true
            }
            fn raise_request_errors(&self) -> bool {
                false
            }
        }
        let stamp = |t: &Live<Ticker>| t.read().timestamp.unwrap_or(-1.0);
        let note = |e: &Emit| match e {
            Emit::Update => "update_event".into(),
            Emit::TickerUpdate(t) => format!("ticker {}", t.read().timestamp.unwrap_or(-1.0)),
            Emit::PendingTickers(v) => format!("pending {}", v.len()),
            _ => String::new(),
        };
        let mut r = Owner(Recorder::new(note, at(0)));
        let mut tickers = Vec::new();
        for id in [2, 1] {
            let c = Contract {
                con_id: id,
                ..Contract::default()
            };
            tickers.push(r.0.state.start_ticker(id, &c, "mktData").expect("hashable"));
        }
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        for t in &tickers {
            let (seen, all) = (seen.clone(), tickers.clone());
            t.update_event().connect(move |_| {
                let stamps: Vec<f64> = all.iter().map(stamp).collect();
                seen.lock().expect("a lock").push(stamps);
            });
        }
        let tick = |req_id| Callback::TickGeneric {
            req_id,
            tick_type: 24,
            value: 0.2,
        };
        crate::event::set_on_owner(true);
        pass(&mut r, vec![tick(2), tick(1), tick(2)], at(10));
        crate::event::set_on_owner(false);
        r.0.log.retain(|l| !l.is_empty());
        // In the order the tickers became pending, each stamped just before
        // its own event, and all after update_event.
        assert_eq!(
            r.0.log,
            ["update_event", "ticker 10", "ticker 10", "pending 2"]
        );
        assert_eq!(*seen.lock().expect("a lock"), [[10.0, -1.0], [10.0, 10.0]]);
    }

    #[test]
    fn a_read_of_retirements_alone_is_no_pass() {
        let mut r = Recorder::new(name, at(0));
        r.requests.ask(Ask::Positions, None);
        r.requests.cancel(Question::Positions);
        let (p, reply) = Pending::<()>::new(None);
        let mut x = r.requests.exec(Ask::Positions.key());
        x.waiter = Some(Box::new(reply));
        assert_eq!(r.requests.ask(Ask::Positions, Some(x)), None);
        drop(p);

        assert!(!pass(
            &mut r,
            vec![Callback::QuestionRetired(Question::Positions)],
            at(5)
        ));
        assert!(r.log.is_empty(), "no update_event: {:?}", r.log);
        assert_eq!(r.state.time, -1.0, "the arrival is not taken");
        assert!(matches!(r.calls[..], [Call::Ask(Ask::Positions)]));

        // With a gateway's callback beside it, one pass.
        r.requests.cancel(Question::Positions);
        let news = Callback::TickNews {
            req_id: 1,
            news: NewsTick {
                time_stamp: 1,
                provider_code: "p".into(),
                article_id: "a".into(),
                headline: "h".into(),
                extra_data: String::new(),
            },
        };
        pass(
            &mut r,
            vec![Callback::QuestionRetired(Question::Positions), news],
            at(6),
        );
        assert_eq!(r.log.iter().filter(|l| *l == "update_event").count(), 1);
        assert_eq!(r.state.time, 6.0);
        assert!(pass(&mut r, vec![Callback::ConnectionClosed], at(7)));
    }

    #[test]
    fn an_orders_error_takes_the_path_its_origin_names() {
        let order = |id, op| ErrorOrigin::Order { id, op };
        // (the operation, code, status before) → status after, events
        let cases = [
            // A refused placement is an error even at 321; a refused
            // modify a warning, the order still live.
            (OrderOp::Place, 321, "PendingSubmit", "Cancelled", true),
            (OrderOp::Modify, 321, "Submitted", "ValidationError", false),
            (OrderOp::Modify, 201, "Submitted", "ValidationError", false),
            // The venue's word by ib_async's code: 110 cancels a new order
            // and warns on a working one.
            (OrderOp::Venue, 110, "PendingSubmit", "Cancelled", true),
            (OrderOp::Venue, 110, "Submitted", "ValidationError", false),
            (OrderOp::Venue, 202, "Submitted", "Cancelled", true),
            (OrderOp::Cancel, 399, "Submitted", "ValidationError", false),
        ];
        for (op, code, before, after, cancelled) in cases {
            let mut r = Recorder::new(name, at(0));
            let t = trade(&mut r, 7, before);
            pass(&mut r, vec![error(order(7, op), code)], at(1));
            let mut want = vec![
                format!("order_status_event {after}"),
                format!("status_event {after}"),
            ];
            if cancelled {
                want.push(format!("cancelled_event {after}"));
            }
            want.extend([format!("error_event 7 {code}"), "update_event".into()]);
            assert_eq!(r.log, want, "{op:?} {code} on {before}");
            let log = &t.read().log;
            assert_eq!(log.len(), 1);
            assert_eq!(i64::from(log[0].error_code), code);
        }
        // A done order keeps its status, and the rejection's JSON.
        let mut r = Recorder::new(name, at(0));
        let t = trade(&mut r, 7, "Filled");
        let json = Callback::Error {
            origin: order(7, OrderOp::Venue),
            code: 201,
            message: "m".into(),
            advanced_order_reject_json: "{}".into(),
        };
        pass(&mut r, vec![json], at(1));
        assert_eq!(
            (
                t.read().order_status.status.as_str(),
                t.read().advanced_error.as_str()
            ),
            ("Filled", "{}")
        );
        assert_eq!(r.log, ["error_event 7 201", "update_event"]);

        // By origin, whatever the number: an order numbered in the
        // engine's reserved band, or past any id, is that order's, and one
        // not known reaches error_event alone.
        for id in [0xC000_0000, 0xC000_0001, i64::from(u32::MAX) - 1, 1 << 33] {
            for known in [true, false] {
                let mut r = Recorder::new(name, at(0));
                let t = known.then(|| trade(&mut r, id, "Submitted"));
                pass(&mut r, vec![error(order(id, OrderOp::Venue), 202)], at(1));
                let mut want = vec![];
                if known {
                    want = vec![
                        "order_status_event Cancelled".to_owned(),
                        "status_event Cancelled".into(),
                        "cancelled_event Cancelled".into(),
                    ];
                }
                want.extend([format!("error_event {id} 202"), "update_event".into()]);
                assert_eq!(r.log, want, "{id} known {known}");
                if let Some(t) = t {
                    assert!(t.read().is_done());
                }
                // Under a request's origin it ends nothing and is kept.
                let mut r = Recorder::new(name, at(0));
                pass(
                    &mut r,
                    vec![error(ErrorOrigin::Request { id, ends: true }, 200)],
                    at(1),
                );
                assert_eq!(
                    r.log,
                    [format!("error_event {id} 200"), "update_event".into()]
                );
            }
        }
    }

    #[test]
    fn a_requests_error_ends_it_only_as_its_origin_says() {
        let mut r = Recorder::new(name, at(0));
        // A notice its answer follows ends nothing, whatever its code.
        let mut p = request::<Vec<Fill>>(&mut r, 5, Some(Vec::new()));
        pass(
            &mut r,
            vec![error(ErrorOrigin::Request { id: 5, ends: false }, 200)],
            at(1),
        );
        assert!(published(&mut p).is_none());
        assert!(r.requests.is_request(5));
        // A refusal ends it, even at a warning's code; without
        // raise_request_errors a collection ends with what arrived.
        pass(
            &mut r,
            vec![error(ErrorOrigin::Request { id: 5, ends: true }, 321)],
            at(1),
        );
        assert!(matches!(published(&mut p), Some(Ok(v)) if v.is_empty()));
        assert_eq!(r.settled.len(), 1);

        // 110 on a what-if ends it with the error: its answer is one value.
        let mut p = request::<OrderState>(&mut r, 6, None);
        let place = ErrorOrigin::Order {
            id: 6,
            op: OrderOp::Venue,
        };
        pass(&mut r, vec![error(place, 110)], at(1));
        match published(&mut p) {
            Some(Err(Error::Request { req_id, code, .. })) => assert_eq!((req_id, code), (6, 110)),
            other => unreachable!("{other:?}"),
        }

        // A question's error that ends its exchange completes no waiter,
        // and the lane's next exchange is sent.
        let mut r = Recorder::new(name, at(0));
        let mut first = None;
        for _ in 0..2 {
            let (p, reply) = Pending::<Vec<Position>>::new(None);
            let mut x = r
                .requests
                .exec(Ask::CompletedOrders { api_only: true }.key());
            x.waiter = Some(Box::new(reply));
            r.requests.ask(
                Ask::CompletedOrders {
                    api_only: first.is_none(),
                },
                Some(x),
            );
            first.get_or_insert(p);
        }
        let q = ErrorOrigin::Question {
            q: Question::CompletedOrders,
            ends: true,
        };
        pass(&mut r, vec![error(q, 200)], at(1));
        assert!(published(first.as_mut().expect("asked")).is_none());
        assert!(matches!(
            r.calls[..],
            [Call::Ask(Ask::CompletedOrders { api_only: false })]
        ));
        assert_eq!(r.log, ["error_event -1 200", "update_event"]);
    }

    #[test]
    fn an_open_order_is_the_answers_while_an_ib_asked_for_open_orders() {
        let open = |order_id| Callback::OpenOrder {
            order_id,
            contract: Contract::default(),
            order: Order {
                order_id,
                client_id: 2,
                perm_id: 900 + order_id,
                ..Order::default()
            },
            order_state: OrderState::default(),
        };
        let mut r = Recorder::new(name, at(0));
        // A `Client` call only sends: the order is an event.
        r.requests.ask(Ask::OpenOrders, None);
        pass(&mut r, vec![open(1), Callback::OpenOrderEnd], at(1));
        assert_eq!(r.log, ["open_order_event", "update_event"]);

        // An IB method's: each order is its answer, until its end, with its
        // waiter there or gone.
        for gone in [false, true] {
            let mut r = Recorder::new(name, at(0));
            let (mut p, reply) = Pending::<Vec<Live<Trade>>>::new(None);
            let mut x = r.requests.exec(Ask::OpenOrders.key());
            let token = x.token;
            x.waiter = Some(Box::new(reply));
            x.acc = Some(Box::new(Vec::<Live<Trade>>::new()));
            r.requests.ask(Ask::OpenOrders, Some(x));
            if gone {
                drop(p);
                p = Pending::failed(Error::Timeout);
                let mut heap = crate::timer::DeadlineHeap::<Token>::default();
                r.requests.retire(token, &mut heap);
            }
            pass(
                &mut r,
                vec![open(1), open(2), Callback::OpenOrderEnd, open(3)],
                at(1),
            );
            assert_eq!(r.log, ["open_order_event", "update_event"], "gone {gone}");
            assert_eq!(r.state.trades.len(), 3);
            if !gone {
                let got = match published(&mut p) {
                    Some(Ok(v)) => v,
                    other => unreachable!("{other:?}"),
                };
                let ids: Vec<i64> = got.iter().map(|t| t.read().order_status.order_id).collect();
                assert_eq!(ids, [1, 2]);
            }
            // Ids above every order seen, another client's included.
            assert_eq!(r.ids.allocate(0, 1).ok(), Some(4));
        }
    }
}

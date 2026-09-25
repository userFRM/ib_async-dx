//! `IB`'s contract, reference and news methods: contract details and
//! qualification, historical data, scanners, news, Wall Street Horizon, the
//! advisor configuration, the server's time and the user's info.

use std::any::Any;
use std::future::poll_fn;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::task::Poll;
use std::time::Duration;

use jiff::Zoned;

use super::{IBHandle, Qualified};
use crate::contract::{Contract, ContractDescription, ContractDetails, TagValue};
use crate::convert::scanner_request;
use crate::engine::{self as e, EClient};
use crate::error::{Error, Result};
use crate::live::{Holder, Live, Observed};
use crate::objects::{
    BarDataList, HistogramData, HistoricalNews, HistoricalSchedule, HistoricalTickAny, NewsArticle,
    NewsProvider, OptionChain, ScanDataList, ScannerSubscription, WshEventData,
};
use crate::owner::{self, Class, Entry, LOG_IB, Shared};
use crate::pending::{Pending, Registration, Reply, Token};
use crate::requests::{Ask, Exec, ReqKey, Route, Waiter, fresh_token};
use crate::state::{Bars, State};
use crate::util::{BarDate, DateTimeArg, block_on, format_ib_datetime};

/// `req_historical_data`'s timeout when none is given: ib_async's 60 s.
pub const HISTORICAL_TIMEOUT: Duration = Duration::from_secs(60);

/// How long `reqMatchingSymbolsAsync`, `reqHistoricalNewsAsync` and
/// `requestFAAsync` wait for their answer.
const ANSWER_WAIT: Duration = Duration::from_secs(4);

/// What a method's own deadline does when it passes before the answer.
type Expired = Box<dyn FnOnce(&Arc<Shared>, Exec) + Send>;

/// What an `Ok` answer does in the owner step that decides it.
type Then = fn(&Arc<Shared>, &EClient, i64);

/// The session and a new request id: ib_async's `getReqId`, which fails
/// when not connected.
fn req_id(ib: &Arc<Shared>) -> Result<(i64, Arc<EClient>)> {
    let client = session(ib)?;
    let floor = client.order_id_floor();
    let id = ib.core().ids.allocate(floor, 1)?;
    Ok((id, client))
}

/// The session, for a send: ib_async's `send` fails when not connected.
fn session(ib: &Arc<Shared>) -> Result<Arc<EClient>> {
    ib.connected().map(|c| c.1).ok_or(Error::NotConnected)
}

/// `v`, bound to `ib`, so a program's edit of it goes through the owner.
fn bound<T: Observed>(ib: &Arc<Shared>, v: T) -> Live<T> {
    let live = Live::new(v);
    let ib: Weak<Shared> = Arc::downgrade(ib);
    let holder: Weak<dyn Holder> = ib;
    live.bind(holder);
    live
}

/// A numbered request as its owner step starts it.
struct Numbered {
    /// The contract it is for, which an error under its id names until it
    /// ends: ib_async's `_reqId2Contract`.
    contract: Option<Contract>,
    /// The collection its answers are gathered in: what it ends with when
    /// an error ends it and errors are not raised.
    acc: Option<Box<dyn Any + Send>>,
    route: Route,
    /// The method's own deadline, and what its passing does.
    deadline: Option<(Duration, Expired)>,
    then: Option<Then>,
    /// The engine call.
    send: Box<dyn FnOnce(&EClient) + Send>,
}

impl Numbered {
    fn of(send: impl FnOnce(&EClient) + Send + 'static) -> Self {
        Numbered {
            contract: None,
            acc: None,
            route: Route::None,
            deadline: None,
            then: None,
            send: Box::new(send),
        }
    }
}

/// Starts a numbered request in its owner step, as ib_async's method does:
/// a new id, the request registered under it, then sent. `make` builds it
/// for the id; its error is the call's, with nothing registered or sent.
fn start<T: Send + 'static>(
    ib: &Arc<Shared>,
    token: Token,
    reply: Reply<T>,
    make: impl FnOnce(&Arc<Shared>, i64) -> Result<Numbered>,
) {
    let made = req_id(ib).and_then(|(id, client)| Ok((id, client, make(ib, id)?)));
    let (id, client, n) = match made {
        Ok(v) => v,
        Err(e) => {
            reply.send(Err(e));
            return;
        }
    };
    {
        let mut c = ib.core();
        let c = &mut *c;
        let mut x = c.requests.exec_as(ReqKey::Id(id), token);
        x.waiter = Some(Box::new(Ends {
            reply,
            ib: Arc::downgrade(ib),
            generation: x.origin.generation,
            id,
            then: n.then,
        }));
        x.acc = n.acc;
        x.route = n.route;
        if let Some((after, expired)) = n.deadline
            && let Some(at) = ib.clock.now().checked_add(after)
        {
            owner::arm(&mut c.heap, &mut x, at, expired);
        }
        if let Some(contract) = n.contract {
            c.state.req_id_to_contract.insert(id, contract);
        }
        c.requests.insert(x);
    }
    (n.send)(&client);
    ib.queue.sent(1);
}

/// A numbered request's waiter.
fn numbered<T: Send + 'static>(
    ib: &Arc<Shared>,
    make: impl FnOnce(&Arc<Shared>, i64) -> Result<Numbered> + Send + 'static,
) -> Pending<T> {
    ib.request(move |ib, token, reply| start(ib, token, reply, make))
}

/// A question's waiter: its exchange is asked, or waits in its lane. With
/// `wait`, the method gives up after `ANSWER_WAIT`, logging `wait`.
fn question<T: Send + 'static>(
    ib: &Arc<Shared>,
    ask: Ask,
    wait: Option<&'static str>,
) -> Pending<T> {
    ib.request(move |ib, token, reply: Reply<T>| {
        let client = match session(ib) {
            Ok(c) => c,
            Err(e) => {
                reply.send(Err(e));
                return;
            }
        };
        let send = {
            let mut c = ib.core();
            let c = &mut *c;
            let mut x = c.requests.exec_as(ask.key(), token);
            x.waiter = Some(Box::new(reply));
            if let Some(what) = wait
                && let Some(at) = ib.clock.now().checked_add(ANSWER_WAIT)
            {
                owner::arm(&mut c.heap, &mut x, at, gave_up(what));
            }
            c.requests.ask(ask, Some(x))
        };
        if let Some(ask) = send {
            ib.send_ask(&client, &ask);
        }
    })
}

/// The waiter of a numbered request. When the request ends its contract is
/// forgotten, as ib_async's `_endReq` pops it. An `Ok` answer runs `then` in
/// the owner step that decides it, where ib_async runs it as its method
/// resumes, so a future dropped after the answer cannot lose it.
struct Ends<T: Send + 'static> {
    reply: Reply<T>,
    ib: Weak<Shared>,
    generation: u64,
    id: i64,
    then: Option<Then>,
}

impl<T: Send + 'static> Waiter for Ends<T> {
    fn finish(self: Box<Self>, r: Result<Box<dyn Any + Send>>) -> bool {
        let Ends {
            reply,
            ib,
            generation,
            id,
            then,
        } = *self;
        let ok = r.is_ok();
        let decided = Box::new(reply).finish(r);
        let Some(ib) = ib.upgrade() else {
            return decided;
        };
        let Some((_, client)) = ib.connected().filter(|c| c.0 == generation) else {
            return decided;
        };
        ib.core().state.req_id_to_contract.remove(&id);
        if let Some(then) = then.filter(|_| ok && decided) {
            then(&ib, &client, id);
        }
        decided
    }
}

/// A fixed wait passing first: `{what}: Timeout` logged at ERROR, and the
/// method gives `None`.
fn gave_up(what: &'static str) -> Expired {
    Box::new(move |_, x| {
        log::error!(target: LOG_IB, "{what}: Timeout");
        x.finish(Err(Error::Timeout));
    })
}

/// A method with a fixed wait: `None` once the wait passed first.
fn none_at_timeout<T>(r: Result<T>) -> Result<Option<T>> {
    match r {
        Ok(v) => Ok(Some(v)),
        Err(Error::Timeout) => Ok(None),
        Err(e) => Err(e),
    }
}

/// ib_async's `endSubscription`: the list is no longer kept up to date.
fn end_subscription(ib: &Arc<Shared>, id: i64) {
    let gone = {
        let mut c = ib.core();
        c.state.req_id_to_contract.remove(&id);
        c.state.req_id_to_subscriber.shift_remove(&id)
    };
    drop(gone);
}

/// A kept list's cancel, then `endSubscription`: ib_async's
/// `cancelHistoricalData` and `cancelScannerSubscription`. Only a list this
/// session keeps up to date is cancelled; one of an earlier session names an
/// id that may now be another request's.
fn cancel_list(
    ib: &Arc<Shared>,
    id: i64,
    is_it: impl FnOnce(&Bars) -> bool,
    cancel: impl FnOnce(&EClient),
) -> Result<()> {
    let client = session(ib)?;
    let kept = ib
        .core()
        .state
        .req_id_to_subscriber
        .get(&id)
        .is_some_and(is_it);
    if kept {
        cancel(&client);
        end_subscription(ib, id);
    }
    Ok(())
}

/// The historical-data timeout: the request is cancelled and detached, the
/// bars cleared and a kept list's subscription ended, so nothing that comes
/// late reaches the list; then the list is the result.
fn historical_timeout(contract: Contract, list: Live<BarDataList>) -> Expired {
    Box::new(move |ib, x| {
        let id = x.origin.req_id;
        if let Some((g, client)) = ib.connected()
            && g == x.origin.generation
        {
            client.cancel_historical_data(id);
        }
        log::warn!(target: LOG_IB, "reqHistoricalData: Timeout for {contract:?}");
        list.update(|l| l.bars.clear());
        end_subscription(ib, id);
        x.finish(Ok(Box::new(list)));
    })
}

/// `contract`'s contract-details request.
fn details(contract: Contract) -> impl FnOnce(&Arc<Shared>, i64) -> Result<Numbered> + Send {
    move |_: &Arc<Shared>, id: i64| {
        let wire = e::Contract::from(&contract);
        Ok(Numbered {
            contract: Some(contract),
            acc: Some(Box::new(Vec::<ContractDetails>::new())),
            ..Numbered::of(move |client| client.req_contract_details(id, &wire))
        })
    }
}

/// One contract-details request per contract, registered and sent in order
/// in one owner step, which its first waiter's first poll admits.
fn details_of_each(
    ib: &Arc<Shared>,
    contracts: Vec<Contract>,
) -> Vec<Pending<Vec<ContractDetails>>> {
    let mut pending = Vec::with_capacity(contracts.len());
    let mut replies = Vec::with_capacity(contracts.len());
    for _ in &contracts {
        let token = fresh_token();
        let (p, reply) = Pending::new(Some(Registration::new(ib.abandoner(), token)));
        pending.push(p);
        replies.push((token, reply));
    }
    let entry = Entry::step(Class::Request, move |ib| {
        let mut gone = Vec::new();
        for (contract, (token, reply)) in contracts.into_iter().zip(replies) {
            if !reply.start() {
                gone.push(token);
            }
            start(ib, token, reply, details(contract));
        }
        if !gone.is_empty() {
            ib.retire(gone);
        }
    });
    if let Some(first) = pending.first_mut() {
        ib.hold(first, entry);
    }
    pending
}

/// Every result, in order, or the first error as it comes: `asyncio.gather`.
async fn gather<T: Send + 'static>(mut pending: Vec<Pending<T>>) -> Result<Vec<T>> {
    let mut done: Vec<Option<T>> = pending.iter().map(|_| None).collect();
    poll_fn(|cx| {
        for (p, d) in pending.iter_mut().zip(done.iter_mut()) {
            if d.is_none() {
                match Pin::new(p).poll(cx) {
                    Poll::Ready(Ok(v)) => *d = Some(v),
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => {}
                }
            }
        }
        if done.iter().all(Option::is_some) {
            Poll::Ready(Ok(done.drain(..).flatten().collect()))
        } else {
            Poll::Pending
        }
    })
    .await
}

/// `qualifyContractsAsync`'s rules for one contract and its details: filled
/// in place when one matches, a requested `SMART` kept.
fn qualified(
    contract: &mut Contract,
    details: Vec<ContractDetails>,
    return_all: bool,
) -> Qualified {
    // The engine states every contract it describes.
    let mut found: Vec<Contract> = details
        .into_iter()
        .map(|d| d.contract.unwrap_or_default())
        .collect();
    if found.is_empty() {
        log::warn!(target: LOG_IB, "Unknown contract: {contract:?}");
        return Qualified::Unknown;
    }
    if found.len() > 1 && !contract.sec_type.is_empty() {
        // Only the security type asked for counts.
        found.retain(|c| c.sec_type == contract.sec_type);
    }
    match <[Contract; 1]>::try_from(found) {
        Ok([mut c]) => {
            if contract.exchange == "SMART" {
                c.exchange.clone_from(&contract.exchange);
            }
            *contract = c;
            Qualified::One(contract.clone())
        }
        Err(possibles) => {
            log::warn!(
                target: LOG_IB,
                "Ambiguous contract: {contract:?}, possibles are {possibles:?}"
            );
            if return_all {
                Qualified::Ambiguous(possibles)
            } else {
                Qualified::Unknown
            }
        }
    }
}

/// `reqScannerSubscription` up to its send: the list, bound and kept up to
/// date under `id`, and the engine call.
fn subscribe_scan(
    ib: &Arc<Shared>,
    id: i64,
    asked: ScanDataList,
) -> (Live<ScanDataList>, impl FnOnce(&EClient) + Send + 'static) {
    let (instrument, location, code, rows, filters) = scanner_request(
        &asked.subscription,
        &asked.scanner_subscription_filter_options,
    );
    let pairs = asked.subscription.scanner_setting_pairs.clone();
    let list = bound(
        ib,
        ScanDataList {
            req_id: id,
            ..asked
        },
    );
    let displaced = ib
        .core()
        .state
        .req_id_to_subscriber
        .insert(id, Bars::Scan(list.clone()));
    drop(displaced);
    let send = move |client: &EClient| {
        client.req_scanner_subscription(id, &instrument, &location, &code, rows, &filters, &pairs);
    };
    (list, send)
}

fn scan_list(
    subscription: &ScannerSubscription,
    options: &[TagValue],
    filter_options: &[TagValue],
) -> ScanDataList {
    ScanDataList {
        data: Vec::new(),
        req_id: 0,
        subscription: subscription.clone(),
        scanner_subscription_options: options.to_vec(),
        scanner_subscription_filter_options: filter_options.to_vec(),
    }
}

/// `reqScannerDataAsync`'s cancel once its results are in, which also ends
/// the subscription, so what still arrives for it is dropped.
fn end_scan(ib: &Arc<Shared>, client: &EClient, id: i64) {
    client.cancel_scanner_subscription(id);
    end_subscription(ib, id);
}

/// The two Wall Street Horizon requests, each with one active id in
/// ib_async's wrapper.
#[derive(Clone, Copy)]
enum Wsh {
    Meta,
    Event,
}

impl Wsh {
    fn active(self, s: &mut State) -> &mut i64 {
        match self {
            Wsh::Meta => &mut s.wsh_meta_req_id,
            Wsh::Event => &mut s.wsh_event_req_id,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Wsh::Meta => "reqWshMetaData",
            Wsh::Event => "reqWshEventData",
        }
    }

    fn send(
        self,
        id: i64,
        data: Option<e::CalendarQuery>,
    ) -> impl FnOnce(&EClient) + Send + 'static {
        move |client: &EClient| match data {
            Some(q) => client.req_wsh_event_data(id, q),
            None => client.req_wsh_meta_data(id),
        }
    }
}

/// `reqWshMetaData` and `reqWshEventData`: nothing but a warning while one
/// is active.
fn req_wsh(ib: &Arc<Shared>, w: Wsh, data: Option<e::CalendarQuery>) -> Result<()> {
    if *w.active(&mut ib.core().state) != 0 {
        log::warn!(target: LOG_IB, "{} already active", w.name());
        return Ok(());
    }
    let (id, client) = req_id(ib)?;
    *w.active(&mut ib.core().state) = id;
    w.send(id, data)(&client);
    ib.queue.sent(1);
    Ok(())
}

/// `cancelWshMetaData` and `cancelWshEventData`: nothing but a warning while
/// none is active.
fn cancel_wsh(ib: &Arc<Shared>, w: Wsh) -> Result<()> {
    let id = *w.active(&mut ib.core().state);
    if id == 0 {
        log::warn!(target: LOG_IB, "{} not active", w.name());
        return Ok(());
    }
    let client = session(ib)?;
    match w {
        Wsh::Meta => client.cancel_wsh_meta_data(id),
        Wsh::Event => client.cancel_wsh_event_data(id),
    }
    *w.active(&mut ib.core().state) = 0;
    Ok(())
}

/// `getWshMetaDataAsync` and `getWshEventDataAsync`: an active request is
/// cancelled, a new one made, and its answer awaited; the events' one is
/// cancelled once answered.
async fn get_wsh(ib: &Arc<Shared>, w: Wsh, data: Option<e::CalendarQuery>) -> Result<String> {
    numbered(ib, move |ib, id| {
        if *w.active(&mut ib.core().state) != 0 {
            cancel_wsh(ib, w)?;
        }
        *w.active(&mut ib.core().state) = id;
        let then: Option<Then> = match w {
            Wsh::Meta => None,
            Wsh::Event => Some(|ib, _, _| {
                let _ = cancel_wsh(ib, Wsh::Event);
            }),
        };
        Ok(Numbered {
            then,
            ..Numbered::of(w.send(id, data))
        })
    })
    .await
}

impl IBHandle {
    // -- Contracts -------------------------------------------------------

    /// Fills in each contract in place from its details, the `con_id`
    /// included: ib_async's `qualifyContracts`. Each result is the contract
    /// filled in, or `Unknown` for one that matched none or several.
    pub fn qualify_contracts(&self, contracts: &mut [Contract]) -> Result<Vec<Qualified>> {
        let timeout = self.config().request_timeout;
        block_on(self.qualify_contracts_async(contracts, false), timeout)?
    }

    /// `qualify_contracts`' async form: `qualifyContractsAsync`. With
    /// `return_all`, a contract that matched several gives them all.
    ///
    /// A contract whose request an error ends is `Unknown` unless
    /// `raise_request_errors` is set, when the error fails the call.
    pub async fn qualify_contracts_async(
        &self,
        contracts: &mut [Contract],
        return_all: bool,
    ) -> Result<Vec<Qualified>> {
        let lists = gather(details_of_each(&self.shared, contracts.to_vec())).await?;
        Ok(contracts
            .iter_mut()
            .zip(lists)
            .map(|(c, l)| qualified(c, l, return_all))
            .collect())
    }

    /// The details of every contract that matches `contract`: ib_async's
    /// `reqContractDetails`. Empty when none does.
    pub fn req_contract_details(&self, contract: &Contract) -> Result<Vec<ContractDetails>> {
        self.req_contract_details_async(contract)
            .wait(self.config().request_timeout)
    }

    /// `req_contract_details`' async form: `reqContractDetailsAsync`.
    pub fn req_contract_details_async(&self, contract: &Contract) -> Pending<Vec<ContractDetails>> {
        numbered(&self.shared, details(contract.clone()))
    }

    /// The contracts whose symbol or name matches `pattern`: ib_async's
    /// `reqMatchingSymbols`. `None` when no answer comes within 4 seconds.
    pub fn req_matching_symbols(&self, pattern: &str) -> Result<Option<Vec<ContractDescription>>> {
        let timeout = self.config().request_timeout;
        block_on(self.req_matching_symbols_async(pattern), timeout)?
    }

    /// `req_matching_symbols`' async form: `reqMatchingSymbolsAsync`.
    pub async fn req_matching_symbols_async(
        &self,
        pattern: &str,
    ) -> Result<Option<Vec<ContractDescription>>> {
        let pattern = pattern.to_owned();
        let p = numbered(&self.shared, move |_, id| {
            Ok(Numbered {
                acc: Some(Box::new(Vec::<ContractDescription>::new())),
                deadline: Some((ANSWER_WAIT, gave_up("reqMatchingSymbolsAsync"))),
                ..Numbered::of(move |client| client.req_matching_symbols(id, &pattern))
            })
        });
        none_at_timeout(p.await)
    }

    /// The option chains of an underlying: ib_async's `reqSecDefOptParams`.
    /// `fut_fop_exchange` is empty but for futures options.
    pub fn req_sec_def_opt_params(
        &self,
        underlying_symbol: &str,
        fut_fop_exchange: &str,
        underlying_sec_type: &str,
        underlying_con_id: i64,
    ) -> Result<Vec<OptionChain>> {
        self.req_sec_def_opt_params_async(
            underlying_symbol,
            fut_fop_exchange,
            underlying_sec_type,
            underlying_con_id,
        )
        .wait(self.config().request_timeout)
    }

    /// `req_sec_def_opt_params`' async form: `reqSecDefOptParamsAsync`.
    pub fn req_sec_def_opt_params_async(
        &self,
        underlying_symbol: &str,
        fut_fop_exchange: &str,
        underlying_sec_type: &str,
        underlying_con_id: i64,
    ) -> Pending<Vec<OptionChain>> {
        let (symbol, exchange, sec_type) = (
            underlying_symbol.to_owned(),
            fut_fop_exchange.to_owned(),
            underlying_sec_type.to_owned(),
        );
        numbered(&self.shared, move |_, id| {
            Ok(Numbered {
                acc: Some(Box::new(Vec::<OptionChain>::new())),
                ..Numbered::of(move |client| {
                    client.req_sec_def_opt_params(
                        id,
                        &symbol,
                        &exchange,
                        &sec_type,
                        underlying_con_id,
                    );
                })
            })
        })
    }

    // -- History ---------------------------------------------------------

    /// Historical bars: ib_async's `reqHistoricalData`. With
    /// `keep_up_to_date` the list is kept up to date afterwards, and
    /// `realtime_bars()` lists it.
    ///
    /// `timeout` `None` is 60 seconds, and zero no limit. When it passes
    /// first the request is cancelled, the bars cleared and the list
    /// returned; nothing that comes later reaches the list.
    /// `chart_options` is taken and not sent: a documented call carries
    /// none.
    #[expect(clippy::too_many_arguments, reason = "ib_async's parameters")]
    pub fn req_historical_data(
        &self,
        contract: &Contract,
        end_date_time: impl Into<DateTimeArg>,
        duration_str: &str,
        bar_size_setting: &str,
        what_to_show: &str,
        use_rth: bool,
        format_date: i32,
        keep_up_to_date: bool,
        chart_options: &[TagValue],
        timeout: Option<Duration>,
    ) -> Result<Live<BarDataList>> {
        let f = self.req_historical_data_async(
            contract,
            end_date_time,
            duration_str,
            bar_size_setting,
            what_to_show,
            use_rth,
            format_date,
            keep_up_to_date,
            chart_options,
            timeout,
        );
        block_on(f, self.config().request_timeout)?
    }

    /// `req_historical_data`'s async form: `reqHistoricalDataAsync`.
    #[expect(clippy::too_many_arguments, reason = "ib_async's parameters")]
    pub async fn req_historical_data_async(
        &self,
        contract: &Contract,
        end_date_time: impl Into<DateTimeArg>,
        duration_str: &str,
        bar_size_setting: &str,
        what_to_show: &str,
        use_rth: bool,
        format_date: i32,
        keep_up_to_date: bool,
        chart_options: &[TagValue],
        timeout: Option<Duration>,
    ) -> Result<Live<BarDataList>> {
        let asked = BarDataList {
            bars: Vec::new(),
            req_id: 0,
            contract: contract.clone(),
            end_date_time: end_date_time.into(),
            duration_str: duration_str.to_owned(),
            bar_size_setting: bar_size_setting.to_owned(),
            what_to_show: what_to_show.to_owned(),
            use_rth,
            format_date,
            keep_up_to_date,
            chart_options: chart_options.to_vec(),
        };
        let timeout = timeout.unwrap_or(HISTORICAL_TIMEOUT);
        numbered(&self.shared, move |ib, id| {
            let end = format_ib_datetime(asked.end_date_time.clone())?;
            let wire = e::Contract::from(&asked.contract);
            let contract = asked.contract.clone();
            let (duration, bar_size, what) = (
                asked.duration_str.clone(),
                asked.bar_size_setting.clone(),
                asked.what_to_show.clone(),
            );
            let list = bound(
                ib,
                BarDataList {
                    req_id: id,
                    ..asked
                },
            );
            if keep_up_to_date {
                let displaced = ib
                    .core()
                    .state
                    .req_id_to_subscriber
                    .insert(id, Bars::Historical(list.clone()));
                drop(displaced);
            }
            Ok(Numbered {
                contract: Some(contract.clone()),
                acc: Some(Box::new(list.clone())),
                route: if keep_up_to_date {
                    Route::List
                } else {
                    Route::None
                },
                deadline: (!timeout.is_zero())
                    .then(|| (timeout, historical_timeout(contract, list))),
                ..Numbered::of(move |client| {
                    client.req_historical_data(
                        id,
                        &wire,
                        &end,
                        &duration,
                        &bar_size,
                        &what,
                        use_rth,
                        format_date,
                        keep_up_to_date,
                    );
                })
            })
        })
        .await
    }

    /// Stops keeping `bars` up to date: ib_async's `cancelHistoricalData`.
    pub fn cancel_historical_data(&self, bars: &Live<BarDataList>) -> Result<()> {
        let bars = bars.clone();
        self.shared.step(Class::Control, move |ib| {
            let id = bars.read().req_id;
            cancel_list(
                ib,
                id,
                |b| matches!(b, Bars::Historical(l) if Live::ptr_eq(l, &bars)),
                |client| client.cancel_historical_data(id),
            )
        })
    }

    /// The trading schedule of `num_days` days up to `end_date_time`:
    /// ib_async's `reqHistoricalSchedule`.
    pub fn req_historical_schedule(
        &self,
        contract: &Contract,
        num_days: i32,
        end_date_time: impl Into<DateTimeArg>,
        use_rth: bool,
    ) -> Result<HistoricalSchedule> {
        self.req_historical_schedule_async(contract, num_days, end_date_time, use_rth)
            .wait(self.config().request_timeout)
    }

    /// `req_historical_schedule`'s async form: `reqHistoricalScheduleAsync`.
    pub fn req_historical_schedule_async(
        &self,
        contract: &Contract,
        num_days: i32,
        end_date_time: impl Into<DateTimeArg>,
        use_rth: bool,
    ) -> Pending<HistoricalSchedule> {
        let (contract, end) = (contract.clone(), end_date_time.into());
        numbered(&self.shared, move |_, id| {
            let end = format_ib_datetime(end)?;
            let wire = e::Contract::from(&contract);
            let duration = format!("{num_days} D");
            Ok(Numbered {
                contract: Some(contract),
                ..Numbered::of(move |client| {
                    client.req_historical_schedule(id, &wire, &end, &duration, use_rth);
                })
            })
        })
    }

    /// Historical ticks from `start_date_time` or up to `end_date_time`,
    /// the other left empty: ib_async's `reqHistoricalTicks`.
    /// `misc_options` is taken and not sent.
    #[expect(clippy::too_many_arguments, reason = "ib_async's parameters")]
    pub fn req_historical_ticks(
        &self,
        contract: &Contract,
        start_date_time: impl Into<DateTimeArg>,
        end_date_time: impl Into<DateTimeArg>,
        number_of_ticks: i32,
        what_to_show: &str,
        use_rth: bool,
        ignore_size: bool,
        misc_options: &[TagValue],
    ) -> Result<Vec<HistoricalTickAny>> {
        self.req_historical_ticks_async(
            contract,
            start_date_time,
            end_date_time,
            number_of_ticks,
            what_to_show,
            use_rth,
            ignore_size,
            misc_options,
        )
        .wait(self.config().request_timeout)
    }

    /// `req_historical_ticks`' async form: `reqHistoricalTicksAsync`.
    #[expect(clippy::too_many_arguments, reason = "ib_async's parameters")]
    pub fn req_historical_ticks_async(
        &self,
        contract: &Contract,
        start_date_time: impl Into<DateTimeArg>,
        end_date_time: impl Into<DateTimeArg>,
        number_of_ticks: i32,
        what_to_show: &str,
        use_rth: bool,
        ignore_size: bool,
        misc_options: &[TagValue],
    ) -> Pending<Vec<HistoricalTickAny>> {
        // Taken and not sent: a documented call carries none.
        let _ = misc_options;
        let (contract, start, end) = (
            contract.clone(),
            start_date_time.into(),
            end_date_time.into(),
        );
        let what = what_to_show.to_owned();
        numbered(&self.shared, move |_, id| {
            let start = format_ib_datetime(start)?;
            let end = format_ib_datetime(end)?;
            let wire = e::Contract::from(&contract);
            Ok(Numbered {
                contract: Some(contract),
                acc: Some(Box::new(Vec::<HistoricalTickAny>::new())),
                ..Numbered::of(move |client| {
                    client.req_historical_ticks(
                        id,
                        &wire,
                        &start,
                        &end,
                        number_of_ticks,
                        &what,
                        use_rth,
                        ignore_size,
                    );
                })
            })
        })
    }

    /// When the earliest data of `contract` begins: ib_async's
    /// `reqHeadTimeStamp`. The request is cancelled once answered.
    pub fn req_head_time_stamp(
        &self,
        contract: &Contract,
        what_to_show: &str,
        use_rth: bool,
        format_date: i32,
    ) -> Result<BarDate> {
        let f = self.req_head_time_stamp_async(contract, what_to_show, use_rth, format_date);
        block_on(f, self.config().request_timeout)?
    }

    /// `req_head_time_stamp`'s async form: `reqHeadTimeStampAsync`.
    pub async fn req_head_time_stamp_async(
        &self,
        contract: &Contract,
        what_to_show: &str,
        use_rth: bool,
        format_date: i32,
    ) -> Result<BarDate> {
        let (contract, what) = (contract.clone(), what_to_show.to_owned());
        numbered(&self.shared, move |_, id| {
            let wire = e::Contract::from(&contract);
            Ok(Numbered {
                contract: Some(contract),
                then: Some(|_, client, id| client.cancel_head_time_stamp(id)),
                ..Numbered::of(move |client| {
                    client.req_head_time_stamp(id, &wire, &what, use_rth, format_date);
                })
            })
        })
        .await
    }

    /// The price histogram of `contract` over `period`, such as `"3 days"`:
    /// ib_async's `reqHistogramData`.
    pub fn req_histogram_data(
        &self,
        contract: &Contract,
        use_rth: bool,
        period: &str,
    ) -> Result<Vec<HistogramData>> {
        self.req_histogram_data_async(contract, use_rth, period)
            .wait(self.config().request_timeout)
    }

    /// `req_histogram_data`'s async form: `reqHistogramDataAsync`.
    pub fn req_histogram_data_async(
        &self,
        contract: &Contract,
        use_rth: bool,
        period: &str,
    ) -> Pending<Vec<HistogramData>> {
        let (contract, period) = (contract.clone(), period.to_owned());
        numbered(&self.shared, move |_, id| {
            let wire = e::Contract::from(&contract);
            Ok(Numbered {
                contract: Some(contract),
                acc: Some(Box::new(Vec::<HistogramData>::new())),
                ..Numbered::of(move |client| {
                    client.req_histogram_data(id, &wire, use_rth, &period);
                })
            })
        })
    }

    /// A fundamental report on `contract`, as XML: ib_async's
    /// `reqFundamentalData`. `fundamental_data_options` is taken and not
    /// sent.
    pub fn req_fundamental_data(
        &self,
        contract: &Contract,
        report_type: &str,
        fundamental_data_options: &[TagValue],
    ) -> Result<String> {
        self.req_fundamental_data_async(contract, report_type, fundamental_data_options)
            .wait(self.config().request_timeout)
    }

    /// `req_fundamental_data`'s async form: `reqFundamentalDataAsync`.
    pub fn req_fundamental_data_async(
        &self,
        contract: &Contract,
        report_type: &str,
        fundamental_data_options: &[TagValue],
    ) -> Pending<String> {
        // Taken and not sent: a documented call carries none.
        let _ = fundamental_data_options;
        let (contract, report_type) = (contract.clone(), report_type.to_owned());
        numbered(&self.shared, move |_, id| {
            let wire = e::Contract::from(&contract);
            Ok(Numbered {
                contract: Some(contract),
                ..Numbered::of(move |client| {
                    client.req_fundamental_data(id, &wire, &report_type);
                })
            })
        })
    }

    // -- Scanners --------------------------------------------------------

    /// One scan's results: ib_async's `reqScannerData`, a subscription
    /// cancelled once its first results are in. Nothing that comes later
    /// reaches the list. `scanner_subscription_options` is taken and not
    /// sent.
    pub fn req_scanner_data(
        &self,
        subscription: &ScannerSubscription,
        scanner_subscription_options: &[TagValue],
        scanner_subscription_filter_options: &[TagValue],
    ) -> Result<Live<ScanDataList>> {
        let f = self.req_scanner_data_async(
            subscription,
            scanner_subscription_options,
            scanner_subscription_filter_options,
        );
        block_on(f, self.config().request_timeout)?
    }

    /// `req_scanner_data`'s async form: `reqScannerDataAsync`.
    pub async fn req_scanner_data_async(
        &self,
        subscription: &ScannerSubscription,
        scanner_subscription_options: &[TagValue],
        scanner_subscription_filter_options: &[TagValue],
    ) -> Result<Live<ScanDataList>> {
        let asked = scan_list(
            subscription,
            scanner_subscription_options,
            scanner_subscription_filter_options,
        );
        numbered(&self.shared, move |ib, id| {
            let (list, send) = subscribe_scan(ib, id, asked);
            Ok(Numbered {
                acc: Some(Box::new(list)),
                route: Route::List,
                then: Some(end_scan),
                ..Numbered::of(send)
            })
        })
        .await
    }

    /// Subscribes to a scan, whose results the list keeps: ib_async's
    /// `reqScannerSubscription`. `realtime_bars()` lists it.
    /// `scanner_subscription_options` is taken and not sent.
    pub fn req_scanner_subscription(
        &self,
        subscription: &ScannerSubscription,
        scanner_subscription_options: &[TagValue],
        scanner_subscription_filter_options: &[TagValue],
    ) -> Result<Live<ScanDataList>> {
        let asked = scan_list(
            subscription,
            scanner_subscription_options,
            scanner_subscription_filter_options,
        );
        self.shared.step(Class::Request, move |ib| {
            let (id, client) = req_id(ib)?;
            let (list, send) = subscribe_scan(ib, id, asked);
            send(&client);
            ib.queue.sent(1);
            Ok(list)
        })
    }

    /// Ends the scan `data_list` keeps: ib_async's
    /// `cancelScannerSubscription`.
    pub fn cancel_scanner_subscription(&self, data_list: &Live<ScanDataList>) -> Result<()> {
        let list = data_list.clone();
        self.shared.step(Class::Control, move |ib| {
            let id = list.read().req_id;
            cancel_list(
                ib,
                id,
                |b| matches!(b, Bars::Scan(l) if Live::ptr_eq(l, &list)),
                |client| client.cancel_scanner_subscription(id),
            )
        })
    }

    /// The scanner's parameters, as XML: ib_async's `reqScannerParameters`.
    pub fn req_scanner_parameters(&self) -> Result<String> {
        self.req_scanner_parameters_async()
            .wait(self.config().request_timeout)
    }

    /// `req_scanner_parameters`' async form: `reqScannerParametersAsync`.
    pub fn req_scanner_parameters_async(&self) -> Pending<String> {
        question(&self.shared, Ask::ScannerParameters, None)
    }

    // -- News ------------------------------------------------------------

    /// The news providers: ib_async's `reqNewsProviders`.
    pub fn req_news_providers(&self) -> Result<Vec<NewsProvider>> {
        self.req_news_providers_async()
            .wait(self.config().request_timeout)
    }

    /// `req_news_providers`' async form: `reqNewsProvidersAsync`.
    pub fn req_news_providers_async(&self) -> Pending<Vec<NewsProvider>> {
        question(&self.shared, Ask::NewsProviders, None)
    }

    /// The body of a news article: ib_async's `reqNewsArticle`.
    /// `news_article_options` is taken and not sent.
    pub fn req_news_article(
        &self,
        provider_code: &str,
        article_id: &str,
        news_article_options: &[TagValue],
    ) -> Result<NewsArticle> {
        self.req_news_article_async(provider_code, article_id, news_article_options)
            .wait(self.config().request_timeout)
    }

    /// `req_news_article`'s async form: `reqNewsArticleAsync`.
    pub fn req_news_article_async(
        &self,
        provider_code: &str,
        article_id: &str,
        news_article_options: &[TagValue],
    ) -> Pending<NewsArticle> {
        // Taken and not sent: a documented call carries none.
        let _ = news_article_options;
        let (provider, article) = (provider_code.to_owned(), article_id.to_owned());
        numbered(&self.shared, move |_, id| {
            Ok(Numbered::of(move |client| {
                client.req_news_article(id, &provider, &article);
            }))
        })
    }

    /// Past headlines on the contract `con_id` from the providers
    /// `provider_codes`, joined by `+`: ib_async's `reqHistoricalNews`. At
    /// most 300. `None` when no answer comes within 4 seconds.
    /// `historical_news_options` is taken and not sent.
    pub fn req_historical_news(
        &self,
        con_id: i64,
        provider_codes: &str,
        start_date_time: impl Into<DateTimeArg>,
        end_date_time: impl Into<DateTimeArg>,
        total_results: i32,
        historical_news_options: &[TagValue],
    ) -> Result<Option<Vec<HistoricalNews>>> {
        let f = self.req_historical_news_async(
            con_id,
            provider_codes,
            start_date_time,
            end_date_time,
            total_results,
            historical_news_options,
        );
        block_on(f, self.config().request_timeout)?
    }

    /// `req_historical_news`' async form: `reqHistoricalNewsAsync`.
    pub async fn req_historical_news_async(
        &self,
        con_id: i64,
        provider_codes: &str,
        start_date_time: impl Into<DateTimeArg>,
        end_date_time: impl Into<DateTimeArg>,
        total_results: i32,
        historical_news_options: &[TagValue],
    ) -> Result<Option<Vec<HistoricalNews>>> {
        // Taken and not sent: a documented call carries none.
        let _ = historical_news_options;
        let (codes, start, end) = (
            provider_codes.to_owned(),
            start_date_time.into(),
            end_date_time.into(),
        );
        let p = numbered(&self.shared, move |_, id| {
            let start = format_ib_datetime(start)?;
            let end = format_ib_datetime(end)?;
            Ok(Numbered {
                acc: Some(Box::new(Vec::<HistoricalNews>::new())),
                deadline: Some((ANSWER_WAIT, gave_up("reqHistoricalNewsAsync"))),
                ..Numbered::of(move |client| {
                    client.req_historical_news(id, con_id, &codes, &start, &end, total_results);
                })
            })
        });
        none_at_timeout(p.await)
    }

    /// Subscribes to IB's news bulletins, the day's earlier ones too with
    /// `all_messages`: ib_async's `reqNewsBulletins`.
    pub fn req_news_bulletins(&self, all_messages: bool) -> Result<()> {
        self.shared.step(Class::Request, move |ib| {
            session(ib)?.req_news_bulletins(all_messages);
            ib.queue.sent(1);
            Ok(())
        })
    }

    /// Ends the news bulletins: ib_async's `cancelNewsBulletins`.
    pub fn cancel_news_bulletins(&self) -> Result<()> {
        self.shared.step(Class::Control, |ib| {
            session(ib)?.cancel_news_bulletins();
            Ok(())
        })
    }

    // -- Wall Street Horizon ---------------------------------------------

    /// Asks for Wall Street Horizon's metadata, which reaches
    /// `wsh_meta_event`: ib_async's `reqWshMetaData`. While one is active
    /// this only warns.
    pub fn req_wsh_meta_data(&self) -> Result<()> {
        self.shared
            .step(Class::Request, |ib| req_wsh(ib, Wsh::Meta, None))
    }

    /// Cancels the active metadata request: ib_async's `cancelWshMetaData`.
    pub fn cancel_wsh_meta_data(&self) -> Result<()> {
        self.shared
            .step(Class::Control, |ib| cancel_wsh(ib, Wsh::Meta))
    }

    /// Asks for the Wall Street Horizon events `data` selects, which reach
    /// `wsh_event`: ib_async's `reqWshEventData`. While one is active this
    /// only warns.
    pub fn req_wsh_event_data(&self, data: &WshEventData) -> Result<()> {
        let query = e::CalendarQuery::from(data);
        self.shared.step(Class::Request, move |ib| {
            req_wsh(ib, Wsh::Event, Some(query))
        })
    }

    /// Cancels the active event request: ib_async's `cancelWshEventData`.
    pub fn cancel_wsh_event_data(&self) -> Result<()> {
        self.shared
            .step(Class::Control, |ib| cancel_wsh(ib, Wsh::Event))
    }

    /// Wall Street Horizon's metadata, as JSON: ib_async's
    /// `getWshMetaData`. An active metadata request is cancelled first.
    pub fn get_wsh_meta_data(&self) -> Result<String> {
        block_on(
            self.get_wsh_meta_data_async(),
            self.config().request_timeout,
        )?
    }

    /// `get_wsh_meta_data`'s async form: `getWshMetaDataAsync`.
    pub async fn get_wsh_meta_data_async(&self) -> Result<String> {
        get_wsh(&self.shared, Wsh::Meta, None).await
    }

    /// The Wall Street Horizon events `data` selects, as JSON: ib_async's
    /// `getWshEventData`. An active event request is cancelled first, and
    /// this one once answered.
    pub fn get_wsh_event_data(&self, data: &WshEventData) -> Result<String> {
        block_on(
            self.get_wsh_event_data_async(data),
            self.config().request_timeout,
        )?
    }

    /// `get_wsh_event_data`'s async form: `getWshEventDataAsync`.
    pub async fn get_wsh_event_data_async(&self, data: &WshEventData) -> Result<String> {
        get_wsh(&self.shared, Wsh::Event, Some(e::CalendarQuery::from(data))).await
    }

    // -- Advisor configuration, time and user ----------------------------

    /// The advisor configuration of type `fa_data_type`, as XML: ib_async's
    /// `requestFA`. `None` when no answer comes within 4 seconds.
    pub fn request_fa(&self, fa_data_type: i32) -> Result<Option<String>> {
        block_on(
            self.request_fa_async(fa_data_type),
            self.config().request_timeout,
        )?
    }

    /// `request_fa`'s async form: `requestFAAsync`.
    pub async fn request_fa_async(&self, fa_data_type: i32) -> Result<Option<String>> {
        let p = question(&self.shared, Ask::Fa(fa_data_type), Some("requestFAAsync"));
        none_at_timeout(p.await)
    }

    /// Replaces the advisor configuration of type `fa_data_type` with
    /// `xml`: ib_async's `replaceFA`.
    pub fn replace_fa(&self, fa_data_type: i32, xml: &str) -> Result<()> {
        let xml = xml.to_owned();
        self.shared.step(Class::Request, move |ib| {
            let (id, client) = req_id(ib)?;
            client.replace_fa(id, fa_data_type, &xml);
            ib.queue.sent(1);
            Ok(())
        })
    }

    /// The server's time, in `IBDefaults.timezone`: ib_async's
    /// `reqCurrentTime`.
    pub fn req_current_time(&self) -> Result<Zoned> {
        self.req_current_time_async()
            .wait(self.config().request_timeout)
    }

    /// `req_current_time`'s async form: `reqCurrentTimeAsync`.
    pub fn req_current_time_async(&self) -> Pending<Zoned> {
        question(&self.shared, Ask::CurrentTime, None)
    }

    /// The user's white-branding id: ib_async's `reqUserInfo`.
    pub fn req_user_info(&self) -> Result<String> {
        self.req_user_info_async()
            .wait(self.config().request_timeout)
    }

    /// `req_user_info`'s async form: `reqUserInfoAsync`.
    pub fn req_user_info_async(&self) -> Pending<String> {
        numbered(&self.shared, |_, id| {
            Ok(Numbered::of(move |client| client.req_user_info(id)))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::task::{Context, Waker};
    use std::thread;

    use jiff::Timestamp;
    use jiff::tz::TimeZone;

    use super::*;
    use crate::engine::{ControlCommand, ErrorOrigin, SharedState};
    use crate::event::{lock, set_on_owner};
    use crate::ib::{ConnectOptions, IB, IBConfig, StartupFetch};
    use crate::objects::{BarData, IBDefaults};
    use crate::owner::Via;
    use crate::record::{Callback, Capture};
    use crate::tests::{GLOBAL_ERRORS, errors_here};
    use crate::timer::Clock;

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
        capture: Capture,
        rx: mpsc::Receiver<ControlCommand>,
        generation: u64,
    }

    /// An IB connected on a harness engine, driven on this thread, which
    /// must be the owner.
    fn connected() -> Session {
        let (client, rx) = engine();
        let shared = Shared::new(
            IBDefaults::default(),
            IBConfig::default(),
            Clock::manual(Timestamp::UNIX_EPOCH),
        );
        shared.connect_internal_slots();
        let ib = IBHandle { shared };
        let mut capture = Capture::new(TimeZone::UTC);
        let via = Via::Test(Some(Arc::new(client)));
        let (mut p, _) = ib.begin_connect(opts(), true, Some(via)).unwrap();
        ib.shared.lap(&mut capture);
        assert!(matches!(poll(Pin::new(&mut p)), Poll::Ready(Ok(()))));
        let generation = ib.shared.connected().unwrap().0;
        Session {
            ib,
            capture,
            rx,
            generation,
        }
    }

    impl Session {
        fn read(&self, callbacks: Vec<Callback>) {
            self.ib.shared.apply_read(self.generation, callbacks);
        }

        /// The one request registered now.
        fn live_id(&self) -> i64 {
            let c = self.ib.shared.core();
            (0..1000).find(|id| c.requests.is_request(*id)).unwrap()
        }
    }

    fn poll<F: Future + ?Sized>(f: Pin<&mut F>) -> Poll<F::Output> {
        f.poll(&mut Context::from_waker(Waker::noop()))
    }

    fn ready<T>(p: Poll<T>) -> T {
        match p {
            Poll::Ready(v) => v,
            Poll::Pending => panic!("not answered"),
        }
    }

    fn ended(id: i64) -> Callback {
        Callback::Error {
            origin: ErrorOrigin::Request { id, ends: true },
            code: 200,
            message: "No security definition has been found for the request".into(),
            advanced_order_reject_json: String::new(),
        }
    }

    fn stock(symbol: &str) -> Contract {
        Contract {
            sec_type: "STK".into(),
            symbol: symbol.into(),
            exchange: "SMART".into(),
            currency: "USD".into(),
            ..Contract::default()
        }
    }

    #[test]
    fn the_historical_data_timeout_cancels_clears_and_ends_the_subscription() {
        let _g = lock(&GLOBAL_ERRORS);
        let (client, rx) = engine();
        let ib = IB::attach(client, opts(), Clock::manual(Timestamp::UNIX_EPOCH)).unwrap();
        let g = ib.shared.connected().unwrap().0;
        let updates = Arc::new(AtomicUsize::new(0));
        let n = updates.clone();
        ib.bar_update_event().connect(move |_| {
            n.fetch_add(1, Ordering::Relaxed);
        });
        let h = ib.handle();
        let asker = thread::spawn(move || {
            let c = stock("AAPL");
            let five = Some(Duration::from_secs(5));
            h.req_historical_data(&c, "", "1 D", "1 min", "TRADES", true, 1, true, &[], five)
        });
        let id = loop {
            match rx.recv_timeout(Duration::from_secs(5)).unwrap() {
                ControlCommand::FetchHistorical { req_id, .. } => break i64::from(req_id),
                _ => continue,
            }
        };
        assert_eq!(ib.realtime_bars().len(), 1);
        let bar = || BarData::default();
        let read = |callbacks: Vec<Callback>| {
            ib.shared
                .step(Class::Control, move |ib| Ok(ib.apply_read(g, callbacks)))
                .unwrap();
        };
        read(vec![Callback::HistoricalData {
            req_id: id,
            bar: bar(),
        }]);

        ib.shared.clock.advance(Duration::from_secs(5));
        let list = asker.join().unwrap().unwrap();
        let cancelled = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(
            matches!(cancelled, ControlCommand::CancelHistorical { req_id } if i64::from(req_id) == id),
            "{cancelled:?}"
        );
        assert!(list.read().bars.is_empty(), "the bars are cleared");
        assert!(ib.realtime_bars().is_empty(), "the subscription is ended");

        // What comes late reaches nothing.
        read(vec![
            Callback::HistoricalData {
                req_id: id,
                bar: bar(),
            },
            Callback::HistoricalDataEnd {
                req_id: id,
                start: String::new(),
                end: String::new(),
            },
            Callback::HistoricalDataUpdate {
                req_id: id,
                bar: bar(),
            },
        ]);
        assert!(list.read().bars.is_empty());
        assert_eq!(updates.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn qualify_contracts_fills_in_what_one_contract_matches() {
        let _o = AsOwner::new();
        let s = connected();
        let detail = |req_id, con_id, sec_type: &str, exchange: &str| Callback::ContractDetails {
            req_id,
            details: ContractDetails {
                contract: Some(Contract {
                    con_id,
                    sec_type: sec_type.into(),
                    symbol: "X".into(),
                    exchange: exchange.into(),
                    primary_exchange: exchange.into(),
                    ..Contract::default()
                }),
                ..ContractDetails::default()
            },
        };
        let ask = |return_all: bool, contracts: &mut [Contract]| {
            let mut f = pin!(s.ib.qualify_contracts_async(contracts, return_all));
            assert!(poll(f.as_mut()).is_pending());
            let ids: Vec<i64> =
                s.rx.try_iter()
                    .filter_map(|c| match c {
                        ControlCommand::FetchContractDetails { req_id, .. } => Some(req_id.into()),
                        _ => None,
                    })
                    .collect();
            assert_eq!(ids.len(), 5, "one request per contract, in one step");
            let end = Callback::ContractDetailsEnd;
            s.read(vec![
                detail(ids[0], 1, "STK", "NASDAQ"),
                end(ids[0]),
                end(ids[1]),
                detail(ids[2], 2, "FOP", "CME"),
                detail(ids[2], 3, "FUT", "CME"),
                end(ids[2]),
                detail(ids[3], 2, "FOP", "CME"),
                detail(ids[3], 3, "FUT", "CME"),
                end(ids[3]),
                ended(ids[4]),
            ]);
            (ready(poll(f.as_mut())), ids)
        };
        let contracts = || {
            let fop = Contract {
                sec_type: "FOP".into(),
                ..stock("ES")
            };
            let any = Contract {
                sec_type: String::new(),
                ..stock("ES")
            };
            vec![stock("X"), stock("NONE"), fop, any, stock("BAD")]
        };
        let with = |con_id| Contract {
            con_id,
            ..Contract::default()
        };
        for (return_all, ambiguous) in [
            (true, Qualified::Ambiguous(vec![with(2), with(3)])),
            (false, Qualified::Unknown),
        ] {
            let mut cs = contracts();
            let (r, _) = ask(return_all, &mut cs);
            let want = [
                Qualified::One(with(1)),
                Qualified::Unknown,
                Qualified::One(with(2)),
                ambiguous,
                Qualified::Unknown,
            ];
            assert_eq!(r.unwrap(), want);
            assert_eq!(
                (
                    cs[0].con_id,
                    cs[0].exchange.as_str(),
                    cs[0].primary_exchange.as_str()
                ),
                (1, "SMART", "NASDAQ"),
                "filled in place, SMART kept"
            );
            assert_eq!(cs[2].con_id, 2, "only the security type asked for counts");
            assert!([1, 3, 4].iter().all(|&i| cs[i] == contracts()[i]));
        }

        // Raised, a request's error fails the call.
        s.ib.set_config(IBConfig {
            raise_request_errors: true,
            ..s.ib.config()
        });
        let mut cs = contracts();
        let (r, ids) = ask(false, &mut cs);
        assert!(
            matches!(r, Err(Error::Request { req_id, code: 200, .. }) if req_id == ids[4]),
            "{r:?}"
        );
    }

    #[test]
    fn a_requests_errors_name_its_contract_until_it_ends() {
        let _o = AsOwner::new();
        let s = connected();
        let named = Arc::new(std::sync::Mutex::new(Vec::new()));
        let n = named.clone();
        s.ib.error_event()
            .connect(move |e| lock(&n).push(e.3.as_ref().map(|c| c.symbol.clone())));
        let mut p = s.ib.req_contract_details_async(&stock("AAPL"));
        let id = s.live_id();
        let notice = || Callback::Error {
            origin: ErrorOrigin::Request { id, ends: false },
            code: 2104,
            message: "notice".into(),
            advanced_order_reject_json: String::new(),
        };
        s.read(vec![notice(), Callback::ContractDetailsEnd(id), notice()]);
        assert!(matches!(ready(poll(Pin::new(&mut p))), Ok(v) if v.is_empty()));
        assert_eq!(*lock(&named), [Some("AAPL".to_owned()), None]);
    }

    type Start = fn(IBHandle) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>;
    type Answer = fn(i64) -> Vec<Callback>;
    type FollowUp = fn(&ControlCommand) -> Option<u32>;

    #[test]
    fn an_answers_follow_up_is_sent_in_the_step_that_decides_it() {
        let _o = AsOwner::new();
        let s = connected();
        let rows: [(Start, Answer, FollowUp); 3] = [
            (
                |ib| {
                    Box::pin(async move {
                        let c = stock("AAPL");
                        ib.req_head_time_stamp_async(&c, "TRADES", true, 1)
                            .await
                            .map(drop)
                    })
                },
                |req_id| {
                    vec![Callback::HeadTimestamp {
                        req_id,
                        head_timestamp: "1577836800".into(),
                    }]
                },
                |c| match c {
                    ControlCommand::CancelHeadTimestamp { req_id } => Some(*req_id),
                    _ => None,
                },
            ),
            (
                |ib| {
                    Box::pin(async move {
                        let sub = ScannerSubscription::default();
                        ib.req_scanner_data_async(&sub, &[], &[]).await.map(drop)
                    })
                },
                |req_id| vec![Callback::ScannerDataEnd(req_id)],
                |c| match c {
                    ControlCommand::CancelScanner { req_id } => Some(*req_id),
                    _ => None,
                },
            ),
            (
                |ib| {
                    Box::pin(async move {
                        let data = WshEventData::default();
                        ib.get_wsh_event_data_async(&data).await.map(drop)
                    })
                },
                |req_id| {
                    vec![Callback::WshEventData {
                        req_id,
                        data_json: "{}".into(),
                    }]
                },
                |c| match c {
                    ControlCommand::CancelCalendar { req_id } => Some(*req_id),
                    _ => None,
                },
            ),
        ];
        for (start, answer, follow_up) in rows {
            let mut f = start(s.ib.clone());
            assert!(poll(f.as_mut()).is_pending());
            let id = s.live_id();
            s.rx.try_iter().for_each(drop);
            s.read(answer(id));
            // Dropped after the answer's step, before it resumes.
            drop(f);
            let sent: Vec<i64> =
                s.rx.try_iter()
                    .filter_map(|c| follow_up(&c))
                    .map(i64::from)
                    .collect();
            assert_eq!(sent, [id]);
            assert!(
                s.ib.realtime_bars().is_empty(),
                "a scan's subscription ends too"
            );
        }

        // A waiter gone before the answer sends nothing more.
        let (start, answer, follow_up) = rows[0];
        let mut f = start(s.ib.clone());
        assert!(poll(f.as_mut()).is_pending());
        let id = s.live_id();
        drop(f);
        s.read(answer(id));
        assert!(s.rx.try_iter().all(|c| follow_up(&c).is_none()));
    }

    #[test]
    fn a_fixed_wait_gives_none_and_logs_as_ib_async_does() {
        crate::tests::capture_logs();
        let _o = AsOwner::new();
        let mut s = connected();
        let rows: [(Start, &str); 3] = [
            (
                |ib| {
                    Box::pin(async move {
                        let r = ib.req_matching_symbols_async("IBM").await?;
                        assert!(r.is_none());
                        Ok(())
                    })
                },
                "reqMatchingSymbolsAsync: Timeout",
            ),
            (
                |ib| {
                    Box::pin(async move {
                        let r = ib
                            .req_historical_news_async(8314, "BZ", "", "", 10, &[])
                            .await?;
                        assert!(r.is_none());
                        Ok(())
                    })
                },
                "reqHistoricalNewsAsync: Timeout",
            ),
            (
                |ib| {
                    Box::pin(async move {
                        assert!(ib.request_fa_async(1).await?.is_none());
                        Ok(())
                    })
                },
                "requestFAAsync: Timeout",
            ),
        ];
        for (start, logged) in rows {
            let mut f = start(s.ib.clone());
            assert!(poll(f.as_mut()).is_pending());
            // ib_async's 4 seconds.
            s.ib.shared.clock.advance(Duration::from_millis(3999));
            s.ib.shared.lap(&mut s.capture);
            assert!(poll(f.as_mut()).is_pending(), "{logged}");
            s.ib.shared.clock.advance(Duration::from_millis(1));
            s.ib.shared.lap(&mut s.capture);
            ready(poll(f.as_mut())).unwrap();
            assert!(
                errors_here().contains(&(LOG_IB.into(), logged.into())),
                "{logged}"
            );
        }
    }

    #[test]
    fn an_error_ends_a_collection_with_what_arrived_and_a_single_value_with_itself() {
        let _o = AsOwner::new();
        let s = connected();
        let rows: [(&str, bool, Start); 15] = [
            ("reqContractDetails", true, |ib| {
                Box::pin(async move { ib.req_contract_details_async(&stock("A")).await.map(drop) })
            }),
            ("reqMatchingSymbols", true, |ib| {
                Box::pin(async move { ib.req_matching_symbols_async("A").await.map(drop) })
            }),
            ("reqSecDefOptParams", true, |ib| {
                Box::pin(async move {
                    ib.req_sec_def_opt_params_async("A", "", "STK", 1)
                        .await
                        .map(drop)
                })
            }),
            ("reqHistoricalData", true, |ib| {
                Box::pin(async move {
                    let c = stock("A");
                    ib.req_historical_data_async(
                        &c,
                        "",
                        "1 D",
                        "1 min",
                        "TRADES",
                        true,
                        1,
                        false,
                        &[],
                        None,
                    )
                    .await
                    .map(drop)
                })
            }),
            ("reqHistoricalSchedule", false, |ib| {
                Box::pin(async move {
                    ib.req_historical_schedule_async(&stock("A"), 1, "", true)
                        .await
                        .map(drop)
                })
            }),
            ("reqHistoricalTicks", true, |ib| {
                Box::pin(async move {
                    let c = stock("A");
                    ib.req_historical_ticks_async(
                        &c,
                        "",
                        "20240102 16:00:00 UTC",
                        10,
                        "TRADES",
                        true,
                        false,
                        &[],
                    )
                    .await
                    .map(drop)
                })
            }),
            ("reqHeadTimeStamp", false, |ib| {
                Box::pin(async move {
                    let c = stock("A");
                    ib.req_head_time_stamp_async(&c, "TRADES", true, 1)
                        .await
                        .map(drop)
                })
            }),
            ("reqHistogramData", true, |ib| {
                Box::pin(async move {
                    ib.req_histogram_data_async(&stock("A"), true, "3 days")
                        .await
                        .map(drop)
                })
            }),
            ("reqFundamentalData", false, |ib| {
                Box::pin(async move {
                    ib.req_fundamental_data_async(&stock("A"), "RESC", &[])
                        .await
                        .map(drop)
                })
            }),
            ("reqScannerData", true, |ib| {
                Box::pin(async move {
                    let sub = ScannerSubscription::default();
                    ib.req_scanner_data_async(&sub, &[], &[]).await.map(drop)
                })
            }),
            ("reqNewsArticle", false, |ib| {
                Box::pin(async move { ib.req_news_article_async("BZ", "1", &[]).await.map(drop) })
            }),
            ("reqHistoricalNews", true, |ib| {
                Box::pin(async move {
                    ib.req_historical_news_async(1, "BZ", "", "", 10, &[])
                        .await
                        .map(drop)
                })
            }),
            ("getWshMetaData", false, |ib| {
                Box::pin(async move { ib.get_wsh_meta_data_async().await.map(drop) })
            }),
            ("getWshEventData", false, |ib| {
                Box::pin(async move {
                    let data = WshEventData::default();
                    ib.get_wsh_event_data_async(&data).await.map(drop)
                })
            }),
            ("reqUserInfo", false, |ib| {
                Box::pin(async move { ib.req_user_info_async().await.map(drop) })
            }),
        ];
        for (name, collection, start) in rows {
            let mut f = start(s.ib.clone());
            assert!(poll(f.as_mut()).is_pending(), "{name}");
            s.read(vec![ended(s.live_id())]);
            let r = ready(poll(f.as_mut()));
            if collection {
                assert!(r.is_ok(), "{name}: {r:?}");
            } else {
                assert!(
                    matches!(r, Err(Error::Request { code: 200, .. })),
                    "{name}: {r:?}"
                );
            }
        }
    }

    #[test]
    fn wsh_requests_are_one_at_a_time_as_ib_asyncs_are() {
        let _o = AsOwner::new();
        let s = connected();
        let sent = || -> Vec<String> {
            s.rx.try_iter()
                .filter_map(|c| match c {
                    ControlCommand::FetchCalendarMetaData { req_id } => {
                        Some(format!("ask {req_id}"))
                    }
                    ControlCommand::CancelCalendar { req_id } => Some(format!("cancel {req_id}")),
                    _ => None,
                })
                .collect()
        };
        s.ib.req_wsh_meta_data().unwrap();
        let first = s.live_id_of_wsh();
        s.ib.req_wsh_meta_data().unwrap();
        assert_eq!(
            sent(),
            [format!("ask {first}")],
            "a second one while active only warns"
        );
        s.ib.cancel_wsh_meta_data().unwrap();
        s.ib.cancel_wsh_meta_data().unwrap();
        assert_eq!(
            sent(),
            [format!("cancel {first}")],
            "a cancel with none active only warns"
        );

        s.ib.req_wsh_meta_data().unwrap();
        let active = s.live_id_of_wsh();
        drop(sent());
        let mut f = pin!(s.ib.get_wsh_meta_data_async());
        assert!(poll(f.as_mut()).is_pending());
        let asked = s.live_id();
        assert_eq!(
            sent(),
            [format!("cancel {active}"), format!("ask {asked}")],
            "the active one is cancelled first"
        );
    }

    impl Session {
        fn live_id_of_wsh(&self) -> i64 {
            self.ib.shared.core().state.wsh_meta_req_id
        }
    }

    #[test]
    fn only_a_list_this_session_keeps_is_cancelled() {
        let _o = AsOwner::new();
        let s = connected();
        let sub = ScannerSubscription::default();
        let list = s.ib.req_scanner_subscription(&sub, &[], &[]).unwrap();
        assert_eq!(s.ib.realtime_bars(), [Bars::Scan(list.clone())]);
        let id = list.read().req_id;
        let cancels = || -> Vec<i64> {
            s.rx.try_iter()
                .filter_map(|c| match c {
                    ControlCommand::CancelScanner { req_id } => Some(i64::from(req_id)),
                    _ => None,
                })
                .collect()
        };
        drop(cancels());
        s.ib.cancel_scanner_subscription(&list).unwrap();
        assert_eq!(cancels(), [id]);
        assert!(s.ib.realtime_bars().is_empty());
        s.ib.cancel_scanner_subscription(&list).unwrap();
        assert!(
            cancels().is_empty(),
            "no longer kept: its id may be another request's"
        );
    }
}

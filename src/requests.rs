//! Request ids, request identity and the lanes of unnumbered questions.
//!
//! The owner numbers every order and request of an IB from one checked id
//! space, as ib_async's `getReqId` does, and keeps each request it sends as
//! an execution: its token, the IB and generation it was started under, its
//! waiter and the result being assembled. A reply completes only an
//! execution that was sent and whose waiter is still there.
//!
//! The questions that carry no number are serialized per key: one exchange
//! in flight, and the calls after it waiting in the key's lane, one exchange
//! per distinct argument set. ib_async keeps one future per key and
//! replaces it at the next call, so the earlier caller is never answered and
//! a late answer completes the later one; the lane answers each in turn.

use std::any::{Any, type_name};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::engine::{ErrorOrigin, FIRST_RESERVED_REQUEST_ID, Question};
use crate::error::{Error, Result};
use crate::pending::{Reply, Token};
use crate::timer::{DeadlineHeap, DeadlineId};

/// The ids of one generation: orders and requests alike, as ib_async's
/// `getReqId` numbers both, while the account's order ids fit a request.
/// Once the engine's order floor is past what a request can carry, orders
/// take the engine's full-width ids and requests keep the counter, which
/// the engine's shared id (`next_shared_id`) started. Only the owner
/// allocates.
pub(crate) struct IdSpace {
    next: i64,
    /// Past the last full-width order id given.
    wide: i64,
    /// The first id the engine keeps for itself.
    end: i64,
}

impl IdSpace {
    /// A generation's ids, which start from the engine's floor.
    pub(crate) fn new() -> Self {
        IdSpace {
            next: 0,
            wide: 0,
            end: FIRST_RESERVED_REQUEST_ID,
        }
    }

    /// `n` consecutive request ids, the first of them given. `floor` is the
    /// engine's `order_id_floor()` read for this allocation, so an id the
    /// venue has named is never given again, even before its `open_order`
    /// arrives. A floor no request can carry is ignored, as `raise` ignores
    /// one.
    pub(crate) fn allocate(&mut self, floor: i64, n: i64) -> Result<i64> {
        let first = if floor < self.end {
            self.next.max(floor)
        } else {
            self.next
        };
        match first.checked_add(n) {
            Some(next) if next <= self.end => {
                self.next = next;
                Ok(first)
            }
            _ => Err(Error::Value(format!(
                "request ids exhausted: {first} is inside the range the engine reserves"
            ))),
        }
    }

    /// `n` consecutive order ids from `floor`, the engine's
    /// `order_id_floor()`: from the counter while the floor is below the
    /// band, else from the floor itself, since the engine refuses a new
    /// order numbered at or below its saved counter (103).
    pub(crate) fn allocate_orders(&mut self, floor: i64, n: i64) -> Result<i64> {
        if floor < self.end {
            return self.allocate(floor, n);
        }
        let first = self.wide.max(floor);
        self.wide = first
            .checked_add(n)
            .ok_or_else(|| Error::Value(format!("order ids exhausted: {first}")))?;
        Ok(first)
    }

    /// Ids below `min` are not given from now on: an `open_order`'s id plus
    /// one (wr:717) or `Client::update_req_id`. An id inside the range the
    /// engine reserves, or past it, is ignored, as the engine ignores it.
    pub(crate) fn raise(&mut self, min: i64) {
        if min <= self.end {
            self.next = self.next.max(min);
        }
    }
}

/// A token no execution of the process has had.
pub(crate) fn fresh_token() -> Token {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    Token(NEXT.fetch_add(1, Ordering::Relaxed))
}

/// An IB, as an origin names it: unique for the process's life.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct IbId(u64);

impl IbId {
    /// A new IB's.
    pub(crate) fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        IbId(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

/// Where an execution, a handle a request feeds, a cleanup or a later step
/// was started: the IB, its generation and the request's id, -1 for a
/// question. Each acts only while its origin is its IB's current
/// generation, so an id numbering a request of another IB or generation is
/// never reached through it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Origin {
    pub(crate) ib: IbId,
    pub(crate) generation: u64,
    pub(crate) req_id: i64,
}

/// What an execution is keyed by: a numbered request's id, or the question
/// whose lane it is in. `req_open_orders` and `req_all_open_orders` share
/// one key, and `request_fa` has one whatever the type, as in ib_async.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ReqKey {
    Id(i64),
    Question(Question),
}

impl ReqKey {
    /// The key of the question `q`, as the engine names it in an error or a
    /// cancel's confirmation.
    pub(crate) fn question(q: Question) -> Self {
        ReqKey::Question(match q {
            Question::AllOpenOrders => Question::OpenOrders,
            q => q,
        })
    }
}

/// An unnumbered question as the owner sends it: the engine call and its
/// arguments. Two calls join one unsent exchange only when these are equal.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Ask {
    OpenOrders,
    AllOpenOrders,
    CompletedOrders { api_only: bool },
    Positions,
    AccountUpdates { account: String },
    CurrentTime,
    CurrentTimeInMillis,
    NewsProviders,
    MarketRule(i32),
    MktDepthExchanges,
    ScannerParameters,
    Fa(i32),
}

impl Ask {
    /// The lane it waits in.
    pub(crate) fn key(&self) -> ReqKey {
        ReqKey::question(match self {
            Ask::OpenOrders => Question::OpenOrders,
            Ask::AllOpenOrders => Question::AllOpenOrders,
            Ask::CompletedOrders { .. } => Question::CompletedOrders,
            Ask::Positions => Question::Positions,
            Ask::AccountUpdates { .. } => Question::AccountUpdates,
            Ask::CurrentTime => Question::CurrentTime,
            Ask::CurrentTimeInMillis => Question::CurrentTimeInMillis,
            Ask::NewsProviders => Question::NewsProviders,
            Ask::MarketRule(id) => Question::MarketRule(*id),
            Ask::MktDepthExchanges => Question::MktDepthExchanges,
            Ask::ScannerParameters => Question::ScannerParameters,
            Ask::Fa(_) => Question::Fa,
        })
    }
}

/// A waiter's slot, whatever its result's type: a command's reply guard.
pub(crate) trait Waiter: Send {
    /// Decides the slot with `r`, whose value must be the waiter's type.
    /// Gives `false` when the waiter left or the slot was decided first.
    fn finish(self: Box<Self>, r: Result<Box<dyn Any + Send>>) -> bool;
}

impl<T: Send + 'static> Waiter for Reply<T> {
    fn finish(self: Box<Self>, r: Result<Box<dyn Any + Send>>) -> bool {
        let r = r.and_then(|v| {
            v.downcast::<T>()
                .map(|v| *v)
                .map_err(|_| Error::Value(format!("an answer that is not {}", type_name::<T>())))
        });
        let reply: Reply<T> = *self;
        reply.send(r)
    }
}

/// What an answer feeds besides its result. An execution that feeds state
/// stays when its waiter leaves, so what still arrives reaches the state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Route {
    /// Only the result.
    None,
    /// A ticker: `req_tickers`' snapshot.
    Ticker,
    /// A list a subscription keeps up to date.
    List,
}

/// The cancel an execution's method runs whenever it ends, abandoned
/// included: the only cancel that follows an abandon. A refusal runs only
/// what [`Cleanup::refused`] leaves of it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Cleanup {
    /// `calculate_implied_volatility`'s `finally` (ib:2499-2518).
    CancelImpliedVolatility,
    /// `calculate_option_price`'s `finally` (ib:2520-2539).
    CancelOptionPrice,
    /// `req_tickers` ends its snapshot ticker on every path, an error's
    /// included, where ib:2196-2200 skips it.
    EndSnapshot,
    /// A corporate actions query given up unanswered is withdrawn: the
    /// venue serves it until then.
    WithdrawAdjustments,
    /// A spread scan's ticker is ended and its subscription cancelled.
    WithdrawScan,
    /// A refused spread scan's ticker is ended.
    EndScan,
}

impl Cleanup {
    /// What is left of it once the venue has refused its request: a refused
    /// request has nothing to withdraw, and the engine refuses a cancel of
    /// one.
    fn refused(self) -> Option<Cleanup> {
        match self {
            Cleanup::WithdrawAdjustments => None,
            Cleanup::WithdrawScan => Some(Cleanup::EndScan),
            c => Some(c),
        }
    }
}

/// One execution of a request.
pub(crate) struct Exec {
    /// Unique for the IB's life: what its waiter reports when it leaves.
    pub(crate) token: Token,
    pub(crate) origin: Origin,
    pub(crate) key: ReqKey,
    /// `None` once the waiter has left, or for one that never had one.
    pub(crate) waiter: Option<Box<dyn Waiter>>,
    /// The result being assembled; `Some` from the start for a request
    /// whose result is a collection.
    pub(crate) acc: Option<Box<dyn Any + Send>>,
    pub(crate) route: Route,
    /// The method's own deadline, in its IB's heap.
    pub(crate) deadline: Option<DeadlineId>,
    pub(crate) guard: Option<Cleanup>,
}

impl Exec {
    /// Completes the waiter with `r`. Gives `false` when it has none or it
    /// left.
    pub(crate) fn finish(self, r: Result<Box<dyn Any + Send>>) -> bool {
        self.waiter.is_some_and(|w| w.finish(r))
    }

    /// Ends the execution with the error that ended its request (ib:142-148,
    /// wr:1646-1650): the error when `raise` (`raise_request_errors`) is
    /// set, and the collection so far when it is not. A request whose
    /// result is a single value gets the error either way, since ib_async's
    /// `[]` is not a value of its type.
    pub(crate) fn fail(self, e: Error, raise: bool) -> bool {
        let Some(w) = self.waiter else {
            return false;
        };
        match self.acc {
            Some(acc) if !raise => w.finish(Ok(acc)),
            _ => w.finish(Err(e)),
        }
    }

    /// Removes its deadline from `heap` and gives its cleanup, to run once:
    /// what is left to do once its result is settled, however it was.
    pub(crate) fn settle<K>(&mut self, heap: &mut DeadlineHeap<K>) -> Option<Cleanup> {
        if let Some(d) = self.deadline.take() {
            heap.cancel(d);
        }
        self.guard.take()
    }
}

/// What an error does to the requests, by the origin the engine gives it.
pub(crate) enum Refused {
    /// Nothing here ends: a notice its answer follows, an order's error, the
    /// session's.
    Nothing,
    /// The numbered request it ends, to end with it ([`Exec::fail`]).
    Request(Exec),
    /// A question's exchange it ends, completing no waiter, and the lane's
    /// next exchange, to send now.
    Question(Option<Ask>),
}

/// An exchange of a question: one send, one answer to every call in it.
struct Exchange {
    ask: Ask,
    /// The executions it answers, in call order.
    execs: Vec<Token>,
    /// An IB method asked it, so an open-orders exchange takes every open
    /// order until its end, with its waiters there or gone. A `Client`
    /// call alone only sends.
    records: bool,
}

/// A question's lane.
#[derive(Default)]
struct Lane {
    /// Sent and not yet at its terminal boundary.
    in_flight: Option<Exchange>,
    /// Waiting their turn, one per distinct argument set.
    unsent: VecDeque<Exchange>,
    /// Cancels of the question sent and not yet confirmed in order.
    cancels: usize,
}

impl Lane {
    /// Sends the next exchange if nothing is in flight and no cancel is
    /// unconfirmed.
    fn next(&mut self) -> Option<Ask> {
        if self.in_flight.is_some() || self.cancels > 0 {
            return None;
        }
        let ask = self.unsent.pop_front()?;
        Some(self.in_flight.insert(ask).ask.clone())
    }
}

/// Every execution of an IB's current generation, and its questions' lanes.
pub(crate) struct Requests {
    ib: IbId,
    generation: u64,
    execs: HashMap<Token, Exec>,
    /// The numbered executions, by id: ids are never reused in a generation.
    ids: HashMap<i64, Token>,
    lanes: HashMap<ReqKey, Lane>,
}

impl Requests {
    /// The requests of the IB `ib`, before its first generation.
    pub(crate) fn new(ib: IbId) -> Self {
        Requests {
            ib,
            generation: 0,
            execs: HashMap::new(),
            ids: HashMap::new(),
            lanes: HashMap::new(),
        }
    }

    /// Starts generation `generation`, with nothing registered.
    pub(crate) fn begin(&mut self, generation: u64) {
        self.generation = generation;
        self.execs.clear();
        self.ids.clear();
        self.lanes.clear();
    }

    /// The generation's end: every execution, to fail with the teardown's
    /// error, in the order they were made. The lanes are emptied.
    pub(crate) fn take_all(&mut self) -> Vec<Exec> {
        self.ids.clear();
        self.lanes.clear();
        let mut all: Vec<Exec> = self.execs.drain().map(|(_, x)| x).collect();
        all.sort_by_key(|x| x.token.0);
        all
    }

    /// Whether `o` is this IB's current generation.
    pub(crate) fn owns(&self, o: &Origin) -> bool {
        o.ib == self.ib && o.generation == self.generation
    }

    /// A new execution keyed by `key`, started in the current generation,
    /// with nothing yet to complete, assemble, feed or run.
    pub(crate) fn exec(&mut self, key: ReqKey) -> Exec {
        self.exec_as(key, fresh_token())
    }

    /// As [`Requests::exec`], under the token its waiter was made with.
    pub(crate) fn exec_as(&mut self, key: ReqKey, token: Token) -> Exec {
        let req_id = match key {
            ReqKey::Id(id) => id,
            ReqKey::Question(_) => -1,
        };
        Exec {
            token,
            origin: Origin {
                ib: self.ib,
                generation: self.generation,
                req_id,
            },
            key,
            waiter: None,
            acc: None,
            route: Route::None,
            deadline: None,
            guard: None,
        }
    }

    /// Where the execution `token` was started.
    pub(crate) fn origin(&self, token: Token) -> Option<Origin> {
        self.execs.get(&token).map(|x| x.origin)
    }

    /// Registers a numbered execution, in the owner step that sends it.
    pub(crate) fn insert(&mut self, x: Exec) {
        if let ReqKey::Id(id) = x.key {
            self.ids.insert(id, x.token);
        }
        self.execs.insert(x.token, x);
    }

    /// Whether `id` numbers a request of this generation still registered:
    /// ib_async's `reqId in self._futures`.
    pub(crate) fn is_request(&self, id: i64) -> bool {
        self.ids.contains_key(&id)
    }

    /// The numbered request `id` at its end: its execution, to complete.
    pub(crate) fn end(&mut self, id: i64) -> Option<Exec> {
        let token = *self.ids.get(&id)?;
        self.take(token)
    }

    /// Calls `add` on the result each execution `key` answers is
    /// assembling: the numbered request's, or those of the question's
    /// exchange in flight. One that left has none.
    pub(crate) fn accumulate<T: 'static>(&mut self, key: ReqKey, mut add: impl FnMut(&mut T)) {
        let tokens = match key {
            ReqKey::Id(id) => self.ids.get(&id).copied().into_iter().collect(),
            ReqKey::Question(_) => self
                .lanes
                .get(&key)
                .and_then(|l| l.in_flight.as_ref())
                .map(|x| x.execs.clone())
                .unwrap_or_default(),
        };
        for t in tokens {
            if let Some(acc) = self
                .execs
                .get_mut(&t)
                .and_then(|x| x.acc.as_mut())
                .and_then(|a| a.downcast_mut::<T>())
            {
                add(acc);
            }
        }
    }

    /// Asks a question: `x` is the IB method's execution, made by
    /// `exec(ask.key())`, and `None` for a `Client` call. Gives `ask` back when it is to be sent now, its lane
    /// being free. Otherwise it joins the unsent exchange with the same
    /// arguments, or waits in the lane as a new one, holding one admission
    /// charge until it is sent ([`Requests::unsent`]).
    pub(crate) fn ask(&mut self, ask: Ask, x: Option<Exec>) -> Option<Ask> {
        let lane = self.lanes.entry(ask.key()).or_default();
        let records = x.is_some();
        let token = x.map(|x| {
            let t = x.token;
            self.execs.insert(t, x);
            t
        });
        let exchange = match lane.unsent.iter().position(|e| e.ask == ask) {
            Some(i) => &mut lane.unsent[i],
            None => {
                lane.unsent.push_back(Exchange {
                    ask,
                    execs: Vec::new(),
                    records: false,
                });
                let last = lane.unsent.len() - 1;
                &mut lane.unsent[last]
            }
        };
        exchange.execs.extend(token);
        exchange.records |= records;
        lane.next()
    }

    /// The exchanges waiting unsent in every lane: the admission charges
    /// the lanes hold.
    pub(crate) fn unsent(&self) -> usize {
        self.lanes.values().map(|l| l.unsent.len()).sum()
    }

    /// The terminal callback of `q`'s exchange in flight: its executions,
    /// to complete with the answer, and the lane's next exchange, to send.
    pub(crate) fn answered(&mut self, q: Question) -> (Vec<Exec>, Option<Ask>) {
        let Some(lane) = self.lanes.get_mut(&ReqKey::question(q)) else {
            return (Vec::new(), None);
        };
        let tokens = lane.in_flight.take().map(|x| x.execs).unwrap_or_default();
        let next = lane.next();
        let execs = tokens.iter().filter_map(|t| self.execs.remove(t)).collect();
        (execs, next)
    }

    /// An error that ends `q`'s exchange in flight. Its waiters are not
    /// completed, as ib_async's `error` ends only a future keyed by its
    /// number: each waits on until its own deadline. Gives the lane's next
    /// exchange, to send.
    fn ended(&mut self, q: Question) -> Option<Ask> {
        let lane = self.lanes.get_mut(&ReqKey::question(q))?;
        lane.in_flight = None;
        lane.next()
    }

    /// A cancel of the question `q` (`Client::cancel_positions`,
    /// `Client::req_account_updates(false, ..)`), in the owner step that
    /// sends it. Every exchange still unsent is withdrawn and never sent,
    /// its charge released and its waiters left to their own deadlines. The
    /// lane then sends nothing until the engine confirms the cancel.
    pub(crate) fn cancel(&mut self, q: Question) {
        let lane = self.lanes.entry(ReqKey::question(q)).or_default();
        lane.unsent.clear();
        lane.cancels += 1;
    }

    /// The engine's confirmation of `q`'s oldest unconfirmed cancel, which
    /// follows everything of the exchange it ends. An exchange still in
    /// flight was sent before that cancel and ends here, completing no
    /// waiter. Gives the lane's next exchange, to send.
    pub(crate) fn retired(&mut self, q: Question) -> Option<Ask> {
        let lane = self.lanes.get_mut(&ReqKey::question(q))?;
        if lane.cancels == 0 {
            return None;
        }
        lane.cancels -= 1;
        lane.in_flight = None;
        lane.next()
    }

    /// Whether an exchange of `q` an IB method asked is in flight, its
    /// waiters there or gone: until its end, every open order that is not a
    /// what-if belongs to an open-orders one and fires no
    /// `open_order_event` (wr:706-712), and completed orders are kept only
    /// for a completed-orders one (wr:722-730).
    pub(crate) fn records(&self, q: Question) -> bool {
        self.lanes
            .get(&ReqKey::question(q))
            .and_then(|l| l.in_flight.as_ref())
            .is_some_and(|x| x.records)
    }

    /// What an error does to the requests, by its origin alone.
    pub(crate) fn error(&mut self, origin: ErrorOrigin) -> Refused {
        match origin {
            ErrorOrigin::Request { id, ends: true } => match self.end(id) {
                Some(mut x) => {
                    x.guard = x.guard.and_then(Cleanup::refused);
                    Refused::Request(x)
                }
                None => Refused::Nothing,
            },
            ErrorOrigin::Question { q, ends: true } => Refused::Question(self.ended(q)),
            _ => Refused::Nothing,
        }
    }

    /// Removes the execution `token`, wherever it is, and gives it: its
    /// method's deadline has fired, or it ended. A question's exchange stays
    /// in its lane.
    pub(crate) fn take(&mut self, token: Token) -> Option<Exec> {
        let x = self.execs.remove(&token)?;
        match x.key {
            ReqKey::Id(id) => {
                if self.ids.get(&id) == Some(&token) {
                    self.ids.remove(&id);
                }
            }
            key @ ReqKey::Question(_) => {
                if let Some(lane) = self.lanes.get_mut(&key) {
                    for e in lane.in_flight.iter_mut().chain(lane.unsent.iter_mut()) {
                        e.execs.retain(|t| *t != token);
                    }
                }
            }
        }
        Some(x)
    }

    /// Retires the execution `token`, whose waiter is gone: its result is
    /// dropped, its deadline removed from `heap`, and it is removed unless
    /// it feeds state. Gives its cleanup, to run. Nothing is sent for it,
    /// and a question's exchange stays in its lane.
    pub(crate) fn retire<K>(
        &mut self,
        token: Token,
        heap: &mut DeadlineHeap<K>,
    ) -> Option<Cleanup> {
        let x = self.execs.get_mut(&token)?;
        x.waiter = None;
        x.acc = None;
        let cleanup = x.settle(heap);
        if x.route == Route::None {
            self.take(token);
        }
        cleanup
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::task::{Context, Poll, Waker};
    use std::time::Instant;

    use super::*;
    use crate::pending::Pending;

    /// A registered waiter, and its execution holding the reply.
    fn waiting<T: Send + 'static>(r: &mut Requests, key: ReqKey) -> (Pending<T>, Exec) {
        let (p, reply) = Pending::new(None);
        let mut x = r.exec(key);
        x.waiter = Some(Box::new(reply));
        (p, x)
    }

    /// The published result, or `None` while nothing is.
    fn result<T>(p: &mut Pending<T>) -> Option<Result<T>> {
        match Pin::new(p).poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(r) => Some(r),
            Poll::Pending => None,
        }
    }

    fn ok<T: std::fmt::Debug>(r: Option<Result<T>>) -> T {
        match r {
            Some(Ok(v)) => v,
            other => unreachable!("{other:?}"),
        }
    }

    #[test]
    fn ids_are_checked_consecutive_and_below_the_reserved_band() {
        let end = FIRST_RESERVED_REQUEST_ID;
        let mut s = IdSpace::new();
        assert_eq!(s.allocate(0, 3).ok(), Some(0));
        assert_eq!(s.allocate(0, 1).ok(), Some(3));
        // The engine's floor above `next`: a named order's id is cleared
        // before its `open_order`, and after a reconnect.
        assert_eq!(s.allocate(100, 1).ok(), Some(100));
        assert_eq!(s.allocate(7, 1).ok(), Some(101));
        // An `open_order` numbered at 2^33 or inside the band is ignored.
        s.raise((1 << 33) + 1);
        s.raise(0xC000_0001 + 1);
        s.raise(end + 1);
        assert_eq!(s.allocate(0, 1).ok(), Some(102));
        s.raise(200);
        assert_eq!(s.allocate(0, 1).ok(), Some(200));
        // The last id below the band, and then none.
        s.raise(end - 1);
        assert_eq!(s.allocate(0, 1).ok(), Some(end - 1));
        match s.allocate(0, 1) {
            Err(Error::Value(m)) => assert_eq!(
                m,
                format!("request ids exhausted: {end} is inside the range the engine reserves")
            ),
            other => unreachable!("{other:?}"),
        }
        // Three that would cross it fail unchanged.
        let mut s = IdSpace::new();
        s.raise(end - 2);
        assert!(s.allocate(0, 3).is_err());
        assert_eq!(s.allocate(0, 2).ok(), Some(end - 2));
        // An order floor no request can carry, as on an account whose orders
        // were numbered past the band (paper, 2026-09-25): requests go on
        // from the counter the engine's shared id started.
        let wide = 1_787_685_160_171_388;
        let mut s = IdSpace::new();
        s.raise(1_790_347_892);
        assert_eq!(s.allocate(wide, 1).unwrap(), 1_790_347_892);
        assert_eq!(s.allocate(wide, 1).unwrap(), 1_790_347_893);
    }

    #[test]
    fn a_numbered_request_is_answered_only_while_its_waiter_is_there() {
        let mut r = Requests::new(IbId::new());
        r.begin(1);
        let mut heap = DeadlineHeap::<Token>::default();

        // Answered: what arrived under its id, with the deadline and the
        // cleanup settled once.
        let (mut p, mut x) = waiting::<Vec<i32>>(&mut r, ReqKey::Id(1));
        x.acc = Some(Box::new(Vec::<i32>::new()));
        x.deadline = Some(heap.insert(Instant::now(), x.token));
        x.guard = Some(Cleanup::CancelOptionPrice);
        r.insert(x);
        assert!(r.is_request(1));
        r.accumulate::<Vec<i32>>(ReqKey::Id(1), |v| v.push(7));
        r.accumulate::<Vec<i32>>(ReqKey::Id(2), |v| v.push(8));
        let mut x = r.end(1).expect("registered");
        assert!(!r.is_request(1));
        assert_eq!(x.settle(&mut heap), Some(Cleanup::CancelOptionPrice));
        assert_eq!(heap.len(), 0);
        let acc = x.acc.take().expect("a collection");
        assert!(x.finish(Ok(acc)));
        assert_eq!(ok(result(&mut p)), vec![7]);

        // Its waiter gone, one feeding a ticker stays: a late answer feeds
        // the ticker and completes nothing.
        for route in [Route::Ticker, Route::None] {
            let (p, mut x) = waiting::<Vec<i32>>(&mut r, ReqKey::Id(2));
            let token = x.token;
            x.acc = Some(Box::new(Vec::<i32>::new()));
            x.route = route;
            x.deadline = Some(heap.insert(Instant::now(), token));
            x.guard = Some(Cleanup::EndSnapshot);
            r.insert(x);
            drop(p);
            assert_eq!(r.retire(token, &mut heap), Some(Cleanup::EndSnapshot));
            assert_eq!(heap.len(), 0);
            let mut fed = 0;
            r.accumulate::<Vec<i32>>(ReqKey::Id(2), |_| fed += 1);
            assert_eq!(fed, 0, "the result was dropped");
            match r.end(2) {
                Some(mut x) => {
                    assert_eq!(route, Route::Ticker);
                    assert_eq!(x.settle(&mut heap), None, "the cleanup ran once");
                    assert!(!x.finish(Ok(Box::new(vec![1]))));
                }
                None => assert_eq!(route, Route::None),
            }
        }

        // A single value arrives as it is; the wrong type is an error.
        let (mut p, x) = waiting::<String>(&mut r, ReqKey::Id(3));
        r.insert(x);
        assert!(
            r.end(3)
                .expect("registered")
                .finish(Ok(Box::new("t".to_owned())))
        );
        assert_eq!(ok(result(&mut p)), "t");
        let (mut p, x) = waiting::<String>(&mut r, ReqKey::Id(4));
        assert!(x.finish(Ok(Box::new(4))));
        assert!(matches!(result(&mut p), Some(Err(Error::Value(_)))));
    }

    #[test]
    fn an_error_ends_what_its_origin_names() {
        let mut r = Requests::new(IbId::new());
        r.begin(1);
        let refused = Error::Request {
            req_id: 1,
            code: 321,
            message: "m".into(),
        };
        // (raise_request_errors, a collection, what the waiter gets)
        for (raise, collection, gets_error) in [
            (false, true, false),
            (true, true, true),
            (false, false, true),
            (true, false, true),
        ] {
            let (mut p, mut x) = waiting::<Vec<i32>>(&mut r, ReqKey::Id(1));
            if collection {
                x.acc = Some(Box::new(vec![5]));
            }
            r.insert(x);
            // A notice its answer follows (the 321 executions notice) ends
            // nothing; the refusal ends it, even at 321.
            let notice = ErrorOrigin::Request { id: 1, ends: false };
            assert!(matches!(r.error(notice), Refused::Nothing));
            assert!(r.is_request(1));
            let Refused::Request(x) = r.error(ErrorOrigin::Request { id: 1, ends: true }) else {
                unreachable!("the request ends");
            };
            assert!(x.fail(refused.clone(), raise));
            match result(&mut p) {
                Some(Err(Error::Request { code: 321, .. })) => assert!(gets_error),
                Some(Ok(v)) => assert!(!gets_error && v == vec![5]),
                other => unreachable!("{other:?}"),
            }
        }
        // An order's error, the session's and one under no request here
        // leave the requests alone.
        let (_p, x) = waiting::<Vec<i32>>(&mut r, ReqKey::Id(2));
        r.insert(x);
        for origin in [
            ErrorOrigin::Order {
                id: 2,
                op: crate::engine::OrderOp::Place,
            },
            ErrorOrigin::Session,
            ErrorOrigin::Request { id: 3, ends: true },
        ] {
            assert!(matches!(r.error(origin), Refused::Nothing));
        }
        assert!(r.is_request(2));
    }

    #[test]
    fn a_question_lane_holds_each_call_until_the_exchange_before_it_ends() {
        let mut r = Requests::new(IbId::new());
        r.begin(1);
        let mut heap = DeadlineHeap::<Token>::default();
        let fa = ReqKey::question(Question::Fa);

        // The first is sent; its waiter leaves and the exchange stays.
        let (_a, x) = waiting::<String>(&mut r, fa);
        let first = x.token;
        assert_eq!(r.ask(Ask::Fa(1), Some(x)), Some(Ask::Fa(1)));
        r.retire(first, &mut heap);
        // A different type waits, an equal one joins it, and a third waits
        // apart: one charge per distinct exchange.
        let (mut b, x) = waiting::<String>(&mut r, fa);
        assert_eq!(r.ask(Ask::Fa(2), Some(x)), None);
        let (mut c, x) = waiting::<String>(&mut r, fa);
        assert_eq!(r.ask(Ask::Fa(2), Some(x)), None);
        let (d, x) = waiting::<String>(&mut r, fa);
        let gone = x.token;
        assert_eq!(r.ask(Ask::Fa(3), Some(x)), None);
        assert_eq!(r.unsent(), 2);
        // A notice moves nothing; the error that ends the exchange does.
        let notice = ErrorOrigin::Question {
            q: Question::Fa,
            ends: false,
        };
        assert!(matches!(r.error(notice), Refused::Nothing));
        assert_eq!(r.unsent(), 2);
        // The first's answer, however late, completes nothing of the
        // second and sends it.
        let (execs, next) = r.answered(Question::Fa);
        assert!(execs.is_empty());
        assert_eq!(next, Some(Ask::Fa(2)));
        assert_eq!(r.unsent(), 1);
        let ends = ErrorOrigin::Question {
            q: Question::Fa,
            ends: true,
        };
        // An exchange whose waiter left in the lane is still sent.
        drop(d);
        r.retire(gone, &mut heap);
        let Refused::Question(next) = r.error(ends) else {
            unreachable!("the question's error ends its exchange");
        };
        assert_eq!(next, Some(Ask::Fa(3)));
        assert_eq!(r.unsent(), 0);
        // The error completed neither caller of the second exchange.
        assert!(result(&mut b).is_none() && result(&mut c).is_none());
        // Sent while one is in flight, the next waits; at its end the
        // exchange's own callers are answered.
        let (mut e, x) = waiting::<String>(&mut r, fa);
        assert_eq!(r.ask(Ask::Fa(4), Some(x)), None);
        assert_eq!(r.answered(Question::Fa).1, Some(Ask::Fa(4)));
        let (execs, next) = r.answered(Question::Fa);
        assert_eq!(next, None);
        for x in execs {
            assert!(x.finish(Ok(Box::new("xml".to_owned()))));
        }
        assert_eq!(ok(result(&mut e)), "xml");
        // The free lane sends at once.
        assert_eq!(r.ask(Ask::Fa(1), None), Some(Ask::Fa(1)));
    }

    #[test]
    fn a_questions_cancel_withdraws_what_is_unsent_and_holds_the_lane_until_confirmed() {
        let key = ReqKey::question(Question::Positions);
        for answered_first in [false, true] {
            let mut r = Requests::new(IbId::new());
            r.begin(1);
            let (mut a, x) = waiting::<()>(&mut r, key);
            assert_eq!(r.ask(Ask::Positions, Some(x)), Some(Ask::Positions));
            let (mut b, x) = waiting::<()>(&mut r, key);
            let withdrawn = x.token;
            assert_eq!(r.ask(Ask::Positions, Some(x)), None);
            r.cancel(Question::Positions);
            assert_eq!(r.unsent(), 0, "the withdrawn exchange's charge is released");
            let (_c, x) = waiting::<()>(&mut r, key);
            assert_eq!(r.ask(Ask::Positions, Some(x)), None);
            r.cancel(Question::Positions);
            let (_d, x) = waiting::<()>(&mut r, key);
            assert_eq!(r.ask(Ask::Positions, Some(x)), None);
            assert_eq!(r.unsent(), 1);
            if answered_first {
                let (execs, next) = r.answered(Question::Positions);
                assert_eq!((execs.len(), next), (1, None));
            }
            // The first confirmation ends the first exchange; the lane holds
            // until the second.
            assert_eq!(r.retired(Question::Positions), None);
            assert_eq!(r.retired(Question::Positions), Some(Ask::Positions));
            assert_eq!(r.retired(Question::Positions), None);
            // The withdrawn exchange's waiter waits on for its deadline.
            assert!(result(&mut b).is_none());
            assert!(r.take(withdrawn).is_some());
            if !answered_first {
                assert!(result(&mut a).is_none());
            }
        }
    }

    #[test]
    fn an_unknown_market_rule_frees_its_lane_and_gets_none_at_its_deadline() {
        let mut r = Requests::new(IbId::new());
        r.begin(1);
        let mut heap = DeadlineHeap::<Token>::default();
        let key = ReqKey::question(Question::MarketRule(99));
        let (mut p, mut x) = waiting::<Option<Vec<f64>>>(&mut r, key);
        let token = x.token;
        x.deadline = Some(heap.insert(Instant::now(), token));
        assert_eq!(
            r.ask(Ask::MarketRule(99), Some(x)),
            Some(Ask::MarketRule(99))
        );
        let ends = ErrorOrigin::Question {
            q: Question::MarketRule(99),
            ends: true,
        };
        assert!(matches!(r.error(ends), Refused::Question(None)));
        assert!(result(&mut p).is_none());
        // The lane is free again for another call of the rule.
        assert_eq!(r.ask(Ask::MarketRule(99), None), Some(Ask::MarketRule(99)));
        // The 1 s deadline fires and answers `None`.
        let fired = heap.take_due(Instant::now());
        assert_eq!(fired, vec![token]);
        let x = r.take(token).expect("waiting for its deadline");
        assert!(x.finish(Ok(Box::new(None::<Vec<f64>>))));
        assert_eq!(ok(result(&mut p)), None);
    }

    #[test]
    fn open_orders_are_recorded_while_an_ib_exchange_is_in_flight() {
        let mut r = Requests::new(IbId::new());
        r.begin(1);
        let mut heap = DeadlineHeap::<Token>::default();
        let key = ReqKey::question(Question::OpenOrders);
        // A `Client` call only sends.
        assert_eq!(r.ask(Ask::OpenOrders, None), Some(Ask::OpenOrders));
        assert!(!r.records(Question::OpenOrders));
        // `req_all_open_orders` waits in the same lane.
        let (_p, x) = waiting::<Vec<i32>>(&mut r, key);
        let token = x.token;
        assert_eq!(r.ask(Ask::AllOpenOrders, Some(x)), None);
        assert_eq!(r.answered(Question::OpenOrders).1, Some(Ask::AllOpenOrders));
        assert!(r.records(Question::OpenOrders));
        // Its waiter gone, it still takes them until its end.
        r.retire(token, &mut heap);
        assert!(r.records(Question::OpenOrders));
        let (execs, _) = r.answered(Question::AllOpenOrders);
        assert!(execs.is_empty());
        assert!(!r.records(Question::OpenOrders));
    }

    #[test]
    fn only_this_ibs_current_generation_is_owned_and_its_end_takes_everything() {
        let (mut a, mut b) = (Requests::new(IbId::new()), Requests::new(IbId::new()));
        a.begin(1);
        b.begin(1);
        let xa = a.exec(ReqKey::Id(1));
        let xb = b.exec(ReqKey::Id(1));
        assert_eq!(xa.origin.req_id, xb.origin.req_id);
        assert!(a.owns(&xa.origin));
        assert!(
            !a.owns(&xb.origin),
            "another IB at the same generation and id"
        );
        let (old, mut made) = (xa.origin, vec![xa.token.0]);
        a.insert(xa);
        let (_p, x) = waiting::<()>(&mut a, ReqKey::question(Question::Positions));
        made.push(x.token.0);
        assert_eq!(a.ask(Ask::Positions, Some(x)), Some(Ask::Positions));
        let (_q, x) = waiting::<()>(&mut a, ReqKey::question(Question::Positions));
        made.push(x.token.0);
        assert_eq!(a.ask(Ask::Positions, Some(x)), None);
        let all = a.take_all();
        assert_eq!(all.iter().map(|x| x.token.0).collect::<Vec<_>>(), made);
        assert_eq!(a.unsent(), 0);
        a.begin(2);
        assert!(!a.owns(&old), "an ended generation");
        assert!(!a.is_request(1));
        assert_eq!(a.ask(Ask::Positions, None), Some(Ask::Positions));
        let x = a.exec(ReqKey::Id(1));
        assert!(a.owns(&x.origin));
        assert!(x.token.0 > made[2], "tokens are unique for the IB's life");
    }
}

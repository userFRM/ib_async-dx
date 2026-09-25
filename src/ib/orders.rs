//! `IB`'s order methods: ib_async's `placeOrder`, `cancelOrder` and the
//! requests about orders and executions.
//!
//! Each runs as one owner step: inline on the thread that runs every IB,
//! otherwise as a command this thread waits for (a fire-and-forget method)
//! or whose answer a [`Pending`] carries (a request). As ib_async's, every
//! method sends first and then changes the state and emits, so the reply
//! comes after the step's emissions.

use std::any::Any;
use std::sync::{Arc, Weak};

use crate::contract::Contract;
use crate::engine::{self as e, EClient, ErrorOrigin, ExerciseStates, OrderOp};
use crate::error::{Error, Result};
use crate::live::{Holder, Live};
use crate::objects::{ExecutionFilter, Fill, TradeLogEntry};
use crate::order::{BracketOrder, Order, OrderState, OrderStatus, Trade};
use crate::owner::{Class, LOG_IB, Shared};
use crate::pending::{Pending, Reply};
use crate::requests::{Ask, ReqKey};
use crate::state::order_key;

use super::{IB, IBHandle};

/// The published session, which every send needs: ib_async's `send` and
/// `getReqId` raise `ConnectionError("Not connected")` without one.
fn session(ib: &Shared) -> Result<Arc<EClient>> {
    ib.connected().map(|(_, c)| c).ok_or(Error::NotConnected)
}

/// `n` consecutive ids from the IB's one space, above the engine's floor:
/// ib_async's `getReqId`, `n` times in a row.
fn allocate(ib: &Shared, client: &EClient, n: i64) -> Result<i64> {
    let floor = client.order_id_floor();
    ib.core().ids.allocate(floor, n)
}

/// What `client.placeOrder` does to the order it sends: only a volatility
/// order carries `volatility` (cl:474-482).
pub(crate) fn clear_volatility(o: &mut Order) {
    if !o.order_type.starts_with("VOL") {
        o.volatility = None;
    }
}

/// Sends `order` under `order_id`, or pushes the refusal of a value the
/// engine cannot carry in its place in the session's order, as a gateway's
/// rejection of it would arrive.
fn send_order(client: &EClient, order_id: i64, contract: &Contract, order: &Order, op: OrderOp) {
    match e::Order::try_from(order) {
        Ok(o) => client.place_order(order_id, &e::Contract::from(contract), &o),
        Err(r) => client.refuse(ErrorOrigin::Order { id: order_id, op }, r.code, &r.msg),
    }
}

/// `placeOrder` (ib:780-814), as one owner step.
fn place(ib: &Arc<Shared>, contract: &Contract, order: &Live<Order>) -> Result<Live<Trade>> {
    let client = ib.connected().map(|(_, c)| c);
    let stated = order.read().order_id;
    let order_id = match (stated, &client) {
        (0, Some(c)) => allocate(ib, c, 1)?,
        (0, None) => return Err(Error::NotConnected),
        (id, _) => id,
    };
    let o = order.read();
    let o = if o.volatility.is_some() && !o.order_type.starts_with("VOL") {
        order.update(clear_volatility);
        order.read()
    } else {
        o
    };
    let client = client.ok_or(Error::NotConnected)?;
    let (key, found) = {
        let c = ib.core();
        let key = order_key(c.state.client_id, order_id, o.perm_id);
        (key, c.state.trades.get(&key).cloned())
    };
    let op = if found.is_some() {
        OrderOp::Modify
    } else {
        OrderOp::Place
    };
    send_order(&client, order_id, contract, &o, op);
    ib.queue.sent(1);
    let now = ib.wall_now();
    if let Some(trade) = found {
        // a modification of an existing order
        if trade.read().is_done() {
            return Err(Error::Value(format!(
                "placeOrder: order {order_id} is done and cannot be modified"
            )));
        }
        trade.update(|t| {
            t.log.push(TradeLogEntry {
                time: now,
                status: t.order_status.status.clone(),
                message: "Modify".into(),
                error_code: 0,
            });
        });
        log::info!(target: LOG_IB, "placeOrder: Modify order {trade:?}");
        trade.modify_event().emit(&trade);
        ib.events.order_modify_event.emit(&trade);
        return Ok(trade);
    }
    // a new order
    let holder: Weak<dyn Holder> = Arc::<Shared>::downgrade(ib);
    order.bind(holder.clone());
    let client_id = ib.core().state.client_id;
    order.update(|o| {
        o.client_id = client_id;
        o.order_id = order_id;
    });
    let trade = Live::new(Trade {
        contract: contract.clone(),
        order: order.clone(),
        order_status: OrderStatus {
            order_id,
            status: OrderStatus::PENDING_SUBMIT.into(),
            ..OrderStatus::default()
        },
        fills: Vec::new(),
        log: vec![TradeLogEntry {
            time: now,
            status: OrderStatus::PENDING_SUBMIT.into(),
            message: String::new(),
            error_code: 0,
        }],
        advanced_error: String::new(),
    });
    trade.bind(holder);
    ib.core().state.trades.insert(key, trade.clone());
    log::info!(target: LOG_IB, "placeOrder: New order {trade:?}");
    ib.events.new_order_event.emit(&trade);
    Ok(trade)
}

/// `cancelOrder` (ib:816-855), as one owner step.
fn cancel(ib: &Arc<Shared>, order: &Live<Order>, time: &str) -> Result<Option<Live<Trade>>> {
    let client = session(ib)?;
    let o = order.read();
    let own = ib.core().state.client_id;
    // An order this IB did not place is withdrawn by its permId: its id,
    // if it has one, numbers another client's order.
    if (o.order_id <= 0 || o.client_id != own) && o.perm_id != 0 {
        client.cancel_order_by_perm_id(o.perm_id);
    } else {
        client.cancel_order(o.order_id, time);
    }
    let now = ib.wall_now();
    let key = order_key(o.client_id, o.order_id, o.perm_id);
    let found = ib.core().state.trades.get(&key).cloned();
    let Some(trade) = found else {
        log::error!(target: LOG_IB, "cancelOrder: Unknown orderId {}", o.order_id);
        return Ok(None);
    };
    if trade.read().is_done() {
        return Ok(Some(trade));
    }
    let cancelled = trade.update(|t| {
        let status = t.order_status.status.as_str();
        let cancelled =
            status == OrderStatus::PENDING_SUBMIT && !o.transmit || status == OrderStatus::INACTIVE;
        let status = if cancelled {
            OrderStatus::CANCELLED
        } else {
            OrderStatus::PENDING_CANCEL
        };
        t.log.push(TradeLogEntry {
            time: now,
            status: status.into(),
            message: String::new(),
            error_code: 0,
        });
        t.order_status.status = status.into();
        cancelled
    });
    log::info!(target: LOG_IB, "cancelOrder: {trade:?}");
    trade.cancel_event().emit(&trade);
    trade.status_event().emit(&trade);
    ib.events.cancel_order_event.emit(&trade);
    ib.events.order_status_event.emit(&trade);
    if cancelled {
        trade.cancelled_event().emit(&trade);
    }
    Ok(Some(trade))
}

/// A numbered request: an id from the IB's space, its execution under
/// that id, and `send` with the id, in one owner step. `acc` is the
/// collection a list result starts from; `contract` is what the
/// request's errors name. ib_async's `getReqId`, `startReq` and send.
fn numbered<T: Send + 'static>(
    shared: &Arc<Shared>,
    acc: Option<Box<dyn Any + Send>>,
    contract: Option<Contract>,
    send: impl FnOnce(&EClient, i64) + Send + 'static,
) -> Pending<T> {
    shared.request_connected(move |ib, token, reply: Reply<T>| {
        let started = session(ib).and_then(|c| allocate(ib, &c, 1).map(|id| (id, c)));
        let (id, client) = match started {
            Ok(v) => v,
            Err(e) => {
                reply.send(Err(e));
                return;
            }
        };
        {
            let mut c = ib.core();
            let mut x = c.requests.exec_as(ReqKey::Id(id), token);
            x.waiter = Some(Box::new(reply));
            x.acc = acc;
            if let Some(contract) = contract {
                c.state.req_id_to_contract.insert(id, contract);
            }
            c.requests.insert(x);
        }
        send(&client, id);
        ib.queue.sent(1);
    })
}

/// A question about orders, in its lane: sent now if the lane is free,
/// else in its turn. Its answer is the trades its exchange recorded.
fn orders_question(shared: &Arc<Shared>, ask: Ask) -> Pending<Vec<Live<Trade>>> {
    shared.request_connected(move |ib, token, reply: Reply<Vec<Live<Trade>>>| {
        let client = match session(ib) {
            Ok(c) => c,
            Err(e) => {
                reply.send(Err(e));
                return;
            }
        };
        let send = {
            let mut c = ib.core();
            let mut x = c.requests.exec_as(ask.key(), token);
            x.waiter = Some(Box::new(reply));
            x.acc = Some(Box::new(Vec::<Live<Trade>>::new()));
            c.requests.ask(ask, Some(x))
        };
        if let Some(ask) = send {
            ib.send_ask(&client, &ask);
        }
    })
}

impl IBHandle {
    /// Places a new order, or modifies the one placed with this order's id:
    /// ib_async's `placeOrder`.
    ///
    /// An order with no id is given one. The order is sent first; then a
    /// new order is numbered in place (`client_id`, `order_id`) and gets a
    /// `PendingSubmit` trade that holds it and `new_order_event` fires, or
    /// the trade of a live order gains a `Modify` entry and
    /// `modify_event`, then `order_modify_event`, fire. A modify of a done
    /// order is still sent, and is then `Err(Value)` with no entry and no
    /// event. A refusal arrives later, on `error_event` and the trade. An
    /// order that is not a volatility order has its `volatility` cleared.
    pub fn place_order(&self, contract: &Contract, order: &Live<Order>) -> Result<Live<Trade>> {
        let (contract, order) = (contract.clone(), order.clone());
        self.shared
            .step(Class::Request, move |ib| place(ib, &contract, &order))
    }

    /// Cancels an order and gives its trade, `None` for an order this IB
    /// does not know: ib_async's `cancelOrder`.
    ///
    /// The cancel is sent first; an order this IB did not place is
    /// cancelled by its `perm_id`. A trade not yet done then becomes
    /// `PendingCancel`, or `Cancelled` when it was never transmitted or is
    /// `Inactive`, with a log entry, and `cancel_event`, `status_event`,
    /// `cancel_order_event`, `order_status_event` and, once `Cancelled`,
    /// `cancelled_event` fire. `manual_cancel_order_time` does not travel:
    /// the engine says so on the order.
    pub fn cancel_order(
        &self,
        order: &Live<Order>,
        manual_cancel_order_time: &str,
    ) -> Result<Option<Live<Trade>>> {
        let (order, time) = (order.clone(), manual_cancel_order_time.to_owned());
        self.shared
            .step(Class::Control, move |ib| cancel(ib, &order, &time))
    }

    /// A limit order bracketed by a take-profit limit order and a stop-loss
    /// stop order, numbered with three consecutive ids and not yet placed:
    /// ib_async's `bracketOrder`. Only the stop loss transmits, so placing
    /// the three in order sends the bracket as one. `action` is `BUY` or
    /// `SELL`, else `Err(Value)`.
    pub fn bracket_order(
        &self,
        action: &str,
        quantity: f64,
        limit_price: f64,
        take_profit_price: f64,
        stop_loss_price: f64,
    ) -> Result<BracketOrder> {
        let reverse = match action {
            "BUY" => "SELL",
            "SELL" => "BUY",
            _ => {
                return Err(Error::Value(format!(
                    "bracketOrder: the action must be BUY or SELL, not {action:?}"
                )));
            }
        };
        let parent_id = self.shared.step(Class::Control, |ib| {
            let client = session(ib)?;
            allocate(ib, &client, 3)
        })?;
        let parent = Order {
            order_id: parent_id,
            transmit: false,
            ..Order::limit(action, quantity, limit_price)
        };
        let take_profit = Order {
            order_id: parent_id + 1,
            transmit: false,
            parent_id,
            ..Order::limit(reverse, quantity, take_profit_price)
        };
        let stop_loss = Order {
            order_id: parent_id + 2,
            transmit: true,
            parent_id,
            ..Order::stop(reverse, quantity, stop_loss_price)
        };
        Ok(BracketOrder {
            parent: Live::new(parent),
            take_profit: Live::new(take_profit),
            stop_loss: Live::new(stop_loss),
        })
    }

    /// The commission and margin the order would have, without placing it:
    /// ib_async's `whatIfOrder`. `order` is not changed. Bounded by
    /// `IBConfig.request_timeout`.
    pub fn what_if_order(&self, contract: &Contract, order: &Order) -> Result<OrderState> {
        self.what_if_order_async(contract, order)
            .wait(self.request_timeout())
    }

    /// `what_if_order`'s async form: ib_async's `whatIfOrderAsync`. A copy
    /// of `order` marked `what_if` is sent under a new id, and the answer is
    /// the first report of it that states a margin change; no trade is made
    /// and no event fires.
    pub fn what_if_order_async(&self, contract: &Contract, order: &Order) -> Pending<OrderState> {
        let mut what_if = order.clone();
        what_if.what_if = true;
        clear_volatility(&mut what_if);
        let c = contract.clone();
        numbered(
            &self.shared,
            None,
            Some(contract.clone()),
            move |client, id| {
                send_order(client, id, &c, &what_if, OrderOp::Place);
            },
        )
    }

    /// Cancels every order the account is working, whoever placed it:
    /// ib_async's `reqGlobalCancel`.
    pub fn req_global_cancel(&self) -> Result<()> {
        self.shared.step(Class::Control, |ib| {
            session(ib)?.req_global_cancel("");
            log::info!(target: LOG_IB, "reqGlobalCancel");
            Ok(())
        })
    }

    /// The open orders, each as its trade: ib_async's `reqOpenOrders`.
    /// Every order reported until the answer ends belongs to it and fires
    /// no `open_order_event`. Bounded by `IBConfig.request_timeout`.
    pub fn req_open_orders(&self) -> Result<Vec<Live<Trade>>> {
        self.req_open_orders_async().wait(self.request_timeout())
    }

    /// `req_open_orders`'s async form: ib_async's `reqOpenOrdersAsync`.
    pub fn req_open_orders_async(&self) -> Pending<Vec<Live<Trade>>> {
        orders_question(&self.shared, Ask::OpenOrders)
    }

    /// The open orders of every client, each as its trade: ib_async's
    /// `reqAllOpenOrders`. It takes turns with `req_open_orders`, one in
    /// flight at a time, as ib_async keys both `openOrders`. Bounded by
    /// `IBConfig.request_timeout`.
    pub fn req_all_open_orders(&self) -> Result<Vec<Live<Trade>>> {
        self.req_all_open_orders_async()
            .wait(self.request_timeout())
    }

    /// `req_all_open_orders`'s async form: ib_async's
    /// `reqAllOpenOrdersAsync`.
    pub fn req_all_open_orders_async(&self) -> Pending<Vec<Live<Trade>>> {
        orders_question(&self.shared, Ask::AllOpenOrders)
    }

    /// The completed orders, `api_only` for those placed through an API,
    /// each as a new trade: ib_async's `reqCompletedOrders`. Only an order
    /// with a `perm_id` not yet seen joins `trades()`. Bounded by
    /// `IBConfig.request_timeout`.
    pub fn req_completed_orders(&self, api_only: bool) -> Result<Vec<Live<Trade>>> {
        self.req_completed_orders_async(api_only)
            .wait(self.request_timeout())
    }

    /// `req_completed_orders`'s async form: ib_async's
    /// `reqCompletedOrdersAsync`.
    pub fn req_completed_orders_async(&self, api_only: bool) -> Pending<Vec<Live<Trade>>> {
        orders_question(&self.shared, Ask::CompletedOrders { api_only })
    }

    /// The fills that match `exec_filter`, every fill when `None`:
    /// ib_async's `reqExecutions`. The fills it gives carry the execution's
    /// own time and fire no event. Bounded by `IBConfig.request_timeout`.
    pub fn req_executions(&self, exec_filter: Option<&ExecutionFilter>) -> Result<Vec<Fill>> {
        self.req_executions_async(exec_filter)
            .wait(self.request_timeout())
    }

    /// `req_executions`'s async form: ib_async's `reqExecutionsAsync`.
    pub fn req_executions_async(
        &self,
        exec_filter: Option<&ExecutionFilter>,
    ) -> Pending<Vec<Fill>> {
        let filter = e::ExecutionFilter::from(exec_filter.unwrap_or(&ExecutionFilter::default()));
        numbered(
            &self.shared,
            Some(Box::new(Vec::<Fill>::new())),
            None,
            move |client, id| {
                client.req_executions(id, &filter);
            },
        )
    }

    /// Asks for orders entered by hand to be bound to this client, or no
    /// longer: ib_async's `reqAutoOpenOrders`, which `connect` calls for
    /// client 0. The engine already reports every order of the account to
    /// every session.
    pub fn req_auto_open_orders(&self, auto_bind: bool) -> Result<()> {
        self.shared.step(Class::Control, move |ib| {
            session(ib)?.req_auto_open_orders(auto_bind);
            Ok(())
        })
    }

    /// Exercises an option position (`exercise_action` 1) or lets it lapse
    /// (2), under a new id: ib_async's `exerciseOptions`. With `override_`
    /// 0, an exercise out of the money or a lapse in it is refused, as a
    /// gateway refuses it; the answer arrives on `error_event`.
    pub fn exercise_options(
        &self,
        contract: &Contract,
        exercise_action: i32,
        exercise_quantity: i32,
        account: &str,
        override_: i32,
    ) -> Result<()> {
        let (contract, account) = (e::Contract::from(contract), account.to_owned());
        self.shared.step(Class::Request, move |ib| {
            let client = session(ib)?;
            let id = allocate(ib, &client, 1)?;
            client.exercise_options(
                id,
                &contract,
                exercise_action,
                exercise_quantity,
                &account,
                override_ != 0,
                ExerciseStates::default(),
            );
            ib.queue.sent(1);
            Ok(())
        })
    }
}

impl IB {
    /// Puts the orders in one One-Cancels-All group and gives them back:
    /// ib_async's `oneCancelsAll`. Each order is changed by
    /// [`Live::edit`], so a placed one is changed by the thread that runs
    /// its IB.
    pub fn one_cancels_all<'a>(
        orders: &'a [Live<Order>],
        oca_group: &str,
        oca_type: i32,
    ) -> Result<&'a [Live<Order>]> {
        for o in orders {
            let group = oca_group.to_owned();
            o.edit(move |o| {
                o.oca_group = group;
                o.oca_type = oca_type;
            })?;
        }
        Ok(orders)
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::sync::mpsc::{self, Receiver};
    use std::task::{Context, Poll, Waker};
    use std::thread;
    use std::time::Duration;

    use jiff::Timestamp;
    use jiff::tz::TimeZone;

    use super::*;
    use crate::engine::{ControlCommand, SharedState};
    use crate::event::{lock, on_owner, set_on_owner};
    use crate::ib::{ConnectOptions, IBConfig, StartupFetch};
    use crate::objects::{Execution, IBDefaults};
    use crate::owner::Via;
    use crate::record::{Callback, Capture};
    use crate::tests::{capture_logs, errors_here};
    use crate::timer::Clock;

    type Log = Arc<Mutex<Vec<String>>>;

    fn note(log: &Log, s: impl Into<String>) {
        lock(log).push(s.into());
    }

    /// What was noted since the last look.
    fn seen(log: &Log) -> Vec<String> {
        std::mem::take(&mut *lock(log))
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
    fn engine() -> (EClient, Receiver<ControlCommand>) {
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

    /// An IB no owner serves: this thread, as its owner, drives it.
    fn unconnected() -> IBHandle {
        let shared = Shared::new(
            IBDefaults::default(),
            IBConfig::default(),
            Clock::manual(Timestamp::UNIX_EPOCH),
        );
        shared.connect_internal_slots();
        IBHandle { shared }
    }

    /// A session of client 1 driven on this thread, as its owner.
    struct Session {
        ib: IBHandle,
        capture: Capture,
        rx: Arc<Mutex<Receiver<ControlCommand>>>,
    }

    impl Session {
        fn new() -> Self {
            let (client, rx) = engine();
            let ib = unconnected();
            let via = Via::Test(Some(Arc::new(client)));
            let (mut p, _) = ib.begin_connect(opts(), true, Some(via)).unwrap();
            let mut capture = Capture::new(TimeZone::UTC);
            ib.shared.lap(&mut capture);
            assert!(matches!(published(&mut p), Some(Ok(()))));
            Session {
                ib,
                capture,
                rx: Arc::new(Mutex::new(rx)),
            }
        }

        /// What the engine was handed since the last look.
        fn sent(&self) -> Vec<String> {
            sent(&self.rx)
        }

        /// Applies `callbacks` as one read of the session.
        fn read(&self, callbacks: Vec<Callback>) {
            let (g, _) = self.ib.shared.connected().unwrap();
            self.ib.shared.apply_read(g, callbacks);
        }

        fn lap(&mut self) {
            self.ib.shared.lap(&mut self.capture);
        }
    }

    /// The engine's commands, each as the fields that tell them apart.
    fn sent(rx: &Mutex<Receiver<ControlCommand>>) -> Vec<String> {
        lock(rx)
            .try_iter()
            .map(|c| match c {
                ControlCommand::Place(p) => {
                    let mut s = format!("place {}", p.order_id);
                    if p.order.what_if {
                        s.push_str(" what-if");
                    }
                    if p.order.volatility != f64::MAX {
                        s.push_str(" with volatility");
                    }
                    s
                }
                ControlCommand::CancelOrder { order_id, .. } => format!("cancel {order_id}"),
                ControlCommand::CancelOrderByPermId { perm_id } => format!("cancel perm {perm_id}"),
                ControlCommand::GlobalCancel { .. } => "global cancel".into(),
                ControlCommand::Exercise(x) => format!(
                    "exercise {} stated {} action {} override {}",
                    x.req_id, x.stated, x.action, x.override_
                ),
                ControlCommand::FetchCompletedOrders { api_only } => {
                    format!("completed orders {api_only}")
                }
                other => format!("{other:?}"),
            })
            .collect()
    }

    fn published<T>(p: &mut Pending<T>) -> Option<Result<T>> {
        match Pin::new(p).poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(r) => Some(r),
            Poll::Pending => None,
        }
    }

    fn stock() -> Contract {
        Contract::stock("AAPL", "SMART", "USD")
    }

    fn status(order_id: i64, status: &str) -> Callback {
        Callback::OrderStatus {
            order_id,
            status: status.into(),
            filled: 0.0,
            remaining: 0.0,
            avg_fill_price: 0.0,
            perm_id: 0,
            parent_id: 0,
            last_fill_price: 0.0,
            client_id: 1,
            why_held: String::new(),
            mkt_cap_price: 0.0,
        }
    }

    /// An order of client 0 the venue reports, working under `perm_id`.
    fn reported(perm_id: i64) -> Callback {
        Callback::OpenOrder {
            order_id: 7,
            contract: stock(),
            order: Order {
                order_id: 7,
                perm_id,
                ..Order::limit("BUY", 100.0, 1.5)
            },
            order_state: OrderState {
                status: OrderStatus::SUBMITTED.into(),
                ..OrderState::default()
            },
        }
    }

    #[test]
    fn placing_sends_first_then_numbers_a_new_order_or_logs_a_modify() {
        let _o = AsOwner::new();
        let mut s = Session::new();
        let log = Log::default();
        let (l, rx) = (log.clone(), s.rx.clone());
        s.ib.new_order_event()
            .connect(move |_| note(&l, format!("newOrderEvent after {:?}", sent(&rx))));

        // A new order: sent under an id of the IB's, then numbered in place
        // and held by its PendingSubmit trade.
        let order = Live::new(Order {
            volatility: Some(0.2),
            ..Order::limit("BUY", 100.0, 1.5)
        });
        let trade = s.ib.place_order(&stock(), &order).unwrap();
        assert_eq!(seen(&log), [r#"newOrderEvent after ["place 1"]"#]);
        let o = order.read();
        assert_eq!((o.client_id, o.order_id, o.volatility), (1, 1, None));
        let t = trade.read();
        assert!(Live::ptr_eq(&t.order, &order));
        assert_eq!(t.order_status.status, OrderStatus::PENDING_SUBMIT);
        let entries: Vec<_> = t.log.iter().map(|e| (&*e.status, &*e.message)).collect();
        assert_eq!(entries, [(OrderStatus::PENDING_SUBMIT, "")]);
        assert!(Live::ptr_eq(&s.ib.trades()[0], &trade));
        // Placed, the order is this IB's: another thread's edit of it runs
        // on the owner.
        let o = order.clone();
        let editor = thread::spawn(move || {
            o.edit(|o| o.order_ref = if on_owner() { "owner" } else { "caller" }.into())
        });
        let end = std::time::Instant::now() + Duration::from_secs(5);
        while !editor.is_finished() && std::time::Instant::now() < end {
            s.lap();
        }
        assert!(editor.is_finished());
        editor.join().unwrap().unwrap();
        assert_eq!(order.read().order_ref, "owner");

        // The same order again: a modify, sent before its log entry and
        // events, the trade keeping its order.
        let (l, rx) = (log.clone(), s.rx.clone());
        trade
            .modify_event()
            .connect(move |_| note(&l, format!("modifyEvent after {:?}", sent(&rx))));
        let l = log.clone();
        s.ib.order_modify_event()
            .connect(move |_| note(&l, "orderModifyEvent"));
        let modified = s.ib.place_order(&stock(), &order).unwrap();
        assert!(Live::ptr_eq(&modified, &trade));
        assert!(Live::ptr_eq(&trade.read().order, &order));
        assert_eq!(
            seen(&log),
            [r#"modifyEvent after ["place 1"]"#, "orderModifyEvent"]
        );
        let last = trade.read().log.last().cloned().unwrap();
        assert_eq!(
            (&*last.status, &*last.message),
            (OrderStatus::PENDING_SUBMIT, "Modify")
        );

        // A modify of a done order is still sent, then refused, with no
        // entry and no event.
        s.read(vec![status(1, OrderStatus::FILLED)]);
        let entries = trade.read().log.len();
        let r = s.ib.place_order(&stock(), &order);
        assert!(matches!(r, Err(Error::Value(_))), "{r:?}");
        assert_eq!(s.sent(), ["place 1"]);
        assert_eq!(trade.read().log.len(), entries);
        assert_eq!(seen(&log), Vec::<String>::new());
    }

    #[test]
    fn cancelling_sends_first_then_marks_a_trade_not_yet_done() {
        struct Case {
            name: &'static str,
            /// The order to cancel, as the session came to hold it.
            order: fn(&Session) -> Live<Order>,
            sent: &'static str,
            /// The trade's status after, `None` for no trade.
            status: Option<&'static str>,
            events: &'static [&'static str],
        }
        const TOLD: &[&str] = &[
            "cancelEvent",
            "statusEvent",
            "cancelOrderEvent",
            "orderStatusEvent",
        ];
        let cases = [
            Case {
                name: "a transmitted order awaits the venue's word",
                order: |s| {
                    let order = Live::new(Order::limit("BUY", 100.0, 1.5));
                    s.ib.place_order(&stock(), &order).unwrap();
                    order
                },
                sent: "cancel 1",
                status: Some(OrderStatus::PENDING_CANCEL),
                events: TOLD,
            },
            Case {
                name: "an order never transmitted is cancelled at once",
                order: |s| {
                    let order = Live::new(Order {
                        transmit: false,
                        ..Order::limit("BUY", 100.0, 1.5)
                    });
                    s.ib.place_order(&stock(), &order).unwrap();
                    order
                },
                sent: "cancel 1",
                status: Some(OrderStatus::CANCELLED),
                events: &[
                    "cancelEvent",
                    "statusEvent",
                    "cancelOrderEvent",
                    "orderStatusEvent",
                    "cancelledEvent",
                ],
            },
            Case {
                name: "another client's order goes by its permId",
                order: |s| {
                    s.read(vec![reported(555)]);
                    s.ib.trades()[0].read().order.clone()
                },
                sent: "cancel perm 555",
                status: Some(OrderStatus::PENDING_CANCEL),
                events: TOLD,
            },
            Case {
                name: "a done order is sent and left as it is",
                order: |s| {
                    let order = Live::new(Order::limit("BUY", 100.0, 1.5));
                    s.ib.place_order(&stock(), &order).unwrap();
                    s.read(vec![status(1, OrderStatus::FILLED)]);
                    order
                },
                sent: "cancel 1",
                status: Some(OrderStatus::FILLED),
                events: &[],
            },
            Case {
                name: "an order the IB does not know is sent and logged",
                order: |_| {
                    Live::new(Order {
                        order_id: 99,
                        ..Order::limit("BUY", 100.0, 1.5)
                    })
                },
                sent: "cancel 99",
                status: None,
                events: &[],
            },
        ];
        capture_logs();
        let _o = AsOwner::new();
        for case in cases {
            let s = Session::new();
            let order = (case.order)(&s);
            s.sent();
            let log = Log::default();
            for t in s.ib.trades() {
                for (event, name) in [
                    (t.cancel_event(), "cancelEvent"),
                    (t.status_event(), "statusEvent"),
                    (t.cancelled_event(), "cancelledEvent"),
                ] {
                    let l = log.clone();
                    event.connect(move |_| note(&l, name));
                }
            }
            for (event, name) in [
                (s.ib.cancel_order_event(), "cancelOrderEvent"),
                (s.ib.order_status_event(), "orderStatusEvent"),
            ] {
                let l = log.clone();
                event.connect(move |_| note(&l, name));
            }
            let trade = s.ib.cancel_order(&order, "").unwrap();
            assert_eq!(s.sent(), [case.sent], "{}", case.name);
            let status = trade.map(|t| t.read().order_status.status.clone());
            assert_eq!(status.as_deref(), case.status, "{}", case.name);
            assert_eq!(seen(&log), case.events, "{}", case.name);
        }
        assert!(errors_here().contains(&(
            LOG_IB.to_owned(),
            "cancelOrder: Unknown orderId 99".to_owned()
        )));
    }

    #[test]
    fn a_bracket_takes_three_consecutive_ids_above_the_engines_floor() {
        let _o = AsOwner::new();
        let s = Session::new();
        // An id the program chose moves the engine's floor past it.
        let chosen = Live::new(Order {
            order_id: 41,
            ..Order::limit("BUY", 100.0, 1.5)
        });
        s.ib.place_order(&stock(), &chosen).unwrap();
        let b =
            s.ib.bracket_order("SELL", 10.0, 100.0, 90.0, 110.0)
                .unwrap();
        let legs: Vec<_> = [&b.parent, &b.take_profit, &b.stop_loss]
            .into_iter()
            .map(|o| {
                let o = o.read();
                (
                    o.order_id,
                    o.action.clone(),
                    o.order_type.clone(),
                    o.lmt_price,
                    o.aux_price,
                    o.total_quantity,
                    o.transmit,
                    o.parent_id,
                )
            })
            .collect();
        assert_eq!(
            legs,
            [
                (
                    42,
                    "SELL".into(),
                    "LMT".into(),
                    Some(100.0),
                    None,
                    10.0,
                    false,
                    0
                ),
                (
                    43,
                    "BUY".into(),
                    "LMT".into(),
                    Some(90.0),
                    None,
                    10.0,
                    false,
                    42
                ),
                (
                    44,
                    "BUY".into(),
                    "STP".into(),
                    None,
                    Some(110.0),
                    10.0,
                    true,
                    42
                ),
            ]
        );
        // Built, not sent; the next id follows the bracket's.
        let next = Live::new(Order::limit("BUY", 100.0, 1.5));
        s.ib.place_order(&stock(), &next).unwrap();
        assert_eq!(s.sent(), ["place 41", "place 45"]);
    }

    #[test]
    fn a_what_if_sends_a_marked_copy_and_answers_with_its_margin() {
        let _o = AsOwner::new();
        let s = Session::new();
        let log = Log::default();
        let l = log.clone();
        s.ib.new_order_event()
            .connect(move |_| note(&l, "newOrderEvent"));
        let order = Order {
            volatility: Some(0.2),
            ..Order::limit("BUY", 100.0, 1.5)
        };
        let mut p = s.ib.what_if_order_async(&stock(), &order);
        assert_eq!(s.sent(), ["place 1 what-if"]);
        // The engine reports the copy back, with the margin it would take.
        let margin = OrderState {
            init_margin_change: "1500.25".into(),
            ..OrderState::default()
        };
        s.read(vec![Callback::OpenOrder {
            order_id: 1,
            contract: stock(),
            order: Order {
                order_id: 1,
                what_if: true,
                ..order
            },
            order_state: margin.clone(),
        }]);
        assert_eq!(published(&mut p).unwrap().unwrap(), margin);
        assert!(s.ib.trades().is_empty());
        assert_eq!(seen(&log), Vec::<String>::new());
    }

    #[test]
    fn questions_about_orders_go_in_their_lane_and_answer_with_the_trades_reported() {
        type Ask = fn(&IBHandle) -> Pending<Vec<Live<Trade>>>;
        let completed = Callback::CompletedOrder {
            contract: stock(),
            order: Order {
                order_id: 7,
                perm_id: 555,
                ..Order::limit("BUY", 100.0, 1.5)
            },
            order_state: OrderState {
                status: OrderStatus::FILLED.into(),
                ..OrderState::default()
            },
        };
        let cases: [(Ask, &str, Vec<Callback>); 3] = [
            (
                |ib| ib.req_open_orders_async(),
                "Ask(OpenOrders(OpenOrders))",
                vec![reported(555), Callback::OpenOrderEnd],
            ),
            (
                |ib| ib.req_all_open_orders_async(),
                "Ask(OpenOrders(AllOpenOrders))",
                vec![reported(555), Callback::OpenOrderEnd],
            ),
            (
                |ib| ib.req_completed_orders_async(true),
                "completed orders true",
                vec![completed, Callback::CompletedOrdersEnd],
            ),
        ];
        let _o = AsOwner::new();
        for (ask, command, answer) in cases {
            let s = Session::new();
            let log = Log::default();
            let l = log.clone();
            s.ib.open_order_event()
                .connect(move |_| note(&l, "openOrderEvent"));
            let mut p = ask(&s.ib);
            assert_eq!(s.sent(), [command]);
            s.read(answer);
            let trades = published(&mut p).unwrap().unwrap();
            let perm_ids: Vec<i64> = trades
                .iter()
                .map(|t| t.read().order.read().perm_id)
                .collect();
            assert_eq!(perm_ids, [555], "{command}");
            // What the exchange recorded is its answer, not an event.
            assert_eq!(seen(&log), Vec::<String>::new(), "{command}");
        }
    }

    #[test]
    fn executions_are_numbered_and_answer_with_their_fills() {
        let _o = AsOwner::new();
        let mut s = Session::new();
        let log = Log::default();
        let l = log.clone();
        s.ib.exec_details_event()
            .connect(move |_| note(&l, "execDetailsEvent"));
        let mut p = s.ib.req_executions_async(None);
        // Reported under the request's id, then ended by the engine's answer.
        s.read(vec![Callback::ExecDetails {
            req_id: 1,
            contract: stock(),
            execution: Execution {
                exec_id: "0001".into(),
                shares: 100.0,
                ..Execution::default()
            },
        }]);
        s.lap();
        let fills = published(&mut p).unwrap().unwrap();
        let ids: Vec<&str> = fills.iter().map(|f| &*f.execution.exec_id).collect();
        assert_eq!(ids, ["0001"]);
        assert_eq!(seen(&log), Vec::<String>::new());
    }

    #[test]
    fn a_cancel_of_everything_and_an_exercise_each_reach_the_engine() {
        let _o = AsOwner::new();
        let s = Session::new();
        let option = Contract::option("AAPL", "20300118", 150.0, "C", "SMART");
        s.ib.req_global_cancel().unwrap();
        s.ib.exercise_options(&option, 1, 2, "DU123", 0).unwrap();
        s.ib.exercise_options(&option, 2, 1, "DU123", 1).unwrap();
        assert_eq!(
            s.sent(),
            [
                "global cancel",
                "exercise 1 stated true action 1 override false",
                "exercise 2 stated true action 2 override true",
            ]
        );
    }

    #[test]
    fn every_order_method_needs_a_session_and_a_bracket_checks_its_action_first() {
        let _o = AsOwner::new();
        let ib = unconnected();
        let order = Live::new(Order::limit("BUY", 100.0, 1.5));
        let not_connected = |r: Result<()>| matches!(r, Err(Error::NotConnected));
        assert!(not_connected(ib.place_order(&stock(), &order).map(drop)));
        assert_eq!(order.read().order_id, 0);
        assert!(not_connected(ib.cancel_order(&order, "").map(drop)));
        assert!(not_connected(
            ib.bracket_order("BUY", 1.0, 1.0, 2.0, 0.5).map(drop)
        ));
        let r = ib.bracket_order("HOLD", 1.0, 1.0, 2.0, 0.5);
        assert!(matches!(r, Err(Error::Value(_))), "{r:?}");
        assert!(not_connected(ib.req_global_cancel()));
        assert!(not_connected(ib.req_auto_open_orders(true)));
        assert!(not_connected(ib.exercise_options(&stock(), 1, 1, "", 0)));
    }

    #[test]
    fn a_placement_from_another_thread_returns_after_new_order_events_handlers() {
        let (client, _rx) = engine();
        let ib = IB::attach(client, opts(), Clock::system()).unwrap();
        // ib_async bounds only its blocking requests: a placement waits for
        // its step however short the bound.
        ib.set_config(IBConfig {
            request_timeout: Some(Duration::from_millis(1)),
            ..IBConfig::default()
        });
        let log = Log::default();
        let l = log.clone();
        ib.new_order_event().connect(move |_| {
            thread::sleep(Duration::from_millis(50));
            note(&l, "handled");
        });
        let order = Live::new(Order::limit("BUY", 100.0, 1.5));
        let trade = ib.place_order(&stock(), &order).unwrap();
        assert_eq!(seen(&log), ["handled"]);
        assert_eq!(
            trade.read().order_status.status,
            OrderStatus::PENDING_SUBMIT
        );
    }
}

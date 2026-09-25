//! Replays checked against ib_async's own results.
//!
//! Each scenario in `tests/oracle` is replayed here as the engine would
//! deliver it: its callbacks made as engine values, recorded by `Capture`
//! and applied as one pass per read, every emission observed as it runs.
//! What is observed must be what `scripts/oracle.py` saw ib_async do.
//! Only the scenarios whose steps the crate can already run are listed; a
//! scenario whose step it cannot run fails.

use std::collections::HashMap;
use std::path::Path;

use jiff::tz::TimeZone;
use jiff::{SignedDuration, Timestamp, Zoned};
use serde_json::{Map, Value, json};

use super::observe::Observe;
use crate::contract::Contract;
use crate::engine::{self as e, ErrorOrigin, OrderOp, Question, Wrapper};
use crate::order::Order;
use crate::record::Capture;
use crate::state::tests::Recorder;
use crate::state::{Emit, State, pass};

/// The scenarios whose every step the crate can run.
const SCENARIOS: &[&str] = &[
    "errors_order_error_before_open_order",
    "orders_placed_elsewhere",
    "requests_unasked_answers",
];

/// The scenarios' clock at their start: 2024-01-02 15:00:00 UTC.
const START: i64 = 1_704_207_600;

#[test]
fn each_scenario_replays_as_ib_async_ran_it() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/oracle");
    let read = |name: &str| -> Value {
        let text = std::fs::read_to_string(dir.join(format!("{name}.json"))).unwrap();
        serde_json::from_str(&text).unwrap()
    };
    let defaults = read("defaults.expected");
    for name in SCENARIOS {
        replay(
            name,
            &read(name),
            &read(&format!("{name}.expected")),
            &defaults,
        );
    }
}

fn replay(name: &str, scenario: &Value, expected: &Value, defaults: &Value) {
    let mut now = Timestamp::from_second(START)
        .unwrap()
        .to_zoned(TimeZone::UTC);
    let mut r = Recorder::new(emission, now.clone());
    if let Some(c) = scenario.get("connected") {
        r.state.client_id = c["client_id"].as_i64().unwrap();
        r.state.accounts = serde_json::from_value(c["accounts"].clone()).unwrap();
    }
    let mut refs = HashMap::new();
    let steps = scenario["steps"].as_array().unwrap();
    for (i, step) in steps.iter().enumerate() {
        let at = format!("{name}, step {i}");
        if let Some(lets) = step.get("let") {
            for (k, v) in lets.as_object().unwrap() {
                refs.insert(k.clone(), v.clone());
            }
        } else if let Some(read) = step.get("read") {
            let mut capture = Capture::new(TimeZone::UTC);
            for cb in read.as_array().unwrap() {
                deliver(&mut capture, cb, &refs);
            }
            pass(&mut r, capture.take(), now.clone());
        } else if let Some(secs) = step.get("advance") {
            now = now
                .checked_add(SignedDuration::from_secs_f64(secs.as_f64().unwrap()))
                .unwrap();
        } else {
            panic!("{at}: a step the crate cannot run yet: {step}");
        }
        let want = &expected["steps"][i];
        let log = Value::Array(std::mem::take(&mut r.log));
        same(&log, &want["log"], defaults, &format!("{at}, log"));
        same(
            &views(&r.state),
            &want["state"],
            defaults,
            &format!("{at}, state"),
        );
        assert!(want.get("results").is_none(), "{at}: results");
    }
    assert_eq!(expected["pending"], json!([]), "{name}: pending");
}

/// Whether `got`, an observation with every field, is `want`, where a
/// record lists only the fields that differ from its type's default and
/// numbers compare by value.
fn same(got: &Value, want: &Value, defaults: &Value, at: &str) {
    let got = trimmed(got, defaults);
    assert!(
        equal(&got, want),
        "{at}\n got: {}\nwant: {}",
        serde_json::to_string(&got).unwrap(),
        serde_json::to_string(want).unwrap()
    );
}

fn trimmed(v: &Value, defaults: &Value) -> Value {
    match v {
        Value::Array(a) => Value::Array(a.iter().map(|x| trimmed(x, defaults)).collect()),
        Value::Object(m) => {
            let table = m.get("@").and_then(Value::as_str).map(|t| &defaults[t]);
            let mut out = Map::new();
            for (k, x) in m {
                let x = trimmed(x, defaults);
                let default = table.and_then(|t| t.get(k));
                if k == "@" || !default.is_some_and(|d| equal(&x, d)) {
                    out.insert(k.clone(), x);
                }
            }
            Value::Object(out)
        }
        _ => v.clone(),
    }
}

fn equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.as_f64() == y.as_f64(),
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(x, y)| equal(x, y))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len() && x.iter().all(|(k, v)| y.get(k).is_some_and(|w| equal(v, w)))
        }
        _ => a == b,
    }
}

/// What the replay records of each step's state: ib_async's `trades()`,
/// `tickers()`, `positions()`, `portfolio()`, `accountValues()`, `fills()`,
/// `pnl()`, `pnlSingle()` and `realtimeBars()`, the empty ones left out.
fn views(s: &State) -> Value {
    assert!(
        s.tickers.is_empty()
            && s.req_id_to_pnl.is_empty()
            && s.req_id_to_pnl_single.is_empty()
            && s.req_id_to_subscriber.is_empty(),
        "tickers, P&L and bar lists are not observed yet"
    );
    let views: [(&str, Vec<Value>); 5] = [
        ("trades", s.trades.values().map(Observe::observe).collect()),
        (
            "positions",
            s.positions
                .values()
                .flat_map(|p| p.values())
                .map(Observe::observe)
                .collect(),
        ),
        (
            "portfolio",
            s.portfolio
                .values()
                .flat_map(|p| p.values())
                .map(Observe::observe)
                .collect(),
        ),
        (
            "account_values",
            s.account_values.values().map(Observe::observe).collect(),
        ),
        ("fills", s.fills.values().map(Observe::observe).collect()),
    ];
    let mut out = Map::new();
    for (k, v) in views {
        if !v.is_empty() {
            out.insert(k.into(), Value::Array(v));
        }
    }
    Value::Object(out)
}

/// An emission as the replay logs it: its name, what owns it, and its
/// arguments as they were when it ran.
fn emission(e: &Emit) -> Value {
    let ib = |name: &str, args: Vec<Value>| json!({"emit": name, "on": "IB", "args": args});
    let on = |name: &str, owner: &str, args: Vec<Value>| json!({"emit": name, "on": owner, "args": args});
    match e {
        Emit::AccountValue(v) => ib("account_value_event", vec![v.observe()]),
        Emit::AccountSummary(v) => ib("account_summary_event", vec![v.observe()]),
        Emit::UpdatePortfolio(v) => ib("update_portfolio_event", vec![v.observe()]),
        Emit::Position(v) => ib("position_event", vec![v.observe()]),
        Emit::OpenOrder(t) => ib("open_order_event", vec![t.observe()]),
        Emit::OrderStatus(t) => ib("order_status_event", vec![t.observe()]),
        Emit::ExecDetails(t, f) => ib("exec_details_event", vec![t.observe(), f.observe()]),
        Emit::CommissionReport(t, f, c) => ib(
            "commission_report_event",
            vec![t.observe(), f.observe(), c.observe()],
        ),
        Emit::TickNews(n) => ib("tick_news_event", vec![n.observe()]),
        Emit::NewsBulletin(n) => ib("news_bulletin_event", vec![n.observe()]),
        Emit::WshMeta(s) => ib("wsh_meta_event", vec![s.observe()]),
        Emit::Wsh(s) => ib("wsh_event", vec![s.observe()]),
        Emit::Error(id, code, msg, c) => ib(
            "error_event",
            vec![id.observe(), code.observe(), msg.observe(), c.observe()],
        ),
        Emit::Update => ib("update_event", vec![]),
        Emit::TradeStatus(t) => on("status_event", "Trade", vec![t.observe()]),
        Emit::TradeFill(t, f) => on("fill_event", "Trade", vec![t.observe(), f.observe()]),
        Emit::TradeCommissionReport(t, f, c) => on(
            "commission_report_event",
            "Trade",
            vec![t.observe(), f.observe(), c.observe()],
        ),
        Emit::TradeFilled(t) => on("filled_event", "Trade", vec![t.observe()]),
        Emit::TradeCancelled(t) => on("cancelled_event", "Trade", vec![t.observe()]),
        Emit::BarUpdate(..)
        | Emit::BarsUpdate(..)
        | Emit::Pnl(_)
        | Emit::PnlSingle(_)
        | Emit::ScannerData(_)
        | Emit::ScanUpdate(_)
        | Emit::PendingTickers(_)
        | Emit::TickerUpdate(_)
        | Emit::Tick(..) => {
            panic!("{e:?} is not observed yet")
        }
    }
}

/// Makes the engine's callback a scenario names, in ib_async's decoded
/// form, on `c`.
fn deliver(c: &mut Capture, cb: &Value, refs: &HashMap<String, Value>) {
    let (name, args, origin) = match cb {
        Value::Array(a) => (a[0].as_str().unwrap(), &a[1..], None),
        _ => (
            cb["cb"].as_str().unwrap(),
            cb["args"].as_array().unwrap().as_slice(),
            cb.get("origin"),
        ),
    };
    let args: Vec<Value> = args
        .iter()
        .map(|a| match a.get("ref").and_then(Value::as_str) {
            Some(r) => refs[r].clone(),
            None => a.clone(),
        })
        .collect();
    let int = |i: usize| args[i].as_i64().unwrap();
    let num = |i: usize| args[i].as_f64().unwrap();
    let text = |i: usize| args[i].as_str().unwrap().to_owned();
    let engine = |v: &Value| e::Contract::from(&contract(v));
    match name {
        "error" => c.error_from(error_origin(origin.unwrap()), int(1), &text(2), &text(3)),
        "openOrder" => c.open_order(
            int(0),
            &engine(&args[1]),
            &e::Order::try_from(&order(&args[2])).unwrap(),
            &order_state(&args[3]),
        ),
        "orderStatus" => c.order_status(
            int(0),
            &text(1),
            num(2),
            num(3),
            num(4),
            int(5),
            int(6),
            num(7),
            int(8),
            &text(9),
            num(10),
        ),
        "completedOrder" => c.completed_order(
            &engine(&args[0]),
            &e::Order::try_from(&order(&args[1])).unwrap(),
            &order_state(&args[2]),
        ),
        "execDetails" => c.exec_details(int(0), &engine(&args[1]), &execution(&args[2])),
        "tickNews" => c.tick_news(int(0), int(1), &text(2), &text(3), &text(4), &text(5)),
        "accountUpdateMulti" => {
            c.account_update_multi(int(0), &text(1), &text(2), &text(3), &text(4), &text(5));
        }
        "commissionReport" => c.commission_and_fees_report(&commission(&args[0])),
        "position" => c.position(&text(0), &engine(&args[1]), num(2), num(3)),
        "updatePortfolio" => c.update_portfolio(
            &engine(&args[0]),
            num(1),
            num(2),
            num(3),
            num(4),
            num(5),
            num(6),
            &text(7),
        ),
        "updateAccountValue" => c.update_account_value(&text(0), &text(1), &text(2), &text(3)),
        "updateNewsBulletin" => {
            c.update_news_bulletin(int(0), i32::try_from(int(1)).unwrap(), &text(2), &text(3))
        }
        other => panic!("the callback {other} is not replayed yet"),
    }
}

fn error_origin(v: &Value) -> ErrorOrigin {
    if v == "Session" {
        return ErrorOrigin::Session;
    }
    let (kind, o) = v.as_object().unwrap().iter().next().unwrap();
    let id = || o["id"].as_i64().unwrap();
    let ends = || o["ends"].as_bool().unwrap();
    match kind.as_str() {
        "Order" => ErrorOrigin::Order {
            id: id(),
            op: match o["op"].as_str().unwrap() {
                "Place" => OrderOp::Place,
                "Modify" => OrderOp::Modify,
                "Cancel" => OrderOp::Cancel,
                "Exercise" => OrderOp::Exercise,
                _ => OrderOp::Venue,
            },
        },
        "Request" => ErrorOrigin::Request {
            id: id(),
            ends: ends(),
        },
        "Question" => ErrorOrigin::Question {
            q: match o["q"].as_str().unwrap() {
                "OpenOrders" => Question::OpenOrders,
                "CompletedOrders" => Question::CompletedOrders,
                "Positions" => Question::Positions,
                other => panic!("the question {other} is not replayed yet"),
            },
            ends: ends(),
        },
        other => panic!("an origin {other}"),
    }
}

/// An object's fields by their ib_async names, its type checked.
fn fields<'a>(v: &'a Value, ty: &str) -> impl Iterator<Item = (&'a str, &'a Value)> {
    assert_eq!(v["@"], ty);
    v.as_object()
        .unwrap()
        .iter()
        .filter(|(k, _)| *k != "@")
        .map(|(k, x)| (k.as_str(), x))
}

fn s(v: &Value) -> String {
    v.as_str().unwrap().to_owned()
}

fn f(v: &Value) -> f64 {
    v.as_f64().unwrap()
}

fn i(v: &Value) -> i64 {
    v.as_i64().unwrap()
}

/// ib_async's UNSET_DOUBLE, which ib_async-dx holds as `None`.
fn stated(v: &Value) -> Option<f64> {
    Some(f(v)).filter(|x| *x != f64::MAX)
}

fn contract(v: &Value) -> Contract {
    let mut c = Contract::default();
    for (k, x) in fields(v, "Contract") {
        match k {
            "conId" => c.con_id = i(x),
            "symbol" => c.symbol = s(x),
            "secType" => c.sec_type = s(x),
            "exchange" => c.exchange = s(x),
            "primaryExchange" => c.primary_exchange = s(x),
            "currency" => c.currency = s(x),
            "lastTradeDateOrContractMonth" => c.last_trade_date_or_contract_month = s(x),
            "strike" => c.strike = f(x),
            "right" => c.right = s(x),
            "multiplier" => c.multiplier = s(x),
            other => panic!("Contract.{other} is not replayed yet"),
        }
    }
    c
}

fn order(v: &Value) -> Order {
    let mut o = Order::default();
    for (k, x) in fields(v, "Order") {
        match k {
            "orderId" => o.order_id = i(x),
            "clientId" => o.client_id = i(x),
            "permId" => o.perm_id = i(x),
            "action" => o.action = s(x),
            "totalQuantity" => o.total_quantity = f(x),
            "orderType" => o.order_type = s(x),
            "lmtPrice" => o.lmt_price = stated(x),
            "auxPrice" => o.aux_price = stated(x),
            "tif" => o.tif = s(x),
            "account" => o.account = s(x),
            "transmit" => o.transmit = x.as_bool().unwrap(),
            "whatIf" => o.what_if = x.as_bool().unwrap(),
            other => panic!("Order.{other} is not replayed yet"),
        }
    }
    o
}

fn order_state(v: &Value) -> e::OrderState {
    let mut o = e::OrderState::default();
    for (k, x) in fields(v, "OrderState") {
        match k {
            "status" => o.status = s(x),
            "initMarginBefore" => o.init_margin_before = s(x),
            "maintMarginBefore" => o.maint_margin_before = s(x),
            "equityWithLoanBefore" => o.equity_with_loan_before = s(x),
            "initMarginChange" => o.init_margin_change = s(x),
            "maintMarginChange" => o.maint_margin_change = s(x),
            "equityWithLoanChange" => o.equity_with_loan_change = s(x),
            "initMarginAfter" => o.init_margin_after = s(x),
            "maintMarginAfter" => o.maint_margin_after = s(x),
            "equityWithLoanAfter" => o.equity_with_loan_after = s(x),
            "commission" => o.commission_and_fees = f(x),
            "minCommission" => o.min_commission_and_fees = f(x),
            "maxCommission" => o.max_commission_and_fees = f(x),
            "commissionCurrency" => o.commission_and_fees_currency = s(x),
            "completedTime" => o.completed_time = s(x),
            "completedStatus" => o.completed_status = s(x),
            other => panic!("OrderState.{other} is not replayed yet"),
        }
    }
    o
}

/// An execution as the engine states it: its time the venue's UTC stamp.
fn execution(v: &Value) -> e::Execution {
    let mut x = e::Execution::default();
    for (k, v) in fields(v, "Execution") {
        match k {
            "execId" => x.exec_id = s(v),
            "time" => {
                let at: Zoned = s(&v["dt"])
                    .parse::<Timestamp>()
                    .unwrap()
                    .to_zoned(TimeZone::UTC);
                x.time = at.strftime("%Y%m%d-%H:%M:%S").to_string();
            }
            "acctNumber" => x.acct_number = s(v),
            "exchange" => x.exchange = s(v),
            "side" => x.side = s(v),
            "shares" => x.shares = f(v),
            "price" => x.price = f(v),
            "permId" => x.perm_id = i(v),
            "clientId" => x.client_id = i(v),
            "orderId" => x.order_id = i(v),
            "cumQty" => x.cum_qty = f(v),
            "avgPrice" => x.avg_price = f(v),
            other => panic!("Execution.{other} is not replayed yet"),
        }
    }
    x
}

fn commission(v: &Value) -> e::CommissionAndFeesReport {
    let mut r = e::CommissionAndFeesReport::default();
    for (k, x) in fields(v, "CommissionReport") {
        match k {
            "execId" => r.exec_id = s(x),
            "commission" => r.commission_and_fees = f(x),
            "currency" => r.currency = s(x),
            "realizedPNL" => r.realized_pnl = f(x),
            "yield_" => r.yield_amount = f(x),
            "yieldRedemptionDate" => r.yield_redemption_date = i(x),
            other => panic!("CommissionReport.{other} is not replayed yet"),
        }
    }
    r
}

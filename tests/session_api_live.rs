//! The session against a paper account, driven as a program drives it.
//!
//! Each test is ignored by default and needs `IB_USERNAME` and `IB_PASSWORD`
//! for a paper login; without them it says so and passes. Run them one at a
//! time, since each opens a session of its own:
//!
//!     cargo test --test session_api_live -- --ignored --test-threads=1 --nocapture
//!
//! The order tests trade: they buy and sell one share of SPY, and need the
//! market open.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ib_async_dx::Qualified;
use ib_async_dx::prelude::*;

/// A paper session with `client_id`, or `None` without credentials.
fn session(client_id: i64) -> Option<IB> {
    let username = std::env::var("IB_USERNAME")
        .ok()
        .filter(|v| !v.trim().is_empty())?;
    let password = std::env::var("IB_PASSWORD")
        .ok()
        .filter(|v| !v.trim().is_empty())?;
    let mut opts = ConnectOptions {
        client_id,
        ..ConnectOptions::default()
    };
    opts.config.username = username;
    opts.config.password = password;
    opts.config.paper = true;
    let ib = IB::new().expect("the owner starts");
    ib.connect(opts).expect("the session opens");
    Some(ib)
}

macro_rules! session_or_skip {
    ($client_id:expr) => {
        match session($client_id) {
            Some(ib) => ib,
            None => {
                println!("SKIP: no IB_USERNAME and IB_PASSWORD");
                return;
            }
        }
    };
}

/// Waits on `ib`'s passes until `done`, for at most `secs` seconds.
fn until(ib: &IB, secs: u64, mut done: impl FnMut() -> bool) -> bool {
    let end = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < end {
        if done() {
            return true;
        }
        let _ = ib.wait_on_update(Some(Duration::from_millis(500)));
    }
    done()
}

fn spy(ib: &IB) -> Contract {
    let mut c = [Contract::stock("SPY", "SMART", "USD")];
    let found = ib.qualify_contracts(&mut c).expect("qualify");
    assert!(matches!(found[..], [Qualified::One(_)]), "{found:?}");
    let [c] = c;
    c
}

type Log = Arc<Mutex<Vec<String>>>;

fn note(log: &Log, s: String) {
    log.lock().unwrap().push(s);
}

#[test]
#[ignore = "needs a paper login"]
fn req_current_time_answers_on_an_idle_session() {
    let ib = session_or_skip!(1);
    // Nothing is asked for a while, so no other exchange carries the answer.
    IB::sleep(Duration::from_secs(3)).unwrap();
    let asked = Instant::now();
    let t = ib.req_current_time().expect("the venue's clock");
    assert!(
        asked.elapsed() < Duration::from_secs(10),
        "{:?}",
        asked.elapsed()
    );
    let skew = jiff::Timestamp::now().duration_since(t.timestamp()).abs();
    assert!(skew < jiff::SignedDuration::from_secs(60), "{skew:?}");
}

#[test]
#[ignore = "needs a paper login and an open market"]
fn a_fill_is_reported_before_its_position_and_its_charge_reaches_the_fill_held() {
    let ib = session_or_skip!(1);
    let spy = spy(&ib);
    let log = Log::default();
    let held: Arc<Mutex<Option<Fill>>> = Arc::default();
    let (l, h, got) = (log.clone(), ib.handle(), held.clone());
    ib.exec_details_event().connect(move |(_, fill)| {
        note(&l, format!("exec {}", fill.execution.exec_id));
        // The fill as fills() gives it now, before its charge arrives.
        let now = h
            .fills()
            .into_iter()
            .find(|f| f.execution.exec_id == fill.execution.exec_id);
        got.lock()
            .unwrap()
            .get_or_insert(now.expect("fills() holds the fill"));
    });
    let l = log.clone();
    ib.commission_report_event()
        .connect(move |(_, fill, _)| note(&l, format!("charge {}", fill.execution.exec_id)));
    let l = log.clone();
    ib.order_status_event()
        .connect(move |t| note(&l, format!("status {}", t.read().order_status.status)));
    let (l, con_id) = (log.clone(), spy.con_id);
    ib.position_event().connect(move |p| {
        if p.contract.con_id == con_id {
            note(&l, format!("position {}", p.position));
        }
    });

    let order = Live::new(Order::market("BUY", 1.0));
    let trade = ib.place_order(&spy, &order).unwrap();
    assert!(until(&ib, 60, || trade.read().order_status.status
        == OrderStatus::FILLED));
    assert!(until(&ib, 30, || log
        .lock()
        .unwrap()
        .iter()
        .any(|s| s.starts_with("charge"))));
    let seen = log.lock().unwrap().clone();
    println!("{seen:#?}");

    // Records come as the venue ordered them, and the position the fill
    // changed comes after its execution.
    let exec = seen.iter().position(|s| s.starts_with("exec")).unwrap();
    let charge = seen.iter().position(|s| s.starts_with("charge")).unwrap();
    assert!(exec < charge, "{seen:?}");
    if let Some(position) = seen.iter().position(|s| s.starts_with("position")) {
        assert!(exec < position, "{seen:?}");
    }
    // The fill held before the charge came shares the charge.
    let fill = held.lock().unwrap().clone().unwrap();
    let report = fill.commission_report.read();
    assert_eq!(report.exec_id, fill.execution.exec_id);
    assert!(report.commission > 0.0, "{report:?}");

    let flat = Live::new(Order::market("SELL", 1.0));
    let back = ib.place_order(&spy, &flat).unwrap();
    assert!(until(&ib, 60, || back.read().is_done()));
}

#[test]
#[ignore = "needs a paper login"]
fn an_unqualified_stock_is_named_before_its_order_is_sent() {
    let ib = session_or_skip!(1);
    let errors = Log::default();
    let l = errors.clone();
    ib.error_event()
        .connect(move |(id, code, msg, _)| note(&l, format!("{id} {code} {msg}")));
    // No con_id: the engine names the contract before the order goes.
    let spy = Contract::stock("SPY", "SMART", "USD");
    let order = Live::new(Order::limit("BUY", 1.0, 1.00));
    let trade = ib.place_order(&spy, &order).unwrap();
    let id = order.read().order_id;
    assert!(until(&ib, 30, || {
        OrderStatus::WORKING_STATES.contains(&trade.read().order_status.status.as_str())
    }));
    let refused: Vec<String> = errors
        .lock()
        .unwrap()
        .iter()
        .filter(|e| e.starts_with(&format!("{id} ")))
        .cloned()
        .collect();
    assert!(refused.is_empty(), "{refused:?}");
    ib.cancel_order(&order, "").unwrap();
    assert!(until(&ib, 30, || trade.read().is_done()));
}

#[test]
#[ignore = "needs a paper login"]
fn a_client_sees_its_own_orders_and_every_clients_through_all_open_orders() {
    // Client 0 leaves a working order behind.
    let ib = session_or_skip!(0);
    let spy = spy(&ib);
    let order = Live::new(Order::limit("BUY", 1.0, 1.00));
    order.edit(|o| o.tif = "GTC".into()).unwrap();
    let trade = ib.place_order(&spy, &order).unwrap();
    assert!(until(&ib, 30, || trade.read().is_working()));
    let perm_id = trade.read().order.read().perm_id;
    drop(ib);

    // Client 1 does not see it among its own, and does among everyone's.
    let ib = session_or_skip!(1);
    let own = ib.req_open_orders().unwrap();
    assert!(own.iter().all(|t| t.read().order.read().perm_id != perm_id));
    let all = ib.req_all_open_orders().unwrap();
    let theirs = all
        .iter()
        .find(|t| t.read().order.read().perm_id == perm_id);
    let theirs = theirs
        .expect("req_all_open_orders gives client 0's order")
        .clone();
    let placed = theirs.read().order.clone();
    ib.cancel_order(&placed, "").unwrap();
    assert!(until(&ib, 30, || theirs.read().is_done()));
    drop(ib);

    // Client 0 binds an order placed outside the API through
    // req_open_orders: it comes back numbered.
    let ib = session_or_skip!(0);
    let manual: Vec<i64> = ib
        .req_all_open_orders()
        .unwrap()
        .iter()
        .map(|t| t.read().order.read().clone())
        .filter(|o| o.order_id == 0)
        .map(|o| o.perm_id)
        .collect();
    if manual.is_empty() {
        println!("SKIP (binding): the account has no order placed outside the API");
        return;
    }
    ib.req_auto_open_orders(true).unwrap();
    let bound = ib.req_open_orders().unwrap();
    for perm_id in manual {
        let t = bound
            .iter()
            .find(|t| t.read().order.read().perm_id == perm_id);
        let t = t.expect("client 0 is given the order placed outside the API");
        assert_ne!(t.read().order.read().order_id, 0, "bound under a number");
    }
}

#[test]
#[ignore = "needs a paper login holding an option out of the money"]
fn an_exercise_out_of_the_money_with_override_0_is_refused_under_322() {
    let ib = session_or_skip!(1);
    IB::sleep(Duration::from_secs(3)).unwrap();
    let options: Vec<Position> = ib
        .positions("")
        .into_iter()
        .filter(|p| p.contract.sec_type == "OPT" && p.position > 0.0)
        .collect();
    // An option is out of the money when its underlying is on the far side
    // of its strike, read from the venue's model.
    let out = options.iter().find_map(|p| {
        let ticker = ib.req_mkt_data(&p.contract, "", false, false, &[]).ok()?;
        until(&ib, 15, || {
            ticker
                .read()
                .model_greeks
                .and_then(|g| g.und_price)
                .is_some()
        });
        let und = ticker.read().model_greeks?.und_price?;
        let c = &p.contract;
        let otm = (c.right.starts_with('C') && und < c.strike)
            || (c.right.starts_with('P') && und > c.strike);
        otm.then(|| p.clone())
    });
    let Some(p) = out else {
        println!("SKIP: the account holds no option out of the money");
        return;
    };
    let errors = Log::default();
    let l = errors.clone();
    ib.error_event()
        .connect(move |(_, code, msg, _)| note(&l, format!("{code} {msg}")));
    ib.exercise_options(&p.contract, 1, 1, &p.account, 0)
        .unwrap();
    assert!(until(&ib, 30, || errors
        .lock()
        .unwrap()
        .iter()
        .any(|e| e.starts_with("322 "))));
}

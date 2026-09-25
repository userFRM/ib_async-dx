//! The session against a paper account, driven as a program drives it.
//!
//! Each test is ignored by default and needs `IB_USERNAME` and `IB_PASSWORD`
//! for a paper login; without them it says so and passes. Run them one at a
//! time, since each opens a session of its own:
//!
//!     cargo test --test session_api_live -- --ignored --test-threads=1 --nocapture
//!
//! The order tests trade the least of BTC on PAXOS, or, with `IB_LIVE_STOCK`
//! naming a US stock (`IB_LIVE_STOCK=SPY`), one share of it. Each trades only
//! inside its contract's trading and liquid sessions, as its details state
//! them (BTC's cover the weekday nights a stock's do not), and says SKIP
//! outside them. Each undoes what it did to the account when it ends, passed
//! or failed.

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

/// What the order tests trade, not yet qualified: BTC on PAXOS, or the US
/// stock `IB_LIVE_STOCK` names.
fn traded() -> Contract {
    match std::env::var("IB_LIVE_STOCK") {
        Ok(symbol) if !symbol.trim().is_empty() => Contract::stock(symbol.trim(), "SMART", "USD"),
        _ => Contract::crypto("BTC", "PAXOS", "USD"),
    }
}

fn qualified(ib: &IB) -> Contract {
    let mut c = [traded()];
    let found = ib.qualify_contracts(&mut c).expect("qualify");
    assert!(matches!(found[..], [Qualified::One(_)]), "{found:?}");
    let [c] = c;
    c
}

fn is_crypto(c: &Contract) -> bool {
    c.sec_type == "CRYPTO"
}

/// The least of `c` the venue takes, a share or a crypto's `min_size`, if
/// `c` is in a trading session and a liquid one now: a stock's regular hours,
/// a crypto's trading hours (its liquid hours, when it is quoted, run on
/// where no order is worked). `None` if not.
fn trading(ib: &IB, c: &Contract) -> Option<f64> {
    let details = ib.req_contract_details(c).expect("the contract's details");
    let [d] = &details[..] else {
        panic!("one contract: {details:?}")
    };
    let now = jiff::Timestamp::now();
    let within = |sessions: Vec<TradingSession>| {
        sessions
            .iter()
            .any(|s| s.start.timestamp() <= now && now < s.end.timestamp())
    };
    if !within(d.trading_sessions().unwrap()) || !within(d.liquid_sessions().unwrap()) {
        return None;
    }
    if !is_crypto(c) {
        return Some(1.0);
    }
    assert!(d.min_size > 0.0, "no minimum size stated: {d:?}");
    Some(d.min_size)
}

macro_rules! trading_or_skip {
    ($ib:expr, $c:expr) => {
        match trading($ib, $c) {
            Some(size) => size,
            None => {
                println!("SKIP: {} is not trading now", $c.symbol);
                return;
            }
        }
    };
}

/// An order to buy `size` of `c` that the venue leaves working, priced far
/// under the market: a share at a dollar, good till cancelled, or a crypto
/// at $10,000, a whole dollar and so on the venue's grid. A crypto buy lives
/// by the minute or is immediate-or-cancel, the venue says ("The crypto buy
/// order must be Minutes or IOC"), and the engine's name for the first is
/// `NMIN`.
fn resting(c: &Contract, size: f64) -> Live<Order> {
    let (price, tif) = if is_crypto(c) {
        (10_000.0, "NMIN")
    } else {
        (1.00, "GTC")
    };
    Live::new(Order {
        tif: tif.into(),
        ..Order::limit("BUY", size, price)
    })
}

/// `c`'s position on the account.
fn position(ib: &IB, c: &Contract) -> f64 {
    ib.positions("")
        .iter()
        .filter(|p| p.contract.con_id == c.con_id)
        .map(|p| p.position)
        .sum()
}

/// Undoes, when it is dropped, what a test did to the account, whether the
/// test passed or failed: cancels the orders of `orders` still open, then
/// trades the contract it holds back to the position it began at.
struct Tidy<'a> {
    ib: &'a IB,
    orders: Vec<i64>,
    /// The contract, the position it began at, and the size it is traded in.
    held: Option<(Contract, f64, f64)>,
    /// A crypto's quote, which prices what `take` sends.
    quote: Option<Live<Ticker>>,
}

impl<'a> Tidy<'a> {
    fn new(ib: &'a IB) -> Self {
        Tidy {
            ib,
            orders: Vec::new(),
            held: None,
            quote: None,
        }
    }

    /// Holds `c`, traded in `size`: its position now is where it is traded
    /// back to. Gives that position.
    fn hold(&mut self, c: &Contract, size: f64) -> f64 {
        if is_crypto(c) {
            let quote = self.ib.req_mkt_data(c, "", false, false, &[]);
            self.quote = Some(quote.expect("the crypto's quote"));
        }
        let start = position(self.ib, c);
        self.held = Some((c.clone(), start, size));
        start
    }

    /// An order that trades `qty` of the held contract now: at market for a
    /// stock; for a crypto, which the venue does not take at market, a limit
    /// through the quote, immediate-or-cancel, in whole dollars and by half
    /// the venue's band ("Buy Limit Orders for this product must be within
    /// the maximum of 10 dollars or 0.25 percent of the best ask price").
    /// `None` while there is no quote.
    fn take(&self, action: &str, qty: f64) -> Option<Live<Order>> {
        let Some(quote) = &self.quote else {
            return Some(Live::new(Order::market(action, qty)));
        };
        let buy = action == "BUY";
        let side = || {
            let q = quote.read();
            if buy { q.ask } else { q.bid }
        };
        if !until(self.ib, 30, || side() > 0.0) {
            return None;
        }
        let touch = side();
        let through = (10f64.max(touch * 0.0025) / 2.0).floor();
        let price = if buy {
            touch.floor() + through
        } else {
            touch.ceil() - through
        };
        Some(Live::new(Order {
            tif: "IOC".into(),
            ..Order::limit(action, qty, price)
        }))
    }
}

impl Drop for Tidy<'_> {
    fn drop(&mut self) {
        let ib = self.ib;
        let mut left = Vec::new();
        if !self.orders.is_empty() {
            for t in ib.req_all_open_orders().unwrap_or_default() {
                let order = t.read().order.clone();
                let id = order.read().order_id;
                if self.orders.contains(&id) && !t.read().is_done() {
                    let _ = ib.cancel_order(&order, "");
                    if !until(ib, 30, || t.read().is_done()) {
                        left.push(format!("order {id}"));
                    }
                }
            }
        }
        if let Some((c, start, size)) = self.held.clone() {
            let off = || ((position(ib, &c) - start) / size).round();
            // A fill's position can come a little after it.
            until(ib, 10, || off() != 0.0);
            for _ in 0..3 {
                let (n, now) = (off(), position(ib, &c));
                // More than the test trades moved it: someone else did.
                if n == 0.0 || n.abs() > 1.0 {
                    break;
                }
                let action = if n > 0.0 { "SELL" } else { "BUY" };
                let Some(order) = self.take(action, size) else {
                    break;
                };
                if ib.place_order(&c, &order).is_ok() {
                    until(ib, 30, || position(ib, &c) != now);
                }
            }
            if off() != 0.0 {
                left.push(format!("{} {} from {start}", c.symbol, position(ib, &c)));
            }
        }
        if !left.is_empty() {
            eprintln!("LEFT ON THE ACCOUNT: {left:?}");
            if !std::thread::panicking() {
                panic!("left on the account: {left:?}");
            }
        }
    }
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
    let c = qualified(&ib);
    let size = trading_or_skip!(&ib, &c);
    let mut tidy = Tidy::new(&ib);
    let start = tidy.hold(&c, size);
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
    let (l, con_id) = (log.clone(), c.con_id);
    ib.position_event().connect(move |p| {
        if p.contract.con_id == con_id && p.position != start {
            note(&l, format!("position {}", p.position));
        }
    });

    let order = tidy.take("BUY", size).expect("a quote to price the order");
    let trade = ib.place_order(&c, &order).unwrap();
    tidy.orders.push(order.read().order_id);
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
}

#[test]
#[ignore = "needs a paper login and an open market"]
fn an_unqualified_contract_is_named_before_its_order_is_sent() {
    let ib = session_or_skip!(1);
    let errors = Log::default();
    let l = errors.clone();
    ib.error_event()
        .connect(move |(id, code, msg, _)| note(&l, format!("{id} {code} {msg}")));
    // No con_id: the engine names the contract before the order goes.
    let c = traded();
    let order = resting(&c, trading_or_skip!(&ib, &c));
    let mut tidy = Tidy::new(&ib);
    let trade = ib.place_order(&c, &order).unwrap();
    let id = order.read().order_id;
    tidy.orders.push(id);
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
#[ignore = "needs a paper login and an open market"]
fn every_session_hears_every_order_on_the_account() {
    // Client 0 leaves a working order behind.
    let ib = session_or_skip!(0);
    let c = qualified(&ib);
    let order = resting(&c, trading_or_skip!(&ib, &c));
    let trade = ib.place_order(&c, &order).unwrap();
    let (id, perm_id) = {
        let mut tidy = Tidy::new(&ib);
        tidy.orders.push(order.read().order_id);
        assert!(until(&ib, 30, || trade.read().is_working()));
        // Working: left for client 1, whose guard cancels it.
        tidy.orders.clear();
        let o = trade.read().order.read();
        (o.order_id, o.perm_id)
    };
    drop(ib);

    // Client 1 hears of it among its own open orders, as among everyone's:
    // the engine tells every session of every order on the account.
    let ib = session_or_skip!(1);
    let mut tidy = Tidy::new(&ib);
    tidy.orders.push(id);
    let own = ib.req_open_orders().unwrap();
    assert!(own.iter().any(|t| t.read().order.read().perm_id == perm_id));
    let all = ib.req_all_open_orders().unwrap();
    let theirs = all
        .iter()
        .find(|t| t.read().order.read().perm_id == perm_id)
        .expect("req_all_open_orders gives client 0's order")
        .clone();
    let placed = theirs.read().order.clone();
    ib.cancel_order(&placed, "").unwrap();
    assert!(until(&ib, 30, || theirs.read().is_done()));
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

//! Sessions on the engine's test harness: an engine session built from its
//! parts, whose loop never runs, served by the process's owner.

use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use super::{GLOBAL_ERRORS, Log, engine, note, opts, read, seen, status, stock, within};
use crate::engine::{ControlCommand, EClient, ErrorOrigin};
use crate::error::Error;
use crate::event::lock;
use crate::ib::{IB, IBConfig};
use crate::live::Live;
use crate::objects::{IBDefaults, RealTimeBar};
use crate::order::Order;
use crate::owner::{Via, park_on};
use crate::record::Callback;
use crate::timer::Clock;

/// The published session's engine.
fn session(ib: &IB) -> Arc<EClient> {
    ib.shared.connected().unwrap().1
}

/// Notes each error `ib` emits: `order CODE` under an order's id, else
/// `session CODE`.
fn errors(ib: &IB) -> Log {
    let log = Log::default();
    let l = log.clone();
    ib.error_event().connect(move |e| {
        let at = if e.0 > 0 { "order" } else { "session" };
        note(&l, format!("{at} {}", e.1));
    });
    log
}

#[test]
fn what_the_engine_said_before_publication_is_applied_by_the_first_read() {
    let _g = lock(&GLOBAL_ERRORS);
    let (client, _rx) = engine();
    // Said between the logon and the session's publication.
    client.refuse(
        ErrorOrigin::Session,
        2104,
        "Market data farm connection is OK",
    );
    let ib = IB::with(IBDefaults::default(), IBConfig::default()).unwrap();
    let log = errors(&ib);
    let via = Via::Test(Some(Arc::new(client)));
    let (p, _logon) = ib.begin_connect(opts(), true, Some(via)).unwrap();
    park_on(p).unwrap();
    assert!(within(|| !seen(&log).is_empty()));
    assert_eq!(seen(&log), ["session 2104"]);
}

#[test]
fn a_lost_connection_the_engine_recovers_leaves_the_session_up() {
    let _g = lock(&GLOBAL_ERRORS);
    let (client, _rx) = engine();
    let ib = IB::attach(client, opts(), Clock::system()).unwrap();
    let log = errors(&ib);
    let engine = session(&ib).shared_state().clone();
    engine.set_connection_lost();
    assert!(within(|| seen(&log) == ["session 1100"]));
    assert!(ib.is_connected());
    engine.set_connection_restored();
    assert!(within(|| seen(&log) == ["session 1100", "session 1102"]));
    assert!(ib.is_connected());
}

#[test]
fn a_disconnect_sends_what_was_admitted_before_it_ahead_of_the_logout() {
    let _g = lock(&GLOBAL_ERRORS);
    let (client, rx) = engine();
    let ib = IB::attach(client, opts(), Clock::system()).unwrap();
    // The owner is held inside a handler, so what follows waits its turn.
    let (entered_tx, entered) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    let released = Mutex::new(released);
    ib.error_event().connect(move |_| {
        let _ = entered_tx.send(());
        let _ = lock(&released).recv();
    });
    session(&ib).refuse(ErrorOrigin::Session, 2104, "farm ok");
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    // A placement admitted in its call, then a disconnect from another thread.
    let placed = ib.what_if_order_async(&stock(), &Order::limit("BUY", 1.0, 1.0));
    let h = ib.handle();
    let closing = thread::spawn(move || h.disconnect());
    thread::sleep(Duration::from_millis(100));
    release.send(()).unwrap();
    assert!(closing.join().unwrap().is_some());
    let sent: Vec<&str> = rx
        .try_iter()
        .filter_map(|c| match c {
            ControlCommand::Place(p) if p.order.what_if => Some("place"),
            ControlCommand::Logout => Some("logout"),
            _ => None,
        })
        .collect();
    assert_eq!(sent, ["place", "logout"]);
    // Its answer never comes: the disconnect fails it.
    let r = placed.wait(Some(Duration::from_secs(5)));
    assert!(matches!(r, Err(Error::NotConnected)), "{r:?}");
}

#[test]
fn a_refusal_the_crate_makes_follows_what_the_engine_said_before_the_call() {
    let _g = lock(&GLOBAL_ERRORS);
    let (client, _rx) = engine();
    let ib = IB::attach(client, opts(), Clock::system()).unwrap();
    let log = errors(&ib);
    // On the owner, inside a read: the engine says something, then an order
    // it cannot carry is placed and refused by the crate.
    let (h, engine) = (ib.handle(), session(&ib));
    ib.error_event().connect(move |e| {
        if e.1 == 2106 {
            engine.refuse(ErrorOrigin::Session, 2104, "farm ok");
            let uncarried = Order {
                e_trade_only: true,
                ..Order::limit("BUY", 1.0, 1.0)
            };
            h.place_order(&stock(), &Live::new(uncarried)).unwrap();
        }
    });
    session(&ib).refuse(ErrorOrigin::Session, 2106, "hmds ok");
    assert!(within(|| seen(&log).len() == 3));
    assert_eq!(seen(&log), ["session 2106", "session 2104", "order 10268"]);
}

#[test]
fn nothing_a_session_kept_outlives_the_ib() {
    let _g = lock(&GLOBAL_ERRORS);
    let (client, _rx) = engine();
    let ib = IB::attach(client, opts(), Clock::system()).unwrap();
    let c = stock();
    let ticker = ib.req_mkt_data(&c, "", false, false, &[]).unwrap();
    let order = Live::new(Order::limit("BUY", 1.0, 1.0));
    let trade = ib.place_order(&c, &order).unwrap();
    let bars = ib.req_real_time_bars(&c, 5, "TRADES", false, &[]).unwrap();
    let (wt, wr, wb) = (ticker.downgrade(), trade.downgrade(), bars.downgrade());
    // A handler that refers to its own object holds it weakly, as a program's
    // must.
    let log = Log::default();
    let (l, w) = (log.clone(), wt.clone());
    ticker
        .update_event()
        .connect(move |_| note(&l, format!("ticker {}", w.upgrade().is_some())));
    let (l, w) = (log.clone(), wr.clone());
    trade
        .status_event()
        .connect(move |_| note(&l, format!("trade {}", w.upgrade().is_some())));
    let (l, w) = (log.clone(), wb.clone());
    bars.update_event()
        .connect(move |_| note(&l, format!("bars {}", w.upgrade().is_some())));
    let ticker_id = ib
        .shared
        .core()
        .state
        .req_id_to_ticker
        .iter()
        .find(|(_, t)| Live::ptr_eq(t, &ticker))
        .map(|(id, _)| *id)
        .unwrap();
    let (order_id, bars_id) = (order.read().order_id, bars.read().req_id);
    read(
        &ib,
        vec![
            Callback::TickPrice {
                req_id: ticker_id,
                tick_type: 4,
                price: 1.5,
            },
            status(order_id, "Submitted"),
            Callback::RealtimeBar {
                req_id: bars_id,
                bar: RealTimeBar::default(),
            },
        ],
    );
    let mut got = seen(&log);
    got.sort();
    assert_eq!(got, ["bars true", "ticker true", "trade true"]);
    // A cancel and a reconnect.
    assert!(ib.cancel_mkt_data(&c).unwrap());
    ib.cancel_real_time_bars(&bars).unwrap();
    assert!(ib.disconnect().is_some());
    let (client, _rx) = engine();
    let via = Via::Test(Some(Arc::new(client)));
    let (p, _logon) = ib.begin_connect(opts(), true, Some(via)).unwrap();
    park_on(p).unwrap();
    // The next session starts with nothing of the last.
    assert!(ib.trades().is_empty() && ib.tickers().is_empty() && ib.realtime_bars().is_empty());
    drop((ticker, trade, order, bars));
    // The IB's events keep the last value each emitted, as eventkit's do, so
    // the last handles go with the IB, which a handler holding a handle to it
    // does not keep.
    let h = ib.handle();
    ib.error_event().connect(move |_| {
        let _ = h.is_connected();
    });
    let shared = Arc::downgrade(&ib.shared);
    drop(ib);
    // Freed on the owner, as it drops the IB's last reference.
    assert!(within(|| {
        shared.upgrade().is_none()
            && wt.upgrade().is_none()
            && wr.upgrade().is_none()
            && wb.upgrade().is_none()
    }));
}

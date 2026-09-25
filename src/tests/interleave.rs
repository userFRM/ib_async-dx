//! Interleavings on real threads, with the process's owner serving sessions
//! on the engine's test harness.

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use super::{GLOBAL_ERRORS, Log, engine, note, opts, read, seen, status, stock};
use crate::event::lock;
use crate::ib::IB;
use crate::live::Live;
use crate::order::Order;
use crate::timer::Clock;

#[test]
fn every_handler_of_an_emission_sees_one_value_of_a_bound_order() {
    let _g = lock(&GLOBAL_ERRORS);
    let (client, _rx) = engine();
    let ib = IB::attach(client, opts(), Clock::system()).unwrap();
    let order = Live::new(Order::limit("BUY", 1.0, 1.0));
    let _trade = ib.place_order(&stock(), &order).unwrap();
    let id = order.read().order_id;
    // The first handler lets another thread edit the order, then looks at
    // it; the second looks after it.
    let log = Log::default();
    let (entered_tx, entered) = mpsc::channel();
    let l = log.clone();
    ib.order_status_event().connect(move |t| {
        let _ = entered_tx.send(());
        thread::sleep(Duration::from_millis(100));
        note(&l, format!("{:?}", t.read().order.read().lmt_price));
    });
    let l = log.clone();
    ib.order_status_event()
        .connect(move |t| note(&l, format!("{:?}", t.read().order.read().lmt_price)));
    let edited = order.clone();
    let editor = thread::spawn(move || {
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        edited.edit(|o| o.lmt_price = Some(2.0))
    });
    read(&ib, vec![status(id, "Submitted")]);
    editor.join().unwrap().unwrap();
    assert_eq!(seen(&log), ["Some(1.0)", "Some(1.0)"]);
    assert_eq!(order.read().lmt_price, Some(2.0));
}

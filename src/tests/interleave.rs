//! Interleavings on real threads, with the process's owner serving sessions
//! on the engine's test harness.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::task::{Context, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};

use super::{GLOBAL_ERRORS, Log, engine, note, opts, read, seen, status, stock, within};
use crate::engine::ControlCommand;
use crate::event::lock;
use crate::ib::IB;
use crate::live::Live;
use crate::order::Order;
use crate::owner::UNSENT;
use crate::timer::Clock;

/// A waker that notes it was woken.
#[derive(Default)]
struct Woken(AtomicBool);

impl Wake for Woken {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[test]
fn a_fut_twin_that_finds_no_room_returns_at_once_and_is_admitted_when_room_comes() {
    let _g = lock(&GLOBAL_ERRORS);
    let (client, rx) = engine();
    let ib = IB::attach(client, opts(), Clock::system()).unwrap();
    let c = stock();
    // The engine's loop never runs, so what it is handed stays unsent until
    // user threads' requests find no room.
    for _ in 0..UNSENT {
        ib.req_mkt_data(&c, "", false, false, &[]).unwrap();
    }
    let handed = rx.try_iter().count();
    assert_eq!(handed, UNSENT);
    let called = Instant::now();
    let mut p = ib.req_contract_details_async(&c);
    assert!(called.elapsed() < Duration::from_secs(1));
    let woken = Arc::new(Woken::default());
    let waker = Waker::from(woken.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(Pin::new(&mut p).poll(&mut cx).is_pending());
    thread::sleep(Duration::from_millis(50));
    assert_eq!(rx.try_iter().count(), 0, "admitted without room");
    // The engine takes its work: the waiter is woken, and a poll once there
    // is room admits the request, which the owner then sends.
    let engine = ib.shared.connected().unwrap().1.shared_state().clone();
    engine.publish_finished(u64::try_from(handed).unwrap());
    let admitted = within(|| {
        if woken.0.swap(false, Ordering::SeqCst) {
            assert!(Pin::new(&mut p).poll(&mut cx).is_pending());
        }
        rx.try_iter()
            .any(|c| matches!(c, ControlCommand::FetchContractDetails { .. }))
    });
    assert!(admitted);
}

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

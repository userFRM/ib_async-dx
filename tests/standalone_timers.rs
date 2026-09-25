//! The owner's timers in a process that builds no `IB`: this binary is its
//! own process, so the owner these start is theirs alone.

use std::future::poll_fn;
use std::pin::pin;
use std::sync::mpsc;
use std::time::Duration;

use futures_core::Stream;
use ib_async_dx::IB;
use jiff::{SignedDuration, Timestamp};

/// `ms` milliseconds from now.
fn soon(ms: i64) -> Timestamp {
    Timestamp::now()
        .checked_add(SignedDuration::from_millis(ms))
        .unwrap()
}

/// Whether `t` has been reached, allowing for the two clocks being read a
/// moment apart.
fn reached(t: Timestamp) -> bool {
    Timestamp::now() >= t - SignedDuration::from_millis(1)
}

#[test]
fn schedule_runs_its_callback_on_the_owner_at_its_time() {
    let at = soon(50);
    let (tx, rx) = mpsc::channel();
    let _handle = IB::schedule(at, move || {
        let name = std::thread::current().name().map(str::to_owned);
        let _ = tx.send((name, reached(at)));
    })
    .unwrap();
    let (name, on_time) = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(name.as_deref(), Some("ib_async_dx-owner"));
    assert!(on_time);
}

#[tokio::test]
async fn wait_until_async_returns_at_its_time() {
    let at = soon(50);
    assert!(IB::wait_until_async(at).await.unwrap());
    assert!(reached(at));
}

#[tokio::test]
async fn time_range_async_gives_each_time_once_it_is_reached() {
    let (start, step) = (soon(50), SignedDuration::from_millis(50));
    let end = start + step * 2;
    let range = IB::time_range_async(start, end, Duration::from_millis(50)).unwrap();
    let mut range = pin!(range);
    let mut got = Vec::new();
    while let Some(t) = poll_fn(|cx| range.as_mut().poll_next(cx)).await {
        assert!(reached(t.timestamp()));
        got.push(t.timestamp());
    }
    assert_eq!(got, [start, start + step, end]);
}

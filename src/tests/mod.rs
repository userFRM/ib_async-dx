//! Tests that span modules, and what the unit tests share.

mod engine;
mod interleave;
mod observe;
mod oracle;

use std::pin::Pin;
use std::sync::{Arc, Mutex, Once, mpsc};
use std::task::{Context, Poll, Waker};
use std::thread::{self, ThreadId};
use std::time::{Duration, Instant};

use jiff::Timestamp;

use crate::contract::Contract;
use crate::engine::{ControlCommand, EClient, SharedState};
use crate::error::{Error, Result};
use crate::event::lock;
use crate::ib::{ConnectOptions, IB, IBConfig, IBHandle, StartupFetch};
use crate::objects::IBDefaults;
use crate::order::Order;
use crate::owner::{Class, Shared};
use crate::pending::Pending;
use crate::record::Callback;
use crate::timer::Clock;

/// Serializes the tests that emit or wait on the process's
/// `global_error_event`.
pub(crate) static GLOBAL_ERRORS: Mutex<()> = Mutex::new(());

/// Every log record of the test binary, with the thread that made it.
struct Capture;

static CAPTURE: Capture = Capture;
static LOGS: Mutex<Vec<(ThreadId, log::Level, String, String)>> = Mutex::new(Vec::new());

impl log::Log for Capture {
    fn enabled(&self, _: &log::Metadata<'_>) -> bool {
        true
    }

    fn log(&self, r: &log::Record<'_>) {
        let entry = (
            thread::current().id(),
            r.level(),
            r.target().to_owned(),
            r.args().to_string(),
        );
        lock(&LOGS).push(entry);
    }

    fn flush(&self) {}
}

/// Installs the capture, the test binary's one logger, if it is not yet.
pub(crate) fn capture_logs() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        log::set_logger(&CAPTURE).unwrap();
        log::set_max_level(log::LevelFilter::Trace);
    });
}

/// This thread's ERROR records since the capture was installed, as
/// (target, text).
pub(crate) fn errors_here() -> Vec<(String, String)> {
    capture_logs();
    let me = thread::current().id();
    lock(&LOGS)
        .iter()
        .filter(|l| l.0 == me && l.1 == log::Level::Error)
        .map(|l| (l.2.clone(), l.3.clone()))
        .collect()
}

/// An engine session on no venue whose loop never runs, and the channel the
/// commands it is handed arrive on.
pub(crate) fn engine() -> (EClient, mpsc::Receiver<ControlCommand>) {
    let (tx, rx) = mpsc::channel();
    let shared = Arc::new(SharedState::new());
    let client = EClient::from_parts(shared, tx, thread::spawn(|| {}), "DU123".into());
    (client, rx)
}

/// A connect that fetches nothing at startup.
pub(crate) fn opts() -> ConnectOptions {
    ConnectOptions {
        fetch_fields: StartupFetch::NONE,
        ..ConnectOptions::default()
    }
}

/// Whether `done` comes to hold within five seconds.
pub(crate) fn within(mut done: impl FnMut() -> bool) -> bool {
    let end = Instant::now() + Duration::from_secs(5);
    while Instant::now() < end {
        if done() {
            return true;
        }
        thread::sleep(Duration::from_millis(1));
    }
    false
}

/// A stock with the number the venue gives it, so a ticker can be kept for it.
pub(crate) fn stock() -> Contract {
    Contract {
        con_id: 265598,
        ..Contract::stock("AAPL", "SMART", "USD")
    }
}

/// What handlers saw, in order.
pub(crate) type Log = Arc<Mutex<Vec<String>>>;

pub(crate) fn note(log: &Log, s: impl Into<String>) {
    lock(log).push(s.into());
}

pub(crate) fn seen(log: &Log) -> Vec<String> {
    lock(log).clone()
}

/// Applies `callbacks` as one read of `ib`'s session, on the thread that runs
/// it, and returns after the read's emissions.
pub(crate) fn read(ib: &IB, callbacks: Vec<Callback>) {
    ib.shared
        .step(Class::Control, move |ib| {
            let (g, _) = ib.connected().ok_or(Error::NotConnected)?;
            ib.apply_read(g, callbacks);
            Result::Ok(())
        })
        .unwrap();
}

/// The venue's status of order `order_id` of client 1.
pub(crate) fn status(order_id: i64, status: &str) -> Callback {
    Callback::OrderStatus {
        order_id,
        status: status.into(),
        filled: 0.0,
        remaining: 1.0,
        avg_fill_price: 0.0,
        perm_id: 0,
        parent_id: 0,
        last_fill_price: 0.0,
        client_id: 1,
        why_held: String::new(),
        mkt_cap_price: 0.0,
    }
}

#[test]
fn every_fut_twin_has_failed_at_the_call_without_a_session() {
    // Nothing runs this IB's steps, so only a failure at the call is seen.
    let shared = Shared::new(
        IBDefaults::default(),
        IBConfig::default(),
        Clock::manual(Timestamp::UNIX_EPOCH),
    );
    shared.connect_internal_slots();
    let ib = IBHandle { shared };
    fn failed<T>(mut p: Pending<T>) -> bool {
        let r = Pin::new(&mut p).poll(&mut Context::from_waker(Waker::noop()));
        matches!(r, Poll::Ready(Err(Error::NotConnected)))
    }
    let (c, order) = (stock(), Order::limit("BUY", 1.0, 1.0));
    let twins = [
        (
            "reqAccountUpdatesAsync",
            failed(ib.req_account_updates_async("")),
        ),
        (
            "reqAccountUpdatesMultiAsync",
            failed(ib.req_account_updates_multi_async("", "")),
        ),
        (
            "reqAccountSummaryAsync",
            failed(ib.req_account_summary_async()),
        ),
        ("reqPositionsAsync", failed(ib.req_positions_async())),
        (
            "reqMktDepthExchangesAsync",
            failed(ib.req_mkt_depth_exchanges_async()),
        ),
        (
            "reqSmartComponentsAsync",
            failed(ib.req_smart_components_async("SMART")),
        ),
        (
            "reqContractDetailsAsync",
            failed(ib.req_contract_details_async(&c)),
        ),
        (
            "reqSecDefOptParamsAsync",
            failed(ib.req_sec_def_opt_params_async("AAPL", "", "STK", 265598)),
        ),
        (
            "reqHistoricalScheduleAsync",
            failed(ib.req_historical_schedule_async(&c, 1, "", true)),
        ),
        (
            "reqHistoricalTicksAsync",
            failed(ib.req_historical_ticks_async(&c, "", "", 10, "TRADES", true, false, &[])),
        ),
        (
            "reqHistogramDataAsync",
            failed(ib.req_histogram_data_async(&c, true, "3 days")),
        ),
        (
            "reqFundamentalDataAsync",
            failed(ib.req_fundamental_data_async(&c, "RESC", &[])),
        ),
        (
            "reqScannerParametersAsync",
            failed(ib.req_scanner_parameters_async()),
        ),
        (
            "reqNewsProvidersAsync",
            failed(ib.req_news_providers_async()),
        ),
        (
            "reqNewsArticleAsync",
            failed(ib.req_news_article_async("BZ", "1", &[])),
        ),
        ("reqCurrentTimeAsync", failed(ib.req_current_time_async())),
        ("reqUserInfoAsync", failed(ib.req_user_info_async())),
        (
            "reqCurrentTimeInMillisAsync",
            failed(ib.req_current_time_in_millis_async()),
        ),
        (
            "whatIfOrderAsync",
            failed(ib.what_if_order_async(&c, &order)),
        ),
        ("reqOpenOrdersAsync", failed(ib.req_open_orders_async())),
        (
            "reqAllOpenOrdersAsync",
            failed(ib.req_all_open_orders_async()),
        ),
        (
            "reqCompletedOrdersAsync",
            failed(ib.req_completed_orders_async(false)),
        ),
        ("reqExecutionsAsync", failed(ib.req_executions_async(None))),
    ];
    for (name, failed) in twins {
        assert!(failed, "{name}");
    }
}

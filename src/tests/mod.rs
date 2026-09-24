//! Tests that span modules, and what the unit tests share.

mod engine;
mod interleave;
mod observe;
mod oracle;

use std::sync::{Mutex, Once};
use std::thread::{self, ThreadId};

use crate::event::lock;

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

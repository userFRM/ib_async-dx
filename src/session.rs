//! The session's connection state, and its logon and closer threads.
//!
//! Only the owner moves an IB's [`Conn`], and every move names the
//! generation it acts for. A logon thread runs the engine's login and its
//! wait for the account's working orders; a closer thread runs the engine's
//! shutdown. Neither is joined: each thread's post to its IB is its last act
//! on the engine.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Weak};
use std::thread;
use std::time::Duration;

use crate::engine::{EClient, EClientConfig};
use crate::error::{Error, Result};
use crate::event::panic_message;
use crate::owner::Shared;

/// The login deadline has not been reached, and the login still runs.
const LOGIN: u8 = 0;
/// The login is done: its deadline is disarmed.
const IN: u8 = 1;
/// The login deadline won: the logon is taken back.
const EXPIRED: u8 = 2;

/// A connect attempt's take-back switch and login stage, shared by the
/// connect's caller, its command and its logon thread.
#[derive(Clone)]
pub(crate) struct Logon {
    /// `EClientConfig.cancel`: set, the engine takes the logon back.
    cancel: Arc<AtomicBool>,
    stage: Arc<AtomicU8>,
}

impl Logon {
    /// An attempt's switch: the caller's own `cancel`, or a new one.
    pub(crate) fn new(cancel: Option<Arc<AtomicBool>>) -> Self {
        Logon {
            cancel: cancel.unwrap_or_default(),
            stage: Arc::new(AtomicU8::new(LOGIN)),
        }
    }

    /// The switch the engine reads.
    pub(crate) fn cancel(&self) -> Arc<AtomicBool> {
        self.cancel.clone()
    }

    pub(crate) fn taken_back(&self) -> bool {
        self.cancel.load(Ordering::Acquire)
    }

    /// Takes the logon back: the engine stops it at its next check.
    pub(crate) fn take_back(&self) {
        self.cancel.store(true, Ordering::Release);
    }

    /// The login deadline, reached by the owner or by a blocking caller:
    /// whichever moves the stage from `Login` takes the logon back and gives
    /// `true`. A login already done gives `false`, and nothing changes.
    pub(crate) fn expire(&self) -> bool {
        let won = self
            .stage
            .compare_exchange(LOGIN, EXPIRED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        if won {
            self.take_back();
        }
        won
    }

    /// Whether the login deadline took the logon back.
    pub(crate) fn expired(&self) -> bool {
        self.stage.load(Ordering::Acquire) == EXPIRED
    }

    /// Whether the login deadline still stands.
    pub(crate) fn logging_in(&self) -> bool {
        self.stage.load(Ordering::Acquire) == LOGIN
    }

    /// The engine's login returned: the deadline is disarmed. `false` when
    /// the deadline had already taken the logon back.
    fn logged_in(&self) -> bool {
        match self
            .stage
            .compare_exchange(LOGIN, IN, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => true,
            Err(now) => now == IN,
        }
    }
}

/// Why a generation ends: who closed it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Cause {
    /// `disconnect()`, a failed or abandoned startup sync, or the IB's drop.
    User,
    /// `Client::disconnect()`.
    ClientUser,
    /// A `connect` on a connected IB.
    Replaced,
    /// The engine's `connection_closed`.
    Peer,
    /// A panic in the owner's own work for the generation.
    Internal(String),
}

/// How the last engine session ended.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum EngineEnd {
    /// Its thread is known to have ended.
    Closed,
    /// A closer, or a failed logon, could not establish that it ended.
    Unconfirmed(String),
}

/// An IB's connection, as the owner moves it.
pub(crate) enum Conn {
    Disconnected {
        engine: EngineEnd,
    },
    /// A logon thread runs for generation `g`.
    Connecting {
        g: u64,
        logon: Logon,
    },
    Connected {
        g: u64,
        client: Arc<EClient>,
    },
    /// `client` is held until its closer has started.
    Closing {
        g: u64,
        client: Option<Arc<EClient>>,
    },
}

/// A logon or closer thread's last word to its IB.
pub(crate) enum Post {
    LoggedOn {
        g: u64,
        client: Arc<EClient>,
    },
    LogonFailed {
        g: u64,
        error: Error,
        engine: EngineEnd,
    },
    EngineClosed {
        g: u64,
        engine: EngineEnd,
    },
}

/// Starts generation `g`'s logon thread: the engine's login, then its wait
/// for the account's working orders, bounded by `timeout`.
pub(crate) fn spawn_logon(
    ib: Weak<Shared>,
    g: u64,
    config: EClientConfig,
    timeout: Option<Duration>,
    logon: Logon,
) -> std::io::Result<()> {
    thread::Builder::new()
        .name("ib_async_dx-logon".into())
        .spawn(move || {
            let post = log_on(g, &config, timeout, &logon);
            deliver(&ib, post);
        })
        .map(drop)
}

fn log_on(g: u64, config: &EClientConfig, timeout: Option<Duration>, logon: &Logon) -> Post {
    let failed = |error, engine| Post::LogonFailed { g, error, engine };
    // The engine's error is not `Send`, so only its text crosses threads.
    let client = match catch_unwind(AssertUnwindSafe(|| {
        EClient::connect(config).map_err(|e| e.to_string())
    })) {
        Ok(Ok(client)) => client,
        Ok(Err(why)) => return failed(Error::Connection(why), EngineEnd::Closed),
        // The engine's thread may have started before the panic, and nothing
        // establishes that it ended.
        Err(p) => {
            let why = panic_message(&*p);
            return failed(Error::Connection(why.clone()), EngineEnd::Unconfirmed(why));
        }
    };
    if !logon.logged_in() {
        // The login deadline took the logon back while it finished.
        return failed(Error::Timeout, shut_down(Arc::new(client)));
    }
    let waited = catch_unwind(AssertUnwindSafe(|| {
        client
            .next_shared_id_within(timeout)
            .map_err(|e| e.to_string())
    }));
    let client = Arc::new(client);
    match waited {
        Ok(Ok(_)) => Post::LoggedOn { g, client },
        Ok(Err(why)) => {
            // The replay wait ran out, as ib_async's wait for `apiStart`
            // does, or the connect was taken back meanwhile.
            let error = if logon.taken_back() {
                Error::Connection(why)
            } else {
                Error::Timeout
            };
            failed(error, shut_down(client))
        }
        Err(p) => {
            let why = panic_message(&*p);
            failed(Error::Connection(why), shut_down(client))
        }
    }
}

/// Starts a closer for generation `g`'s engine session.
pub(crate) fn spawn_closer(ib: Weak<Shared>, g: u64, client: Arc<EClient>) -> std::io::Result<()> {
    thread::Builder::new()
        .name("ib_async_dx-close".into())
        .spawn(move || {
            let engine = shut_down(client);
            deliver(&ib, Post::EngineClosed { g, engine });
        })
        .map(drop)
}

/// Ends an engine session: its `disconnect`, which returns once the
/// engine's thread has ended, then the drop of this handle, each under its
/// own `catch_unwind` so a destructor never runs inside the first unwind.
fn shut_down(client: Arc<EClient>) -> EngineEnd {
    let ended = catch_unwind(AssertUnwindSafe(|| {
        client.disconnect();
    }));
    let _ = catch_unwind(AssertUnwindSafe(move || drop(client)));
    match ended {
        Ok(()) => EngineEnd::Closed,
        Err(p) => EngineEnd::Unconfirmed(panic_message(&*p)),
    }
}

/// Posts to the IB. A post its IB no longer takes comes back, and a client
/// it carries is shut down here.
fn deliver(ib: &Weak<Shared>, post: Post) {
    let back = match ib.upgrade() {
        Some(ib) => ib.post(post).err(),
        None => Some(post),
    };
    if let Some(Post::LoggedOn { client, .. }) = back {
        shut_down(client);
    }
}

/// The engine settings a connect hands its logon: the caller's, with the
/// attempt's take-back switch.
pub(crate) fn logon_config(mut config: EClientConfig, logon: &Logon) -> EClientConfig {
    config.cancel = Some(logon.cancel());
    config
}

/// `client_id` as the TWS API's `int`.
pub(crate) fn client_id(id: i64) -> Result<i64> {
    i32::try_from(id)
        .map(i64::from)
        .map_err(|_| Error::Value(format!("clientId {id} is not a 32-bit integer")))
}

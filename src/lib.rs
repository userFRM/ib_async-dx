//! ib_async in Rust: its model and names in Rust spelling, on the ibkr-dx
//! engine, with no gateway in between.
//!
//! [`IB`] is ib_async's `IB`. The types ib_async exports, from `Contract` and
//! `Order` to `Ticker` and `Trade`, carry the same fields under snake_case
//! names. `use ib_async_dx::prelude::*;` is `from ib_async import *`.

#![forbid(unsafe_code)]
#![cfg_attr(
    not(test),
    deny(
        missing_docs,
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::todo,
        clippy::unimplemented
    )
)]

mod client;
mod contract;
mod convert;
mod engine;
mod error;
mod event;
#[cfg(feature = "flex")]
pub mod flex;
mod ib;
mod live;
mod objects;
mod order;
mod owner;
mod pending;
mod record;
mod requests;
mod session;
mod state;
#[cfg(test)]
mod tests;
mod ticker;
mod timer;
pub mod util;

pub use client::{Client, ConnState};
pub use contract::*;
pub use engine::{EClientConfig, HeldElsewhere, ScannedStrategy, SpreadScan};
pub use error::{Error, Result};
pub use event::*;
pub use ib::*;
pub use live::*;
pub use objects::*;
pub use order::*;
pub use pending::*;
pub use ticker::*;
pub use timer::*;

/// This crate's version: ib_async's `__version__`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// What `from ib_async import *` gives: `IB` and its settings, the live
/// handles and events, the errors, `util`, and every model type.
pub mod prelude {
    pub use crate::contract::*;
    pub use crate::objects::*;
    pub use crate::order::*;
    pub use crate::ticker::*;
    pub use crate::{
        ConnectOptions, Error, Event, IB, IBConfig, IBHandle, Live, Pending, Result, StartupFetch,
        Subscription, WeakLive, util,
    };
}

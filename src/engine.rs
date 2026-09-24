//! The engine paths this crate uses. This is the only file that names
//! `ibkr_dx`; every other file reaches the engine through these.
//!
//! Each path names where the engine exports it, at the pinned commit. `api`
//! is the engine's documented surface; `types` is public and documented;
//! the crate root re-exports the rest.

// The crate root (src/lib.rs:97-120).
//
// lib.rs:99: the settings a session is opened with, `ConnectOptions.config`.
pub use ibkr_dx::EClientConfig;
// lib.rs:120.
#[expect(unused_imports, reason = "the owner's id space ends at it")]
pub(crate) use ibkr_dx::FIRST_RESERVED_REQUEST_ID;
// lib.rs:97; control/adjustments.rs:11,89.
pub(crate) use ibkr_dx::{Adjustment, AdjustmentKind};
// lib.rs:99; api/mod.rs:14,16.
#[expect(
    unused_imports,
    reason = "the owner and Capture drive the engine through them"
)]
pub(crate) use ibkr_dx::{EClient, Wrapper};
// lib.rs:106,110.
#[expect(
    unused_imports,
    reason = "exercise_options and Client::server_version read them"
)]
pub(crate) use ibkr_dx::{ExerciseStates, PROTOCOL_LEVEL};

// The caller's model types: `api::types` is `types::model` (api/mod.rs:11).
// Lines of types/model.rs, in order: 1707, 15, 1629, 54, 1977, 1765, 41,
// 1533, 1597, 116, 1385, 2004, 1354, 1671, 1696, 1684.
pub(crate) use ibkr_dx::api::types::{
    BarData, ComboLeg, CommissionAndFeesReport, Contract, ContractDescription, ContractDetails,
    DeltaNeutralContract, Execution, ExecutionFilter, Order, OrderState, PriceIncrement, TagValue,
    TickAttrib, TickAttribBidAsk, TickAttribLast,
};

// `types` (lib.rs:64), with `types::orders` and `types::commands` glob
// re-exported into it (types/mod.rs:20-25). Lines, in order:
// commands.rs:75; mod.rs:785, 977, 821, 345; orders.rs:840; mod.rs:62, 311,
// 810, 123.
pub(crate) use ibkr_dx::types::{
    CalendarQuery, DepthMktDataDescription, HistoricalTickData, NewsProvider, OptionComputation,
    OrderCondition, PRICE_SCALE, PositionElsewhere, SmartComponent, price_from_f64,
};
// types/mod.rs:330, 586, 490: in the signatures of `account_values_elsewhere`
// and `req_spread_scan`.
pub use ibkr_dx::types::{HeldElsewhere, ScannedStrategy, SpreadScan};

// Test-only: the harness builds a session with `EClient::from_parts`
// (api/client/mod.rs:686-691) on a bare `SharedState` (lib.rs:77;
// bridge/mod.rs:188). Both are `#[doc(hidden)]`, which is why they are reached
// only under `cfg(test)`.
#[cfg(test)]
#[expect(
    unused_imports,
    reason = "the engine-backed tests build sessions on it"
)]
pub(crate) use ibkr_dx::bridge::SharedState;

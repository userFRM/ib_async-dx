//! The engine paths this crate uses. This is the only file that names
//! `ibkr_dx`; every other file reaches the engine through these.
//!
//! Each path names where the engine exports it, at the pinned commit. `api`
//! is the engine's documented surface; `types` is public and documented;
//! the crate root re-exports the rest.

// The crate root (src/lib.rs:99-122).
//
// lib.rs:101: the settings a session is opened with, `ConnectOptions.config`.
pub use ibkr_dx::EClientConfig;
// lib.rs:122.
#[expect(unused_imports, reason = "the owner's id space ends at it")]
pub(crate) use ibkr_dx::FIRST_RESERVED_REQUEST_ID;
// lib.rs:99; control/adjustments.rs:11,89.
pub(crate) use ibkr_dx::{Adjustment, AdjustmentKind};
// lib.rs:101; api/mod.rs:14,17.
#[expect(unused_imports, reason = "the owner drives the engine through it")]
pub(crate) use ibkr_dx::EClient;
pub(crate) use ibkr_dx::Wrapper;
// lib.rs:108,112.
#[expect(
    unused_imports,
    reason = "exercise_options and Client::server_version read them"
)]
pub(crate) use ibkr_dx::{ExerciseStates, PROTOCOL_LEVEL};

// The caller's model types: `api::types` is `types::model` (api/mod.rs:11).
// Lines of types/model.rs, in order: 1789, 15, 1711, 54, 2059, 1847, 41,
// 1615, 1679, 116, 1467, 2086, 1384, 1753, 1778, 1766.
pub(crate) use ibkr_dx::api::types::{
    BarData, ComboLeg, CommissionAndFeesReport, Contract, ContractDescription, ContractDetails,
    DeltaNeutralContract, Execution, ExecutionFilter, Order, OrderState, PriceIncrement, TagValue,
    TickAttrib, TickAttribBidAsk, TickAttribLast,
};
// What an error is about, as `Wrapper::error_from` states it, and the
// question `Wrapper::question_retired` confirms: types/model.rs:2121, 2169,
// 2184.
#[cfg_attr(
    not(test),
    expect(
        unused_imports,
        reason = "Apply tells a refused modify from a refused order by it"
    )
)]
pub(crate) use ibkr_dx::api::types::OrderOp;
pub(crate) use ibkr_dx::api::types::{ErrorOrigin, Question};

// `types` (lib.rs:64), with `types::orders` and `types::commands` glob
// re-exported into it (types/mod.rs:20-25). Lines, in order:
// commands.rs:78; mod.rs:789, 981, 825, 339; orders.rs:854; mod.rs:62, 305,
// 814, 123.
pub(crate) use ibkr_dx::types::{
    CalendarQuery, DepthMktDataDescription, HistoricalTickData, NewsProvider, OptionComputation,
    OrderCondition, PRICE_SCALE, PositionElsewhere, SmartComponent, price_from_f64,
};
// types/mod.rs:324, 580, 484: in the signatures of `account_values_elsewhere`
// and `req_spread_scan`.
pub use ibkr_dx::types::{HeldElsewhere, ScannedStrategy, SpreadScan};

// Test-only: the harness builds a session with `EClient::from_parts`
// (api/client/mod.rs:875-880) on a bare `SharedState` (lib.rs:77;
// bridge/mod.rs:191). Both are `#[doc(hidden)]`, which is why they are reached
// only under `cfg(test)`.
#[cfg(test)]
#[expect(
    unused_imports,
    reason = "the engine-backed tests build sessions on it"
)]
pub(crate) use ibkr_dx::bridge::SharedState;

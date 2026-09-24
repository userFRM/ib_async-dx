//! ib_async's `IB`: the session, its settings and its methods.

mod account;
mod extras;
mod market_data;
mod orders;
mod reference;

/// One session: ib_async's `IB`.
pub struct IB;

/// A handle to an [`IB`]'s methods that does not keep its session open.
pub struct IBHandle;

/// The settings ib_async keeps as `IB` class attributes: `RequestTimeout`,
/// `RaiseRequestErrors`, `MaxSyncedSubAccounts` and `TimezoneTWS`.
pub struct IBConfig;

/// What `connect` takes: ib_async's `connect` arguments.
pub struct ConnectOptions;

/// What `connect` fetches at startup: ib_async's `StartupFetch`.
pub struct StartupFetch;

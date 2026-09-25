//! ib_async's `Client`: the session under an `IB`.
//!
//! A [`Client`] is a handle to its IB's own session, as `ib.client` is in
//! ib_async. Each request is one engine call of the same name, made in a
//! step of the thread that runs every IB: from another thread it is a
//! command that returns once its step has run. What the engine answers is
//! applied as it is for the IB's own requests; an answer under a number no
//! IB object holds is ignored, as ib_async ignores it.

use std::sync::Arc;
use std::time::Duration;

use crate::contract::{Contract, TagValue};
use crate::engine::{self as e, EClient, EClientConfig, ErrorOrigin, OrderOp, Question};
use crate::error::{Error, Result};
use crate::event::Event;
use crate::ib::defaults::CLIENT_CONNECT_TIMEOUT;
use crate::ib::{ConnectOptions, IBHandle, StartupFetch, clear_volatility};
use crate::objects::{ConnectionStats, ExecutionFilter, ScannerSubscription, WshEventData};
use crate::order::Order;
use crate::owner::{Class, Shared};
use crate::requests::Ask;
use crate::session::{Conn, Logon};
use crate::state::OrderKey;
use crate::util::block_on;

/// A connection's state: ib_async's `Client.DISCONNECTED`, `CONNECTING` and
/// `CONNECTED`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ConnState {
    /// No session, or one closing: `DISCONNECTED`.
    Disconnected = 0,
    /// Logging on: `CONNECTING`.
    Connecting = 1,
    /// The session is up: `CONNECTED`.
    Connected = 2,
}

/// ib_async's `Client`: the requests an IB's session takes, as the TWS API
/// names them, with the request numbers the caller chooses.
///
/// It is a handle to the same session as the [`IBHandle`] it came from, and
/// keeps it open no more than that handle does.
#[derive(Clone)]
pub struct Client {
    ib: IBHandle,
}

impl IBHandle {
    /// The session's client: ib_async's `ib.client`.
    pub fn client(&self) -> Client {
        Client { ib: self.clone() }
    }
}

/// Takes a connect back when its future is dropped before it ends.
struct TakeBack(Option<Logon>);

impl Drop for TakeBack {
    fn drop(&mut self) {
        if let Some(l) = self.0.take() {
            l.take_back();
        }
    }
}

/// What `Client::connect` asks of the IB's logon: no startup sync, and
/// `timeout` as ib_async's, 2 seconds unless given, zero being no limit.
fn options(config: EClientConfig, client_id: i64, timeout: Option<Duration>) -> ConnectOptions {
    ConnectOptions {
        config,
        client_id,
        timeout: Some(timeout.unwrap_or(CLIENT_CONNECT_TIMEOUT)),
        logon_timeout: None,
        account: String::new(),
        raise_sync_errors: false,
        fetch_fields: StartupFetch::NONE,
    }
}

impl Client {
    /// ib_async's `Client.events`, less the throttle events, since the
    /// engine paces its own requests.
    pub const EVENTS: [&'static str; 3] = ["apiStart", "apiEnd", "apiError"];

    fn shared(&self) -> &Arc<Shared> {
        &self.ib.shared
    }

    /// The published session, while the engine has not given it up.
    fn ready(&self) -> Option<Arc<EClient>> {
        self.shared()
            .connected()
            .map(|(_, c)| c)
            .filter(|c| !c.session_over())
    }

    /// Runs `f` with the session in an owner step of `class`; no session is
    /// `NotConnected`, as ib_async's `send` raises.
    fn step(
        &self,
        class: Class,
        f: impl FnOnce(&Arc<Shared>, &EClient) + Send + 'static,
    ) -> Result<()> {
        self.shared().step(class, move |ib| {
            let (_, client) = ib.connected().ok_or(Error::NotConnected)?;
            f(ib, &client);
            Ok(())
        })
    }

    /// A step that hands the engine new work.
    fn request(&self, f: impl FnOnce(&EClient) + Send + 'static) -> Result<()> {
        self.step(Class::Request, move |ib, client| {
            f(client);
            ib.queue.sent(1);
        })
    }

    /// A step that withdraws or sets something.
    fn control(&self, f: impl FnOnce(&EClient) + Send + 'static) -> Result<()> {
        self.step(Class::Control, move |_, client| f(client))
    }

    /// An unnumbered question, asked in its lane behind the IB's own
    /// exchanges of it. A `Client` exchange records nothing: what it
    /// answers reaches the IB's events as unsolicited.
    fn ask(&self, ask: Ask) -> Result<()> {
        self.step(Class::Request, move |ib, client| {
            let send = ib.core().requests.ask(ask, None);
            if let Some(ask) = send {
                ib.send_ask(client, &ask);
            }
        })
    }

    /// A question's cancel: every exchange of it still unsent is withdrawn,
    /// and the lane sends nothing more until the engine confirms the cancel.
    fn cancel_question(
        &self,
        q: Question,
        f: impl FnOnce(&EClient) + Send + 'static,
    ) -> Result<()> {
        self.step(Class::Control, move |ib, client| {
            ib.core().requests.cancel(q);
            f(client);
        })
    }

    // -- The connection ----------------------------------------------------

    /// Logs on and publishes the session, without the IB's startup sync or
    /// its `connected_event`: ib_async's `Client.connect`. `timeout` bounds
    /// the wait for the account's working orders, ib_async's handshake:
    /// `None` is 2 seconds and zero no limit. A peer or internal close of
    /// any IB fails it.
    pub fn connect(
        &self,
        config: EClientConfig,
        client_id: i64,
        timeout: Option<Duration>,
    ) -> Result<()> {
        block_on(self.connect_async(config, client_id, timeout), None)?
    }

    /// `connect`'s async form: ib_async's `Client.connectAsync`. Dropping
    /// the future takes the connect back.
    pub async fn connect_async(
        &self,
        config: EClientConfig,
        client_id: i64,
        timeout: Option<Duration>,
    ) -> Result<()> {
        let opts = options(config, client_id, timeout);
        let (p, logon) = self.ib.begin_connect(opts, false, None)?;
        let mut guard = TakeBack(Some(logon));
        let r = p.await;
        guard.0 = None;
        r
    }

    /// Closes the session and fails its requests with `NotConnected`,
    /// keeping what the IB holds readable until the next session starts,
    /// with no `disconnected_event`: ib_async's `Client.disconnect`. Every
    /// call ends every `run()`.
    pub fn disconnect(&self) {
        let _ = self.shared().step(Class::Control, |ib| {
            ib.client_disconnect();
            Ok(())
        });
    }

    /// Restarts the connection statistics' clock: ib_async's
    /// `Client.reset`. The rest of what ib_async resets is read from the
    /// session here.
    pub fn reset(&self) {
        self.shared().control(|ib| {
            let now = ib.clock.now();
            ib.core().started = now;
        });
    }

    /// Blocks until the next `disconnect()` of the IB: ib_async's
    /// `Client.run`.
    pub fn run(&self) -> Result<()> {
        self.ib.run()
    }

    /// `Client.clientId`: -1 before any connect, then the last connect's.
    pub fn client_id(&self) -> i64 {
        self.ib.client_id()
    }

    /// `Client.connState`. A session closing or given up by the engine, and
    /// a logon taken back, read `Disconnected`: `is_connected()` is whether
    /// this reads `Connected`, as in ib_async.
    pub fn conn_state(&self) -> ConnState {
        if self.ready().is_some() {
            return ConnState::Connected;
        }
        match &self.shared().core().conn {
            Conn::Connecting { logon, .. } if !logon.taken_back() => ConnState::Connecting,
            _ => ConnState::Disconnected,
        }
    }

    /// Whether the session is up: ib_async's `Client.isConnected`.
    pub fn is_connected(&self) -> bool {
        self.ready().is_some()
    }

    /// Whether the session is up: ib_async's `Client.isReady`. The engine's
    /// logon covers the handshake ib_async waits for.
    pub fn is_ready(&self) -> bool {
        self.ready().is_some()
    }

    /// The accounts the login holds: ib_async's `Client.getAccounts`.
    pub fn get_accounts(&self) -> Result<Vec<String>> {
        let client = self.ready().ok_or(Error::NotConnected)?;
        Ok(client.accounts.clone())
    }

    /// The protocol level the session speaks, 0 without one: ib_async's
    /// `Client.serverVersion`.
    pub fn server_version(&self) -> i32 {
        self.shared()
            .connected()
            .and_then(|(_, c)| c.server_version())
            .unwrap_or(0)
    }

    /// What the session has sent and received since it started or was
    /// reset: ib_async's `Client.connectionStats`.
    pub fn connection_stats(&self) -> Result<ConnectionStats> {
        let ib = self.shared();
        let client = self.ready().ok_or(Error::NotConnected)?;
        let started = ib.core().started;
        let duration = ib
            .clock
            .now()
            .saturating_duration_since(started)
            .as_secs_f64();
        let now = ib.clock.wall().as_duration().as_secs_f64();
        let t = client.traffic();
        let n = |v: u64| i64::try_from(v).unwrap_or(i64::MAX);
        Ok(ConnectionStats {
            start_time: now - duration,
            duration,
            num_bytes_recv: n(t.bytes_received),
            num_bytes_sent: n(t.bytes_sent),
            num_msg_recv: n(t.messages_received),
            num_msg_sent: n(t.messages_sent),
        })
    }

    /// A new request number from the IB's own id space, which numbers its
    /// orders and requests too: ib_async's `Client.getReqId`.
    pub fn get_req_id(&self) -> Result<i64> {
        self.shared().step(Class::Control, |ib| {
            let client = ib
                .connected()
                .map(|(_, c)| c)
                .filter(|c| !c.session_over())
                .ok_or(Error::NotConnected)?;
            let floor = client.order_id_floor();
            ib.core().ids.allocate(floor, 1)
        })
    }

    /// Numbers below `min_req_id` are not given from now on: ib_async's
    /// `Client.updateReqId`. A number inside the range the engine keeps for
    /// itself is ignored.
    pub fn update_req_id(&self, min_req_id: i64) {
        self.shared()
            .control(move |ib| ib.core().ids.raise(min_req_id));
    }

    /// `Client.apiStart`: the session is published.
    pub fn api_start(&self) -> &Event<()> {
        &self.shared().events.api_start
    }

    /// `Client.apiEnd`: the peer closed the session.
    pub fn api_end(&self) -> &Event<()> {
        &self.shared().events.api_end
    }

    /// `Client.apiError`: a failed logon, or the peer's close, with its
    /// message.
    pub fn api_error(&self) -> &Event<String> {
        &self.shared().events.api_error
    }

    // -- Market data -------------------------------------------------------

    /// ib_async's `Client.reqMktData`. `mkt_data_options` is taken and not
    /// sent.
    pub fn req_mkt_data(
        &self,
        req_id: i64,
        contract: &Contract,
        generic_tick_list: &str,
        snapshot: bool,
        regulatory_snapshot: bool,
        _mkt_data_options: &[TagValue],
    ) -> Result<()> {
        let c = e::Contract::from(contract);
        let ticks = generic_tick_list.to_owned();
        self.request(move |s| {
            s.req_mkt_data(req_id, &c, &ticks, snapshot, regulatory_snapshot);
        })
    }

    /// ib_async's `Client.cancelMktData`.
    pub fn cancel_mkt_data(&self, req_id: i64) -> Result<()> {
        self.control(move |s| s.cancel_mkt_data(req_id))
    }

    /// ib_async's `Client.reqMarketDataType`.
    pub fn req_market_data_type(&self, market_data_type: i32) -> Result<()> {
        self.control(move |s| s.req_market_data_type(market_data_type))
    }

    /// ib_async's `Client.reqTickByTickData`.
    pub fn req_tick_by_tick_data(
        &self,
        req_id: i64,
        contract: &Contract,
        tick_type: &str,
        number_of_ticks: i32,
        ignore_size: bool,
    ) -> Result<()> {
        let c = e::Contract::from(contract);
        let tick_type = tick_type.to_owned();
        self.request(move |s| {
            s.req_tick_by_tick_data(req_id, &c, &tick_type, number_of_ticks, ignore_size);
        })
    }

    /// ib_async's `Client.cancelTickByTickData`.
    pub fn cancel_tick_by_tick_data(&self, req_id: i64) -> Result<()> {
        self.control(move |s| s.cancel_tick_by_tick_data(req_id))
    }

    /// ib_async's `Client.reqMktDepth`. `mkt_depth_options` is taken and
    /// not sent.
    pub fn req_mkt_depth(
        &self,
        req_id: i64,
        contract: &Contract,
        num_rows: i32,
        is_smart_depth: bool,
        _mkt_depth_options: &[TagValue],
    ) -> Result<()> {
        let c = e::Contract::from(contract);
        self.request(move |s| s.req_mkt_depth(req_id, &c, num_rows, is_smart_depth))
    }

    /// ib_async's `Client.cancelMktDepth`. The engine knows the book by its
    /// number, so `is_smart_depth` is taken and not sent.
    pub fn cancel_mkt_depth(&self, req_id: i64, _is_smart_depth: bool) -> Result<()> {
        self.control(move |s| s.cancel_mkt_depth(req_id))
    }

    /// ib_async's `Client.reqMktDepthExchanges`.
    pub fn req_mkt_depth_exchanges(&self) -> Result<()> {
        self.ask(Ask::MktDepthExchanges)
    }

    /// ib_async's `Client.reqSmartComponents`.
    pub fn req_smart_components(&self, req_id: i64, bbo_exchange: &str) -> Result<()> {
        let bbo = bbo_exchange.to_owned();
        self.request(move |s| s.req_smart_components(req_id, &bbo))
    }

    /// ib_async's `Client.reqRealTimeBars`. `real_time_bars_options` is
    /// taken and not sent.
    pub fn req_real_time_bars(
        &self,
        req_id: i64,
        contract: &Contract,
        bar_size: i32,
        what_to_show: &str,
        use_rth: bool,
        _real_time_bars_options: &[TagValue],
    ) -> Result<()> {
        let c = e::Contract::from(contract);
        let what = what_to_show.to_owned();
        self.request(move |s| s.req_real_time_bars(req_id, &c, bar_size, &what, use_rth))
    }

    /// ib_async's `Client.cancelRealTimeBars`.
    pub fn cancel_real_time_bars(&self, req_id: i64) -> Result<()> {
        self.control(move |s| s.cancel_real_time_bars(req_id))
    }

    /// ib_async's `Client.calculateImpliedVolatility`, answered from the
    /// engine's own model. `impl_vol_options` is taken and not sent.
    pub fn calculate_implied_volatility(
        &self,
        req_id: i64,
        contract: &Contract,
        option_price: f64,
        under_price: f64,
        _impl_vol_options: &[TagValue],
    ) -> Result<()> {
        let c = e::Contract::from(contract);
        self.request(move |s| {
            s.calculate_implied_volatility(req_id, &c, option_price, under_price);
        })
    }

    /// ib_async's `Client.calculateOptionPrice`, answered from the engine's
    /// own model. `opt_prc_options` is taken and not sent.
    pub fn calculate_option_price(
        &self,
        req_id: i64,
        contract: &Contract,
        volatility: f64,
        under_price: f64,
        _opt_prc_options: &[TagValue],
    ) -> Result<()> {
        let c = e::Contract::from(contract);
        self.request(move |s| s.calculate_option_price(req_id, &c, volatility, under_price))
    }

    /// ib_async's `Client.cancelCalculateImpliedVolatility`.
    pub fn cancel_calculate_implied_volatility(&self, req_id: i64) -> Result<()> {
        self.control(move |s| s.cancel_calculate_implied_volatility(req_id))
    }

    /// ib_async's `Client.cancelCalculateOptionPrice`.
    pub fn cancel_calculate_option_price(&self, req_id: i64) -> Result<()> {
        self.control(move |s| s.cancel_calculate_option_price(req_id))
    }

    /// ib_async's `Client.reqMarketRule`.
    pub fn req_market_rule(&self, market_rule_id: i32) -> Result<()> {
        self.ask(Ask::MarketRule(market_rule_id))
    }

    // -- Orders --------------------------------------------------------------

    /// ib_async's `Client.placeOrder`. No trade is made here: the order's
    /// reports make one, as another client's order's do. An order that is not
    /// a volatility order is sent without `volatility`. An order the engine
    /// cannot carry is refused under its number, as a gateway refuses it: as
    /// a refused change when the IB holds it working, else as a refused new
    /// order.
    pub fn place_order(&self, order_id: i64, contract: &Contract, order: &Order) -> Result<()> {
        let c = e::Contract::from(contract);
        let mut order = order.clone();
        clear_volatility(&mut order);
        let order = e::Order::try_from(&order);
        self.step(Class::Request, move |ib, client| match order {
            Ok(o) => {
                client.place_order(order_id, &c, &o);
                ib.queue.sent(1);
            }
            Err(r) => {
                let working = {
                    let core = ib.core();
                    let key = OrderKey::Order {
                        client_id: core.state.client_id,
                        order_id,
                    };
                    core.state
                        .trades
                        .get(&key)
                        .is_some_and(|t| !t.read().is_done())
                };
                let op = if working {
                    OrderOp::Modify
                } else {
                    OrderOp::Place
                };
                client.refuse(ErrorOrigin::Order { id: order_id, op }, r.code, &r.msg);
            }
        })
    }

    /// ib_async's `Client.cancelOrder`. A manual cancel time goes with the
    /// cancel's notice, as the engine states it.
    pub fn cancel_order(&self, order_id: i64, manual_cancel_order_time: &str) -> Result<()> {
        let at = manual_cancel_order_time.to_owned();
        self.control(move |s| s.cancel_order(order_id, at.as_str()))
    }

    /// ib_async's `Client.reqGlobalCancel`.
    pub fn req_global_cancel(&self) -> Result<()> {
        self.control(|s| s.req_global_cancel(""))
    }

    /// ib_async's `Client.reqOpenOrders`: its orders fire `open_order_event`.
    pub fn req_open_orders(&self) -> Result<()> {
        self.ask(Ask::OpenOrders)
    }

    /// ib_async's `Client.reqAllOpenOrders`: its orders fire
    /// `open_order_event`.
    pub fn req_all_open_orders(&self) -> Result<()> {
        self.ask(Ask::AllOpenOrders)
    }

    /// ib_async's `Client.reqAutoOpenOrders`.
    pub fn req_auto_open_orders(&self, b_auto_bind: bool) -> Result<()> {
        self.control(move |s| s.req_auto_open_orders(b_auto_bind))
    }

    /// ib_async's `Client.reqCompletedOrders`.
    pub fn req_completed_orders(&self, api_only: bool) -> Result<()> {
        self.ask(Ask::CompletedOrders { api_only })
    }

    /// ib_async's `Client.reqExecutions`: the fills reach the IB's trades
    /// and events.
    pub fn req_executions(&self, req_id: i64, exec_filter: &ExecutionFilter) -> Result<()> {
        let filter = e::ExecutionFilter::from(exec_filter);
        self.request(move |s| s.req_executions(req_id, &filter))
    }

    /// ib_async's `Client.reqIds`. The session already holds the next id,
    /// which [`Client::get_req_id`] gives, so nothing is sent.
    pub fn req_ids(&self, _num_ids: i32) -> Result<()> {
        self.shared()
            .connected()
            .map(drop)
            .ok_or(Error::NotConnected)
    }

    /// ib_async's `Client.exerciseOptions`, checked as a gateway checks it,
    /// `override_` included.
    pub fn exercise_options(
        &self,
        req_id: i64,
        contract: &Contract,
        exercise_action: i32,
        exercise_quantity: i32,
        account: &str,
        override_: i32,
    ) -> Result<()> {
        let c = e::Contract::from(contract);
        let account = account.to_owned();
        self.request(move |s| {
            s.exercise_options(
                req_id,
                &c,
                exercise_action,
                exercise_quantity,
                &account,
                override_ != 0,
                Default::default(),
            );
        })
    }

    // -- Account -------------------------------------------------------------

    /// ib_async's `Client.reqAccountUpdates`: subscribing asks in the
    /// question's lane; unsubscribing is the question's cancel.
    pub fn req_account_updates(&self, subscribe: bool, acct_code: &str) -> Result<()> {
        let account = acct_code.to_owned();
        if subscribe {
            return self.ask(Ask::AccountUpdates { account });
        }
        self.cancel_question(Question::AccountUpdates, move |s| {
            s.req_account_updates(false, &account);
        })
    }

    /// ib_async's `Client.reqPositions`.
    pub fn req_positions(&self) -> Result<()> {
        self.ask(Ask::Positions)
    }

    /// ib_async's `Client.cancelPositions`: the question's cancel.
    pub fn cancel_positions(&self) -> Result<()> {
        self.cancel_question(Question::Positions, EClient::cancel_positions)
    }

    /// ib_async's `Client.reqManagedAccts`.
    pub fn req_managed_accts(&self) -> Result<()> {
        self.request(EClient::req_managed_accts)
    }

    /// ib_async's `Client.reqAccountSummary`.
    pub fn req_account_summary(&self, req_id: i64, group_name: &str, tags: &str) -> Result<()> {
        let (group, tags) = (group_name.to_owned(), tags.to_owned());
        self.request(move |s| s.req_account_summary(req_id, &group, &tags))
    }

    /// ib_async's `Client.cancelAccountSummary`.
    pub fn cancel_account_summary(&self, req_id: i64) -> Result<()> {
        self.control(move |s| s.cancel_account_summary(req_id))
    }

    /// ib_async's `Client.reqPositionsMulti`.
    pub fn req_positions_multi(&self, req_id: i64, account: &str, model_code: &str) -> Result<()> {
        let (account, model) = (account.to_owned(), model_code.to_owned());
        self.request(move |s| s.req_positions_multi(req_id, &account, &model))
    }

    /// ib_async's `Client.cancelPositionsMulti`.
    pub fn cancel_positions_multi(&self, req_id: i64) -> Result<()> {
        self.control(move |s| s.cancel_positions_multi(req_id))
    }

    /// ib_async's `Client.reqAccountUpdatesMulti`.
    pub fn req_account_updates_multi(
        &self,
        req_id: i64,
        account: &str,
        model_code: &str,
        ledger_and_nlv: bool,
    ) -> Result<()> {
        let (account, model) = (account.to_owned(), model_code.to_owned());
        self.request(move |s| s.req_account_updates_multi(req_id, &account, &model, ledger_and_nlv))
    }

    /// ib_async's `Client.cancelAccountUpdatesMulti`.
    pub fn cancel_account_updates_multi(&self, req_id: i64) -> Result<()> {
        self.control(move |s| s.cancel_account_updates_multi(req_id))
    }

    /// ib_async's `Client.reqPnL`.
    pub fn req_pnl(&self, req_id: i64, account: &str, model_code: &str) -> Result<()> {
        let (account, model) = (account.to_owned(), model_code.to_owned());
        self.request(move |s| s.req_pnl(req_id, &account, &model))
    }

    /// ib_async's `Client.cancelPnL`.
    pub fn cancel_pnl(&self, req_id: i64) -> Result<()> {
        self.control(move |s| s.cancel_pnl(req_id))
    }

    /// ib_async's `Client.reqPnLSingle`.
    pub fn req_pnl_single(
        &self,
        req_id: i64,
        account: &str,
        model_code: &str,
        conid: i64,
    ) -> Result<()> {
        let (account, model) = (account.to_owned(), model_code.to_owned());
        self.request(move |s| s.req_pnl_single(req_id, &account, &model, conid))
    }

    /// ib_async's `Client.cancelPnLSingle`.
    pub fn cancel_pnl_single(&self, req_id: i64) -> Result<()> {
        self.control(move |s| s.cancel_pnl_single(req_id))
    }

    /// ib_async's `Client.reqSoftDollarTiers`.
    pub fn req_soft_dollar_tiers(&self, req_id: i64) -> Result<()> {
        self.request(move |s| s.req_soft_dollar_tiers(req_id))
    }

    /// ib_async's `Client.reqFamilyCodes`.
    pub fn req_family_codes(&self) -> Result<()> {
        self.request(EClient::req_family_codes)
    }

    /// ib_async's `Client.reqUserInfo`.
    pub fn req_user_info(&self, req_id: i64) -> Result<()> {
        self.request(move |s| s.req_user_info(req_id))
    }

    // -- Financial advisors --------------------------------------------------

    /// ib_async's `Client.requestFA`.
    pub fn request_fa(&self, fa_data: i32) -> Result<()> {
        self.ask(Ask::Fa(fa_data))
    }

    /// ib_async's `Client.replaceFA`.
    pub fn replace_fa(&self, req_id: i64, fa_data: i32, cxml: &str) -> Result<()> {
        let cxml = cxml.to_owned();
        self.request(move |s| s.replace_fa(req_id, fa_data, &cxml))
    }

    // -- Reference data and history -----------------------------------------

    /// ib_async's `Client.reqContractDetails`.
    pub fn req_contract_details(&self, req_id: i64, contract: &Contract) -> Result<()> {
        let c = e::Contract::from(contract);
        self.request(move |s| s.req_contract_details(req_id, &c))
    }

    /// ib_async's `Client.reqMatchingSymbols`.
    pub fn req_matching_symbols(&self, req_id: i64, pattern: &str) -> Result<()> {
        let pattern = pattern.to_owned();
        self.request(move |s| s.req_matching_symbols(req_id, &pattern))
    }

    /// ib_async's `Client.reqSecDefOptParams`.
    pub fn req_sec_def_opt_params(
        &self,
        req_id: i64,
        underlying_symbol: &str,
        fut_fop_exchange: &str,
        underlying_sec_type: &str,
        underlying_con_id: i64,
    ) -> Result<()> {
        let symbol = underlying_symbol.to_owned();
        let exchange = fut_fop_exchange.to_owned();
        let sec_type = underlying_sec_type.to_owned();
        self.request(move |s| {
            s.req_sec_def_opt_params(req_id, &symbol, &exchange, &sec_type, underlying_con_id);
        })
    }

    /// ib_async's `Client.reqHistoricalData`. `end_date_time` is sent as
    /// given; `chart_options` is taken and not sent.
    #[expect(clippy::too_many_arguments, reason = "ib_async's parameters")]
    pub fn req_historical_data(
        &self,
        req_id: i64,
        contract: &Contract,
        end_date_time: &str,
        duration_str: &str,
        bar_size_setting: &str,
        what_to_show: &str,
        use_rth: bool,
        format_date: i32,
        keep_up_to_date: bool,
        _chart_options: &[TagValue],
    ) -> Result<()> {
        let c = e::Contract::from(contract);
        let end = end_date_time.to_owned();
        let duration = duration_str.to_owned();
        let bar_size = bar_size_setting.to_owned();
        let what = what_to_show.to_owned();
        self.request(move |s| {
            s.req_historical_data(
                req_id,
                &c,
                &end,
                &duration,
                &bar_size,
                &what,
                use_rth,
                format_date,
                keep_up_to_date,
            );
        })
    }

    /// ib_async's `Client.cancelHistoricalData`.
    pub fn cancel_historical_data(&self, req_id: i64) -> Result<()> {
        self.control(move |s| s.cancel_historical_data(req_id))
    }

    /// ib_async's `Client.reqHistoricalTicks`. The times are sent as given;
    /// `misc_options` is taken and not sent.
    #[expect(clippy::too_many_arguments, reason = "ib_async's parameters")]
    pub fn req_historical_ticks(
        &self,
        req_id: i64,
        contract: &Contract,
        start_date_time: &str,
        end_date_time: &str,
        number_of_ticks: i32,
        what_to_show: &str,
        use_rth: bool,
        ignore_size: bool,
        _misc_options: &[TagValue],
    ) -> Result<()> {
        let c = e::Contract::from(contract);
        let (start, end) = (start_date_time.to_owned(), end_date_time.to_owned());
        let what = what_to_show.to_owned();
        self.request(move |s| {
            s.req_historical_ticks(
                req_id,
                &c,
                &start,
                &end,
                number_of_ticks,
                &what,
                use_rth,
                ignore_size,
            );
        })
    }

    /// ib_async's `Client.reqHeadTimeStamp`.
    pub fn req_head_time_stamp(
        &self,
        req_id: i64,
        contract: &Contract,
        what_to_show: &str,
        use_rth: bool,
        format_date: i32,
    ) -> Result<()> {
        let c = e::Contract::from(contract);
        let what = what_to_show.to_owned();
        self.request(move |s| s.req_head_time_stamp(req_id, &c, &what, use_rth, format_date))
    }

    /// ib_async's `Client.cancelHeadTimeStamp`.
    pub fn cancel_head_time_stamp(&self, req_id: i64) -> Result<()> {
        self.control(move |s| s.cancel_head_time_stamp(req_id))
    }

    /// ib_async's `Client.reqHistogramData`.
    pub fn req_histogram_data(
        &self,
        ticker_id: i64,
        contract: &Contract,
        use_rth: bool,
        time_period: &str,
    ) -> Result<()> {
        let c = e::Contract::from(contract);
        let period = time_period.to_owned();
        self.request(move |s| s.req_histogram_data(ticker_id, &c, use_rth, &period))
    }

    /// ib_async's `Client.cancelHistogramData`.
    pub fn cancel_histogram_data(&self, ticker_id: i64) -> Result<()> {
        self.control(move |s| s.cancel_histogram_data(ticker_id))
    }

    /// ib_async's `Client.reqFundamentalData`. `fundamental_data_options`
    /// is taken and not sent.
    pub fn req_fundamental_data(
        &self,
        req_id: i64,
        contract: &Contract,
        report_type: &str,
        _fundamental_data_options: &[TagValue],
    ) -> Result<()> {
        let c = e::Contract::from(contract);
        let report = report_type.to_owned();
        self.request(move |s| s.req_fundamental_data(req_id, &c, &report))
    }

    /// ib_async's `Client.cancelFundamentalData`.
    pub fn cancel_fundamental_data(&self, req_id: i64) -> Result<()> {
        self.control(move |s| s.cancel_fundamental_data(req_id))
    }

    // -- Scanner -------------------------------------------------------------

    /// ib_async's `Client.reqScannerSubscription`. Each field of
    /// `subscription` set, and each filter option, goes as the scan's
    /// filter; `scanner_subscription_options` is taken and not sent.
    pub fn req_scanner_subscription(
        &self,
        req_id: i64,
        subscription: &ScannerSubscription,
        _scanner_subscription_options: &[TagValue],
        scanner_subscription_filter_options: &[TagValue],
    ) -> Result<()> {
        let (instrument, location, scan_code, rows, filters) =
            crate::convert::scanner_request(subscription, scanner_subscription_filter_options);
        let pairs = subscription.scanner_setting_pairs.clone();
        self.request(move |s| {
            s.req_scanner_subscription(
                req_id,
                &instrument,
                &location,
                &scan_code,
                rows,
                &filters,
                &pairs,
            );
        })
    }

    /// ib_async's `Client.cancelScannerSubscription`.
    pub fn cancel_scanner_subscription(&self, req_id: i64) -> Result<()> {
        self.control(move |s| s.cancel_scanner_subscription(req_id))
    }

    /// ib_async's `Client.reqScannerParameters`.
    pub fn req_scanner_parameters(&self) -> Result<()> {
        self.ask(Ask::ScannerParameters)
    }

    // -- News, bulletins, time -----------------------------------------------

    /// ib_async's `Client.reqNewsBulletins`.
    pub fn req_news_bulletins(&self, all_msgs: bool) -> Result<()> {
        self.request(move |s| s.req_news_bulletins(all_msgs))
    }

    /// ib_async's `Client.cancelNewsBulletins`.
    pub fn cancel_news_bulletins(&self) -> Result<()> {
        self.control(EClient::cancel_news_bulletins)
    }

    /// ib_async's `Client.reqNewsProviders`.
    pub fn req_news_providers(&self) -> Result<()> {
        self.ask(Ask::NewsProviders)
    }

    /// ib_async's `Client.reqNewsArticle`. `news_article_options` is taken
    /// and not sent.
    pub fn req_news_article(
        &self,
        req_id: i64,
        provider_code: &str,
        article_id: &str,
        _news_article_options: &[TagValue],
    ) -> Result<()> {
        let (provider, article) = (provider_code.to_owned(), article_id.to_owned());
        self.request(move |s| s.req_news_article(req_id, &provider, &article))
    }

    /// ib_async's `Client.reqHistoricalNews`. The times are sent as given;
    /// `historical_news_options` is taken and not sent.
    #[expect(clippy::too_many_arguments, reason = "ib_async's parameters")]
    pub fn req_historical_news(
        &self,
        req_id: i64,
        con_id: i64,
        provider_codes: &str,
        start_date_time: &str,
        end_date_time: &str,
        total_results: i32,
        _historical_news_options: &[TagValue],
    ) -> Result<()> {
        let providers = provider_codes.to_owned();
        let (start, end) = (start_date_time.to_owned(), end_date_time.to_owned());
        self.request(move |s| {
            s.req_historical_news(req_id, con_id, &providers, &start, &end, total_results);
        })
    }

    /// ib_async's `Client.reqCurrentTime`.
    pub fn req_current_time(&self) -> Result<()> {
        self.ask(Ask::CurrentTime)
    }

    /// ib_async's `Client.setServerLogLevel`.
    pub fn set_server_log_level(&self, log_level: i32) -> Result<()> {
        self.control(move |s| s.set_server_log_level(log_level))
    }

    // -- Wall Street Horizon -------------------------------------------------

    /// ib_async's `Client.reqWshMetaData`.
    pub fn req_wsh_meta_data(&self, req_id: i64) -> Result<()> {
        self.request(move |s| s.req_wsh_meta_data(req_id))
    }

    /// ib_async's `Client.cancelWshMetaData`.
    pub fn cancel_wsh_meta_data(&self, req_id: i64) -> Result<()> {
        self.control(move |s| s.cancel_wsh_meta_data(req_id))
    }

    /// ib_async's `Client.reqWshEventData`.
    pub fn req_wsh_event_data(&self, req_id: i64, data: &WshEventData) -> Result<()> {
        let query = e::CalendarQuery::from(data);
        self.request(move |s| s.req_wsh_event_data(req_id, query))
    }

    /// ib_async's `Client.cancelWshEventData`.
    pub fn cancel_wsh_event_data(&self, req_id: i64) -> Result<()> {
        self.control(move |s| s.cancel_wsh_event_data(req_id))
    }

    // -- Display groups ------------------------------------------------------

    /// ib_async's `Client.queryDisplayGroups`. ib_async's wrapper has no
    /// handler for the answer.
    pub fn query_display_groups(&self, req_id: i64) -> Result<()> {
        self.request(move |s| s.query_display_groups(req_id))
    }

    /// ib_async's `Client.subscribeToGroupEvents`.
    pub fn subscribe_to_group_events(&self, req_id: i64, group_id: i32) -> Result<()> {
        self.request(move |s| s.subscribe_to_group_events(req_id, group_id))
    }

    /// ib_async's `Client.updateDisplayGroup`.
    pub fn update_display_group(&self, req_id: i64, contract_info: &str) -> Result<()> {
        let info = contract_info.to_owned();
        self.request(move |s| s.update_display_group(req_id, &info))
    }

    /// ib_async's `Client.unsubscribeFromGroupEvents`.
    pub fn unsubscribe_from_group_events(&self, req_id: i64) -> Result<()> {
        self.control(move |s| s.unsubscribe_from_group_events(req_id))
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::{Mutex, mpsc};
    use std::task::{Context, Poll, Waker};
    use std::thread;

    use jiff::Timestamp;
    use jiff::tz::TimeZone;

    use super::*;
    use crate::engine::{ControlCommand, SharedState};
    use crate::event::{lock, set_on_owner};
    use crate::ib::IBConfig;
    use crate::live::Live;
    use crate::objects::IBDefaults;
    use crate::order::{OrderState, OrderStatus, Trade};
    use crate::owner::Via;
    use crate::record::{Callback, Capture};
    use crate::timer::Clock;

    type Log = Arc<Mutex<Vec<String>>>;

    fn note(log: &Log, s: impl Into<String>) {
        lock(log).push(s.into());
    }

    fn seen(log: &Log) -> Vec<String> {
        lock(log).clone()
    }

    /// Marks this test's thread as the owner while it lives, so every step
    /// runs inline.
    struct AsOwner;

    impl AsOwner {
        fn new() -> Self {
            set_on_owner(true);
            AsOwner
        }
    }

    impl Drop for AsOwner {
        fn drop(&mut self) {
            set_on_owner(false);
        }
    }

    /// An IB this thread drives as its owner, and its client.
    fn ib() -> (Arc<Shared>, Client) {
        let ib = Shared::new(
            IBDefaults::default(),
            IBConfig::default(),
            Clock::manual(Timestamp::UNIX_EPOCH),
        );
        ib.connect_internal_slots();
        let client = IBHandle { shared: ib.clone() }.client();
        (ib, client)
    }

    /// Connects as `Client::connect` asks, on an engine session whose loop
    /// never runs; gives the channel its commands arrive on.
    fn connect(ib: &Arc<Shared>, client: &Client) -> mpsc::Receiver<ControlCommand> {
        let (tx, rx) = mpsc::channel();
        let engine = EClient::from_parts(
            Arc::new(SharedState::new()),
            tx,
            thread::spawn(|| {}),
            "DU123".into(),
        );
        let via = Via::Test(Some(Arc::new(engine)));
        let opts = options(EClientConfig::default(), 1, None);
        let (mut p, _) = client.ib.begin_connect(opts, false, Some(via)).unwrap();
        ib.lap(&mut Capture::new(TimeZone::UTC));
        let r = Pin::new(&mut p).poll(&mut Context::from_waker(Waker::noop()));
        assert!(matches!(r, Poll::Ready(Ok(()))));
        rx
    }

    /// The commands the engine has been handed since the last look.
    fn sent(rx: &mpsc::Receiver<ControlCommand>) -> Vec<String> {
        rx.try_iter().map(|c| format!("{c:?}")).collect()
    }

    #[test]
    fn a_client_session_reads_the_engine_and_its_close_keeps_the_ibs_state() {
        let _o = AsOwner::new();
        let (ib, client) = ib();
        let log = Log::default();
        for (event, name) in [
            (&ib.events.api_start, "apiStart"),
            (&ib.events.api_end, "apiEnd"),
            (&ib.events.connected_event, "connected"),
            (&ib.events.disconnected_event, "disconnected"),
        ] {
            let l = log.clone();
            event.connect(move |()| note(&l, name));
        }
        assert_eq!(client.client_id(), -1);
        assert!(matches!(client.get_req_id(), Err(Error::NotConnected)));

        // Published with no startup sync and no connected_event.
        let rx = connect(&ib, &client);
        assert_eq!(seen(&log), ["apiStart"]);
        assert!(sent(&rx).is_empty());
        assert_eq!(client.conn_state(), ConnState::Connected);
        assert!(client.is_connected() && client.is_ready());
        assert_eq!(client.client_id(), 1);
        assert_eq!(client.get_accounts().unwrap(), ["DU123"]);
        assert_ne!(client.server_version(), 0);

        // The statistics' clock runs from publication, and from a reset.
        ib.clock.advance(Duration::from_secs(5));
        let s = client.connection_stats().unwrap();
        assert_eq!((s.start_time, s.duration), (0.0, 5.0));
        client.reset();
        let s = client.connection_stats().unwrap();
        assert_eq!((s.start_time, s.duration), (5.0, 0.0));

        // One id space with the IB's own orders and requests.
        client.update_req_id(1000);
        assert_eq!(client.get_req_id().unwrap(), 1000);
        assert_eq!(ib.core().ids.allocate(0, 1).unwrap(), 1001);

        // The close: no disconnected_event, the IB's state kept, run() ended.
        let trade = Live::new(Trade::default());
        ib.core().state.trades.insert(OrderKey::Perm(1), trade);
        let stops = ib.progress(|p| p.stops);
        client.disconnect();
        assert_eq!(seen(&log), ["apiStart"]);
        assert_eq!(ib.core().state.trades.len(), 1);
        assert_eq!(ib.progress(|p| p.stops), stops + 1);
        assert_eq!(client.conn_state(), ConnState::Disconnected);
        assert!(!client.is_ready());
        assert_eq!(client.server_version(), 0);
        assert_eq!(client.client_id(), 1);
        assert!(matches!(client.get_accounts(), Err(Error::NotConnected)));
        assert!(matches!(
            client.connection_stats(),
            Err(Error::NotConnected)
        ));
        assert!(matches!(client.get_req_id(), Err(Error::NotConnected)));
        assert!(matches!(client.req_positions(), Err(Error::NotConnected)));
    }

    type Call = fn(&Client) -> Result<()>;

    #[test]
    fn a_client_question_waits_in_its_lane_and_behind_its_cancel() {
        let _o = AsOwner::new();
        let cases: [(Call, Call, Question, &str, &str); 2] = [
            (
                Client::req_positions,
                Client::cancel_positions,
                Question::Positions,
                "Ask(Positions)",
                "Retire(Question(Positions))",
            ),
            (
                |c| c.req_account_updates(true, "DU123"),
                |c| c.req_account_updates(false, "DU123"),
                Question::AccountUpdates,
                r#"Ask(AccountUpdates { account: "DU123" })"#,
                "Retire(Question(AccountUpdates))",
            ),
        ];
        for (ask, cancel, q, asked, retired) in cases {
            let (ib, client) = ib();
            let rx = connect(&ib, &client);
            ask(&client).unwrap();
            ask(&client).unwrap();
            assert_eq!(sent(&rx), [asked], "the second waits in the lane");
            cancel(&client).unwrap();
            assert_eq!(sent(&rx), [retired]);
            assert_eq!(ib.core().requests.unsent(), 0, "the waiting one withdrawn");
            ask(&client).unwrap();
            assert!(sent(&rx).is_empty(), "held until the cancel is confirmed");
            ib.apply_read(1, vec![Callback::QuestionRetired(q)]);
            assert_eq!(sent(&rx), [asked]);
        }

        // The client's open-orders exchange records nothing: its orders are
        // the IB's unsolicited ones.
        let (ib, client) = ib();
        let _rx = connect(&ib, &client);
        let log = Log::default();
        let l = log.clone();
        ib.events
            .open_order_event
            .connect(move |t| note(&l, format!("open {}", t.read().order.read().order_id)));
        client.req_open_orders().unwrap();
        let open = Callback::OpenOrder {
            order_id: 1,
            contract: Contract::default(),
            order: Order {
                order_id: 1,
                client_id: 1,
                perm_id: 901,
                ..Order::default()
            },
            order_state: OrderState::default(),
        };
        ib.apply_read(1, vec![open, Callback::OpenOrderEnd]);
        assert_eq!(seen(&log), ["open 1"]);
    }

    #[test]
    fn an_order_that_is_not_a_volatility_order_is_sent_without_volatility() {
        let _o = AsOwner::new();
        let (ib, client) = ib();
        let rx = connect(&ib, &client);
        let order = Order {
            volatility: Some(0.2),
            ..Order::limit("BUY", 1.0, 1.0)
        };
        client
            .place_order(7, &Contract::stock("AAPL", "SMART", "USD"), &order)
            .unwrap();
        // The engine's unset volatility is `f64::MAX`.
        let placed: Vec<f64> = rx
            .try_iter()
            .filter_map(|c| match c {
                ControlCommand::Place(p) => Some(p.order.volatility),
                _ => None,
            })
            .collect();
        assert_eq!(placed, [f64::MAX]);
    }

    #[test]
    fn an_order_the_engine_cannot_carry_is_refused_under_its_number() {
        let _o = AsOwner::new();
        let uncarried = Order {
            client_id: i64::MAX,
            ..Order::default()
        };
        // Whether the IB works the order: a refused change leaves it live,
        // and a refused new order registers no trade.
        for (working, status) in [(false, None), (true, Some("ValidationError"))] {
            let (ib, client) = ib();
            let _rx = connect(&ib, &client);
            let log = Log::default();
            let l = log.clone();
            ib.events
                .error_event
                .connect(move |e| note(&l, format!("error {} {}", e.0, e.1)));
            let trade = Live::new(Trade {
                order_status: OrderStatus {
                    order_id: 7,
                    status: OrderStatus::SUBMITTED.into(),
                    ..OrderStatus::default()
                },
                ..Trade::default()
            });
            if working {
                let key = OrderKey::Order {
                    client_id: 1,
                    order_id: 7,
                };
                ib.core().state.trades.insert(key, trade.clone());
            }
            client
                .place_order(7, &Contract::default(), &uncarried)
                .unwrap();
            ib.lap(&mut Capture::new(TimeZone::UTC));
            assert_eq!(seen(&log), ["error 7 320"], "working {working}");
            let trades = ib.core().state.trades.len();
            assert_eq!(trades, usize::from(working));
            if let Some(status) = status {
                assert_eq!(trade.read().order_status.status, status);
            }
        }
    }
}

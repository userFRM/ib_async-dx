//! The engine's callbacks, recorded as this crate's values for the owner to
//! apply.
//!
//! `Capture` is the `Wrapper` a read hands the engine. Each callback becomes
//! one `Callback`, in the order the engine delivers them, carrying what
//! ib_async's wrapper method receives: the engine's values converted to this
//! crate's, and each value ib_async builds from the arguments alone built
//! here. A callback whose value does not convert is dropped and logged, as
//! ib_async's decoder drops a message it cannot read.
//!
//! The filter by origin: an error the engine raised for a lookup of its own
//! answers nothing this crate asked, and is dropped here. Every other error is
//! kept with its origin, whatever its number.

use jiff::tz::TimeZone;
use jiff::{Timestamp, Zoned};

use crate::contract::{Contract, ContractDescription, ContractDetails, ScanData};
use crate::convert;
use crate::engine::{self as e, ErrorOrigin, Question};
use crate::error::{Error, Result};
use crate::objects::{
    AccountValue, BarData, CommissionReport, DepthMktDataDescription, EfpData, Execution,
    HistogramData, HistoricalNews, HistoricalSchedule, HistoricalSession, HistoricalTickAny,
    NewsArticle, NewsBulletin, NewsProvider, NewsTick, OptionChain, OptionComputation,
    PortfolioItem, Position, PriceIncrement, RealTimeBar, SmartComponent, TickAttribBidAsk,
    TickAttribLast,
};
use crate::order::{Order, OrderState};

/// One engine callback, as ib_async's wrapper method of the same name
/// receives it. A request's number is `req_id`, an order's `order_id`.
#[derive(Clone, Debug)]
pub(crate) enum Callback {
    // The session.
    ConnectionClosed,
    ManagedAccounts(Vec<String>),
    Error {
        origin: ErrorOrigin,
        code: i64,
        message: String,
        advanced_order_reject_json: String,
    },
    CurrentTime(Zoned),
    CurrentTimeInMillis(i64),
    /// A question's cancel, confirmed after every callback of the exchange
    /// it ends. A gateway sends nothing for it.
    QuestionRetired(Question),

    // Quotes and streams.
    TickPrice {
        req_id: i64,
        tick_type: i32,
        price: f64,
    },
    TickSize {
        req_id: i64,
        tick_type: i32,
        size: f64,
    },
    TickString {
        req_id: i64,
        tick_type: i32,
        value: String,
    },
    TickGeneric {
        req_id: i64,
        tick_type: i32,
        value: f64,
    },
    TickSnapshotEnd(i64),
    MarketDataType {
        req_id: i64,
        market_data_type: i32,
    },
    TickReqParams {
        req_id: i64,
        min_tick: f64,
        bbo_exchange: String,
        snapshot_permissions: i64,
    },
    TickOptionComputation {
        req_id: i64,
        tick_type: i32,
        computation: OptionComputation,
    },
    TickEfp {
        req_id: i64,
        tick_type: i32,
        efp: EfpData,
    },
    TickNews {
        news: NewsTick,
    },
    TickByTickAllLast {
        req_id: i64,
        tick_type: i32,
        price: f64,
        size: f64,
        attrib: TickAttribLast,
        exchange: String,
        special_conditions: String,
    },
    TickByTickBidAsk {
        req_id: i64,
        bid_price: f64,
        ask_price: f64,
        bid_size: f64,
        ask_size: f64,
        attrib: TickAttribBidAsk,
    },
    TickByTickMidPoint {
        req_id: i64,
        mid_point: f64,
    },
    UpdateMktDepth {
        req_id: i64,
        position: i32,
        operation: i32,
        side: i32,
        price: f64,
        size: f64,
    },
    UpdateMktDepthL2 {
        req_id: i64,
        position: i32,
        market_maker: String,
        operation: i32,
        side: i32,
        price: f64,
        size: f64,
    },
    MktDepthExchanges(Vec<DepthMktDataDescription>),
    SmartComponents {
        req_id: i64,
        components: Vec<SmartComponent>,
    },
    RealtimeBar {
        req_id: i64,
        bar: RealTimeBar,
    },

    // Orders.
    OrderStatus {
        order_id: i64,
        status: String,
        filled: f64,
        remaining: f64,
        avg_fill_price: f64,
        perm_id: i64,
        parent_id: i64,
        last_fill_price: f64,
        client_id: i64,
        why_held: String,
        mkt_cap_price: f64,
    },
    OpenOrder {
        order_id: i64,
        contract: Contract,
        order: Order,
        order_state: OrderState,
    },
    OpenOrderEnd,
    CompletedOrder {
        contract: Contract,
        order: Order,
        order_state: OrderState,
    },
    CompletedOrdersEnd,
    ExecDetails {
        req_id: i64,
        contract: Contract,
        execution: Execution,
    },
    ExecDetailsEnd(i64),
    CommissionReport(CommissionReport),

    // The account.
    UpdateAccountValue(AccountValue),
    UpdatePortfolio(PortfolioItem),
    AccountDownloadEnd,
    AccountSummary {
        value: AccountValue,
    },
    AccountSummaryEnd(i64),
    AccountUpdateMulti {
        value: AccountValue,
    },
    AccountUpdateMultiEnd(i64),
    Position(Position),
    PositionEnd,
    Pnl {
        req_id: i64,
        daily_pnl: f64,
        unrealized_pnl: f64,
        realized_pnl: f64,
    },
    PnlSingle {
        req_id: i64,
        pos: f64,
        daily_pnl: f64,
        unrealized_pnl: f64,
        realized_pnl: f64,
        value: f64,
    },

    // History and reference data.
    HistoricalData {
        req_id: i64,
        bar: BarData,
    },
    HistoricalDataEnd {
        req_id: i64,
    },
    HistoricalDataUpdate {
        req_id: i64,
        bar: BarData,
    },
    HeadTimestamp {
        req_id: i64,
        head_timestamp: String,
    },
    HistoricalTicks {
        req_id: i64,
        ticks: Vec<HistoricalTickAny>,
        done: bool,
    },
    HistoricalSchedule {
        req_id: i64,
        schedule: HistoricalSchedule,
    },
    HistogramData {
        req_id: i64,
        items: Vec<HistogramData>,
    },
    ContractDetails {
        req_id: i64,
        details: ContractDetails,
    },
    ContractDetailsEnd(i64),
    SymbolSamples {
        req_id: i64,
        descriptions: Vec<ContractDescription>,
    },
    SecurityDefinitionOptionParameter {
        req_id: i64,
        chain: OptionChain,
    },
    SecurityDefinitionOptionParameterEnd(i64),
    MarketRule {
        market_rule_id: i64,
        price_increments: Vec<PriceIncrement>,
    },
    FundamentalData {
        req_id: i64,
        data: String,
    },
    ScannerParameters(String),
    ScannerData {
        req_id: i64,
        data: ScanData,
    },
    ScannerDataEnd(i64),

    // News.
    NewsProviders(Vec<NewsProvider>),
    NewsArticle {
        req_id: i64,
        article: NewsArticle,
    },
    HistoricalNews {
        req_id: i64,
        news: HistoricalNews,
    },
    HistoricalNewsEnd {
        req_id: i64,
    },
    UpdateNewsBulletin(NewsBulletin),

    // The rest.
    ReceiveFa {
        xml: String,
    },
    WshMetaData {
        req_id: i64,
        data_json: String,
    },
    WshEventData {
        req_id: i64,
        data_json: String,
    },
    UserInfo {
        req_id: i64,
        white_branding_id: String,
    },
}

/// The `Wrapper` a read hands the engine: it records each callback as a
/// `Callback`, for the owner to take once the read returns.
pub(crate) struct Capture {
    /// The IB's `IBDefaults.timezone`: where times ib_async stamps with
    /// `defaultTimezone` are put.
    timezone: TimeZone,
    /// `IBConfig.timezone_tws` as the owner last read it: where a naive
    /// execution time is placed.
    pub(crate) timezone_tws: Option<TimeZone>,
    callbacks: Vec<Callback>,
}

impl Capture {
    pub(crate) fn new(timezone: TimeZone) -> Self {
        Capture {
            timezone,
            timezone_tws: None,
            callbacks: Vec::new(),
        }
    }

    /// The callbacks recorded since the last take, in the engine's order.
    pub(crate) fn take(&mut self) -> Vec<Callback> {
        std::mem::take(&mut self.callbacks)
    }

    fn push(&mut self, callback: Callback) {
        self.callbacks.push(callback);
    }

    /// Records a callback whose value converts, and drops one whose value
    /// does not, logging which callback and why, as ib_async's decoder logs
    /// a message it cannot read and goes on.
    fn push_converted(&mut self, name: &str, callback: Result<Callback>) {
        match callback {
            Ok(callback) => self.push(callback),
            Err(why) => log::error!(target: "ib_async.Decoder", "Error for {name}: {why}"),
        }
    }

    /// ib_async's three historical-tick handlers are one:
    /// `_results[reqId] += ticks`, and the end when `done`.
    fn ticks(&mut self, name: &str, req_id: i64, ticks: &e::HistoricalTickData, done: bool) {
        let callback = convert::historical_ticks(ticks, &self.timezone).map(|ticks| {
            Callback::HistoricalTicks {
                req_id,
                ticks,
                done,
            }
        });
        self.push_converted(name, callback);
    }

    /// `datetime.fromtimestamp(secs, defaultTimezone)`.
    fn at(&self, field: &str, secs: i64) -> Result<Zoned> {
        Timestamp::from_second(secs)
            .map(|t| t.to_zoned(self.timezone.clone()))
            .map_err(|why| Error::Value(format!("{field} {secs}: {why}")))
    }
}

fn ours<'a, E: 'a, T: From<&'a E>>(v: &'a [E]) -> Vec<T> {
    v.iter().map(T::from).collect()
}

impl e::Wrapper for Capture {
    fn connection_closed(&mut self) {
        self.push(Callback::ConnectionClosed);
    }

    fn managed_accounts(&mut self, accounts_list: &str) {
        let accounts = accounts_list.split(',').filter(|a| !a.is_empty());
        self.push(Callback::ManagedAccounts(
            accounts.map(str::to_owned).collect(),
        ));
    }

    fn error_from(
        &mut self,
        origin: ErrorOrigin,
        error_code: i64,
        error_string: &str,
        advanced_order_reject_json: &str,
    ) {
        if let ErrorOrigin::Internal(id) = origin {
            log::debug!(
                target: "ib_async.wrapper",
                "Dropped error {error_code} of the engine's own lookup {id}: {error_string}"
            );
            return;
        }
        self.push(Callback::Error {
            origin,
            code: error_code,
            message: error_string.to_owned(),
            advanced_order_reject_json: advanced_order_reject_json.to_owned(),
        });
    }

    fn current_time(&mut self, time: i64) {
        let callback = self.at("currentTime.time", time).map(Callback::CurrentTime);
        self.push_converted("currentTime", callback);
    }

    fn current_time_in_millis(&mut self, time_in_millis: i64) {
        self.push(Callback::CurrentTimeInMillis(time_in_millis));
    }

    fn question_retired(&mut self, q: Question) {
        self.push(Callback::QuestionRetired(q));
    }

    fn tick_price(&mut self, req_id: i64, tick_type: i32, price: f64, _attrib: &e::TickAttrib) {
        // ib_async's decoder passes no attributes on.
        self.push(Callback::TickPrice {
            req_id,
            tick_type,
            price,
        });
    }

    fn tick_size(&mut self, req_id: i64, tick_type: i32, size: f64) {
        self.push(Callback::TickSize {
            req_id,
            tick_type,
            size,
        });
    }

    fn tick_string(&mut self, req_id: i64, tick_type: i32, value: &str) {
        self.push(Callback::TickString {
            req_id,
            tick_type,
            value: value.to_owned(),
        });
    }

    fn tick_generic(&mut self, req_id: i64, tick_type: i32, value: f64) {
        self.push(Callback::TickGeneric {
            req_id,
            tick_type,
            value,
        });
    }

    fn tick_snapshot_end(&mut self, req_id: i64) {
        self.push(Callback::TickSnapshotEnd(req_id));
    }

    fn market_data_type(&mut self, req_id: i64, market_data_type: i32) {
        self.push(Callback::MarketDataType {
            req_id,
            market_data_type,
        });
    }

    fn tick_req_params(
        &mut self,
        ticker_id: i64,
        min_tick: f64,
        bbo_exchange: &str,
        snapshot_permissions: i64,
    ) {
        self.push(Callback::TickReqParams {
            req_id: ticker_id,
            min_tick,
            bbo_exchange: bbo_exchange.to_owned(),
            snapshot_permissions,
        });
    }

    fn tick_option_computation(
        &mut self,
        req_id: i64,
        tick_type: i32,
        tick_attrib: i32,
        implied_vol: f64,
        delta: f64,
        opt_price: f64,
        pv_dividend: f64,
        gamma: f64,
        vega: f64,
        theta: f64,
        und_price: f64,
    ) {
        let figures = [
            implied_vol,
            delta,
            opt_price,
            pv_dividend,
            gamma,
            vega,
            theta,
            und_price,
        ];
        self.push(Callback::TickOptionComputation {
            req_id,
            tick_type,
            computation: convert::option_computation(tick_attrib, figures),
        });
    }

    fn tick_efp(
        &mut self,
        req_id: i64,
        tick_type: i32,
        basis_points: f64,
        formatted_basis_points: &str,
        implied_future: f64,
        hold_days: i32,
        future_last_trade_date: &str,
        dividend_impact: f64,
        dividends_to_last_trade_date: f64,
    ) {
        self.push(Callback::TickEfp {
            req_id,
            tick_type,
            efp: EfpData {
                basis_points,
                formatted_basis_points: formatted_basis_points.to_owned(),
                implied_future,
                hold_days,
                future_last_trade_date: future_last_trade_date.to_owned(),
                dividend_impact,
                dividends_to_last_trade_date,
            },
        });
    }

    fn tick_news(
        &mut self,
        _ticker_id: i64,
        timestamp: i64,
        provider_code: &str,
        article_id: &str,
        headline: &str,
        extra_data: &str,
    ) {
        self.push(Callback::TickNews {
            news: NewsTick {
                time_stamp: timestamp,
                provider_code: provider_code.to_owned(),
                article_id: article_id.to_owned(),
                headline: headline.to_owned(),
                extra_data: extra_data.to_owned(),
            },
        });
    }

    fn tick_by_tick_all_last(
        &mut self,
        req_id: i64,
        tick_type: i32,
        _time: i64,
        price: f64,
        size: f64,
        attrib: &e::TickAttribLast,
        exchange: &str,
        special_conditions: &str,
    ) {
        self.push(Callback::TickByTickAllLast {
            req_id,
            tick_type,
            price,
            size,
            attrib: attrib.into(),
            exchange: exchange.to_owned(),
            special_conditions: special_conditions.to_owned(),
        });
    }

    fn tick_by_tick_bid_ask(
        &mut self,
        req_id: i64,
        _time: i64,
        bid_price: f64,
        ask_price: f64,
        bid_size: f64,
        ask_size: f64,
        attrib: &e::TickAttribBidAsk,
    ) {
        self.push(Callback::TickByTickBidAsk {
            req_id,
            bid_price,
            ask_price,
            bid_size,
            ask_size,
            attrib: attrib.into(),
        });
    }

    fn tick_by_tick_mid_point(&mut self, req_id: i64, _time: i64, mid_point: f64) {
        self.push(Callback::TickByTickMidPoint { req_id, mid_point });
    }

    fn update_mkt_depth(
        &mut self,
        req_id: i64,
        position: i32,
        operation: i32,
        side: i32,
        price: f64,
        size: f64,
    ) {
        self.push(Callback::UpdateMktDepth {
            req_id,
            position,
            operation,
            side,
            price,
            size,
        });
    }

    fn update_mkt_depth_l2(
        &mut self,
        req_id: i64,
        position: i32,
        market_maker: &str,
        operation: i32,
        side: i32,
        price: f64,
        size: f64,
        _is_smart_depth: bool,
    ) {
        self.push(Callback::UpdateMktDepthL2 {
            req_id,
            position,
            market_maker: market_maker.to_owned(),
            operation,
            side,
            price,
            size,
        });
    }

    fn mkt_depth_exchanges(&mut self, descriptions: &[e::DepthMktDataDescription]) {
        self.push(Callback::MktDepthExchanges(ours(descriptions)));
    }

    fn smart_components(&mut self, req_id: i64, components: &[e::SmartComponent]) {
        self.push(Callback::SmartComponents {
            req_id,
            components: ours(components),
        });
    }

    fn real_time_bar(
        &mut self,
        req_id: i64,
        date: i64,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        volume: f64,
        wap: f64,
        count: i32,
    ) {
        let bar = self.at("RealTimeBar.time", date).map(|time| RealTimeBar {
            time,
            end_time: -1,
            open_: open,
            high,
            low,
            close,
            volume,
            wap,
            count,
        });
        let callback = bar.map(|bar| Callback::RealtimeBar { req_id, bar });
        self.push_converted("realtimeBar", callback);
    }

    fn order_status(
        &mut self,
        order_id: i64,
        status: &str,
        filled: f64,
        remaining: f64,
        avg_fill_price: f64,
        perm_id: i64,
        parent_id: i64,
        last_fill_price: f64,
        client_id: i64,
        why_held: &str,
        mkt_cap_price: f64,
    ) {
        self.push(Callback::OrderStatus {
            order_id,
            status: status.to_owned(),
            filled,
            remaining,
            avg_fill_price,
            perm_id,
            parent_id,
            last_fill_price,
            client_id,
            why_held: why_held.to_owned(),
            mkt_cap_price,
        });
    }

    fn open_order(
        &mut self,
        order_id: i64,
        contract: &e::Contract,
        order: &e::Order,
        order_state: &e::OrderState,
    ) {
        self.push(Callback::OpenOrder {
            order_id,
            contract: contract.into(),
            order: order.into(),
            order_state: order_state.into(),
        });
    }

    fn open_order_end(&mut self) {
        self.push(Callback::OpenOrderEnd);
    }

    fn completed_order(
        &mut self,
        contract: &e::Contract,
        order: &e::Order,
        order_state: &e::OrderState,
    ) {
        self.push(Callback::CompletedOrder {
            contract: contract.into(),
            order: order.into(),
            order_state: order_state.into(),
        });
    }

    fn completed_orders_end(&mut self) {
        self.push(Callback::CompletedOrdersEnd);
    }

    fn exec_details(&mut self, req_id: i64, contract: &e::Contract, execution: &e::Execution) {
        let tws = self.timezone_tws.as_ref();
        let callback = convert::execution(execution, &self.timezone, tws).map(|execution| {
            Callback::ExecDetails {
                req_id,
                contract: contract.into(),
                execution,
            }
        });
        self.push_converted("execDetails", callback);
    }

    fn exec_details_end(&mut self, req_id: i64) {
        self.push(Callback::ExecDetailsEnd(req_id));
    }

    fn commission_and_fees_report(&mut self, report: &e::CommissionAndFeesReport) {
        let callback = CommissionReport::try_from(report).map(Callback::CommissionReport);
        self.push_converted("commissionReport", callback);
    }

    fn update_account_value(&mut self, key: &str, value: &str, currency: &str, account_name: &str) {
        self.push(Callback::UpdateAccountValue(AccountValue {
            account: account_name.to_owned(),
            tag: key.to_owned(),
            value: value.to_owned(),
            currency: currency.to_owned(),
            model_code: String::new(),
        }));
    }

    fn update_portfolio(
        &mut self,
        contract: &e::Contract,
        position: f64,
        market_price: f64,
        market_value: f64,
        average_cost: f64,
        unrealized_pnl: f64,
        realized_pnl: f64,
        account_name: &str,
    ) {
        self.push(Callback::UpdatePortfolio(PortfolioItem {
            contract: contract.into(),
            position,
            market_price,
            market_value,
            average_cost,
            unrealized_pnl,
            realized_pnl,
            account: account_name.to_owned(),
        }));
    }

    fn account_download_end(&mut self, _account: &str) {
        self.push(Callback::AccountDownloadEnd);
    }

    fn account_summary(
        &mut self,
        _req_id: i64,
        account: &str,
        tag: &str,
        value: &str,
        currency: &str,
    ) {
        self.push(Callback::AccountSummary {
            value: AccountValue {
                account: account.to_owned(),
                tag: tag.to_owned(),
                value: value.to_owned(),
                currency: currency.to_owned(),
                model_code: String::new(),
            },
        });
    }

    fn account_summary_end(&mut self, req_id: i64) {
        self.push(Callback::AccountSummaryEnd(req_id));
    }

    fn account_update_multi(
        &mut self,
        _req_id: i64,
        account: &str,
        model_code: &str,
        key: &str,
        value: &str,
        currency: &str,
    ) {
        self.push(Callback::AccountUpdateMulti {
            value: AccountValue {
                account: account.to_owned(),
                tag: key.to_owned(),
                value: value.to_owned(),
                currency: currency.to_owned(),
                model_code: model_code.to_owned(),
            },
        });
    }

    fn account_update_multi_end(&mut self, req_id: i64) {
        self.push(Callback::AccountUpdateMultiEnd(req_id));
    }

    fn position(&mut self, account: &str, contract: &e::Contract, pos: f64, avg_cost: f64) {
        self.push(Callback::Position(Position {
            account: account.to_owned(),
            contract: contract.into(),
            position: pos,
            avg_cost,
        }));
    }

    fn position_end(&mut self) {
        self.push(Callback::PositionEnd);
    }

    fn pnl(&mut self, req_id: i64, daily_pnl: f64, unrealized_pnl: f64, realized_pnl: f64) {
        self.push(Callback::Pnl {
            req_id,
            daily_pnl,
            unrealized_pnl,
            realized_pnl,
        });
    }

    fn pnl_single(
        &mut self,
        req_id: i64,
        pos: f64,
        daily_pnl: f64,
        unrealized_pnl: f64,
        realized_pnl: f64,
        value: f64,
    ) {
        self.push(Callback::PnlSingle {
            req_id,
            pos,
            daily_pnl,
            unrealized_pnl,
            realized_pnl,
            value,
        });
    }

    fn historical_data(&mut self, req_id: i64, bar: &e::BarData) {
        let callback = BarData::try_from(bar).map(|bar| Callback::HistoricalData { req_id, bar });
        self.push_converted("historicalData", callback);
    }

    fn historical_data_end(&mut self, req_id: i64, _start: &str, _end: &str) {
        self.push(Callback::HistoricalDataEnd { req_id });
    }

    fn historical_data_update(&mut self, req_id: i64, bar: &e::BarData) {
        let callback =
            BarData::try_from(bar).map(|bar| Callback::HistoricalDataUpdate { req_id, bar });
        self.push_converted("historicalDataUpdate", callback);
    }

    fn head_timestamp(&mut self, req_id: i64, head_timestamp: &str) {
        self.push(Callback::HeadTimestamp {
            req_id,
            head_timestamp: head_timestamp.to_owned(),
        });
    }

    fn historical_ticks(&mut self, req_id: i64, ticks: &e::HistoricalTickData, done: bool) {
        self.ticks("historicalTicks", req_id, ticks, done);
    }

    fn historical_ticks_bid_ask(&mut self, req_id: i64, ticks: &e::HistoricalTickData, done: bool) {
        self.ticks("historicalTicksBidAsk", req_id, ticks, done);
    }

    fn historical_ticks_last(&mut self, req_id: i64, ticks: &e::HistoricalTickData, done: bool) {
        self.ticks("historicalTicksLast", req_id, ticks, done);
    }

    fn historical_schedule(
        &mut self,
        req_id: i64,
        start_date_time: &str,
        end_date_time: &str,
        time_zone: &str,
        sessions: &[(String, String, String)],
    ) {
        let sessions = sessions
            .iter()
            .map(|(start, end, ref_date)| HistoricalSession {
                start_date_time: start.clone(),
                end_date_time: end.clone(),
                ref_date: ref_date.clone(),
            });
        self.push(Callback::HistoricalSchedule {
            req_id,
            schedule: HistoricalSchedule {
                start_date_time: start_date_time.to_owned(),
                end_date_time: end_date_time.to_owned(),
                time_zone: time_zone.to_owned(),
                sessions: sessions.collect(),
            },
        });
    }

    fn histogram_data(&mut self, req_id: i64, items: &[(f64, i64)]) {
        let items = items
            .iter()
            .map(|&(price, count)| HistogramData { price, count });
        self.push(Callback::HistogramData {
            req_id,
            items: items.collect(),
        });
    }

    fn contract_details(&mut self, req_id: i64, details: &e::ContractDetails) {
        self.push(Callback::ContractDetails {
            req_id,
            details: details.into(),
        });
    }

    /// ib_async's `bondContractDetails` is its `contractDetails`.
    fn bond_contract_details(&mut self, req_id: i64, details: &e::ContractDetails) {
        self.contract_details(req_id, details);
    }

    fn contract_details_end(&mut self, req_id: i64) {
        self.push(Callback::ContractDetailsEnd(req_id));
    }

    fn symbol_samples(&mut self, req_id: i64, descriptions: &[e::ContractDescription]) {
        self.push(Callback::SymbolSamples {
            req_id,
            descriptions: ours(descriptions),
        });
    }

    fn security_definition_option_parameter(
        &mut self,
        req_id: i64,
        exchange: &str,
        underlying_con_id: i64,
        trading_class: &str,
        multiplier: &str,
        expirations: &[String],
        strikes: &[f64],
    ) {
        self.push(Callback::SecurityDefinitionOptionParameter {
            req_id,
            chain: OptionChain {
                exchange: exchange.to_owned(),
                underlying_con_id,
                trading_class: trading_class.to_owned(),
                multiplier: multiplier.to_owned(),
                expirations: expirations.to_vec(),
                strikes: strikes.to_vec(),
            },
        });
    }

    fn security_definition_option_parameter_end(&mut self, req_id: i64) {
        self.push(Callback::SecurityDefinitionOptionParameterEnd(req_id));
    }

    fn market_rule(&mut self, market_rule_id: i64, price_increments: &[e::PriceIncrement]) {
        self.push(Callback::MarketRule {
            market_rule_id,
            price_increments: ours(price_increments),
        });
    }

    fn fundamental_data(&mut self, req_id: i64, data: &str) {
        self.push(Callback::FundamentalData {
            req_id,
            data: data.to_owned(),
        });
    }

    fn scanner_parameters(&mut self, xml: &str) {
        self.push(Callback::ScannerParameters(xml.to_owned()));
    }

    fn scanner_data(
        &mut self,
        req_id: i64,
        rank: i32,
        details: &e::ContractDetails,
        distance: &str,
        benchmark: &str,
        projection: &str,
        legs_str: &str,
    ) {
        self.push(Callback::ScannerData {
            req_id,
            data: ScanData {
                rank,
                contract_details: details.into(),
                distance: distance.to_owned(),
                benchmark: benchmark.to_owned(),
                projection: projection.to_owned(),
                legs_str: legs_str.to_owned(),
            },
        });
    }

    fn scanner_data_end(&mut self, req_id: i64) {
        self.push(Callback::ScannerDataEnd(req_id));
    }

    fn news_providers(&mut self, providers: &[e::NewsProvider]) {
        self.push(Callback::NewsProviders(ours(providers)));
    }

    fn news_article(&mut self, req_id: i64, article_type: i32, article_text: &str) {
        self.push(Callback::NewsArticle {
            req_id,
            article: NewsArticle {
                article_type,
                article_text: article_text.to_owned(),
            },
        });
    }

    fn historical_news(
        &mut self,
        req_id: i64,
        time: &str,
        provider_code: &str,
        article_id: &str,
        headline: &str,
    ) {
        let news = convert::historical_news(time, provider_code, article_id, headline);
        let callback = news.map(|news| Callback::HistoricalNews { req_id, news });
        self.push_converted("historicalNews", callback);
    }

    fn historical_news_end(&mut self, req_id: i64, _has_more: bool) {
        self.push(Callback::HistoricalNewsEnd { req_id });
    }

    fn update_news_bulletin(
        &mut self,
        msg_id: i64,
        msg_type: i32,
        message: &str,
        orig_exchange: &str,
    ) {
        self.push(Callback::UpdateNewsBulletin(NewsBulletin {
            msg_id,
            msg_type,
            message: message.to_owned(),
            orig_exchange: orig_exchange.to_owned(),
        }));
    }

    fn receive_fa(&mut self, _fa_data_type: i32, cxml: &str) {
        self.push(Callback::ReceiveFa {
            xml: cxml.to_owned(),
        });
    }

    fn wsh_meta_data(&mut self, req_id: i64, data_json: &str) {
        self.push(Callback::WshMetaData {
            req_id,
            data_json: data_json.to_owned(),
        });
    }

    fn wsh_event_data(&mut self, req_id: i64, data_json: &str) {
        self.push(Callback::WshEventData {
            req_id,
            data_json: data_json.to_owned(),
        });
    }

    fn user_info(&mut self, req_id: i64, white_branding_id: &str) {
        self.push(Callback::UserInfo {
            req_id,
            white_branding_id: white_branding_id.to_owned(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{OrderOp, Wrapper};

    /// Whether `got` is `want`, where each `…` in `want` stands for any text.
    fn fits(got: &str, want: &str) -> bool {
        let mut parts = want.split('…');
        let Some(mut rest) = parts.next().and_then(|first| got.strip_prefix(first)) else {
            return false;
        };
        let parts: Vec<&str> = parts.collect();
        let Some((last, middle)) = parts.split_last() else {
            return rest.is_empty();
        };
        for part in middle {
            let Some(at) = rest.find(part) else {
                return false;
            };
            rest = &rest[at + part.len()..];
        }
        rest.ends_with(last)
    }

    fn capture() -> Capture {
        Capture::new(TimeZone::UTC)
    }

    #[test]
    fn each_engine_callback_becomes_its_own_record_with_its_arguments_in_place() {
        let contract = e::Contract::default();
        let details = e::ContractDetails::default();
        let bar = e::BarData {
            date: "20260925".into(),
            ..e::BarData::default()
        };
        let fill = e::Execution {
            time: "20260925-13:30:00".into(),
            ..e::Execution::default()
        };
        type Call<'a> = Box<dyn Fn(&mut Capture) + 'a>;
        let cases: Vec<(Call<'_>, Option<&str>)> = vec![
            (
                Box::new(|c| c.connection_closed()),
                Some("ConnectionClosed"),
            ),
            (
                Box::new(|c| c.managed_accounts("DU1,,DU2")),
                Some(r#"ManagedAccounts(["DU1", "DU2"])"#),
            ),
            (
                Box::new(|c| {
                    c.error_from(ErrorOrigin::Request { id: 1, ends: true }, 200, "m", "{}")
                }),
                Some(
                    r#"Error { origin: Request { id: 1, ends: true }, code: 200, message: "m", advanced_order_reject_json: "{}" }"#,
                ),
            ),
            (
                Box::new(|c| c.current_time(1_700_000_000)),
                Some("CurrentTime(2023-11-14T22:13:20+00:00[UTC])"),
            ),
            // A time no clock can hold is dropped, as ib_async's decoder
            // drops a message it cannot read.
            (Box::new(|c| c.current_time(i64::MAX)), None),
            (
                Box::new(|c| c.current_time_in_millis(5)),
                Some("CurrentTimeInMillis(5)"),
            ),
            (
                Box::new(|c| c.question_retired(Question::Positions)),
                Some("QuestionRetired(Positions)"),
            ),
            (
                Box::new(|c| c.tick_price(1, 2, 3.5, &e::TickAttrib::default())),
                Some("TickPrice { req_id: 1, tick_type: 2, price: 3.5 }"),
            ),
            (
                Box::new(|c| c.tick_size(1, 3, 4.5)),
                Some("TickSize { req_id: 1, tick_type: 3, size: 4.5 }"),
            ),
            (
                Box::new(|c| c.tick_string(1, 45, "x")),
                Some(r#"TickString { req_id: 1, tick_type: 45, value: "x" }"#),
            ),
            (
                Box::new(|c| c.tick_generic(1, 49, 0.5)),
                Some("TickGeneric { req_id: 1, tick_type: 49, value: 0.5 }"),
            ),
            (
                Box::new(|c| c.tick_snapshot_end(1)),
                Some("TickSnapshotEnd(1)"),
            ),
            (
                Box::new(|c| c.market_data_type(1, 3)),
                Some("MarketDataType { req_id: 1, market_data_type: 3 }"),
            ),
            (
                Box::new(|c| c.tick_req_params(1, 0.01, "a", 3)),
                Some(
                    r#"TickReqParams { req_id: 1, min_tick: 0.01, bbo_exchange: "a", snapshot_permissions: 3 }"#,
                ),
            ),
            (
                Box::new(|c| {
                    c.tick_option_computation(1, 13, 1, 0.2, 0.5, 3.5, 0.25, 0.1, 0.3, -0.4, 99.5)
                }),
                Some(
                    "TickOptionComputation { req_id: 1, tick_type: 13, computation: OptionComputation { tick_attrib: 1, implied_vol: Some(0.2), delta: Some(0.5), opt_price: Some(3.5), pv_dividend: Some(0.25), gamma: Some(0.1), vega: Some(0.3), theta: Some(-0.4), und_price: Some(99.5) } }",
                ),
            ),
            (
                Box::new(|c| c.tick_efp(1, 38, 1.5, "1.5%", 2.5, 3, "20261218", 4.5, 5.5)),
                Some(
                    r#"TickEfp { req_id: 1, tick_type: 38, efp: EfpData { basis_points: 1.5, formatted_basis_points: "1.5%", implied_future: 2.5, hold_days: 3, future_last_trade_date: "20261218", dividend_impact: 4.5, dividends_to_last_trade_date: 5.5 } }"#,
                ),
            ),
            (
                Box::new(|c| c.tick_news(1, 2, "p", "a", "h", "x")),
                Some(
                    r#"TickNews { news: NewsTick { time_stamp: 2, provider_code: "p", article_id: "a", headline: "h", extra_data: "x" } }"#,
                ),
            ),
            (
                Box::new(|c| {
                    let attrib = e::TickAttribLast {
                        past_limit: true,
                        ..e::TickAttribLast::default()
                    };
                    c.tick_by_tick_all_last(1, 2, 3, 4.5, 5.5, &attrib, "e", "s")
                }),
                Some(
                    r#"TickByTickAllLast { req_id: 1, tick_type: 2, price: 4.5, size: 5.5, attrib: TickAttribLast { past_limit: true, unreported: false }, exchange: "e", special_conditions: "s" }"#,
                ),
            ),
            (
                Box::new(|c| {
                    let attrib = e::TickAttribBidAsk {
                        bid_past_low: true,
                        ..e::TickAttribBidAsk::default()
                    };
                    c.tick_by_tick_bid_ask(1, 2, 3.5, 4.5, 5.5, 6.5, &attrib)
                }),
                Some(
                    "TickByTickBidAsk { req_id: 1, bid_price: 3.5, ask_price: 4.5, bid_size: 5.5, ask_size: 6.5, attrib: TickAttribBidAsk { bid_past_low: true, ask_past_high: false } }",
                ),
            ),
            (
                Box::new(|c| c.tick_by_tick_mid_point(1, 2, 3.5)),
                Some("TickByTickMidPoint { req_id: 1, mid_point: 3.5 }"),
            ),
            (
                Box::new(|c| c.update_mkt_depth(1, 2, 0, 1, 3.5, 4.5)),
                Some(
                    "UpdateMktDepth { req_id: 1, position: 2, operation: 0, side: 1, price: 3.5, size: 4.5 }",
                ),
            ),
            (
                Box::new(|c| c.update_mkt_depth_l2(1, 2, "m", 1, 0, 3.5, 4.5, true)),
                Some(
                    r#"UpdateMktDepthL2 { req_id: 1, position: 2, market_maker: "m", operation: 1, side: 0, price: 3.5, size: 4.5 }"#,
                ),
            ),
            (
                Box::new(|c| {
                    c.mkt_depth_exchanges(&[e::DepthMktDataDescription {
                        exchange: "X".into(),
                        sec_type: "STK".into(),
                        listing_exch: "Y".into(),
                        service_data_type: "Deep".into(),
                        agg_group: 2,
                    }])
                }),
                Some(
                    r#"MktDepthExchanges([DepthMktDataDescription { exchange: "X", sec_type: "STK", listing_exch: "Y", service_data_type: "Deep", agg_group: Some(2) }])"#,
                ),
            ),
            (
                Box::new(|c| {
                    c.smart_components(
                        1,
                        &[e::SmartComponent {
                            bit_number: 2,
                            exchange: "X".into(),
                            exchange_letter: "x".into(),
                        }],
                    )
                }),
                Some(
                    r#"SmartComponents { req_id: 1, components: [SmartComponent { bit_number: 2, exchange: "X", exchange_letter: "x" }] }"#,
                ),
            ),
            (
                Box::new(|c| c.real_time_bar(1, 1_700_000_000, 1.5, 2.5, 0.5, 2.0, 100.5, 1.75, 7)),
                Some(
                    "RealtimeBar { req_id: 1, bar: RealTimeBar { time: 2023-11-14T22:13:20+00:00[UTC], end_time: -1, open_: 1.5, high: 2.5, low: 0.5, close: 2.0, volume: 100.5, wap: 1.75, count: 7 } }",
                ),
            ),
            (
                Box::new(|c| c.order_status(1, "Filled", 2.5, 3.5, 4.5, 5, 6, 7.5, 8, "w", 9.5)),
                Some(
                    r#"OrderStatus { order_id: 1, status: "Filled", filled: 2.5, remaining: 3.5, avg_fill_price: 4.5, perm_id: 5, parent_id: 6, last_fill_price: 7.5, client_id: 8, why_held: "w", mkt_cap_price: 9.5 }"#,
                ),
            ),
            (
                Box::new(|c| {
                    c.open_order(
                        7,
                        &contract,
                        &e::Order::default(),
                        &e::OrderState::default(),
                    )
                }),
                Some(
                    "OpenOrder { order_id: 7, contract: Contract {…}, order: Order {…}, order_state: OrderState {…} }",
                ),
            ),
            (Box::new(|c| c.open_order_end()), Some("OpenOrderEnd")),
            (
                Box::new(|c| {
                    c.completed_order(&contract, &e::Order::default(), &e::OrderState::default())
                }),
                Some(
                    "CompletedOrder { contract: Contract {…}, order: Order {…}, order_state: OrderState {…} }",
                ),
            ),
            (
                Box::new(|c| c.completed_orders_end()),
                Some("CompletedOrdersEnd"),
            ),
            (
                Box::new(|c| c.exec_details(1, &contract, &fill)),
                Some(
                    "ExecDetails { req_id: 1, contract: Contract {…}, execution: Execution {…time: 2026-09-25T13:30:00+00:00[UTC]…} }",
                ),
            ),
            (
                Box::new(|c| {
                    let never = e::Execution {
                        time: "never".into(),
                        ..e::Execution::default()
                    };
                    c.exec_details(1, &contract, &never)
                }),
                None,
            ),
            (
                Box::new(|c| c.exec_details_end(1)),
                Some("ExecDetailsEnd(1)"),
            ),
            (
                Box::new(|c| {
                    c.commission_and_fees_report(&e::CommissionAndFeesReport {
                        exec_id: "x".into(),
                        commission_and_fees: 1.5,
                        ..e::CommissionAndFeesReport::default()
                    })
                }),
                Some(r#"CommissionReport(CommissionReport { exec_id: "x", commission: 1.5, …})"#),
            ),
            (
                Box::new(|c| c.update_account_value("NetLiquidation", "1", "USD", "DU1")),
                Some(
                    r#"UpdateAccountValue(AccountValue { account: "DU1", tag: "NetLiquidation", value: "1", currency: "USD", model_code: "" })"#,
                ),
            ),
            (
                Box::new(|c| c.update_portfolio(&contract, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, "DU1")),
                Some(
                    r#"UpdatePortfolio(PortfolioItem { contract: Contract {…}, position: 1.5, market_price: 2.5, market_value: 3.5, average_cost: 4.5, unrealized_pnl: 5.5, realized_pnl: 6.5, account: "DU1" })"#,
                ),
            ),
            (
                Box::new(|c| c.account_download_end("DU1")),
                Some("AccountDownloadEnd"),
            ),
            (
                Box::new(|c| c.account_summary(1, "DU1", "t", "v", "USD")),
                Some(
                    r#"AccountSummary { value: AccountValue { account: "DU1", tag: "t", value: "v", currency: "USD", model_code: "" } }"#,
                ),
            ),
            (
                Box::new(|c| c.account_summary_end(1)),
                Some("AccountSummaryEnd(1)"),
            ),
            (
                Box::new(|c| c.account_update_multi(1, "DU1", "m", "t", "v", "USD")),
                Some(
                    r#"AccountUpdateMulti { value: AccountValue { account: "DU1", tag: "t", value: "v", currency: "USD", model_code: "m" } }"#,
                ),
            ),
            (
                Box::new(|c| c.account_update_multi_end(1)),
                Some("AccountUpdateMultiEnd(1)"),
            ),
            (
                Box::new(|c| c.position("DU1", &contract, 1.5, 2.5)),
                Some(
                    r#"Position(Position { account: "DU1", contract: Contract {…}, position: 1.5, avg_cost: 2.5 })"#,
                ),
            ),
            (Box::new(|c| c.position_end()), Some("PositionEnd")),
            (
                Box::new(|c| c.pnl(1, 1.5, 2.5, 3.5)),
                Some("Pnl { req_id: 1, daily_pnl: 1.5, unrealized_pnl: 2.5, realized_pnl: 3.5 }"),
            ),
            (
                Box::new(|c| c.pnl_single(1, 1.5, 2.5, 3.5, 4.5, 5.5)),
                Some(
                    "PnlSingle { req_id: 1, pos: 1.5, daily_pnl: 2.5, unrealized_pnl: 3.5, realized_pnl: 4.5, value: 5.5 }",
                ),
            ),
            (
                Box::new(|c| c.historical_data(1, &bar)),
                Some("HistoricalData { req_id: 1, bar: BarData { date: Day(2026-09-25), …} }"),
            ),
            (
                Box::new(|c| c.historical_data_end(1, "a", "b")),
                Some("HistoricalDataEnd { req_id: 1 }"),
            ),
            (
                Box::new(|c| c.historical_data_update(1, &bar)),
                Some(
                    "HistoricalDataUpdate { req_id: 1, bar: BarData { date: Day(2026-09-25), …} }",
                ),
            ),
            (
                Box::new(|c| c.head_timestamp(1, "20260101")),
                Some(r#"HeadTimestamp { req_id: 1, head_timestamp: "20260101" }"#),
            ),
            (
                Box::new(|c| {
                    c.historical_ticks(1, &e::HistoricalTickData::Midpoint(Vec::new()), true)
                }),
                Some("HistoricalTicks { req_id: 1, ticks: [], done: true }"),
            ),
            (
                Box::new(|c| {
                    c.historical_ticks_bid_ask(2, &e::HistoricalTickData::BidAsk(Vec::new()), false)
                }),
                Some("HistoricalTicks { req_id: 2, ticks: [], done: false }"),
            ),
            (
                Box::new(|c| {
                    c.historical_ticks_last(3, &e::HistoricalTickData::Last(Vec::new()), true)
                }),
                Some("HistoricalTicks { req_id: 3, ticks: [], done: true }"),
            ),
            (
                Box::new(|c| {
                    c.historical_schedule(
                        1,
                        "a",
                        "b",
                        "tz",
                        &[("s".into(), "e".into(), "r".into())],
                    )
                }),
                Some(
                    r#"HistoricalSchedule { req_id: 1, schedule: HistoricalSchedule { start_date_time: "a", end_date_time: "b", time_zone: "tz", sessions: [HistoricalSession { start_date_time: "s", end_date_time: "e", ref_date: "r" }] } }"#,
                ),
            ),
            (
                Box::new(|c| c.histogram_data(1, &[(1.5, 2)])),
                Some(
                    "HistogramData { req_id: 1, items: [HistogramData { price: 1.5, count: 2 }] }",
                ),
            ),
            (
                Box::new(|c| c.contract_details(1, &details)),
                Some("ContractDetails { req_id: 1, details: ContractDetails {…} }"),
            ),
            (
                Box::new(|c| c.bond_contract_details(2, &details)),
                Some("ContractDetails { req_id: 2, details: ContractDetails {…} }"),
            ),
            (
                Box::new(|c| c.contract_details_end(1)),
                Some("ContractDetailsEnd(1)"),
            ),
            (
                Box::new(|c| c.symbol_samples(1, &[e::ContractDescription::default()])),
                Some("SymbolSamples { req_id: 1, descriptions: [ContractDescription {…}] }"),
            ),
            (
                Box::new(|c| {
                    c.security_definition_option_parameter(
                        1,
                        "SMART",
                        2,
                        "SPX",
                        "100",
                        &["20261218".into()],
                        &[5000.5],
                    )
                }),
                Some(
                    r#"SecurityDefinitionOptionParameter { req_id: 1, chain: OptionChain { exchange: "SMART", underlying_con_id: 2, trading_class: "SPX", multiplier: "100", expirations: ["20261218"], strikes: [5000.5] } }"#,
                ),
            ),
            (
                Box::new(|c| c.security_definition_option_parameter_end(1)),
                Some("SecurityDefinitionOptionParameterEnd(1)"),
            ),
            (
                Box::new(|c| {
                    c.market_rule(
                        26,
                        &[e::PriceIncrement {
                            low_edge: 0.5,
                            increment: 0.01,
                        }],
                    )
                }),
                Some(
                    "MarketRule { market_rule_id: 26, price_increments: [PriceIncrement { low_edge: 0.5, increment: 0.01 }] }",
                ),
            ),
            (
                Box::new(|c| c.fundamental_data(1, "<x/>")),
                Some(r#"FundamentalData { req_id: 1, data: "<x/>" }"#),
            ),
            (
                Box::new(|c| c.scanner_parameters("<p/>")),
                Some(r#"ScannerParameters("<p/>")"#),
            ),
            (
                Box::new(|c| c.scanner_data(1, 2, &details, "a", "b", "c", "l")),
                Some(
                    r#"ScannerData { req_id: 1, data: ScanData { rank: 2, contract_details: ContractDetails {…}, distance: "a", benchmark: "b", projection: "c", legs_str: "l" } }"#,
                ),
            ),
            (
                Box::new(|c| c.scanner_data_end(1)),
                Some("ScannerDataEnd(1)"),
            ),
            (
                Box::new(|c| {
                    c.news_providers(&[e::NewsProvider {
                        code: "BZ".into(),
                        name: "Benzinga".into(),
                    }])
                }),
                Some(r#"NewsProviders([NewsProvider { code: "BZ", name: "Benzinga" }])"#),
            ),
            (
                Box::new(|c| c.news_article(1, 0, "t")),
                Some(
                    r#"NewsArticle { req_id: 1, article: NewsArticle { article_type: 0, article_text: "t" } }"#,
                ),
            ),
            (
                Box::new(|c| c.historical_news(1, "2026-09-25 13:30:00.0", "BZ", "a", "h")),
                Some(
                    r#"HistoricalNews { req_id: 1, news: HistoricalNews { time: Naive(2026-09-25T13:30:00), provider_code: "BZ", article_id: "a", headline: "h" } }"#,
                ),
            ),
            (
                Box::new(|c| c.historical_news_end(1, true)),
                Some("HistoricalNewsEnd { req_id: 1 }"),
            ),
            (
                Box::new(|c| c.update_news_bulletin(1, 2, "m", "X")),
                Some(
                    r#"UpdateNewsBulletin(NewsBulletin { msg_id: 1, msg_type: 2, message: "m", orig_exchange: "X" })"#,
                ),
            ),
            (
                Box::new(|c| c.receive_fa(1, "<fa/>")),
                Some(r#"ReceiveFa { xml: "<fa/>" }"#),
            ),
            (
                Box::new(|c| c.wsh_meta_data(1, "{}")),
                Some(r#"WshMetaData { req_id: 1, data_json: "{}" }"#),
            ),
            (
                Box::new(|c| c.wsh_event_data(1, "[]")),
                Some(r#"WshEventData { req_id: 1, data_json: "[]" }"#),
            ),
            (
                Box::new(|c| c.user_info(1, "w")),
                Some(r#"UserInfo { req_id: 1, white_branding_id: "w" }"#),
            ),
        ];
        let mut c = capture();
        for (call, want) in cases {
            call(&mut c);
            let got: Vec<String> = c.take().iter().map(|cb| format!("{cb:?}")).collect();
            match want {
                Some(want) => assert!(
                    matches!(&got[..], [one] if fits(one, want)),
                    "want {want}\n got {got:?}"
                ),
                None => assert!(got.is_empty(), "want nothing, got {got:?}"),
            }
        }
    }

    /// Only the engine's own lookups are dropped. An order or request is
    /// kept whatever its number, the reserved band included: an order
    /// another session placed can be numbered there.
    #[test]
    fn an_error_is_kept_by_its_origin_and_only_the_engines_own_dropped() {
        let mut c = capture();
        let numbers = [0xC000_0000, 0xC000_0001, i64::from(u32::MAX) - 1, 1 << 33];
        for n in numbers {
            for origin in [
                ErrorOrigin::Request { id: n, ends: true },
                ErrorOrigin::Order {
                    id: n,
                    op: OrderOp::Venue,
                },
            ] {
                c.error_from(origin, 201, "m", "");
                assert!(
                    matches!(&c.take()[..], [Callback::Error { origin: o, .. }] if *o == origin),
                    "{origin:?}"
                );
            }
            if let Ok(id) = u32::try_from(n) {
                c.error_from(ErrorOrigin::Internal(id), 200, "m", "");
                assert!(c.take().is_empty(), "Internal({id})");
            }
        }
        for origin in [
            ErrorOrigin::Question {
                q: Question::OpenOrders,
                ends: false,
            },
            ErrorOrigin::Session,
        ] {
            c.error_from(origin, 2104, "m", "");
            assert!(matches!(&c.take()[..], [Callback::Error { origin: o, .. }] if *o == origin));
        }
    }
}

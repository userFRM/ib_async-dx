//! ib_async 2.1.0's public names in Rust spelling, generated from ib_async's source by
//! `scripts/parity.py --rust`. Do not edit: regenerate.
//!
//! A name the crate lacks fails to compile. The functions are compiled and never run, but
//! `defaults`, which holds every default with an ib_async counterpart to ib_async's value.

#![allow(
    dead_code,
    unused_imports,
    clippy::unit_arg,
    clippy::eq_op,
    clippy::type_complexity
)]

use std::future::Future;
use std::path::Path;
use std::time::Duration;

use ib_async_dx::defaults::{CORPORATE_ACTIONS_TIMEOUT, HISTORICAL_TIMEOUT, SPREAD_SCAN_TIMEOUT};
use ib_async_dx::util::{self, BarDate, DateTimeArg, TimeT};
use ib_async_dx::*;
use jiff::tz::TimeZone;
use jiff::{Timestamp, Zoned};

/// A value of any type, for arguments: never called.
fn any<T>() -> T {
    unreachable!()
}

fn used<T>(_: T) {}
fn eq<T: PartialEq>() {}
fn debug_clone<T: std::fmt::Debug + Clone>() {}
fn default<T: Default>() {}

/// The 133 callables of `IB`, called as ib_async code calls them: `ib.x(..)` through
/// `Deref`, or `IB::x(..)` for what ib_async aliases from util or declares static.
fn callables(ib: &IB) {
    used(ib.connect(any())); // connect
    used(ib.disconnect()); // disconnect
    used(ib.is_connected()); // isConnected
    used(ib.run()); // run
    used(IB::schedule(Timestamp::UNIX_EPOCH, || {})); // schedule
    used(IB::sleep(any())); // sleep
    used(IB::time_range(
        Timestamp::UNIX_EPOCH,
        Timestamp::UNIX_EPOCH,
        any(),
    )); // timeRange
    used(IB::time_range_async(
        Timestamp::UNIX_EPOCH,
        Timestamp::UNIX_EPOCH,
        any(),
    )); // timeRangeAsync
    used(IB::wait_until(Timestamp::UNIX_EPOCH)); // waitUntil
    used(ib.wait_on_update(any())); // waitOnUpdate
    used(ib.loop_until(|| None::<()>, any())); // loopUntil
    used(ib.set_timeout(any())); // setTimeout
    used(ib.managed_accounts()); // managedAccounts
    used(ib.account_values(any())); // accountValues
    used(ib.account_summary(any())); // accountSummary
    used(ib.portfolio(any())); // portfolio
    used(ib.positions(any())); // positions
    used(ib.pnl(any(), any())); // pnl
    used(ib.pnl_single(any(), any(), any())); // pnlSingle
    used(ib.trades()); // trades
    used(ib.open_trades()); // openTrades
    used(ib.orders()); // orders
    used(ib.open_orders()); // openOrders
    used(ib.fills()); // fills
    used(ib.executions()); // executions
    used(ib.ticker(any())); // ticker
    used(ib.tickers()); // tickers
    used(ib.pending_tickers()); // pendingTickers
    used(ib.realtime_bars()); // realtimeBars
    used(ib.news_ticks()); // newsTicks
    used(ib.news_bulletins()); // newsBulletins
    used(ib.req_tickers(any(), any())); // reqTickers
    used(ib.qualify_contracts(any())); // qualifyContracts
    used(ib.bracket_order(any(), any(), any(), any(), any())); // bracketOrder
    used(IB::one_cancels_all(any(), any(), any())); // oneCancelsAll
    used(ib.what_if_order(any(), any())); // whatIfOrder
    used(ib.place_order(any(), any())); // placeOrder
    used(ib.cancel_order(any(), any())); // cancelOrder
    used(ib.req_global_cancel()); // reqGlobalCancel
    used(ib.req_current_time()); // reqCurrentTime
    used(ib.req_account_updates(any())); // reqAccountUpdates
    used(ib.req_account_updates_multi(any(), any())); // reqAccountUpdatesMulti
    used(ib.req_account_summary()); // reqAccountSummary
    used(ib.req_auto_open_orders(any())); // reqAutoOpenOrders
    used(ib.req_open_orders()); // reqOpenOrders
    used(ib.req_all_open_orders()); // reqAllOpenOrders
    used(ib.req_completed_orders(any())); // reqCompletedOrders
    used(ib.req_executions(any())); // reqExecutions
    used(ib.req_positions()); // reqPositions
    used(ib.req_pnl(any(), any())); // reqPnL
    used(ib.cancel_pnl(any(), any())); // cancelPnL
    used(ib.req_pnl_single(any(), any(), any())); // reqPnLSingle
    used(ib.cancel_pnl_single(any(), any(), any())); // cancelPnLSingle
    used(ib.req_contract_details(any())); // reqContractDetails
    used(ib.req_matching_symbols(any())); // reqMatchingSymbols
    used(ib.req_market_rule(any())); // reqMarketRule
    used(ib.req_real_time_bars(any(), any(), any(), any(), any())); // reqRealTimeBars
    used(ib.cancel_real_time_bars(any())); // cancelRealTimeBars
    used(ib.req_historical_data(
        any(),
        "",
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
    )); // reqHistoricalData
    used(ib.cancel_historical_data(any())); // cancelHistoricalData
    used(ib.req_historical_schedule(any(), any(), "", any())); // reqHistoricalSchedule
    used(ib.req_historical_ticks(any(), "", "", any(), any(), any(), any(), any())); // reqHistoricalTicks
    used(ib.req_market_data_type(any())); // reqMarketDataType
    used(ib.req_head_time_stamp(any(), any(), any(), any())); // reqHeadTimeStamp
    used(ib.req_mkt_data(any(), any(), any(), any(), any())); // reqMktData
    used(ib.cancel_mkt_data(any())); // cancelMktData
    used(ib.req_tick_by_tick_data(any(), any(), any(), any())); // reqTickByTickData
    used(ib.cancel_tick_by_tick_data(any(), any())); // cancelTickByTickData
    used(ib.req_smart_components(any())); // reqSmartComponents
    used(ib.req_mkt_depth_exchanges()); // reqMktDepthExchanges
    used(ib.req_mkt_depth(any(), any(), any(), any())); // reqMktDepth
    used(ib.cancel_mkt_depth(any(), any())); // cancelMktDepth
    used(ib.req_histogram_data(any(), any(), any())); // reqHistogramData
    used(ib.req_fundamental_data(any(), any(), any())); // reqFundamentalData
    used(ib.req_scanner_data(any(), any(), any())); // reqScannerData
    used(ib.req_scanner_subscription(any(), any(), any())); // reqScannerSubscription
    used(ib.cancel_scanner_subscription(any())); // cancelScannerSubscription
    used(ib.req_scanner_parameters()); // reqScannerParameters
    used(ib.calculate_implied_volatility(any(), any(), any(), any())); // calculateImpliedVolatility
    used(ib.calculate_option_price(any(), any(), any(), any())); // calculateOptionPrice
    used(ib.req_sec_def_opt_params(any(), any(), any(), any())); // reqSecDefOptParams
    used(ib.exercise_options(any(), any(), any(), any(), any())); // exerciseOptions
    used(ib.req_news_providers()); // reqNewsProviders
    used(ib.req_news_article(any(), any(), any())); // reqNewsArticle
    used(ib.req_historical_news(any(), any(), "", "", any(), any())); // reqHistoricalNews
    used(ib.req_news_bulletins(any())); // reqNewsBulletins
    used(ib.cancel_news_bulletins()); // cancelNewsBulletins
    used(ib.request_fa(any())); // requestFA
    used(ib.replace_fa(any(), any())); // replaceFA
    used(ib.req_wsh_meta_data()); // reqWshMetaData
    used(ib.cancel_wsh_meta_data()); // cancelWshMetaData
    used(ib.req_wsh_event_data(any())); // reqWshEventData
    used(ib.cancel_wsh_event_data()); // cancelWshEventData
    used(ib.get_wsh_meta_data()); // getWshMetaData
    used(ib.get_wsh_event_data(any())); // getWshEventData
    used(ib.req_user_info()); // reqUserInfo
    used(ib.connect_async(any())); // connectAsync
    used(ib.qualify_contracts_async(any(), any())); // qualifyContractsAsync
    used(ib.req_tickers_async(any(), any())); // reqTickersAsync
    used(ib.what_if_order_async(any(), any())); // whatIfOrderAsync
    used(ib.req_current_time_async()); // reqCurrentTimeAsync
    used(ib.req_account_updates_async(any())); // reqAccountUpdatesAsync
    used(ib.req_account_updates_multi_async(any(), any())); // reqAccountUpdatesMultiAsync
    used(ib.account_summary_async(any())); // accountSummaryAsync
    used(ib.req_account_summary_async()); // reqAccountSummaryAsync
    used(ib.req_open_orders_async()); // reqOpenOrdersAsync
    used(ib.req_all_open_orders_async()); // reqAllOpenOrdersAsync
    used(ib.req_completed_orders_async(any())); // reqCompletedOrdersAsync
    used(ib.req_executions_async(any())); // reqExecutionsAsync
    used(ib.req_positions_async()); // reqPositionsAsync
    used(ib.req_contract_details_async(any())); // reqContractDetailsAsync
    used(ib.req_matching_symbols_async(any())); // reqMatchingSymbolsAsync
    used(ib.req_market_rule_async(any())); // reqMarketRuleAsync
    used(ib.req_historical_data_async(
        any(),
        "",
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
    )); // reqHistoricalDataAsync
    used(ib.req_historical_schedule_async(any(), any(), "", any())); // reqHistoricalScheduleAsync
    used(ib.req_historical_ticks_async(any(), "", "", any(), any(), any(), any(), any())); // reqHistoricalTicksAsync
    used(ib.req_head_time_stamp_async(any(), any(), any(), any())); // reqHeadTimeStampAsync
    used(ib.req_smart_components_async(any())); // reqSmartComponentsAsync
    used(ib.req_mkt_depth_exchanges_async()); // reqMktDepthExchangesAsync
    used(ib.req_histogram_data_async(any(), any(), any())); // reqHistogramDataAsync
    used(ib.req_fundamental_data_async(any(), any(), any())); // reqFundamentalDataAsync
    used(ib.req_scanner_data_async(any(), any(), any())); // reqScannerDataAsync
    used(ib.req_scanner_parameters_async()); // reqScannerParametersAsync
    used(ib.calculate_implied_volatility_async(any(), any(), any(), any())); // calculateImpliedVolatilityAsync
    used(ib.calculate_option_price_async(any(), any(), any(), any())); // calculateOptionPriceAsync
    used(ib.req_sec_def_opt_params_async(any(), any(), any(), any())); // reqSecDefOptParamsAsync
    used(ib.req_news_providers_async()); // reqNewsProvidersAsync
    used(ib.req_news_article_async(any(), any(), any())); // reqNewsArticleAsync
    used(ib.req_historical_news_async(any(), any(), "", "", any(), any())); // reqHistoricalNewsAsync
    used(ib.request_fa_async(any())); // requestFAAsync
    used(ib.get_wsh_meta_data_async()); // getWshMetaDataAsync
    used(ib.get_wsh_event_data_async(any())); // getWshEventDataAsync
    used(ib.req_user_info_async()); // reqUserInfoAsync
    used(IB::new()); // IB()
    used(IB::with(any(), any())); // IB(defaults=..)
    used(ib.handle());
    used(ib.client()); // ib.client
    used(IB::EVENTS); // IB.events
}

/// `IB`'s class attributes are `IBConfig`'s fields.
fn ib_config(c: &IBConfig) {
    used((
        &c.request_timeout,
        &c.raise_request_errors,
        &c.max_synced_sub_accounts,
        &c.timezone_tws,
    ));
}

/// The 25 events of `IB` and the added `tick_event`, and the 12 of the objects reached
/// through it.
fn events(ib: &IB) {
    used(ib.connected_event());
    used(ib.disconnected_event());
    used(ib.update_event());
    used(ib.pending_tickers_event());
    used(ib.bar_update_event());
    used(ib.new_order_event());
    used(ib.order_modify_event());
    used(ib.cancel_order_event());
    used(ib.open_order_event());
    used(ib.order_status_event());
    used(ib.exec_details_event());
    used(ib.commission_report_event());
    used(ib.update_portfolio_event());
    used(ib.position_event());
    used(ib.account_value_event());
    used(ib.account_summary_event());
    used(ib.pnl_event());
    used(ib.pnl_single_event());
    used(ib.scanner_data_event());
    used(ib.tick_news_event());
    used(ib.news_bulletin_event());
    used(ib.wsh_meta_event());
    used(ib.wsh_event());
    used(ib.error_event());
    used(ib.timeout_event());
    used(ib.tick_event());
    for trade in ib.trades() {
        used(trade.status_event());
        used(trade.modify_event());
        used(trade.fill_event());
        used(trade.commission_report_event());
        used(trade.filled_event());
        used(trade.cancel_event());
        used(trade.cancelled_event());
    }
    used(Trade::EVENTS);
    for ticker in ib.tickers() {
        used(ticker.update_event());
        let bars = ticker.update_event().trades().tickbars(1);
        used(bars.bars.update_event());
    }
    used(Ticker::EVENTS);
    for bars in ib.realtime_bars() {
        match bars {
            Bars::Historical(l) => used(l.update_event()),
            Bars::RealTime(l) => used(l.update_event()),
            Bars::Scan(l) => used(l.update_event()),
        }
    }
}

/// Every exported name that is not a type of its own name.
fn exports() {
    used(Event::<()>::new("event"));
    used(any::<Client>());
    used(Contract::bag()); // Bag
    used(Contract::bond()); // Bond
    used(Contract::cfd("", "", "")); // CFD
    used(Contract::commodity("", "", "")); // Commodity
    used(Contract::cont_future("", "")); // ContFuture
    used(Contract::crypto("", "", "")); // Crypto
    used(Contract::forex("")); // Forex
    used(Contract::future("", "", "")); // Future
    used(Contract::futures_option("", "", any(), "", "")); // FuturesOption
    used(Contract::index("", "", "")); // Index
    used(Contract::mutual_fund()); // MutualFund
    used(Contract::option("", "", any(), "", "")); // Option
    used(Contract::stock("", "", "")); // Stock
    used(Contract::warrant()); // Warrant
    #[cfg(feature = "flex")]
    let _ = |e: Error| matches!(e, Error::Flex(..));
    #[cfg(feature = "flex")]
    used(any::<flex::FlexReport>());
    used(any::<IB>());
    used(Order::limit("", 0.0, 0.0)); // LimitOrder
    used(Order::market("", 0.0)); // MarketOrder
    used(Order::stop_limit("", 0.0, 0.0, 0.0)); // StopLimitOrder
    used(Order::stop("", 0.0, 0.0)); // StopOrder
    used(VERSION);
    let _ = |e: Error| matches!(e, Error::Request { .. });
    used(any::<StartupFetch>());
    used(StartupFetch::ALL);
    used(StartupFetch::NONE);
    used(Contract::news("")); // the crate's news contract
    used(StartupFetch::POSITIONS | StartupFetch::ORDERS_OPEN | StartupFetch::ORDERS_COMPLETE);
    used(
        StartupFetch::ACCOUNT_UPDATES
            | StartupFetch::SUB_ACCOUNT_UPDATES
            | StartupFetch::EXECUTIONS,
    );
}

/// Every field of every mapped type, through a borrowed value, and each type's members.
fn fields_combo_leg(x: &ComboLeg) {
    used((
        &x.con_id,
        &x.ratio,
        &x.action,
        &x.exchange,
        &x.open_close,
        &x.short_sale_slot,
        &x.designated_location,
        &x.exempt_code,
    ));
}

fn fields_contract(x: &Contract) {
    used((
        &x.sec_type,
        &x.con_id,
        &x.symbol,
        &x.last_trade_date_or_contract_month,
        &x.strike,
        &x.right,
        &x.multiplier,
        &x.exchange,
        &x.primary_exchange,
        &x.currency,
        &x.local_symbol,
        &x.trading_class,
    ));
    used((
        &x.include_expired,
        &x.sec_id_type,
        &x.sec_id,
        &x.description,
        &x.issuer_id,
        &x.combo_legs_descrip,
        &x.combo_legs,
        &x.delta_neutral_contract,
    ));
}

fn fields_contract_description(x: &ContractDescription) {
    used((&x.contract, &x.derivative_sec_types));
}

fn fields_contract_details(x: &ContractDetails) {
    used((
        &x.contract,
        &x.market_name,
        &x.min_tick,
        &x.order_types,
        &x.valid_exchanges,
        &x.price_magnifier,
        &x.under_con_id,
        &x.long_name,
        &x.contract_month,
        &x.industry,
        &x.category,
        &x.subcategory,
    ));
    used((
        &x.time_zone_id,
        &x.trading_hours,
        &x.liquid_hours,
        &x.ev_rule,
        &x.ev_multiplier,
        &x.md_size_multiplier,
        &x.agg_group,
        &x.under_symbol,
        &x.under_sec_type,
        &x.market_rule_ids,
        &x.sec_id_list,
        &x.real_expiration_date,
    ));
    used((
        &x.last_trade_time,
        &x.stock_type,
        &x.min_size,
        &x.size_increment,
        &x.suggested_size_increment,
        &x.cusip,
        &x.ratings,
        &x.desc_append,
        &x.bond_type,
        &x.coupon_type,
        &x.callable,
        &x.putable,
    ));
    used((
        &x.coupon,
        &x.convertible,
        &x.maturity,
        &x.issue_date,
        &x.next_option_date,
        &x.next_option_type,
        &x.next_option_partial,
        &x.notes,
    ));
}

fn fields_delta_neutral_contract(x: &DeltaNeutralContract) {
    used((&x.con_id, &x.delta, &x.price));
}

fn fields_scan_data(x: &ScanData) {
    used((
        &x.rank,
        &x.contract_details,
        &x.distance,
        &x.benchmark,
        &x.projection,
        &x.legs_str,
    ));
}

fn fields_tag_value(x: &TagValue) {
    used((&x.tag, &x.value));
}

fn fields_ib_defaults(x: &IBDefaults) {
    used((&x.empty_price, &x.empty_size, &x.unset, &x.timezone));
}

fn fields_order_state_numeric(x: &OrderStateNumeric) {
    used((
        &x.status,
        &x.init_margin_before,
        &x.maint_margin_before,
        &x.equity_with_loan_before,
        &x.init_margin_change,
        &x.maint_margin_change,
        &x.equity_with_loan_change,
        &x.init_margin_after,
        &x.maint_margin_after,
        &x.equity_with_loan_after,
        &x.commission,
        &x.min_commission,
    ));
    used((
        &x.max_commission,
        &x.commission_currency,
        &x.warning_text,
        &x.completed_time,
        &x.completed_status,
    ));
}

fn fields_account_value(x: &AccountValue) {
    used((&x.account, &x.tag, &x.value, &x.currency, &x.model_code));
}

fn fields_bar_data(x: &BarData) {
    used((
        &x.date,
        &x.open,
        &x.high,
        &x.low,
        &x.close,
        &x.volume,
        &x.average,
        &x.bar_count,
    ));
}

fn fields_bar_data_list(x: &BarDataList) {
    used((
        &x.bars,
        &x.req_id,
        &x.contract,
        &x.end_date_time,
        &x.duration_str,
        &x.bar_size_setting,
        &x.what_to_show,
        &x.use_rth,
        &x.format_date,
        &x.keep_up_to_date,
        &x.chart_options,
    ));
}

fn fields_commission_report(x: &CommissionReport) {
    used((
        &x.exec_id,
        &x.commission,
        &x.currency,
        &x.realized_pnl,
        &x.yield_,
        &x.yield_redemption_date,
    ));
}

fn fields_connection_stats(x: &ConnectionStats) {
    used((
        &x.start_time,
        &x.duration,
        &x.num_bytes_recv,
        &x.num_bytes_sent,
        &x.num_msg_recv,
        &x.num_msg_sent,
    ));
}

fn fields_dom_level(x: &DOMLevel) {
    used((&x.price, &x.size, &x.market_maker));
}

fn fields_depth_mkt_data_description(x: &DepthMktDataDescription) {
    used((
        &x.exchange,
        &x.sec_type,
        &x.listing_exch,
        &x.service_data_type,
        &x.agg_group,
    ));
}

fn fields_dividends(x: &Dividends) {
    used((
        &x.past_12_months,
        &x.next_12_months,
        &x.next_date,
        &x.next_amount,
    ));
}

fn fields_execution(x: &Execution) {
    used((
        &x.exec_id,
        &x.time,
        &x.acct_number,
        &x.exchange,
        &x.side,
        &x.shares,
        &x.price,
        &x.perm_id,
        &x.client_id,
        &x.order_id,
        &x.liquidation,
        &x.cum_qty,
    ));
    used((
        &x.avg_price,
        &x.order_ref,
        &x.ev_rule,
        &x.ev_multiplier,
        &x.model_code,
        &x.last_liquidity,
        &x.pending_price_revision,
    ));
}

fn fields_execution_filter(x: &ExecutionFilter) {
    used((
        &x.client_id,
        &x.acct_code,
        &x.time,
        &x.symbol,
        &x.sec_type,
        &x.exchange,
        &x.side,
    ));
}

fn fields_family_code(x: &FamilyCode) {
    used((&x.account_id, &x.family_code_str));
}

fn fields_fill(x: &Fill) {
    used((&x.contract, &x.execution, &x.commission_report, &x.time));
}

fn fields_histogram_data(x: &HistogramData) {
    used((&x.price, &x.count));
}

fn fields_historical_news(x: &HistoricalNews) {
    used((&x.time, &x.provider_code, &x.article_id, &x.headline));
}

fn fields_historical_tick(x: &HistoricalTick) {
    used((&x.time, &x.price, &x.size));
}

fn fields_historical_tick_bid_ask(x: &HistoricalTickBidAsk) {
    used((
        &x.time,
        &x.tick_attrib_bid_ask,
        &x.price_bid,
        &x.price_ask,
        &x.size_bid,
        &x.size_ask,
    ));
}

fn fields_historical_tick_last(x: &HistoricalTickLast) {
    used((
        &x.time,
        &x.tick_attrib_last,
        &x.price,
        &x.size,
        &x.exchange,
        &x.special_conditions,
    ));
}

fn fields_historical_schedule(x: &HistoricalSchedule) {
    used((
        &x.start_date_time,
        &x.end_date_time,
        &x.time_zone,
        &x.sessions,
    ));
}

fn fields_historical_session(x: &HistoricalSession) {
    used((&x.start_date_time, &x.end_date_time, &x.ref_date));
}

fn fields_mkt_depth_data(x: &MktDepthData) {
    used((
        &x.time,
        &x.position,
        &x.market_maker,
        &x.operation,
        &x.side,
        &x.price,
        &x.size,
    ));
}

fn fields_news_article(x: &NewsArticle) {
    used((&x.article_type, &x.article_text));
}

fn fields_news_bulletin(x: &NewsBulletin) {
    used((&x.msg_id, &x.msg_type, &x.message, &x.orig_exchange));
}

fn fields_news_provider(x: &NewsProvider) {
    used((&x.code, &x.name));
}

fn fields_news_tick(x: &NewsTick) {
    used((
        &x.time_stamp,
        &x.provider_code,
        &x.article_id,
        &x.headline,
        &x.extra_data,
    ));
}

fn fields_option_chain(x: &OptionChain) {
    used((
        &x.exchange,
        &x.underlying_con_id,
        &x.trading_class,
        &x.multiplier,
        &x.expirations,
        &x.strikes,
    ));
}

fn fields_option_computation(x: &OptionComputation) {
    used((
        &x.tick_attrib,
        &x.implied_vol,
        &x.delta,
        &x.opt_price,
        &x.pv_dividend,
        &x.gamma,
        &x.vega,
        &x.theta,
        &x.und_price,
    ));
}

fn fields_pnl(x: &PnL) {
    used((
        &x.account,
        &x.model_code,
        &x.daily_pnl,
        &x.unrealized_pnl,
        &x.realized_pnl,
    ));
}

fn fields_pnl_single(x: &PnLSingle) {
    used((
        &x.account,
        &x.model_code,
        &x.con_id,
        &x.daily_pnl,
        &x.unrealized_pnl,
        &x.realized_pnl,
        &x.position,
        &x.value,
    ));
}

fn fields_portfolio_item(x: &PortfolioItem) {
    used((
        &x.contract,
        &x.position,
        &x.market_price,
        &x.market_value,
        &x.average_cost,
        &x.unrealized_pnl,
        &x.realized_pnl,
        &x.account,
    ));
}

fn fields_position(x: &Position) {
    used((&x.account, &x.contract, &x.position, &x.avg_cost));
}

fn fields_price_increment(x: &PriceIncrement) {
    used((&x.low_edge, &x.increment));
}

fn fields_real_time_bar(x: &RealTimeBar) {
    used((
        &x.time,
        &x.end_time,
        &x.open_,
        &x.high,
        &x.low,
        &x.close,
        &x.volume,
        &x.wap,
        &x.count,
    ));
}

fn fields_real_time_bar_list(x: &RealTimeBarList) {
    used((
        &x.bars,
        &x.req_id,
        &x.contract,
        &x.bar_size,
        &x.what_to_show,
        &x.use_rth,
        &x.real_time_bars_options,
    ));
}

fn fields_scan_data_list(x: &ScanDataList) {
    used((
        &x.data,
        &x.req_id,
        &x.subscription,
        &x.scanner_subscription_options,
        &x.scanner_subscription_filter_options,
    ));
}

fn fields_scanner_subscription(x: &ScannerSubscription) {
    used((
        &x.number_of_rows,
        &x.instrument,
        &x.location_code,
        &x.scan_code,
        &x.above_price,
        &x.below_price,
        &x.above_volume,
        &x.market_cap_above,
        &x.market_cap_below,
        &x.moody_rating_above,
        &x.moody_rating_below,
        &x.sp_rating_above,
    ));
    used((
        &x.sp_rating_below,
        &x.maturity_date_above,
        &x.maturity_date_below,
        &x.coupon_rate_above,
        &x.coupon_rate_below,
        &x.exclude_convertible,
        &x.average_option_volume_above,
        &x.scanner_setting_pairs,
        &x.stock_type_filter,
    ));
}

fn fields_smart_component(x: &SmartComponent) {
    used((&x.bit_number, &x.exchange, &x.exchange_letter));
}

fn fields_soft_dollar_tier(x: &SoftDollarTier) {
    used((&x.name, &x.val, &x.display_name));
}

fn fields_tick_attrib(x: &TickAttrib) {
    used((&x.can_auto_execute, &x.past_limit, &x.pre_open));
}

fn fields_tick_attrib_bid_ask(x: &TickAttribBidAsk) {
    used((&x.bid_past_low, &x.ask_past_high));
}

fn fields_tick_attrib_last(x: &TickAttribLast) {
    used((&x.past_limit, &x.unreported));
}

fn fields_tick_by_tick_all_last(x: &TickByTickAllLast) {
    used((
        &x.tick_type,
        &x.time,
        &x.price,
        &x.size,
        &x.tick_attrib_last,
        &x.exchange,
        &x.special_conditions,
    ));
}

fn fields_wsh_event_data(x: &WshEventData) {
    used((
        &x.con_id,
        &x.filter,
        &x.fill_watchlist,
        &x.fill_portfolio,
        &x.fill_competitors,
        &x.start_date,
        &x.end_date,
        &x.total_limit,
    ));
}

fn fields_tick_by_tick_bid_ask(x: &TickByTickBidAsk) {
    used((
        &x.time,
        &x.bid_price,
        &x.ask_price,
        &x.bid_size,
        &x.ask_size,
        &x.tick_attrib_bid_ask,
    ));
}

fn fields_tick_by_tick_mid_point(x: &TickByTickMidPoint) {
    used((&x.time, &x.mid_point));
}

fn fields_tick_data(x: &TickData) {
    used((&x.time, &x.tick_type, &x.price, &x.size));
}

fn fields_trade_log_entry(x: &TradeLogEntry) {
    used((&x.time, &x.status, &x.message, &x.error_code));
}

fn fields_bracket_order(x: &BracketOrder) {
    used((&x.parent, &x.take_profit, &x.stop_loss));
}

fn fields_execution_condition(x: &ExecutionCondition) {
    used((
        &x.cond_type,
        &x.conjunction,
        &x.sec_type,
        &x.exch,
        &x.symbol,
    ));
}

fn fields_margin_condition(x: &MarginCondition) {
    used((&x.cond_type, &x.conjunction, &x.is_more, &x.percent));
}

fn fields_order(x: &Order) {
    used((
        &x.order_id,
        &x.client_id,
        &x.perm_id,
        &x.action,
        &x.total_quantity,
        &x.order_type,
        &x.lmt_price,
        &x.aux_price,
        &x.tif,
        &x.active_start_time,
        &x.active_stop_time,
        &x.oca_group,
    ));
    used((
        &x.oca_type,
        &x.order_ref,
        &x.transmit,
        &x.parent_id,
        &x.block_order,
        &x.sweep_to_fill,
        &x.display_size,
        &x.trigger_method,
        &x.outside_rth,
        &x.hidden,
        &x.good_after_time,
        &x.good_till_date,
    ));
    used((
        &x.rule_80_a,
        &x.all_or_none,
        &x.min_qty,
        &x.percent_offset,
        &x.override_percentage_constraints,
        &x.trail_stop_price,
        &x.trailing_percent,
        &x.fa_group,
        &x.fa_profile,
        &x.fa_method,
        &x.fa_percentage,
        &x.designated_location,
    ));
    used((
        &x.open_close,
        &x.origin,
        &x.short_sale_slot,
        &x.exempt_code,
        &x.discretionary_amt,
        &x.e_trade_only,
        &x.firm_quote_only,
        &x.nbbo_price_cap,
        &x.opt_out_smart_routing,
        &x.auction_strategy,
        &x.starting_price,
        &x.stock_ref_price,
    ));
    used((
        &x.delta,
        &x.stock_range_lower,
        &x.stock_range_upper,
        &x.randomize_price,
        &x.randomize_size,
        &x.volatility,
        &x.volatility_type,
        &x.delta_neutral_order_type,
        &x.delta_neutral_aux_price,
        &x.delta_neutral_con_id,
        &x.delta_neutral_settling_firm,
        &x.delta_neutral_clearing_account,
    ));
    used((
        &x.delta_neutral_clearing_intent,
        &x.delta_neutral_open_close,
        &x.delta_neutral_short_sale,
        &x.delta_neutral_short_sale_slot,
        &x.delta_neutral_designated_location,
        &x.continuous_update,
        &x.reference_price_type,
        &x.basis_points,
        &x.basis_points_type,
        &x.scale_init_level_size,
        &x.scale_subs_level_size,
        &x.scale_price_increment,
    ));
    used((
        &x.scale_price_adjust_value,
        &x.scale_price_adjust_interval,
        &x.scale_profit_offset,
        &x.scale_auto_reset,
        &x.scale_init_position,
        &x.scale_init_fill_qty,
        &x.scale_random_percent,
        &x.scale_table,
        &x.hedge_type,
        &x.hedge_param,
        &x.account,
        &x.settling_firm,
    ));
    used((
        &x.clearing_account,
        &x.clearing_intent,
        &x.algo_strategy,
        &x.algo_params,
        &x.smart_combo_routing_params,
        &x.algo_id,
        &x.what_if,
        &x.not_held,
        &x.solicited,
        &x.model_code,
        &x.order_combo_legs,
        &x.order_misc_options,
    ));
    used((
        &x.reference_contract_id,
        &x.pegged_change_amount,
        &x.is_pegged_change_amount_decrease,
        &x.reference_change_amount,
        &x.reference_exchange_id,
        &x.adjusted_order_type,
        &x.trigger_price,
        &x.adjusted_stop_price,
        &x.adjusted_stop_limit_price,
        &x.adjusted_trailing_amount,
        &x.adjustable_trailing_unit,
        &x.lmt_price_offset,
    ));
    used((
        &x.conditions,
        &x.conditions_cancel_order,
        &x.conditions_ignore_rth,
        &x.ext_operator,
        &x.soft_dollar_tier,
        &x.cash_qty,
        &x.mifid_2_decision_maker,
        &x.mifid_2_decision_algo,
        &x.mifid_2_execution_trader,
        &x.mifid_2_execution_algo,
        &x.dont_use_auto_price_for_hedge,
        &x.is_oms_container,
    ));
    used((
        &x.discretionary_up_to_limit_price,
        &x.auto_cancel_date,
        &x.filled_quantity,
        &x.ref_futures_con_id,
        &x.auto_cancel_parent,
        &x.shareholder,
        &x.imbalance_only,
        &x.route_marketable_to_bbo,
        &x.parent_perm_id,
        &x.use_price_mgmt_algo,
        &x.duration,
        &x.post_to_ats,
    ));
    used((
        &x.advanced_error_override,
        &x.manual_order_time,
        &x.min_trade_qty,
        &x.min_compete_size,
        &x.compete_against_best_offset,
        &x.mid_offset_at_whole,
        &x.mid_offset_at_half,
    ));
}

fn fields_order_combo_leg(x: &OrderComboLeg) {
    used((&x.price,));
}

fn fields_order_state(x: &OrderState) {
    used((
        &x.status,
        &x.init_margin_before,
        &x.maint_margin_before,
        &x.equity_with_loan_before,
        &x.init_margin_change,
        &x.maint_margin_change,
        &x.equity_with_loan_change,
        &x.init_margin_after,
        &x.maint_margin_after,
        &x.equity_with_loan_after,
        &x.commission,
        &x.min_commission,
    ));
    used((
        &x.max_commission,
        &x.commission_currency,
        &x.warning_text,
        &x.completed_time,
        &x.completed_status,
    ));
}

fn fields_order_status(x: &OrderStatus) {
    used((
        &x.order_id,
        &x.status,
        &x.filled,
        &x.remaining,
        &x.avg_fill_price,
        &x.perm_id,
        &x.parent_id,
        &x.last_fill_price,
        &x.client_id,
        &x.why_held,
        &x.mkt_cap_price,
    ));
}

fn fields_percent_change_condition(x: &PercentChangeCondition) {
    used((
        &x.cond_type,
        &x.conjunction,
        &x.is_more,
        &x.change_percent,
        &x.con_id,
        &x.exch,
    ));
}

fn fields_price_condition(x: &PriceCondition) {
    used((
        &x.cond_type,
        &x.conjunction,
        &x.is_more,
        &x.price,
        &x.con_id,
        &x.exch,
        &x.trigger_method,
    ));
}

fn fields_time_condition(x: &TimeCondition) {
    used((&x.cond_type, &x.conjunction, &x.is_more, &x.time));
}

fn fields_trade(x: &Trade) {
    used((
        &x.contract,
        &x.order,
        &x.order_status,
        &x.fills,
        &x.log,
        &x.advanced_error,
    ));
}

fn fields_volume_condition(x: &VolumeCondition) {
    used((
        &x.cond_type,
        &x.conjunction,
        &x.is_more,
        &x.volume,
        &x.con_id,
        &x.exch,
    ));
}

fn fields_ticker(x: &Ticker) {
    used((
        &x.contract,
        &x.time,
        &x.timestamp,
        &x.market_data_type,
        &x.min_tick,
        &x.bid,
        &x.bid_size,
        &x.bid_exchange,
        &x.ask,
        &x.ask_size,
        &x.ask_exchange,
        &x.last,
    ));
    used((
        &x.last_size,
        &x.last_exchange,
        &x.last_timestamp,
        &x.prev_bid,
        &x.prev_bid_size,
        &x.prev_ask,
        &x.prev_ask_size,
        &x.prev_last,
        &x.prev_last_size,
        &x.volume,
        &x.open,
        &x.high,
    ));
    used((
        &x.low,
        &x.close,
        &x.vwap,
        &x.low_13_week,
        &x.high_13_week,
        &x.low_26_week,
        &x.high_26_week,
        &x.low_52_week,
        &x.high_52_week,
        &x.bid_yield,
        &x.ask_yield,
        &x.last_yield,
    ));
    used((
        &x.mark_price,
        &x.halted,
        &x.rt_hist_volatility,
        &x.rt_volume,
        &x.rt_trade_volume,
        &x.rt_time,
        &x.av_volume,
        &x.trade_count,
        &x.trade_rate,
        &x.volume_rate,
        &x.volume_rate_3_min,
        &x.volume_rate_5_min,
    ));
    used((
        &x.volume_rate_10_min,
        &x.shortable,
        &x.shortable_shares,
        &x.index_future_premium,
        &x.futures_open_interest,
        &x.put_open_interest,
        &x.call_open_interest,
        &x.put_volume,
        &x.call_volume,
        &x.av_option_volume,
        &x.hist_volatility,
        &x.implied_volatility,
    ));
    used((
        &x.open_interest,
        &x.last_rth_trade,
        &x.last_reg_time,
        &x.option_bid_exch,
        &x.option_ask_exch,
        &x.bond_factor_multiplier,
        &x.creditman_mark_price,
        &x.creditman_slow_mark_price,
        &x.delayed_last_timestamp,
        &x.delayed_halted,
        &x.reuters_mutual_funds,
        &x.etf_nav_close,
    ));
    used((
        &x.etf_nav_prior_close,
        &x.etf_nav_bid,
        &x.etf_nav_ask,
        &x.etf_nav_last,
        &x.etf_frozen_nav_last,
        &x.etf_nav_high,
        &x.etf_nav_low,
        &x.social_market_analytics,
        &x.estimated_ipo_midpoint,
        &x.final_ipo_last,
        &x.dividends,
        &x.fundamental_ratios,
    ));
    used((
        &x.ticks,
        &x.tick_by_ticks,
        &x.dom_bids,
        &x.dom_bids_dict,
        &x.dom_asks,
        &x.dom_asks_dict,
        &x.dom_ticks,
        &x.bid_greeks,
        &x.ask_greeks,
        &x.last_greeks,
        &x.model_greeks,
        &x.cust_greeks,
    ));
    used((
        &x.bid_efp,
        &x.ask_efp,
        &x.last_efp,
        &x.open_efp,
        &x.high_efp,
        &x.low_efp,
        &x.close_efp,
        &x.auction_volume,
        &x.auction_price,
        &x.auction_imbalance,
        &x.regulatory_imbalance,
        &x.bbo_exchange,
    ));
    used((&x.snapshot_permissions, &x.defaults));
}

fn fields_trading_session(x: &TradingSession) {
    used((&x.start, &x.end));
}

fn fields_bar(x: &Bar) {
    used((
        &x.time, &x.open, &x.high, &x.low, &x.close, &x.volume, &x.count,
    ));
}

fn fields_bar_list(x: &BarList) {
    used((&x.bars,));
}

/// Each type's traits: ib_async's equality, where a Live handle's is identity, and the
/// Debug and Clone every value has.
fn traits() {
    debug_clone::<ComboLeg>();
    eq::<ComboLeg>();
    debug_clone::<Contract>();
    eq::<Contract>();
    debug_clone::<ContractDescription>();
    eq::<ContractDescription>();
    debug_clone::<ContractDetails>();
    eq::<ContractDetails>();
    debug_clone::<DeltaNeutralContract>();
    eq::<DeltaNeutralContract>();
    debug_clone::<ScanData>();
    eq::<ScanData>();
    debug_clone::<TagValue>();
    eq::<TagValue>();
    debug_clone::<IBDefaults>();
    eq::<IBDefaults>();
    debug_clone::<OrderStateNumeric>();
    eq::<OrderStateNumeric>();
    debug_clone::<AccountValue>();
    eq::<AccountValue>();
    debug_clone::<BarData>();
    eq::<BarData>();
    debug_clone::<BarDataList>();
    eq::<Live<BarDataList>>();
    debug_clone::<CommissionReport>();
    eq::<CommissionReport>();
    debug_clone::<ConnectionStats>();
    eq::<ConnectionStats>();
    debug_clone::<DOMLevel>();
    eq::<DOMLevel>();
    debug_clone::<DepthMktDataDescription>();
    eq::<DepthMktDataDescription>();
    debug_clone::<Dividends>();
    eq::<Dividends>();
    debug_clone::<Execution>();
    eq::<Execution>();
    debug_clone::<ExecutionFilter>();
    eq::<ExecutionFilter>();
    debug_clone::<FamilyCode>();
    eq::<FamilyCode>();
    debug_clone::<Fill>();
    eq::<Fill>();
    debug_clone::<FundamentalRatios>();
    eq::<FundamentalRatios>();
    debug_clone::<HistogramData>();
    eq::<HistogramData>();
    debug_clone::<HistoricalNews>();
    eq::<HistoricalNews>();
    debug_clone::<HistoricalTick>();
    eq::<HistoricalTick>();
    debug_clone::<HistoricalTickBidAsk>();
    eq::<HistoricalTickBidAsk>();
    debug_clone::<HistoricalTickLast>();
    eq::<HistoricalTickLast>();
    debug_clone::<HistoricalSchedule>();
    eq::<HistoricalSchedule>();
    debug_clone::<HistoricalSession>();
    eq::<HistoricalSession>();
    debug_clone::<MktDepthData>();
    eq::<MktDepthData>();
    debug_clone::<NewsArticle>();
    eq::<NewsArticle>();
    debug_clone::<NewsBulletin>();
    eq::<NewsBulletin>();
    debug_clone::<NewsProvider>();
    eq::<NewsProvider>();
    debug_clone::<NewsTick>();
    eq::<NewsTick>();
    debug_clone::<OptionChain>();
    eq::<OptionChain>();
    debug_clone::<OptionComputation>();
    eq::<OptionComputation>();
    debug_clone::<PnL>();
    eq::<PnL>();
    debug_clone::<PnLSingle>();
    eq::<PnLSingle>();
    debug_clone::<PortfolioItem>();
    eq::<PortfolioItem>();
    debug_clone::<Position>();
    eq::<Position>();
    debug_clone::<PriceIncrement>();
    eq::<PriceIncrement>();
    debug_clone::<RealTimeBar>();
    eq::<RealTimeBar>();
    debug_clone::<RealTimeBarList>();
    eq::<Live<RealTimeBarList>>();
    debug_clone::<ScanDataList>();
    eq::<Live<ScanDataList>>();
    debug_clone::<ScannerSubscription>();
    eq::<ScannerSubscription>();
    debug_clone::<SmartComponent>();
    eq::<SmartComponent>();
    debug_clone::<SoftDollarTier>();
    eq::<SoftDollarTier>();
    debug_clone::<TickAttrib>();
    eq::<TickAttrib>();
    debug_clone::<TickAttribBidAsk>();
    eq::<TickAttribBidAsk>();
    debug_clone::<TickAttribLast>();
    eq::<TickAttribLast>();
    debug_clone::<TickByTickAllLast>();
    eq::<TickByTickAllLast>();
    debug_clone::<WshEventData>();
    eq::<WshEventData>();
    debug_clone::<TickByTickBidAsk>();
    eq::<TickByTickBidAsk>();
    debug_clone::<TickByTickMidPoint>();
    eq::<TickByTickMidPoint>();
    debug_clone::<TickData>();
    eq::<TickData>();
    debug_clone::<TradeLogEntry>();
    eq::<TradeLogEntry>();
    debug_clone::<BracketOrder>();
    eq::<BracketOrder>();
    debug_clone::<ExecutionCondition>();
    eq::<ExecutionCondition>();
    debug_clone::<MarginCondition>();
    eq::<MarginCondition>();
    debug_clone::<Order>();
    eq::<Live<Order>>();
    debug_clone::<OrderComboLeg>();
    eq::<OrderComboLeg>();
    debug_clone::<OrderCondition>();
    eq::<OrderCondition>();
    debug_clone::<OrderState>();
    eq::<OrderState>();
    debug_clone::<OrderStatus>();
    eq::<OrderStatus>();
    debug_clone::<PercentChangeCondition>();
    eq::<PercentChangeCondition>();
    debug_clone::<PriceCondition>();
    eq::<PriceCondition>();
    debug_clone::<TimeCondition>();
    eq::<TimeCondition>();
    debug_clone::<Trade>();
    eq::<Trade>();
    debug_clone::<VolumeCondition>();
    eq::<VolumeCondition>();
    debug_clone::<Ticker>();
    eq::<Live<Ticker>>();
    debug_clone::<TradingSession>();
    eq::<TradingSession>();
    debug_clone::<Bar>();
    eq::<Bar>();
    eq::<Live<BarList>>();
}

/// Where ib_async's type has no required field, a Rust Default.
fn defaults_exist() {
    default::<ComboLeg>();
    default::<Contract>();
    default::<ContractDescription>();
    default::<ContractDetails>();
    default::<DeltaNeutralContract>();
    default::<IBDefaults>();
    default::<OrderStateNumeric>();
    default::<BarData>();
    default::<CommissionReport>();
    default::<DepthMktDataDescription>();
    default::<Execution>();
    default::<ExecutionFilter>();
    default::<HistogramData>();
    default::<HistoricalSchedule>();
    default::<HistoricalSession>();
    default::<NewsProvider>();
    default::<PnL>();
    default::<PnLSingle>();
    default::<RealTimeBar>();
    default::<ScannerSubscription>();
    default::<SoftDollarTier>();
    default::<TickAttrib>();
    default::<TickAttribBidAsk>();
    default::<TickAttribLast>();
    default::<WshEventData>();
    default::<ExecutionCondition>();
    default::<MarginCondition>();
    default::<Order>();
    default::<OrderComboLeg>();
    default::<OrderState>();
    default::<OrderStatus>();
    default::<PercentChangeCondition>();
    default::<PriceCondition>();
    default::<TimeCondition>();
    default::<Trade>();
    default::<VolumeCondition>();
    default::<Ticker>();
}

/// The methods and class constants of each exported class, and the ticker helpers.
fn members(ib: &IB) {
    used(any::<&Contract>().is_hashable()); // Contract.isHashable
    used(any::<&ContractDetails>().trading_sessions()); // ContractDetails.tradingSessions
    used(any::<&ContractDetails>().liquid_sessions()); // ContractDetails.liquidSessions
    used(any::<&Contract>().pair()); // Forex.pair
    #[cfg(feature = "flex")]
    used(any::<&flex::FlexReport>().topics()); // FlexReport.topics
    #[cfg(feature = "flex")]
    used(any::<&flex::FlexReport>().extract(any(), any())); // FlexReport.extract
    #[cfg(feature = "flex")]
    used(flex::FlexReport::get_url()); // FlexReport.get_url
    #[cfg(feature = "flex")]
    used(flex::FlexReport::download(any(), any())); // FlexReport.download
    #[cfg(feature = "flex")]
    used(flex::FlexReport::load("")); // FlexReport.load
    #[cfg(feature = "flex")]
    used(any::<&flex::FlexReport>().save("")); // FlexReport.save
    used(OrderCondition::create_class(any())); // OrderCondition.createClass
    used(any::<OrderCondition>().and()); // OrderCondition.And
    used(any::<OrderCondition>().or()); // OrderCondition.Or
    used(any::<&OrderState>().transform(|_| ())); // OrderState.transform
    used(any::<&OrderState>().numeric(any())); // OrderState.numeric
    used(any::<&OrderState>().formatted(any())); // OrderState.formatted
    used(any::<&OrderStatus>().total()); // OrderStatus.total
    used(OrderStatus::PENDING_SUBMIT); // OrderStatus.PendingSubmit
    used(OrderStatus::PENDING_CANCEL); // OrderStatus.PendingCancel
    used(OrderStatus::PRE_SUBMITTED); // OrderStatus.PreSubmitted
    used(OrderStatus::SUBMITTED); // OrderStatus.Submitted
    used(OrderStatus::API_PENDING); // OrderStatus.ApiPending
    used(OrderStatus::API_CANCELLED); // OrderStatus.ApiCancelled
    used(OrderStatus::API_UPDATE); // OrderStatus.ApiUpdate
    used(OrderStatus::CANCELLED); // OrderStatus.Cancelled
    used(OrderStatus::FILLED); // OrderStatus.Filled
    used(OrderStatus::INACTIVE); // OrderStatus.Inactive
    used(OrderStatus::VALIDATION_ERROR); // OrderStatus.ValidationError
    used(OrderStatus::DONE_STATES); // OrderStatus.DoneStates
    used(OrderStatus::ACTIVE_STATES); // OrderStatus.ActiveStates
    used(OrderStatus::WAITING_STATES); // OrderStatus.WaitingStates
    used(OrderStatus::WORKING_STATES); // OrderStatus.WorkingStates
    used(any::<&Trade>().is_waiting()); // Trade.isWaiting
    used(any::<&Trade>().is_working()); // Trade.isWorking
    used(any::<&Trade>().is_active()); // Trade.isActive
    used(any::<&Trade>().is_done()); // Trade.isDone
    used(any::<&Trade>().filled()); // Trade.filled
    used(any::<&Trade>().remaining()); // Trade.remaining
    used(any::<&Ticker>().is_unset(any())); // Ticker.isUnset
    used(any::<&Ticker>().has_bid_ask()); // Ticker.hasBidAsk
    used(any::<&Ticker>().midpoint()); // Ticker.midpoint
    used(any::<&Ticker>().market_price()); // Ticker.marketPrice
    used(StartupFetch::POSITIONS); // StartupFetch.POSITIONS
    used(StartupFetch::ORDERS_OPEN); // StartupFetch.ORDERS_OPEN
    used(StartupFetch::ORDERS_COMPLETE); // StartupFetch.ORDERS_COMPLETE
    used(StartupFetch::ACCOUNT_UPDATES); // StartupFetch.ACCOUNT_UPDATES
    used(StartupFetch::SUB_ACCOUNT_UPDATES); // StartupFetch.SUB_ACCOUNT_UPDATES
    used(StartupFetch::EXECUTIONS); // StartupFetch.EXECUTIONS
    used(ib.tickers()[0].update_event().trades()); // TickerUpdateEvent.trades
    used(ib.tickers()[0].update_event().bids()); // TickerUpdateEvent.bids
    used(ib.tickers()[0].update_event().asks()); // TickerUpdateEvent.asks
    used(ib.tickers()[0].update_event().bidasks()); // TickerUpdateEvent.bidasks
    used(ib.tickers()[0].update_event().midpoints()); // TickerUpdateEvent.midpoints
    used(any::<&TickFilter>().timebars(any())); // Tickfilter.timebars
    used(any::<&TickFilter>().tickbars(any())); // Tickfilter.tickbars
    used(any::<&TickFilter>().volumebars(any())); // Tickfilter.volumebars
    let a: OptionComputation = any();
    used((a + a, a - a, a * 2.0)); // OptionComputation.__add__, __sub__, __mul__
    #[cfg(feature = "flex")]
    used((flex::FLEXREPORT_URL, any::<&flex::FlexReport>().root())); // FLEXREPORT_URL, root
}

/// The Wrapper's fields a program reads, as the accessors that give them.
fn wrapper_fields(ib: &IB) {
    used(ib.account_values(any())); // Wrapper.accountValues
    used(ib.account_summary(any())); // Wrapper.acctSummary
    used(ib.portfolio(any())); // Wrapper.portfolio
    used(ib.positions(any())); // Wrapper.positions
    used(ib.trades()); // Wrapper.trades
    used(ib.fills()); // Wrapper.fills
    used(ib.news_ticks()); // Wrapper.newsTicks
    used(ib.news_bulletins()); // Wrapper.msgId2NewsBulletin
    used(ib.tickers()); // Wrapper.tickers
    used(ib.pending_tickers()); // Wrapper.pendingTickers
    used(ib.realtime_bars()); // Wrapper.reqId2Subscriber
    used(ib.pnl(any(), any())); // Wrapper.reqId2PnL
    used(ib.pnl_single(any(), any(), any())); // Wrapper.reqId2PnlSingle
    used(ib.managed_accounts()); // Wrapper.accounts
    used(ib.client().client_id()); // Wrapper.clientId
}

/// `ib.client`: ib_async's Client surface.
fn client(c: &Client) {
    used(c.reset()); // Client.reset
    used(c.server_version()); // Client.serverVersion
    used(c.run()); // Client.run
    used(c.is_connected()); // Client.isConnected
    used(c.is_ready()); // Client.isReady
    used(c.connection_stats()); // Client.connectionStats
    used(c.get_req_id()); // Client.getReqId
    used(c.update_req_id(any())); // Client.updateReqId
    used(c.get_accounts()); // Client.getAccounts
    used(c.connect(any(), any(), any())); // Client.connect
    used(c.connect_async(any(), any(), any())); // Client.connectAsync
    used(c.disconnect()); // Client.disconnect
    used(c.req_mkt_data(any(), any(), any(), any(), any(), any())); // Client.reqMktData
    used(c.cancel_mkt_data(any())); // Client.cancelMktData
    used(c.place_order(any(), any(), any())); // Client.placeOrder
    used(c.cancel_order(any(), any())); // Client.cancelOrder
    used(c.req_open_orders()); // Client.reqOpenOrders
    used(c.req_account_updates(any(), any())); // Client.reqAccountUpdates
    used(c.req_executions(any(), any())); // Client.reqExecutions
    used(c.req_ids(any())); // Client.reqIds
    used(c.req_contract_details(any(), any())); // Client.reqContractDetails
    used(c.req_mkt_depth(any(), any(), any(), any(), any())); // Client.reqMktDepth
    used(c.cancel_mkt_depth(any(), any())); // Client.cancelMktDepth
    used(c.req_news_bulletins(any())); // Client.reqNewsBulletins
    used(c.cancel_news_bulletins()); // Client.cancelNewsBulletins
    used(c.set_server_log_level(any())); // Client.setServerLogLevel
    used(c.req_auto_open_orders(any())); // Client.reqAutoOpenOrders
    used(c.req_all_open_orders()); // Client.reqAllOpenOrders
    used(c.req_managed_accts()); // Client.reqManagedAccts
    used(c.request_fa(any())); // Client.requestFA
    used(c.replace_fa(any(), any(), any())); // Client.replaceFA
    used(c.req_historical_data(
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
    )); // Client.reqHistoricalData
    used(c.exercise_options(any(), any(), any(), any(), any(), any())); // Client.exerciseOptions
    used(c.req_scanner_subscription(any(), any(), any(), any())); // Client.reqScannerSubscription
    used(c.cancel_scanner_subscription(any())); // Client.cancelScannerSubscription
    used(c.req_scanner_parameters()); // Client.reqScannerParameters
    used(c.cancel_historical_data(any())); // Client.cancelHistoricalData
    used(c.req_current_time()); // Client.reqCurrentTime
    used(c.req_real_time_bars(any(), any(), any(), any(), any(), any())); // Client.reqRealTimeBars
    used(c.cancel_real_time_bars(any())); // Client.cancelRealTimeBars
    used(c.req_fundamental_data(any(), any(), any(), any())); // Client.reqFundamentalData
    used(c.cancel_fundamental_data(any())); // Client.cancelFundamentalData
    used(c.calculate_implied_volatility(any(), any(), any(), any(), any())); // Client.calculateImpliedVolatility
    used(c.calculate_option_price(any(), any(), any(), any(), any())); // Client.calculateOptionPrice
    used(c.cancel_calculate_implied_volatility(any())); // Client.cancelCalculateImpliedVolatility
    used(c.cancel_calculate_option_price(any())); // Client.cancelCalculateOptionPrice
    used(c.req_global_cancel()); // Client.reqGlobalCancel
    used(c.req_market_data_type(any())); // Client.reqMarketDataType
    used(c.req_positions()); // Client.reqPositions
    used(c.req_account_summary(any(), any(), any())); // Client.reqAccountSummary
    used(c.cancel_account_summary(any())); // Client.cancelAccountSummary
    used(c.cancel_positions()); // Client.cancelPositions
    used(c.query_display_groups(any())); // Client.queryDisplayGroups
    used(c.subscribe_to_group_events(any(), any())); // Client.subscribeToGroupEvents
    used(c.update_display_group(any(), any())); // Client.updateDisplayGroup
    used(c.unsubscribe_from_group_events(any())); // Client.unsubscribeFromGroupEvents
    used(c.req_positions_multi(any(), any(), any())); // Client.reqPositionsMulti
    used(c.cancel_positions_multi(any())); // Client.cancelPositionsMulti
    used(c.req_account_updates_multi(any(), any(), any(), any())); // Client.reqAccountUpdatesMulti
    used(c.cancel_account_updates_multi(any())); // Client.cancelAccountUpdatesMulti
    used(c.req_sec_def_opt_params(any(), any(), any(), any(), any())); // Client.reqSecDefOptParams
    used(c.req_soft_dollar_tiers(any())); // Client.reqSoftDollarTiers
    used(c.req_family_codes()); // Client.reqFamilyCodes
    used(c.req_matching_symbols(any(), any())); // Client.reqMatchingSymbols
    used(c.req_mkt_depth_exchanges()); // Client.reqMktDepthExchanges
    used(c.req_smart_components(any(), any())); // Client.reqSmartComponents
    used(c.req_news_article(any(), any(), any(), any())); // Client.reqNewsArticle
    used(c.req_news_providers()); // Client.reqNewsProviders
    used(c.req_historical_news(any(), any(), any(), any(), any(), any(), any())); // Client.reqHistoricalNews
    used(c.req_head_time_stamp(any(), any(), any(), any(), any())); // Client.reqHeadTimeStamp
    used(c.req_histogram_data(any(), any(), any(), any())); // Client.reqHistogramData
    used(c.cancel_histogram_data(any())); // Client.cancelHistogramData
    used(c.cancel_head_time_stamp(any())); // Client.cancelHeadTimeStamp
    used(c.req_market_rule(any())); // Client.reqMarketRule
    used(c.req_pnl(any(), any(), any())); // Client.reqPnL
    used(c.cancel_pnl(any())); // Client.cancelPnL
    used(c.req_pnl_single(any(), any(), any(), any())); // Client.reqPnLSingle
    used(c.cancel_pnl_single(any())); // Client.cancelPnLSingle
    used(c.req_historical_ticks(
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
    )); // Client.reqHistoricalTicks
    used(c.req_tick_by_tick_data(any(), any(), any(), any(), any())); // Client.reqTickByTickData
    used(c.cancel_tick_by_tick_data(any())); // Client.cancelTickByTickData
    used(c.req_completed_orders(any())); // Client.reqCompletedOrders
    used(c.req_wsh_meta_data(any())); // Client.reqWshMetaData
    used(c.cancel_wsh_meta_data(any())); // Client.cancelWshMetaData
    used(c.req_wsh_event_data(any(), any())); // Client.reqWshEventData
    used(c.cancel_wsh_event_data(any())); // Client.cancelWshEventData
    used(c.req_user_info(any())); // Client.reqUserInfo
    used(Client::EVENTS);
    used(ConnState::Disconnected);
    used(ConnState::Connecting);
    used(ConnState::Connected);
    used(c.api_start()); // Client.apiStart
    used(c.api_end()); // Client.apiEnd
    used(c.api_error()); // Client.apiError
    used(c.client_id()); // Client.clientId
    used(c.conn_state()); // Client.connState
}

/// util's names.
fn util_names() {
    used(util::global_error_event()); // util.globalErrorEvent
    used(&util::EPOCH); // util.EPOCH
    used(&util::UNSET_INTEGER); // util.UNSET_INTEGER
    used(&util::UNSET_DOUBLE); // util.UNSET_DOUBLE
    used(any::<util::TimeT>()); // util.Time_t
    used(util::format_si(any())); // util.formatSI
    used(any::<&IBHandle>().run_until(std::future::ready(()), any())); // util.run
    used(IB::schedule(Timestamp::UNIX_EPOCH, || {})); // util.schedule
    used(IB::sleep(any())); // util.sleep
    used(IB::time_range(
        Timestamp::UNIX_EPOCH,
        Timestamp::UNIX_EPOCH,
        any(),
    )); // util.timeRange
    used(IB::wait_until(Timestamp::UNIX_EPOCH)); // util.waitUntil
    used(IB::time_range_async(
        Timestamp::UNIX_EPOCH,
        Timestamp::UNIX_EPOCH,
        any(),
    )); // util.timeRangeAsync
    used(IB::wait_until_async(Timestamp::UNIX_EPOCH)); // util.waitUntilAsync
    used(util::format_ib_datetime("")); // util.formatIBDatetime
    used(util::parse_ib_datetime(any())); // util.parseIBDatetime
}

/// eventkit's `Event`, member by member.
fn event(e: &Event<()>) {
    used(e.error_event()); // Event.error_event
    used(e.done_event()); // Event.done_event
    used(Event::<()>::new("name")); // Event.__post_init__
    used(e.name()); // Event.name
    used(e.done()); // Event.done
    used(e.set_done()); // Event.set_done
    used(e.value()); // Event.value
    used(e.connect(|_| {})); // Event.connect
    used(e.disconnect(any())); // Event.disconnect
    used(e.emit(&())); // Event.emit
    used(e.emit(&())); // Event.emit_threadsafe
    used(e.clear()); // Event.clear
    used(e.subscribe().recv_async()); // Event.aiter
    used(e.connect(|_| {})); // Event.__iadd__
    used(e.disconnect(any())); // Event.__isub__
    used(e.emit(&())); // Event.__call__
    used(format!("{e:?}")); // Event.__repr__
    used(e.len()); // Event.__len__
    used(e.subscribe().recv()); // Event.__await__
    used(e.subscribe().try_recv()); // Event.__aiter__
    used(e.contains(any())); // Event.__contains__
    used(e.connect_async(|_| async {}));
    let mut s = e.subscribe();
    used((s.recv_timeout(any()), s.next()));
}

/// The methods beyond ib_async's API.
fn extras(ib: &IB) {
    used(ib.req_mkt_data_ex(any(), any(), any(), any(), any(), any())); // reqMktDataEx
    used(ib.req_current_time_in_millis()); // reqCurrentTimeInMillis
    used(ib.req_current_time_in_millis_async()); // reqCurrentTimeInMillisAsync
    used(ib.req_corporate_actions(any(), any(), any(), any())); // reqCorporateActions
    used(ib.req_corporate_actions_async(any(), any(), any(), any())); // reqCorporateActionsAsync
    used(ib.req_spread_scan(any(), any(), any())); // reqSpreadScan
    used(ib.req_spread_scan_async(any(), any(), any())); // reqSpreadScanAsync
    used(ib.ticker_extras(any())); // tickerExtras
    used(ib.option_model(any())); // optionModel
    used(ib.closing_option_model(any())); // closingOptionModel
    used(ib.company_data(any())); // companyData
    used(ib.enabled_features()); // enabledFeatures
    used(ib.order_permissions()); // orderPermissions
    used(ib.permitted_order_types(any())); // permittedOrderTypes
    used(ib.algorithms()); // algorithms
    used(ib.algorithms_for(any())); // algorithmsFor
    used(ib.order_presets()); // orderPresets
    used(ib.positions_elsewhere()); // positionsElsewhere
    used(ib.account_values_elsewhere(any())); // accountValuesElsewhere
    used(ib.competing_session()); // competingSession
    used(ib.req_ping()); // reqPing
    used(ib.last_rtt()); // lastRtt
}

/// Typed signatures: a changed parameter or result fails to compile.
fn signatures() {
    let _: fn(&IBHandle, ConnectOptions) -> Result<()> = IBHandle::connect;
    let _: fn(&IBHandle) -> Option<String> = IBHandle::disconnect;
    let _: fn(&IBHandle) -> bool = IBHandle::is_connected;
    let _: fn(&IBHandle, Option<Duration>) -> Result<bool> = IBHandle::wait_on_update;
    let _: fn(&IBHandle, Option<Duration>) = IBHandle::set_timeout;
    let _: fn(&IBHandle) -> Result<()> = IBHandle::run;
    let _: fn(Duration) -> Result<bool> = IB::sleep;
    let _: fn(Timestamp) -> Result<bool> = IB::wait_until;
    let _: fn(Timestamp, fn()) -> Result<TimerHandle> = IB::schedule;
    {
        let p0: &IBHandle = any();
        let p1: ConnectOptions = any();
        fn out<F: Future<Output = Result<()>> + Send>(_: F) {}
        out(IBHandle::connect_async(p0, p1));
    }
    let _: fn(&IBHandle, &str) -> Vec<AccountValue> = IBHandle::account_values;
    let _: fn(&IBHandle) -> Vec<Live<Trade>> = IBHandle::trades;
    let _: fn(&IBHandle) -> Vec<Live<Order>> = IBHandle::open_orders;
    let _: fn(&IBHandle, &Contract) -> Result<Option<Live<Ticker>>> = IBHandle::ticker;
    let _: fn(&IBHandle) -> Vec<Bars> = IBHandle::realtime_bars;
    let _: fn(&IBHandle, &Contract, &Live<Order>) -> Result<Live<Trade>> = IBHandle::place_order;
    let _: fn(&IBHandle, &Contract, &Order) -> Result<OrderState> = IBHandle::what_if_order;
    let _: fn(&IBHandle, &Contract, &Order) -> Pending<OrderState> = IBHandle::what_if_order_async;
    let _: fn(&IBHandle) -> Result<Vec<Live<Trade>>> = IBHandle::req_open_orders;
    let _: fn(&IBHandle) -> Pending<Vec<Live<Trade>>> = IBHandle::req_open_orders_async;
    let _: fn(&IBHandle, Option<&ExecutionFilter>) -> Result<Vec<Fill>> = IBHandle::req_executions;
    let _: fn(&IBHandle) -> Result<()> = IBHandle::req_global_cancel;
    let _: fn(&IBHandle, &str) -> Result<()> = IBHandle::req_account_updates;
    let _: fn(&IBHandle, &str) -> Pending<()> = IBHandle::req_account_updates_async;
    let _: fn(&IBHandle) -> Result<Vec<Position>> = IBHandle::req_positions;
    let _: fn(&IBHandle, &str, &str) -> Result<Live<PnL>> = IBHandle::req_pnl;
    let _: fn(&IBHandle, &str) -> Result<Vec<AccountValue>> = IBHandle::account_summary;
    {
        let p0: &IBHandle = any();
        let p1: &str = any();
        fn out<F: Future<Output = Result<Vec<AccountValue>>> + Send>(_: F) {}
        out(IBHandle::account_summary_async(p0, p1));
    }
    let _: fn(&IBHandle, &Contract, &str, bool, bool, &[TagValue]) -> Result<Live<Ticker>> =
        IBHandle::req_mkt_data;
    let _: fn(&IBHandle, &Contract) -> Result<bool> = IBHandle::cancel_mkt_data;
    let _: fn(&IBHandle, i32) -> Result<Option<Vec<PriceIncrement>>> = IBHandle::req_market_rule;
    {
        let p0: &IBHandle = any();
        let p1: i32 = any();
        fn out<F: Future<Output = Result<Option<Vec<PriceIncrement>>>> + Send>(_: F) {}
        out(IBHandle::req_market_rule_async(p0, p1));
    }
    let _: fn(&IBHandle, &Contract, f64, f64, &[TagValue]) -> Result<Option<OptionComputation>> =
        IBHandle::calculate_implied_volatility;
    let _: fn(&IBHandle, &Contract, i32, &str, bool, &[TagValue]) -> Result<Live<RealTimeBarList>> =
        IBHandle::req_real_time_bars;
    let _: fn(&IBHandle, &mut [Contract]) -> Result<Vec<Qualified>> = IBHandle::qualify_contracts;
    {
        let p0: &IBHandle = any();
        let p1: &mut [Contract] = any();
        let p2: bool = any();
        fn out<F: Future<Output = Result<Vec<Qualified>>> + Send>(_: F) {}
        out(IBHandle::qualify_contracts_async(p0, p1, p2));
    }
    let _: fn(&IBHandle, &Contract) -> Result<Vec<ContractDetails>> =
        IBHandle::req_contract_details;
    let _: fn(&IBHandle, &Contract) -> Pending<Vec<ContractDetails>> =
        IBHandle::req_contract_details_async;
    let _: fn(&IBHandle, &str) -> Result<Option<Vec<ContractDescription>>> =
        IBHandle::req_matching_symbols;
    let _: fn(
        &IBHandle,
        &Contract,
        String,
        &str,
        &str,
        &str,
        bool,
        i32,
        bool,
        &[TagValue],
        Option<Duration>,
    ) -> Result<Live<BarDataList>> = IBHandle::req_historical_data;
    let _: fn(&IBHandle, &Contract, &str, bool, i32) -> Result<BarDate> =
        IBHandle::req_head_time_stamp;
    let _: fn(&IBHandle, &Live<ScanDataList>) -> Result<()> = IBHandle::cancel_scanner_subscription;
    let _: fn(&IBHandle) -> Result<Zoned> = IBHandle::req_current_time;
    let _: fn(&IBHandle) -> Pending<Zoned> = IBHandle::req_current_time_async;
    let _: fn(&IBHandle, i32) -> Result<Option<String>> = IBHandle::request_fa;
    let _: fn(&IBHandle, &Contract, &str, &str, Option<Duration>) -> Result<Vec<CorporateAction>> =
        IBHandle::req_corporate_actions;
    let _: fn(&IBHandle, &Contract, &SpreadScan, Option<Duration>) -> Result<Vec<ScannedStrategy>> =
        IBHandle::req_spread_scan;
    let _: fn(&IBHandle) -> Pending<i64> = IBHandle::req_current_time_in_millis_async;
    let _: fn(&Client, EClientConfig, i64, Option<Duration>) -> Result<()> = Client::connect;
    let _: fn(&Client) -> Result<i64> = Client::get_req_id;
    let _: fn(&Client) -> ConnState = Client::conn_state;
}

/// Every default with an ib_async counterpart holds ib_async's value.
#[test]
fn defaults() {
    let c = IBConfig::default();
    assert!(c.request_timeout.is_none(), "RequestTimeout 0: no limit");
    assert!(!c.raise_request_errors, "c.raise_request_errors");
    assert_eq!(c.max_synced_sub_accounts, 50, "c.max_synced_sub_accounts");
    assert!(c.timezone_tws.is_none(), "TimezoneTWS '': no zone");
    let o = ConnectOptions::default();
    assert_eq!(o.client_id, 1, "o.client_id");
    assert_eq!(o.timeout, Some(Duration::from_secs(4)), "timeout");
    assert!(!o.config.readonly, "o.config.readonly");
    assert_eq!(o.account, "", "o.account");
    assert!(!o.raise_sync_errors, "o.raise_sync_errors");
    assert!(o.fetch_fields == StartupFetch::ALL, "fetchFields");
    assert_eq!(HISTORICAL_TIMEOUT, Duration::from_secs(60));
    // No ib_async counterpart: the engine's own 15 s, and the spread scan's 10 s.
    assert_eq!(CORPORATE_ACTIONS_TIMEOUT, Duration::from_secs(15));
    assert_eq!(SPREAD_SCAN_TIMEOUT, Duration::from_secs(10));
    let d = ComboLeg::default();
    assert_eq!(d.con_id, 0, "d.con_id");
    assert_eq!(d.ratio, 0, "d.ratio");
    assert_eq!(d.action, "", "d.action");
    assert_eq!(d.exchange, "", "d.exchange");
    assert_eq!(d.open_close, 0, "d.open_close");
    assert_eq!(d.short_sale_slot, 0, "d.short_sale_slot");
    assert_eq!(d.designated_location, "", "d.designated_location");
    assert_eq!(d.exempt_code, -1, "d.exempt_code");
    let d = Contract::default();
    assert_eq!(d.sec_type, "", "d.sec_type");
    assert_eq!(d.con_id, 0, "d.con_id");
    assert_eq!(d.symbol, "", "d.symbol");
    assert_eq!(
        d.last_trade_date_or_contract_month, "",
        "d.last_trade_date_or_contract_month"
    );
    assert_eq!(d.strike, 0.0, "d.strike");
    assert_eq!(d.right, "", "d.right");
    assert_eq!(d.multiplier, "", "d.multiplier");
    assert_eq!(d.exchange, "", "d.exchange");
    assert_eq!(d.primary_exchange, "", "d.primary_exchange");
    assert_eq!(d.currency, "", "d.currency");
    assert_eq!(d.local_symbol, "", "d.local_symbol");
    assert_eq!(d.trading_class, "", "d.trading_class");
    assert!(!d.include_expired, "d.include_expired");
    assert_eq!(d.sec_id_type, "", "d.sec_id_type");
    assert_eq!(d.sec_id, "", "d.sec_id");
    assert_eq!(d.description, "", "d.description");
    assert_eq!(d.issuer_id, "", "d.issuer_id");
    assert_eq!(d.combo_legs_descrip, "", "d.combo_legs_descrip");
    assert!(d.combo_legs.is_empty(), "d.combo_legs");
    assert!(
        d.delta_neutral_contract.is_none(),
        "d.delta_neutral_contract"
    );
    let d = ContractDescription::default();
    assert!(d.contract.is_none(), "d.contract");
    assert!(d.derivative_sec_types.is_empty(), "d.derivative_sec_types");
    let d = ContractDetails::default();
    assert!(d.contract.is_none(), "d.contract");
    assert_eq!(d.market_name, "", "d.market_name");
    assert_eq!(d.min_tick, 0.0, "d.min_tick");
    assert_eq!(d.order_types, "", "d.order_types");
    assert_eq!(d.valid_exchanges, "", "d.valid_exchanges");
    assert_eq!(d.price_magnifier, 0, "d.price_magnifier");
    assert_eq!(d.under_con_id, 0, "d.under_con_id");
    assert_eq!(d.long_name, "", "d.long_name");
    assert_eq!(d.contract_month, "", "d.contract_month");
    assert_eq!(d.industry, "", "d.industry");
    assert_eq!(d.category, "", "d.category");
    assert_eq!(d.subcategory, "", "d.subcategory");
    assert_eq!(d.time_zone_id, "", "d.time_zone_id");
    assert_eq!(d.trading_hours, "", "d.trading_hours");
    assert_eq!(d.liquid_hours, "", "d.liquid_hours");
    assert_eq!(d.ev_rule, "", "d.ev_rule");
    assert_eq!(d.ev_multiplier, 0.0, "d.ev_multiplier");
    assert_eq!(d.md_size_multiplier, 1, "d.md_size_multiplier");
    assert_eq!(d.agg_group, 0, "d.agg_group");
    assert_eq!(d.under_symbol, "", "d.under_symbol");
    assert_eq!(d.under_sec_type, "", "d.under_sec_type");
    assert_eq!(d.market_rule_ids, "", "d.market_rule_ids");
    assert!(d.sec_id_list.is_empty(), "d.sec_id_list");
    assert_eq!(d.real_expiration_date, "", "d.real_expiration_date");
    assert_eq!(d.last_trade_time, "", "d.last_trade_time");
    assert_eq!(d.stock_type, "", "d.stock_type");
    assert_eq!(d.min_size, 0.0, "d.min_size");
    assert_eq!(d.size_increment, 0.0, "d.size_increment");
    assert_eq!(
        d.suggested_size_increment, 0.0,
        "d.suggested_size_increment"
    );
    assert_eq!(d.cusip, "", "d.cusip");
    assert_eq!(d.ratings, "", "d.ratings");
    assert_eq!(d.desc_append, "", "d.desc_append");
    assert_eq!(d.bond_type, "", "d.bond_type");
    assert_eq!(d.coupon_type, "", "d.coupon_type");
    assert!(!d.callable, "d.callable");
    assert!(!d.putable, "d.putable");
    assert_eq!(d.coupon, 0.0, "d.coupon");
    assert!(!d.convertible, "d.convertible");
    assert_eq!(d.maturity, "", "d.maturity");
    assert_eq!(d.issue_date, "", "d.issue_date");
    assert_eq!(d.next_option_date, "", "d.next_option_date");
    assert_eq!(d.next_option_type, "", "d.next_option_type");
    assert!(!d.next_option_partial, "d.next_option_partial");
    assert_eq!(d.notes, "", "d.notes");
    let d = DeltaNeutralContract::default();
    assert_eq!(d.con_id, 0, "d.con_id");
    assert_eq!(d.delta, 0.0, "d.delta");
    assert_eq!(d.price, 0.0, "d.price");
    let d = IBDefaults::default();
    assert_eq!(d.empty_price, -1.0, "d.empty_price");
    assert_eq!(d.empty_size, 0.0, "d.empty_size");
    assert!(d.unset.is_nan(), "d.unset");
    assert_eq!(d.timezone, TimeZone::UTC, "d.timezone");
    let d = OrderStateNumeric::default();
    assert_eq!(d.status, "", "d.status");
    assert!(
        d.init_margin_before.is_some_and(f64::is_nan),
        "d.init_margin_before"
    );
    assert!(
        d.maint_margin_before.is_some_and(f64::is_nan),
        "d.maint_margin_before"
    );
    assert!(
        d.equity_with_loan_before.is_some_and(f64::is_nan),
        "d.equity_with_loan_before"
    );
    assert!(
        d.init_margin_change.is_some_and(f64::is_nan),
        "d.init_margin_change"
    );
    assert!(
        d.maint_margin_change.is_some_and(f64::is_nan),
        "d.maint_margin_change"
    );
    assert!(
        d.equity_with_loan_change.is_some_and(f64::is_nan),
        "d.equity_with_loan_change"
    );
    assert!(
        d.init_margin_after.is_some_and(f64::is_nan),
        "d.init_margin_after"
    );
    assert!(
        d.maint_margin_after.is_some_and(f64::is_nan),
        "d.maint_margin_after"
    );
    assert!(
        d.equity_with_loan_after.is_some_and(f64::is_nan),
        "d.equity_with_loan_after"
    );
    assert!(d.commission.is_none(), "d.commission");
    assert!(d.min_commission.is_none(), "d.min_commission");
    assert!(d.max_commission.is_none(), "d.max_commission");
    assert_eq!(d.commission_currency, "", "d.commission_currency");
    assert_eq!(d.warning_text, "", "d.warning_text");
    assert_eq!(d.completed_time, "", "d.completed_time");
    assert_eq!(d.completed_status, "", "d.completed_status");
    let d = BarData::default();
    assert!(
        matches!(&d.date, BarDate::At(t) if t.timestamp() == Timestamp::UNIX_EPOCH),
        "d.date"
    );
    assert_eq!(d.open, 0.0, "d.open");
    assert_eq!(d.high, 0.0, "d.high");
    assert_eq!(d.low, 0.0, "d.low");
    assert_eq!(d.close, 0.0, "d.close");
    assert_eq!(d.volume, 0.0, "d.volume");
    assert_eq!(d.average, 0.0, "d.average");
    assert_eq!(d.bar_count, 0, "d.bar_count");
    let d = CommissionReport::default();
    assert_eq!(d.exec_id, "", "d.exec_id");
    assert_eq!(d.commission, 0.0, "d.commission");
    assert_eq!(d.currency, "", "d.currency");
    assert_eq!(d.realized_pnl, 0.0, "d.realized_pnl");
    assert_eq!(d.yield_, 0.0, "d.yield_");
    assert_eq!(d.yield_redemption_date, 0, "d.yield_redemption_date");
    let d = DepthMktDataDescription::default();
    assert_eq!(d.exchange, "", "d.exchange");
    assert_eq!(d.sec_type, "", "d.sec_type");
    assert_eq!(d.listing_exch, "", "d.listing_exch");
    assert_eq!(d.service_data_type, "", "d.service_data_type");
    assert!(d.agg_group.is_none(), "d.agg_group");
    let d = Execution::default();
    assert_eq!(d.exec_id, "", "d.exec_id");
    assert_eq!(d.time.timestamp(), Timestamp::UNIX_EPOCH, "d.time");
    assert_eq!(d.acct_number, "", "d.acct_number");
    assert_eq!(d.exchange, "", "d.exchange");
    assert_eq!(d.side, "", "d.side");
    assert_eq!(d.shares, 0.0, "d.shares");
    assert_eq!(d.price, 0.0, "d.price");
    assert_eq!(d.perm_id, 0, "d.perm_id");
    assert_eq!(d.client_id, 0, "d.client_id");
    assert_eq!(d.order_id, 0, "d.order_id");
    assert_eq!(d.liquidation, 0, "d.liquidation");
    assert_eq!(d.cum_qty, 0.0, "d.cum_qty");
    assert_eq!(d.avg_price, 0.0, "d.avg_price");
    assert_eq!(d.order_ref, "", "d.order_ref");
    assert_eq!(d.ev_rule, "", "d.ev_rule");
    assert_eq!(d.ev_multiplier, 0.0, "d.ev_multiplier");
    assert_eq!(d.model_code, "", "d.model_code");
    assert_eq!(d.last_liquidity, 0, "d.last_liquidity");
    assert!(!d.pending_price_revision, "d.pending_price_revision");
    let d = ExecutionFilter::default();
    assert_eq!(d.client_id, 0, "d.client_id");
    assert_eq!(d.acct_code, "", "d.acct_code");
    assert_eq!(d.time, "", "d.time");
    assert_eq!(d.symbol, "", "d.symbol");
    assert_eq!(d.sec_type, "", "d.sec_type");
    assert_eq!(d.exchange, "", "d.exchange");
    assert_eq!(d.side, "", "d.side");
    let d = HistogramData::default();
    assert_eq!(d.price, 0.0, "d.price");
    assert_eq!(d.count, 0, "d.count");
    let d = HistoricalSchedule::default();
    assert_eq!(d.start_date_time, "", "d.start_date_time");
    assert_eq!(d.end_date_time, "", "d.end_date_time");
    assert_eq!(d.time_zone, "", "d.time_zone");
    assert!(d.sessions.is_empty(), "d.sessions");
    let d = HistoricalSession::default();
    assert_eq!(d.start_date_time, "", "d.start_date_time");
    assert_eq!(d.end_date_time, "", "d.end_date_time");
    assert_eq!(d.ref_date, "", "d.ref_date");
    let d = NewsProvider::default();
    assert_eq!(d.code, "", "d.code");
    assert_eq!(d.name, "", "d.name");
    let d = PnL::default();
    assert_eq!(d.account, "", "d.account");
    assert_eq!(d.model_code, "", "d.model_code");
    assert!(d.daily_pnl.is_nan(), "d.daily_pnl");
    assert!(d.unrealized_pnl.is_nan(), "d.unrealized_pnl");
    assert!(d.realized_pnl.is_nan(), "d.realized_pnl");
    let d = PnLSingle::default();
    assert_eq!(d.account, "", "d.account");
    assert_eq!(d.model_code, "", "d.model_code");
    assert_eq!(d.con_id, 0, "d.con_id");
    assert!(d.daily_pnl.is_nan(), "d.daily_pnl");
    assert!(d.unrealized_pnl.is_nan(), "d.unrealized_pnl");
    assert!(d.realized_pnl.is_nan(), "d.realized_pnl");
    assert_eq!(d.position, 0.0, "d.position");
    assert!(d.value.is_nan(), "d.value");
    let d = RealTimeBar::default();
    assert_eq!(d.time.timestamp(), Timestamp::UNIX_EPOCH, "d.time");
    assert_eq!(d.end_time, -1, "d.end_time");
    assert_eq!(d.open_, 0.0, "d.open_");
    assert_eq!(d.high, 0.0, "d.high");
    assert_eq!(d.low, 0.0, "d.low");
    assert_eq!(d.close, 0.0, "d.close");
    assert_eq!(d.volume, 0.0, "d.volume");
    assert_eq!(d.wap, 0.0, "d.wap");
    assert_eq!(d.count, 0, "d.count");
    let d = ScannerSubscription::default();
    assert_eq!(d.number_of_rows, -1, "d.number_of_rows");
    assert_eq!(d.instrument, "", "d.instrument");
    assert_eq!(d.location_code, "", "d.location_code");
    assert_eq!(d.scan_code, "", "d.scan_code");
    assert!(d.above_price.is_none(), "d.above_price");
    assert!(d.below_price.is_none(), "d.below_price");
    assert!(d.above_volume.is_none(), "d.above_volume");
    assert!(d.market_cap_above.is_none(), "d.market_cap_above");
    assert!(d.market_cap_below.is_none(), "d.market_cap_below");
    assert_eq!(d.moody_rating_above, "", "d.moody_rating_above");
    assert_eq!(d.moody_rating_below, "", "d.moody_rating_below");
    assert_eq!(d.sp_rating_above, "", "d.sp_rating_above");
    assert_eq!(d.sp_rating_below, "", "d.sp_rating_below");
    assert_eq!(d.maturity_date_above, "", "d.maturity_date_above");
    assert_eq!(d.maturity_date_below, "", "d.maturity_date_below");
    assert!(d.coupon_rate_above.is_none(), "d.coupon_rate_above");
    assert!(d.coupon_rate_below.is_none(), "d.coupon_rate_below");
    assert!(!d.exclude_convertible, "d.exclude_convertible");
    assert!(
        d.average_option_volume_above.is_none(),
        "d.average_option_volume_above"
    );
    assert_eq!(d.scanner_setting_pairs, "", "d.scanner_setting_pairs");
    assert_eq!(d.stock_type_filter, "", "d.stock_type_filter");
    let d = SoftDollarTier::default();
    assert_eq!(d.name, "", "d.name");
    assert_eq!(d.val, "", "d.val");
    assert_eq!(d.display_name, "", "d.display_name");
    let d = TickAttrib::default();
    assert!(!d.can_auto_execute, "d.can_auto_execute");
    assert!(!d.past_limit, "d.past_limit");
    assert!(!d.pre_open, "d.pre_open");
    let d = TickAttribBidAsk::default();
    assert!(!d.bid_past_low, "d.bid_past_low");
    assert!(!d.ask_past_high, "d.ask_past_high");
    let d = TickAttribLast::default();
    assert!(!d.past_limit, "d.past_limit");
    assert!(!d.unreported, "d.unreported");
    let d = WshEventData::default();
    assert!(d.con_id.is_none(), "d.con_id");
    assert_eq!(d.filter, "", "d.filter");
    assert!(!d.fill_watchlist, "d.fill_watchlist");
    assert!(!d.fill_portfolio, "d.fill_portfolio");
    assert!(!d.fill_competitors, "d.fill_competitors");
    assert_eq!(d.start_date, "", "d.start_date");
    assert_eq!(d.end_date, "", "d.end_date");
    assert!(d.total_limit.is_none(), "d.total_limit");
    let d = ExecutionCondition::default();
    assert_eq!(d.cond_type, 5, "d.cond_type");
    assert_eq!(d.conjunction, "a", "d.conjunction");
    assert_eq!(d.sec_type, "", "d.sec_type");
    assert_eq!(d.exch, "", "d.exch");
    assert_eq!(d.symbol, "", "d.symbol");
    let d = MarginCondition::default();
    assert_eq!(d.cond_type, 4, "d.cond_type");
    assert_eq!(d.conjunction, "a", "d.conjunction");
    assert!(d.is_more, "d.is_more");
    assert_eq!(d.percent, 0, "d.percent");
    let d = Order::default();
    assert_eq!(d.order_id, 0, "d.order_id");
    assert_eq!(d.client_id, 0, "d.client_id");
    assert_eq!(d.perm_id, 0, "d.perm_id");
    assert_eq!(d.action, "", "d.action");
    assert_eq!(d.total_quantity, 0.0, "d.total_quantity");
    assert_eq!(d.order_type, "", "d.order_type");
    assert!(d.lmt_price.is_none(), "d.lmt_price");
    assert!(d.aux_price.is_none(), "d.aux_price");
    assert_eq!(d.tif, "", "d.tif");
    assert_eq!(d.active_start_time, "", "d.active_start_time");
    assert_eq!(d.active_stop_time, "", "d.active_stop_time");
    assert_eq!(d.oca_group, "", "d.oca_group");
    assert_eq!(d.oca_type, 0, "d.oca_type");
    assert_eq!(d.order_ref, "", "d.order_ref");
    assert!(d.transmit, "d.transmit");
    assert_eq!(d.parent_id, 0, "d.parent_id");
    assert!(!d.block_order, "d.block_order");
    assert!(!d.sweep_to_fill, "d.sweep_to_fill");
    assert_eq!(d.display_size, 0, "d.display_size");
    assert_eq!(d.trigger_method, 0, "d.trigger_method");
    assert!(!d.outside_rth, "d.outside_rth");
    assert!(!d.hidden, "d.hidden");
    assert_eq!(d.good_after_time, "", "d.good_after_time");
    assert_eq!(d.good_till_date, "", "d.good_till_date");
    assert_eq!(d.rule_80_a, "", "d.rule_80_a");
    assert!(!d.all_or_none, "d.all_or_none");
    assert!(d.min_qty.is_none(), "d.min_qty");
    assert!(d.percent_offset.is_none(), "d.percent_offset");
    assert!(
        !d.override_percentage_constraints,
        "d.override_percentage_constraints"
    );
    assert!(d.trail_stop_price.is_none(), "d.trail_stop_price");
    assert!(d.trailing_percent.is_none(), "d.trailing_percent");
    assert_eq!(d.fa_group, "", "d.fa_group");
    assert_eq!(d.fa_profile, "", "d.fa_profile");
    assert_eq!(d.fa_method, "", "d.fa_method");
    assert_eq!(d.fa_percentage, "", "d.fa_percentage");
    assert_eq!(d.designated_location, "", "d.designated_location");
    assert_eq!(d.open_close, "O", "d.open_close");
    assert_eq!(d.origin, 0, "d.origin");
    assert_eq!(d.short_sale_slot, 0, "d.short_sale_slot");
    assert_eq!(d.exempt_code, -1, "d.exempt_code");
    assert_eq!(d.discretionary_amt, 0.0, "d.discretionary_amt");
    assert!(!d.e_trade_only, "d.e_trade_only");
    assert!(!d.firm_quote_only, "d.firm_quote_only");
    assert!(d.nbbo_price_cap.is_none(), "d.nbbo_price_cap");
    assert!(!d.opt_out_smart_routing, "d.opt_out_smart_routing");
    assert_eq!(d.auction_strategy, 0, "d.auction_strategy");
    assert!(d.starting_price.is_none(), "d.starting_price");
    assert!(d.stock_ref_price.is_none(), "d.stock_ref_price");
    assert!(d.delta.is_none(), "d.delta");
    assert!(d.stock_range_lower.is_none(), "d.stock_range_lower");
    assert!(d.stock_range_upper.is_none(), "d.stock_range_upper");
    assert!(!d.randomize_price, "d.randomize_price");
    assert!(!d.randomize_size, "d.randomize_size");
    assert!(d.volatility.is_none(), "d.volatility");
    assert!(d.volatility_type.is_none(), "d.volatility_type");
    assert_eq!(d.delta_neutral_order_type, "", "d.delta_neutral_order_type");
    assert!(
        d.delta_neutral_aux_price.is_none(),
        "d.delta_neutral_aux_price"
    );
    assert_eq!(d.delta_neutral_con_id, 0, "d.delta_neutral_con_id");
    assert_eq!(
        d.delta_neutral_settling_firm, "",
        "d.delta_neutral_settling_firm"
    );
    assert_eq!(
        d.delta_neutral_clearing_account, "",
        "d.delta_neutral_clearing_account"
    );
    assert_eq!(
        d.delta_neutral_clearing_intent, "",
        "d.delta_neutral_clearing_intent"
    );
    assert_eq!(d.delta_neutral_open_close, "", "d.delta_neutral_open_close");
    assert!(!d.delta_neutral_short_sale, "d.delta_neutral_short_sale");
    assert_eq!(
        d.delta_neutral_short_sale_slot, 0,
        "d.delta_neutral_short_sale_slot"
    );
    assert_eq!(
        d.delta_neutral_designated_location, "",
        "d.delta_neutral_designated_location"
    );
    assert!(!d.continuous_update, "d.continuous_update");
    assert!(d.reference_price_type.is_none(), "d.reference_price_type");
    assert!(d.basis_points.is_none(), "d.basis_points");
    assert!(d.basis_points_type.is_none(), "d.basis_points_type");
    assert!(d.scale_init_level_size.is_none(), "d.scale_init_level_size");
    assert!(d.scale_subs_level_size.is_none(), "d.scale_subs_level_size");
    assert!(d.scale_price_increment.is_none(), "d.scale_price_increment");
    assert!(
        d.scale_price_adjust_value.is_none(),
        "d.scale_price_adjust_value"
    );
    assert!(
        d.scale_price_adjust_interval.is_none(),
        "d.scale_price_adjust_interval"
    );
    assert!(d.scale_profit_offset.is_none(), "d.scale_profit_offset");
    assert!(!d.scale_auto_reset, "d.scale_auto_reset");
    assert!(d.scale_init_position.is_none(), "d.scale_init_position");
    assert!(d.scale_init_fill_qty.is_none(), "d.scale_init_fill_qty");
    assert!(!d.scale_random_percent, "d.scale_random_percent");
    assert_eq!(d.scale_table, "", "d.scale_table");
    assert_eq!(d.hedge_type, "", "d.hedge_type");
    assert_eq!(d.hedge_param, "", "d.hedge_param");
    assert_eq!(d.account, "", "d.account");
    assert_eq!(d.settling_firm, "", "d.settling_firm");
    assert_eq!(d.clearing_account, "", "d.clearing_account");
    assert_eq!(d.clearing_intent, "", "d.clearing_intent");
    assert_eq!(d.algo_strategy, "", "d.algo_strategy");
    assert!(d.algo_params.is_empty(), "d.algo_params");
    assert!(
        d.smart_combo_routing_params.is_empty(),
        "d.smart_combo_routing_params"
    );
    assert_eq!(d.algo_id, "", "d.algo_id");
    assert!(!d.what_if, "d.what_if");
    assert!(!d.not_held, "d.not_held");
    assert!(!d.solicited, "d.solicited");
    assert_eq!(d.model_code, "", "d.model_code");
    assert!(d.order_combo_legs.is_empty(), "d.order_combo_legs");
    assert!(d.order_misc_options.is_empty(), "d.order_misc_options");
    assert_eq!(d.reference_contract_id, 0, "d.reference_contract_id");
    assert_eq!(d.pegged_change_amount, 0.0, "d.pegged_change_amount");
    assert!(
        !d.is_pegged_change_amount_decrease,
        "d.is_pegged_change_amount_decrease"
    );
    assert_eq!(d.reference_change_amount, 0.0, "d.reference_change_amount");
    assert_eq!(d.reference_exchange_id, "", "d.reference_exchange_id");
    assert_eq!(d.adjusted_order_type, "", "d.adjusted_order_type");
    assert!(d.trigger_price.is_none(), "d.trigger_price");
    assert!(d.adjusted_stop_price.is_none(), "d.adjusted_stop_price");
    assert!(
        d.adjusted_stop_limit_price.is_none(),
        "d.adjusted_stop_limit_price"
    );
    assert!(
        d.adjusted_trailing_amount.is_none(),
        "d.adjusted_trailing_amount"
    );
    assert_eq!(d.adjustable_trailing_unit, 0, "d.adjustable_trailing_unit");
    assert!(d.lmt_price_offset.is_none(), "d.lmt_price_offset");
    assert!(d.conditions.is_empty(), "d.conditions");
    assert!(!d.conditions_cancel_order, "d.conditions_cancel_order");
    assert!(!d.conditions_ignore_rth, "d.conditions_ignore_rth");
    assert_eq!(d.ext_operator, "", "d.ext_operator");
    assert_eq!(
        format!("{:?}", d.soft_dollar_tier),
        format!("{:?}", SoftDollarTier::default()),
        "d.soft_dollar_tier"
    );
    assert!(d.cash_qty.is_none(), "d.cash_qty");
    assert_eq!(d.mifid_2_decision_maker, "", "d.mifid_2_decision_maker");
    assert_eq!(d.mifid_2_decision_algo, "", "d.mifid_2_decision_algo");
    assert_eq!(d.mifid_2_execution_trader, "", "d.mifid_2_execution_trader");
    assert_eq!(d.mifid_2_execution_algo, "", "d.mifid_2_execution_algo");
    assert!(
        !d.dont_use_auto_price_for_hedge,
        "d.dont_use_auto_price_for_hedge"
    );
    assert!(!d.is_oms_container, "d.is_oms_container");
    assert!(
        !d.discretionary_up_to_limit_price,
        "d.discretionary_up_to_limit_price"
    );
    assert_eq!(d.auto_cancel_date, "", "d.auto_cancel_date");
    assert!(d.filled_quantity.is_none(), "d.filled_quantity");
    assert_eq!(d.ref_futures_con_id, 0, "d.ref_futures_con_id");
    assert!(!d.auto_cancel_parent, "d.auto_cancel_parent");
    assert_eq!(d.shareholder, "", "d.shareholder");
    assert!(!d.imbalance_only, "d.imbalance_only");
    assert!(!d.route_marketable_to_bbo, "d.route_marketable_to_bbo");
    assert_eq!(d.parent_perm_id, 0, "d.parent_perm_id");
    assert!(!d.use_price_mgmt_algo, "d.use_price_mgmt_algo");
    assert!(d.duration.is_none(), "d.duration");
    assert!(d.post_to_ats.is_none(), "d.post_to_ats");
    assert_eq!(d.advanced_error_override, "", "d.advanced_error_override");
    assert_eq!(d.manual_order_time, "", "d.manual_order_time");
    assert!(d.min_trade_qty.is_none(), "d.min_trade_qty");
    assert!(d.min_compete_size.is_none(), "d.min_compete_size");
    assert!(
        d.compete_against_best_offset.is_none(),
        "d.compete_against_best_offset"
    );
    assert!(d.mid_offset_at_whole.is_none(), "d.mid_offset_at_whole");
    assert!(d.mid_offset_at_half.is_none(), "d.mid_offset_at_half");
    let d = OrderComboLeg::default();
    assert!(d.price.is_none(), "d.price");
    let d = OrderState::default();
    assert_eq!(d.status, "", "d.status");
    assert_eq!(d.init_margin_before, "", "d.init_margin_before");
    assert_eq!(d.maint_margin_before, "", "d.maint_margin_before");
    assert_eq!(d.equity_with_loan_before, "", "d.equity_with_loan_before");
    assert_eq!(d.init_margin_change, "", "d.init_margin_change");
    assert_eq!(d.maint_margin_change, "", "d.maint_margin_change");
    assert_eq!(d.equity_with_loan_change, "", "d.equity_with_loan_change");
    assert_eq!(d.init_margin_after, "", "d.init_margin_after");
    assert_eq!(d.maint_margin_after, "", "d.maint_margin_after");
    assert_eq!(d.equity_with_loan_after, "", "d.equity_with_loan_after");
    assert!(d.commission.is_none(), "d.commission");
    assert!(d.min_commission.is_none(), "d.min_commission");
    assert!(d.max_commission.is_none(), "d.max_commission");
    assert_eq!(d.commission_currency, "", "d.commission_currency");
    assert_eq!(d.warning_text, "", "d.warning_text");
    assert_eq!(d.completed_time, "", "d.completed_time");
    assert_eq!(d.completed_status, "", "d.completed_status");
    let d = OrderStatus::default();
    assert_eq!(d.order_id, 0, "d.order_id");
    assert_eq!(d.status, "", "d.status");
    assert_eq!(d.filled, 0.0, "d.filled");
    assert_eq!(d.remaining, 0.0, "d.remaining");
    assert_eq!(d.avg_fill_price, 0.0, "d.avg_fill_price");
    assert_eq!(d.perm_id, 0, "d.perm_id");
    assert_eq!(d.parent_id, 0, "d.parent_id");
    assert_eq!(d.last_fill_price, 0.0, "d.last_fill_price");
    assert_eq!(d.client_id, 0, "d.client_id");
    assert_eq!(d.why_held, "", "d.why_held");
    assert_eq!(d.mkt_cap_price, 0.0, "d.mkt_cap_price");
    let d = PercentChangeCondition::default();
    assert_eq!(d.cond_type, 7, "d.cond_type");
    assert_eq!(d.conjunction, "a", "d.conjunction");
    assert!(d.is_more, "d.is_more");
    assert_eq!(d.change_percent, 0.0, "d.change_percent");
    assert_eq!(d.con_id, 0, "d.con_id");
    assert_eq!(d.exch, "", "d.exch");
    let d = PriceCondition::default();
    assert_eq!(d.cond_type, 1, "d.cond_type");
    assert_eq!(d.conjunction, "a", "d.conjunction");
    assert!(d.is_more, "d.is_more");
    assert_eq!(d.price, 0.0, "d.price");
    assert_eq!(d.con_id, 0, "d.con_id");
    assert_eq!(d.exch, "", "d.exch");
    assert_eq!(d.trigger_method, 0, "d.trigger_method");
    let d = TimeCondition::default();
    assert_eq!(d.cond_type, 3, "d.cond_type");
    assert_eq!(d.conjunction, "a", "d.conjunction");
    assert!(d.is_more, "d.is_more");
    assert_eq!(d.time, "", "d.time");
    let d = Trade::default();
    assert_eq!(
        format!("{:?}", d.contract),
        format!("{:?}", Contract::default()),
        "d.contract"
    );
    assert_eq!(
        format!("{:?}", d.order),
        format!("{:?}", Order::default()),
        "d.order"
    );
    assert_eq!(
        format!("{:?}", d.order_status),
        format!("{:?}", OrderStatus::default()),
        "d.order_status"
    );
    assert!(d.fills.is_empty(), "d.fills");
    assert!(d.log.is_empty(), "d.log");
    assert_eq!(d.advanced_error, "", "d.advanced_error");
    let d = VolumeCondition::default();
    assert_eq!(d.cond_type, 6, "d.cond_type");
    assert_eq!(d.conjunction, "a", "d.conjunction");
    assert!(d.is_more, "d.is_more");
    assert_eq!(d.volume, 0, "d.volume");
    assert_eq!(d.con_id, 0, "d.con_id");
    assert_eq!(d.exch, "", "d.exch");
    let d = Ticker::default();
    assert!(d.contract.is_none(), "d.contract");
    assert!(d.time.is_none(), "d.time");
    assert!(d.timestamp.is_none(), "d.timestamp");
    assert_eq!(d.market_data_type, 1, "d.market_data_type");
    assert!(d.min_tick.is_nan(), "d.min_tick");
    assert!(d.bid.is_nan(), "d.bid");
    assert!(d.bid_size.is_nan(), "d.bid_size");
    assert_eq!(d.bid_exchange, "", "d.bid_exchange");
    assert!(d.ask.is_nan(), "d.ask");
    assert!(d.ask_size.is_nan(), "d.ask_size");
    assert_eq!(d.ask_exchange, "", "d.ask_exchange");
    assert!(d.last.is_nan(), "d.last");
    assert!(d.last_size.is_nan(), "d.last_size");
    assert_eq!(d.last_exchange, "", "d.last_exchange");
    assert!(d.last_timestamp.is_none(), "d.last_timestamp");
    assert!(d.prev_bid.is_nan(), "d.prev_bid");
    assert!(d.prev_bid_size.is_nan(), "d.prev_bid_size");
    assert!(d.prev_ask.is_nan(), "d.prev_ask");
    assert!(d.prev_ask_size.is_nan(), "d.prev_ask_size");
    assert!(d.prev_last.is_nan(), "d.prev_last");
    assert!(d.prev_last_size.is_nan(), "d.prev_last_size");
    assert!(d.volume.is_nan(), "d.volume");
    assert!(d.open.is_nan(), "d.open");
    assert!(d.high.is_nan(), "d.high");
    assert!(d.low.is_nan(), "d.low");
    assert!(d.close.is_nan(), "d.close");
    assert!(d.vwap.is_nan(), "d.vwap");
    assert!(d.low_13_week.is_nan(), "d.low_13_week");
    assert!(d.high_13_week.is_nan(), "d.high_13_week");
    assert!(d.low_26_week.is_nan(), "d.low_26_week");
    assert!(d.high_26_week.is_nan(), "d.high_26_week");
    assert!(d.low_52_week.is_nan(), "d.low_52_week");
    assert!(d.high_52_week.is_nan(), "d.high_52_week");
    assert!(d.bid_yield.is_nan(), "d.bid_yield");
    assert!(d.ask_yield.is_nan(), "d.ask_yield");
    assert!(d.last_yield.is_nan(), "d.last_yield");
    assert!(d.mark_price.is_nan(), "d.mark_price");
    assert!(d.halted.is_nan(), "d.halted");
    assert!(d.rt_hist_volatility.is_nan(), "d.rt_hist_volatility");
    assert!(d.rt_volume.is_nan(), "d.rt_volume");
    assert!(d.rt_trade_volume.is_nan(), "d.rt_trade_volume");
    assert!(d.rt_time.is_none(), "d.rt_time");
    assert!(d.av_volume.is_nan(), "d.av_volume");
    assert!(d.trade_count.is_nan(), "d.trade_count");
    assert!(d.trade_rate.is_nan(), "d.trade_rate");
    assert!(d.volume_rate.is_nan(), "d.volume_rate");
    assert!(d.volume_rate_3_min.is_nan(), "d.volume_rate_3_min");
    assert!(d.volume_rate_5_min.is_nan(), "d.volume_rate_5_min");
    assert!(d.volume_rate_10_min.is_nan(), "d.volume_rate_10_min");
    assert!(d.shortable.is_nan(), "d.shortable");
    assert!(d.shortable_shares.is_nan(), "d.shortable_shares");
    assert!(d.index_future_premium.is_nan(), "d.index_future_premium");
    assert!(d.futures_open_interest.is_nan(), "d.futures_open_interest");
    assert!(d.put_open_interest.is_nan(), "d.put_open_interest");
    assert!(d.call_open_interest.is_nan(), "d.call_open_interest");
    assert!(d.put_volume.is_nan(), "d.put_volume");
    assert!(d.call_volume.is_nan(), "d.call_volume");
    assert!(d.av_option_volume.is_nan(), "d.av_option_volume");
    assert!(d.hist_volatility.is_nan(), "d.hist_volatility");
    assert!(d.implied_volatility.is_nan(), "d.implied_volatility");
    assert!(d.open_interest.is_nan(), "d.open_interest");
    assert!(d.last_rth_trade.is_nan(), "d.last_rth_trade");
    assert_eq!(d.last_reg_time, "", "d.last_reg_time");
    assert_eq!(d.option_bid_exch, "", "d.option_bid_exch");
    assert_eq!(d.option_ask_exch, "", "d.option_ask_exch");
    assert!(
        d.bond_factor_multiplier.is_nan(),
        "d.bond_factor_multiplier"
    );
    assert!(d.creditman_mark_price.is_nan(), "d.creditman_mark_price");
    assert!(
        d.creditman_slow_mark_price.is_nan(),
        "d.creditman_slow_mark_price"
    );
    assert!(
        d.delayed_last_timestamp.is_none(),
        "d.delayed_last_timestamp"
    );
    assert!(d.delayed_halted.is_nan(), "d.delayed_halted");
    assert_eq!(d.reuters_mutual_funds, "", "d.reuters_mutual_funds");
    assert!(d.etf_nav_close.is_nan(), "d.etf_nav_close");
    assert!(d.etf_nav_prior_close.is_nan(), "d.etf_nav_prior_close");
    assert!(d.etf_nav_bid.is_nan(), "d.etf_nav_bid");
    assert!(d.etf_nav_ask.is_nan(), "d.etf_nav_ask");
    assert!(d.etf_nav_last.is_nan(), "d.etf_nav_last");
    assert!(d.etf_frozen_nav_last.is_nan(), "d.etf_frozen_nav_last");
    assert!(d.etf_nav_high.is_nan(), "d.etf_nav_high");
    assert!(d.etf_nav_low.is_nan(), "d.etf_nav_low");
    assert_eq!(d.social_market_analytics, "", "d.social_market_analytics");
    assert!(
        d.estimated_ipo_midpoint.is_nan(),
        "d.estimated_ipo_midpoint"
    );
    assert!(d.final_ipo_last.is_nan(), "d.final_ipo_last");
    assert!(d.dividends.is_none(), "d.dividends");
    assert!(d.fundamental_ratios.is_none(), "d.fundamental_ratios");
    assert!(d.ticks.is_empty(), "d.ticks");
    assert!(d.tick_by_ticks.is_empty(), "d.tick_by_ticks");
    assert!(d.dom_bids.is_empty(), "d.dom_bids");
    assert!(d.dom_bids_dict.is_empty(), "d.dom_bids_dict");
    assert!(d.dom_asks.is_empty(), "d.dom_asks");
    assert!(d.dom_asks_dict.is_empty(), "d.dom_asks_dict");
    assert!(d.dom_ticks.is_empty(), "d.dom_ticks");
    assert!(d.bid_greeks.is_none(), "d.bid_greeks");
    assert!(d.ask_greeks.is_none(), "d.ask_greeks");
    assert!(d.last_greeks.is_none(), "d.last_greeks");
    assert!(d.model_greeks.is_none(), "d.model_greeks");
    assert!(d.cust_greeks.is_none(), "d.cust_greeks");
    assert!(d.bid_efp.is_none(), "d.bid_efp");
    assert!(d.ask_efp.is_none(), "d.ask_efp");
    assert!(d.last_efp.is_none(), "d.last_efp");
    assert!(d.open_efp.is_none(), "d.open_efp");
    assert!(d.high_efp.is_none(), "d.high_efp");
    assert!(d.low_efp.is_none(), "d.low_efp");
    assert!(d.close_efp.is_none(), "d.close_efp");
    assert!(d.auction_volume.is_nan(), "d.auction_volume");
    assert!(d.auction_price.is_nan(), "d.auction_price");
    assert!(d.auction_imbalance.is_nan(), "d.auction_imbalance");
    assert!(d.regulatory_imbalance.is_nan(), "d.regulatory_imbalance");
    assert_eq!(d.bbo_exchange, "", "d.bbo_exchange");
    assert_eq!(d.snapshot_permissions, 0, "d.snapshot_permissions");
    assert_eq!(
        format!("{:?}", d.defaults),
        format!("{:?}", IBDefaults::default()),
        "d.defaults"
    );
}

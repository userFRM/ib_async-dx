//! ib_async's `order` module: orders, their states, trades and conditions.

use std::hash::{Hash, Hasher};

use crate::contract::{Contract, TagValue};
use crate::error::{Error, Result};
use crate::event::Event;
use crate::live::{Live, Observed, sealed};
use crate::objects::{CommissionReport, Fill, SoftDollarTier, TradeLogEntry};
use crate::util::{UNSET_DOUBLE, py_float};

/// An order for a contract: ib_async's `Order`.
///
/// A value until it is placed; `place_order` takes it as a [`Live<Order>`],
/// whose clones are one order, as ib_async's `Order` object is. Two
/// `Live<Order>` handles are equal only when they are the same order, as
/// ib_async's `Order.__eq__` is `is`; the value itself has no equality.
#[derive(Clone, Debug)]
pub struct Order {
    /// ib_async's `orderId`.
    pub order_id: i64,
    /// ib_async's `clientId`.
    pub client_id: i64,
    /// ib_async's `permId`.
    pub perm_id: i64,
    /// ib_async's `action`.
    pub action: String,
    /// ib_async's `totalQuantity`.
    pub total_quantity: f64,
    /// ib_async's `orderType`.
    pub order_type: String,
    /// ib_async's `lmtPrice`; `None` is its `UNSET_DOUBLE`.
    pub lmt_price: Option<f64>,
    /// ib_async's `auxPrice`; `None` is its `UNSET_DOUBLE`.
    pub aux_price: Option<f64>,
    /// ib_async's `tif`.
    pub tif: String,
    /// ib_async's `activeStartTime`.
    pub active_start_time: String,
    /// ib_async's `activeStopTime`.
    pub active_stop_time: String,
    /// ib_async's `ocaGroup`.
    pub oca_group: String,
    /// ib_async's `ocaType`.
    pub oca_type: i32,
    /// ib_async's `orderRef`.
    pub order_ref: String,
    /// ib_async's `transmit`.
    pub transmit: bool,
    /// ib_async's `parentId`.
    pub parent_id: i64,
    /// ib_async's `blockOrder`.
    pub block_order: bool,
    /// ib_async's `sweepToFill`.
    pub sweep_to_fill: bool,
    /// ib_async's `displaySize`.
    pub display_size: i32,
    /// ib_async's `triggerMethod`.
    pub trigger_method: i32,
    /// ib_async's `outsideRth`.
    pub outside_rth: bool,
    /// ib_async's `hidden`.
    pub hidden: bool,
    /// ib_async's `goodAfterTime`.
    pub good_after_time: String,
    /// ib_async's `goodTillDate`.
    pub good_till_date: String,
    /// ib_async's `rule80A`.
    pub rule_80_a: String,
    /// ib_async's `allOrNone`.
    pub all_or_none: bool,
    /// ib_async's `minQty`; `None` is its `UNSET_INTEGER`.
    pub min_qty: Option<i32>,
    /// ib_async's `percentOffset`; `None` is its `UNSET_DOUBLE`.
    pub percent_offset: Option<f64>,
    /// ib_async's `overridePercentageConstraints`.
    pub override_percentage_constraints: bool,
    /// ib_async's `trailStopPrice`; `None` is its `UNSET_DOUBLE`.
    pub trail_stop_price: Option<f64>,
    /// ib_async's `trailingPercent`; `None` is its `UNSET_DOUBLE`.
    pub trailing_percent: Option<f64>,
    /// ib_async's `faGroup`.
    pub fa_group: String,
    /// ib_async's `faProfile`, which it marks obsolete.
    pub fa_profile: String,
    /// ib_async's `faMethod`.
    pub fa_method: String,
    /// ib_async's `faPercentage`.
    pub fa_percentage: String,
    /// ib_async's `designatedLocation`.
    pub designated_location: String,
    /// ib_async's `openClose`.
    pub open_close: String,
    /// ib_async's `origin`.
    pub origin: i32,
    /// ib_async's `shortSaleSlot`.
    pub short_sale_slot: i32,
    /// ib_async's `exemptCode`.
    pub exempt_code: i32,
    /// ib_async's `discretionaryAmt`.
    pub discretionary_amt: f64,
    /// ib_async's `eTradeOnly`.
    pub e_trade_only: bool,
    /// ib_async's `firmQuoteOnly`.
    pub firm_quote_only: bool,
    /// ib_async's `nbboPriceCap`; `None` is its `UNSET_DOUBLE`.
    pub nbbo_price_cap: Option<f64>,
    /// ib_async's `optOutSmartRouting`.
    pub opt_out_smart_routing: bool,
    /// ib_async's `auctionStrategy`.
    pub auction_strategy: i32,
    /// ib_async's `startingPrice`; `None` is its `UNSET_DOUBLE`.
    pub starting_price: Option<f64>,
    /// ib_async's `stockRefPrice`; `None` is its `UNSET_DOUBLE`.
    pub stock_ref_price: Option<f64>,
    /// ib_async's `delta`; `None` is its `UNSET_DOUBLE`.
    pub delta: Option<f64>,
    /// ib_async's `stockRangeLower`; `None` is its `UNSET_DOUBLE`.
    pub stock_range_lower: Option<f64>,
    /// ib_async's `stockRangeUpper`; `None` is its `UNSET_DOUBLE`.
    pub stock_range_upper: Option<f64>,
    /// ib_async's `randomizePrice`.
    pub randomize_price: bool,
    /// ib_async's `randomizeSize`.
    pub randomize_size: bool,
    /// ib_async's `volatility`; `None` is its `UNSET_DOUBLE`.
    pub volatility: Option<f64>,
    /// ib_async's `volatilityType`; `None` is its `UNSET_INTEGER`.
    pub volatility_type: Option<i32>,
    /// ib_async's `deltaNeutralOrderType`.
    pub delta_neutral_order_type: String,
    /// ib_async's `deltaNeutralAuxPrice`; `None` is its `UNSET_DOUBLE`.
    pub delta_neutral_aux_price: Option<f64>,
    /// ib_async's `deltaNeutralConId`.
    pub delta_neutral_con_id: i64,
    /// ib_async's `deltaNeutralSettlingFirm`.
    pub delta_neutral_settling_firm: String,
    /// ib_async's `deltaNeutralClearingAccount`.
    pub delta_neutral_clearing_account: String,
    /// ib_async's `deltaNeutralClearingIntent`.
    pub delta_neutral_clearing_intent: String,
    /// ib_async's `deltaNeutralOpenClose`.
    pub delta_neutral_open_close: String,
    /// ib_async's `deltaNeutralShortSale`.
    pub delta_neutral_short_sale: bool,
    /// ib_async's `deltaNeutralShortSaleSlot`.
    pub delta_neutral_short_sale_slot: i32,
    /// ib_async's `deltaNeutralDesignatedLocation`.
    pub delta_neutral_designated_location: String,
    /// ib_async's `continuousUpdate`.
    pub continuous_update: bool,
    /// ib_async's `referencePriceType`; `None` is its `UNSET_INTEGER`.
    pub reference_price_type: Option<i32>,
    /// ib_async's `basisPoints`; `None` is its `UNSET_DOUBLE`.
    pub basis_points: Option<f64>,
    /// ib_async's `basisPointsType`; `None` is its `UNSET_INTEGER`.
    pub basis_points_type: Option<i32>,
    /// ib_async's `scaleInitLevelSize`; `None` is its `UNSET_INTEGER`.
    pub scale_init_level_size: Option<i32>,
    /// ib_async's `scaleSubsLevelSize`; `None` is its `UNSET_INTEGER`.
    pub scale_subs_level_size: Option<i32>,
    /// ib_async's `scalePriceIncrement`; `None` is its `UNSET_DOUBLE`.
    pub scale_price_increment: Option<f64>,
    /// ib_async's `scalePriceAdjustValue`; `None` is its `UNSET_DOUBLE`.
    pub scale_price_adjust_value: Option<f64>,
    /// ib_async's `scalePriceAdjustInterval`; `None` is its `UNSET_INTEGER`.
    pub scale_price_adjust_interval: Option<i32>,
    /// ib_async's `scaleProfitOffset`; `None` is its `UNSET_DOUBLE`.
    pub scale_profit_offset: Option<f64>,
    /// ib_async's `scaleAutoReset`.
    pub scale_auto_reset: bool,
    /// ib_async's `scaleInitPosition`; `None` is its `UNSET_INTEGER`.
    pub scale_init_position: Option<i32>,
    /// ib_async's `scaleInitFillQty`; `None` is its `UNSET_INTEGER`.
    pub scale_init_fill_qty: Option<i32>,
    /// ib_async's `scaleRandomPercent`.
    pub scale_random_percent: bool,
    /// ib_async's `scaleTable`.
    pub scale_table: String,
    /// ib_async's `hedgeType`.
    pub hedge_type: String,
    /// ib_async's `hedgeParam`.
    pub hedge_param: String,
    /// ib_async's `account`.
    pub account: String,
    /// ib_async's `settlingFirm`.
    pub settling_firm: String,
    /// ib_async's `clearingAccount`.
    pub clearing_account: String,
    /// ib_async's `clearingIntent`.
    pub clearing_intent: String,
    /// ib_async's `algoStrategy`.
    pub algo_strategy: String,
    /// ib_async's `algoParams`.
    pub algo_params: Vec<TagValue>,
    /// ib_async's `smartComboRoutingParams`.
    pub smart_combo_routing_params: Vec<TagValue>,
    /// ib_async's `algoId`.
    pub algo_id: String,
    /// ib_async's `whatIf`.
    pub what_if: bool,
    /// ib_async's `notHeld`.
    pub not_held: bool,
    /// ib_async's `solicited`.
    pub solicited: bool,
    /// ib_async's `modelCode`.
    pub model_code: String,
    /// ib_async's `orderComboLegs`.
    pub order_combo_legs: Vec<OrderComboLeg>,
    /// ib_async's `orderMiscOptions`.
    pub order_misc_options: Vec<TagValue>,
    /// ib_async's `referenceContractId`.
    pub reference_contract_id: i64,
    /// ib_async's `peggedChangeAmount`.
    pub pegged_change_amount: f64,
    /// ib_async's `isPeggedChangeAmountDecrease`.
    pub is_pegged_change_amount_decrease: bool,
    /// ib_async's `referenceChangeAmount`.
    pub reference_change_amount: f64,
    /// ib_async's `referenceExchangeId`.
    pub reference_exchange_id: String,
    /// ib_async's `adjustedOrderType`.
    pub adjusted_order_type: String,
    /// ib_async's `triggerPrice`; `None` is its `UNSET_DOUBLE`.
    pub trigger_price: Option<f64>,
    /// ib_async's `adjustedStopPrice`; `None` is its `UNSET_DOUBLE`.
    pub adjusted_stop_price: Option<f64>,
    /// ib_async's `adjustedStopLimitPrice`; `None` is its `UNSET_DOUBLE`.
    pub adjusted_stop_limit_price: Option<f64>,
    /// ib_async's `adjustedTrailingAmount`; `None` is its `UNSET_DOUBLE`.
    pub adjusted_trailing_amount: Option<f64>,
    /// ib_async's `adjustableTrailingUnit`.
    pub adjustable_trailing_unit: i32,
    /// ib_async's `lmtPriceOffset`; `None` is its `UNSET_DOUBLE`.
    pub lmt_price_offset: Option<f64>,
    /// ib_async's `conditions`.
    pub conditions: Vec<OrderCondition>,
    /// ib_async's `conditionsCancelOrder`.
    pub conditions_cancel_order: bool,
    /// ib_async's `conditionsIgnoreRth`.
    pub conditions_ignore_rth: bool,
    /// ib_async's `extOperator`.
    pub ext_operator: String,
    /// ib_async's `softDollarTier`.
    pub soft_dollar_tier: SoftDollarTier,
    /// ib_async's `cashQty`; `None` is its `UNSET_DOUBLE`.
    pub cash_qty: Option<f64>,
    /// ib_async's `mifid2DecisionMaker`.
    pub mifid_2_decision_maker: String,
    /// ib_async's `mifid2DecisionAlgo`.
    pub mifid_2_decision_algo: String,
    /// ib_async's `mifid2ExecutionTrader`.
    pub mifid_2_execution_trader: String,
    /// ib_async's `mifid2ExecutionAlgo`.
    pub mifid_2_execution_algo: String,
    /// ib_async's `dontUseAutoPriceForHedge`.
    pub dont_use_auto_price_for_hedge: bool,
    /// ib_async's `isOmsContainer`.
    pub is_oms_container: bool,
    /// ib_async's `discretionaryUpToLimitPrice`.
    pub discretionary_up_to_limit_price: bool,
    /// ib_async's `autoCancelDate`.
    pub auto_cancel_date: String,
    /// ib_async's `filledQuantity`; `None` is its `UNSET_DOUBLE`.
    pub filled_quantity: Option<f64>,
    /// ib_async's `refFuturesConId`.
    pub ref_futures_con_id: i64,
    /// ib_async's `autoCancelParent`.
    pub auto_cancel_parent: bool,
    /// ib_async's `shareholder`.
    pub shareholder: String,
    /// ib_async's `imbalanceOnly`.
    pub imbalance_only: bool,
    /// ib_async's `routeMarketableToBbo`.
    pub route_marketable_to_bbo: bool,
    /// ib_async's `parentPermId`.
    pub parent_perm_id: i64,
    /// ib_async's `usePriceMgmtAlgo`.
    pub use_price_mgmt_algo: bool,
    /// ib_async's `duration`; `None` is its `UNSET_INTEGER`.
    pub duration: Option<i32>,
    /// ib_async's `postToAts`; `None` is its `UNSET_INTEGER`.
    pub post_to_ats: Option<i32>,
    /// ib_async's `advancedErrorOverride`.
    pub advanced_error_override: String,
    /// ib_async's `manualOrderTime`.
    pub manual_order_time: String,
    /// ib_async's `minTradeQty`; `None` is its `UNSET_INTEGER`.
    pub min_trade_qty: Option<i32>,
    /// ib_async's `minCompeteSize`; `None` is its `UNSET_INTEGER`.
    pub min_compete_size: Option<i32>,
    /// ib_async's `competeAgainstBestOffset`; `None` is its `UNSET_DOUBLE`.
    pub compete_against_best_offset: Option<f64>,
    /// ib_async's `midOffsetAtWhole`; `None` is its `UNSET_DOUBLE`.
    pub mid_offset_at_whole: Option<f64>,
    /// ib_async's `midOffsetAtHalf`; `None` is its `UNSET_DOUBLE`.
    pub mid_offset_at_half: Option<f64>,
}

impl Default for Order {
    fn default() -> Self {
        Order {
            order_id: 0,
            client_id: 0,
            perm_id: 0,
            action: String::new(),
            total_quantity: 0.0,
            order_type: String::new(),
            lmt_price: None,
            aux_price: None,
            tif: String::new(),
            active_start_time: String::new(),
            active_stop_time: String::new(),
            oca_group: String::new(),
            oca_type: 0,
            order_ref: String::new(),
            transmit: true,
            parent_id: 0,
            block_order: false,
            sweep_to_fill: false,
            display_size: 0,
            trigger_method: 0,
            outside_rth: false,
            hidden: false,
            good_after_time: String::new(),
            good_till_date: String::new(),
            rule_80_a: String::new(),
            all_or_none: false,
            min_qty: None,
            percent_offset: None,
            override_percentage_constraints: false,
            trail_stop_price: None,
            trailing_percent: None,
            fa_group: String::new(),
            fa_profile: String::new(),
            fa_method: String::new(),
            fa_percentage: String::new(),
            designated_location: String::new(),
            open_close: "O".into(),
            origin: 0,
            short_sale_slot: 0,
            exempt_code: -1,
            discretionary_amt: 0.0,
            e_trade_only: false,
            firm_quote_only: false,
            nbbo_price_cap: None,
            opt_out_smart_routing: false,
            auction_strategy: 0,
            starting_price: None,
            stock_ref_price: None,
            delta: None,
            stock_range_lower: None,
            stock_range_upper: None,
            randomize_price: false,
            randomize_size: false,
            volatility: None,
            volatility_type: None,
            delta_neutral_order_type: String::new(),
            delta_neutral_aux_price: None,
            delta_neutral_con_id: 0,
            delta_neutral_settling_firm: String::new(),
            delta_neutral_clearing_account: String::new(),
            delta_neutral_clearing_intent: String::new(),
            delta_neutral_open_close: String::new(),
            delta_neutral_short_sale: false,
            delta_neutral_short_sale_slot: 0,
            delta_neutral_designated_location: String::new(),
            continuous_update: false,
            reference_price_type: None,
            basis_points: None,
            basis_points_type: None,
            scale_init_level_size: None,
            scale_subs_level_size: None,
            scale_price_increment: None,
            scale_price_adjust_value: None,
            scale_price_adjust_interval: None,
            scale_profit_offset: None,
            scale_auto_reset: false,
            scale_init_position: None,
            scale_init_fill_qty: None,
            scale_random_percent: false,
            scale_table: String::new(),
            hedge_type: String::new(),
            hedge_param: String::new(),
            account: String::new(),
            settling_firm: String::new(),
            clearing_account: String::new(),
            clearing_intent: String::new(),
            algo_strategy: String::new(),
            algo_params: Vec::new(),
            smart_combo_routing_params: Vec::new(),
            algo_id: String::new(),
            what_if: false,
            not_held: false,
            solicited: false,
            model_code: String::new(),
            order_combo_legs: Vec::new(),
            order_misc_options: Vec::new(),
            reference_contract_id: 0,
            pegged_change_amount: 0.0,
            is_pegged_change_amount_decrease: false,
            reference_change_amount: 0.0,
            reference_exchange_id: String::new(),
            adjusted_order_type: String::new(),
            trigger_price: None,
            adjusted_stop_price: None,
            adjusted_stop_limit_price: None,
            adjusted_trailing_amount: None,
            adjustable_trailing_unit: 0,
            lmt_price_offset: None,
            conditions: Vec::new(),
            conditions_cancel_order: false,
            conditions_ignore_rth: false,
            ext_operator: String::new(),
            soft_dollar_tier: SoftDollarTier::default(),
            cash_qty: None,
            mifid_2_decision_maker: String::new(),
            mifid_2_decision_algo: String::new(),
            mifid_2_execution_trader: String::new(),
            mifid_2_execution_algo: String::new(),
            dont_use_auto_price_for_hedge: false,
            is_oms_container: false,
            discretionary_up_to_limit_price: false,
            auto_cancel_date: String::new(),
            filled_quantity: None,
            ref_futures_con_id: 0,
            auto_cancel_parent: false,
            shareholder: String::new(),
            imbalance_only: false,
            route_marketable_to_bbo: false,
            parent_perm_id: 0,
            use_price_mgmt_algo: false,
            duration: None,
            post_to_ats: None,
            advanced_error_override: String::new(),
            manual_order_time: String::new(),
            min_trade_qty: None,
            min_compete_size: None,
            compete_against_best_offset: None,
            mid_offset_at_whole: None,
            mid_offset_at_half: None,
        }
    }
}

impl Order {
    /// A limit order: ib_async's `LimitOrder(action, totalQuantity,
    /// lmtPrice)`, of order type `LMT`.
    pub fn limit(
        action: impl Into<String>,
        qty: impl Into<f64>,
        lmt_price: impl Into<f64>,
    ) -> Self {
        Order {
            order_type: "LMT".into(),
            action: action.into(),
            total_quantity: qty.into(),
            lmt_price: Some(lmt_price.into()),
            ..Order::default()
        }
    }

    /// A market order: ib_async's `MarketOrder(action, totalQuantity)`, of
    /// order type `MKT`.
    pub fn market(action: impl Into<String>, qty: impl Into<f64>) -> Self {
        Order {
            order_type: "MKT".into(),
            action: action.into(),
            total_quantity: qty.into(),
            ..Order::default()
        }
    }

    /// A stop order: ib_async's `StopOrder(action, totalQuantity,
    /// stopPrice)`, of order type `STP`, with the stop price in `aux_price`.
    pub fn stop(
        action: impl Into<String>,
        qty: impl Into<f64>,
        stop_price: impl Into<f64>,
    ) -> Self {
        Order {
            order_type: "STP".into(),
            action: action.into(),
            total_quantity: qty.into(),
            aux_price: Some(stop_price.into()),
            ..Order::default()
        }
    }

    /// A stop-limit order: ib_async's `StopLimitOrder(action, totalQuantity,
    /// lmtPrice, stopPrice)`, of order type `STP LMT`, with the stop price
    /// in `aux_price`.
    pub fn stop_limit(
        action: impl Into<String>,
        qty: impl Into<f64>,
        lmt_price: impl Into<f64>,
        stop_price: impl Into<f64>,
    ) -> Self {
        Order {
            order_type: "STP LMT".into(),
            action: action.into(),
            total_quantity: qty.into(),
            lmt_price: Some(lmt_price.into()),
            aux_price: Some(stop_price.into()),
            ..Order::default()
        }
    }
}

impl sealed::Storage for Order {
    type Events = ();
    fn events(_: &sealed::Maker) {}
}

impl Observed for Order {}

/// The same order, as ib_async's `Order.__eq__` is `is`.
impl PartialEq for Live<Order> {
    fn eq(&self, other: &Self) -> bool {
        Live::ptr_eq(self, other)
    }
}

impl Eq for Live<Order> {}

/// By identity, as ib_async's `Order.__hash__` is `id`.
impl Hash for Live<Order> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.addr().hash(state);
    }
}

/// A combo order's price for one leg: ib_async's `OrderComboLeg`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OrderComboLeg {
    /// ib_async's `price`; `None` is its `UNSET_DOUBLE`.
    pub price: Option<f64>,
}

/// An order's status as last reported: ib_async's `OrderStatus`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OrderStatus {
    /// ib_async's `orderId`.
    pub order_id: i64,
    /// ib_async's `status`: one of the names below.
    pub status: String,
    /// ib_async's `filled`.
    pub filled: f64,
    /// ib_async's `remaining`.
    pub remaining: f64,
    /// ib_async's `avgFillPrice`.
    pub avg_fill_price: f64,
    /// ib_async's `permId`.
    pub perm_id: i64,
    /// ib_async's `parentId`.
    pub parent_id: i64,
    /// ib_async's `lastFillPrice`.
    pub last_fill_price: f64,
    /// ib_async's `clientId`.
    pub client_id: i64,
    /// ib_async's `whyHeld`.
    pub why_held: String,
    /// ib_async's `mktCapPrice`.
    pub mkt_cap_price: f64,
}

impl OrderStatus {
    /// ib_async's `OrderStatus.PendingSubmit`.
    pub const PENDING_SUBMIT: &'static str = "PendingSubmit";
    /// ib_async's `OrderStatus.PendingCancel`.
    pub const PENDING_CANCEL: &'static str = "PendingCancel";
    /// ib_async's `OrderStatus.PreSubmitted`.
    pub const PRE_SUBMITTED: &'static str = "PreSubmitted";
    /// ib_async's `OrderStatus.Submitted`.
    pub const SUBMITTED: &'static str = "Submitted";
    /// ib_async's `OrderStatus.ApiPending`.
    pub const API_PENDING: &'static str = "ApiPending";
    /// ib_async's `OrderStatus.ApiCancelled`.
    pub const API_CANCELLED: &'static str = "ApiCancelled";
    /// ib_async's `OrderStatus.ApiUpdate`.
    pub const API_UPDATE: &'static str = "ApiUpdate";
    /// ib_async's `OrderStatus.Cancelled`.
    pub const CANCELLED: &'static str = "Cancelled";
    /// ib_async's `OrderStatus.Filled`.
    pub const FILLED: &'static str = "Filled";
    /// ib_async's `OrderStatus.Inactive`.
    pub const INACTIVE: &'static str = "Inactive";
    /// ib_async's `OrderStatus.ValidationError`.
    pub const VALIDATION_ERROR: &'static str = "ValidationError";

    /// Filled, cancelled, or ended by the broker's risk checks: ib_async's
    /// `OrderStatus.DoneStates`.
    pub const DONE_STATES: &'static [&'static str] = &[
        Self::FILLED,
        Self::CANCELLED,
        Self::API_CANCELLED,
        Self::INACTIVE,
    ];

    /// Able to execute now or later: ib_async's `OrderStatus.ActiveStates`.
    pub const ACTIVE_STATES: &'static [&'static str] = &[
        Self::PENDING_SUBMIT,
        Self::API_PENDING,
        Self::PRE_SUBMITTED,
        Self::SUBMITTED,
        Self::VALIDATION_ERROR,
        Self::API_UPDATE,
    ];

    /// Not yet working, though it may start to and execute before the
    /// notice arrives: ib_async's `OrderStatus.WaitingStates`.
    pub const WAITING_STATES: &'static [&'static str] =
        &[Self::PENDING_SUBMIT, Self::API_PENDING, Self::PRE_SUBMITTED];

    /// Working at the broker against public exchanges: ib_async's
    /// `OrderStatus.WorkingStates`.
    pub const WORKING_STATES: &'static [&'static str] =
        &[Self::SUBMITTED, Self::VALIDATION_ERROR, Self::API_UPDATE];

    /// `filled + remaining`, the order's total size: ib_async's `total`.
    pub fn total(&self) -> f64 {
        self.filled + self.remaining
    }
}

/// An order's margin and commission figures: ib_async's `OrderState`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OrderState {
    /// ib_async's `status`.
    pub status: String,
    /// ib_async's `initMarginBefore`.
    pub init_margin_before: String,
    /// ib_async's `maintMarginBefore`.
    pub maint_margin_before: String,
    /// ib_async's `equityWithLoanBefore`.
    pub equity_with_loan_before: String,
    /// ib_async's `initMarginChange`.
    pub init_margin_change: String,
    /// ib_async's `maintMarginChange`.
    pub maint_margin_change: String,
    /// ib_async's `equityWithLoanChange`.
    pub equity_with_loan_change: String,
    /// ib_async's `initMarginAfter`.
    pub init_margin_after: String,
    /// ib_async's `maintMarginAfter`.
    pub maint_margin_after: String,
    /// ib_async's `equityWithLoanAfter`.
    pub equity_with_loan_after: String,
    /// ib_async's `commission`; `None` is its `UNSET_DOUBLE`.
    pub commission: Option<f64>,
    /// ib_async's `minCommission`; `None` is its `UNSET_DOUBLE`.
    pub min_commission: Option<f64>,
    /// ib_async's `maxCommission`; `None` is its `UNSET_DOUBLE`.
    pub max_commission: Option<f64>,
    /// ib_async's `commissionCurrency`.
    pub commission_currency: String,
    /// ib_async's `warningText`.
    pub warning_text: String,
    /// ib_async's `completedTime`.
    pub completed_time: String,
    /// ib_async's `completedStatus`.
    pub completed_status: String,
}

/// What [`OrderState::transform`] hands its function: a margin figure as
/// text, or a commission as a number, as ib_async's `transform` passes a
/// `str` or a `float` to the same callable.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum OrderStateValue<'a> {
    /// One of the nine margin figures, as the API sent it.
    Text(&'a str),
    /// One of the three commissions; `None` is `UNSET_DOUBLE`.
    Number(Option<f64>),
}

/// An [`OrderState`] whose nine margin figures and three commissions have
/// been turned into `U`: ib_async's `OrderStateNumeric`, which the default
/// `U` is.
#[derive(Clone, Debug, PartialEq)]
pub struct OrderStateNumeric<U = Option<f64>> {
    /// ib_async's `status`.
    pub status: String,
    /// ib_async's `initMarginBefore`.
    pub init_margin_before: U,
    /// ib_async's `maintMarginBefore`.
    pub maint_margin_before: U,
    /// ib_async's `equityWithLoanBefore`.
    pub equity_with_loan_before: U,
    /// ib_async's `initMarginChange`.
    pub init_margin_change: U,
    /// ib_async's `maintMarginChange`.
    pub maint_margin_change: U,
    /// ib_async's `equityWithLoanChange`.
    pub equity_with_loan_change: U,
    /// ib_async's `initMarginAfter`.
    pub init_margin_after: U,
    /// ib_async's `maintMarginAfter`.
    pub maint_margin_after: U,
    /// ib_async's `equityWithLoanAfter`.
    pub equity_with_loan_after: U,
    /// ib_async's `commission`.
    pub commission: U,
    /// ib_async's `minCommission`.
    pub min_commission: U,
    /// ib_async's `maxCommission`.
    pub max_commission: U,
    /// ib_async's `commissionCurrency`.
    pub commission_currency: String,
    /// ib_async's `warningText`.
    pub warning_text: String,
    /// ib_async's `completedTime`.
    pub completed_time: String,
    /// ib_async's `completedStatus`.
    pub completed_status: String,
}

/// ib_async's `OrderStateNumeric()`: the margin figures NaN, the
/// commissions unset.
impl Default for OrderStateNumeric {
    fn default() -> Self {
        let nan = Some(f64::NAN);
        OrderStateNumeric {
            status: String::new(),
            init_margin_before: nan,
            maint_margin_before: nan,
            equity_with_loan_before: nan,
            init_margin_change: nan,
            maint_margin_change: nan,
            equity_with_loan_change: nan,
            init_margin_after: nan,
            maint_margin_after: nan,
            equity_with_loan_after: nan,
            commission: None,
            min_commission: None,
            max_commission: None,
            commission_currency: String::new(),
            warning_text: String::new(),
            completed_time: String::new(),
            completed_status: String::new(),
        }
    }
}

impl OrderState {
    /// A copy with each margin figure and each commission replaced by what
    /// `f` makes of it, in ib_async's order: the nine margins, then
    /// `commission`, `min_commission`, `max_commission`. ib_async's
    /// `transform`.
    pub fn transform<U>(
        &self,
        mut f: impl FnMut(OrderStateValue<'_>) -> U,
    ) -> OrderStateNumeric<U> {
        use OrderStateValue::{Number, Text};
        OrderStateNumeric {
            status: self.status.clone(),
            init_margin_before: f(Text(&self.init_margin_before)),
            maint_margin_before: f(Text(&self.maint_margin_before)),
            equity_with_loan_before: f(Text(&self.equity_with_loan_before)),
            init_margin_change: f(Text(&self.init_margin_change)),
            maint_margin_change: f(Text(&self.maint_margin_change)),
            equity_with_loan_change: f(Text(&self.equity_with_loan_change)),
            init_margin_after: f(Text(&self.init_margin_after)),
            maint_margin_after: f(Text(&self.maint_margin_after)),
            equity_with_loan_after: f(Text(&self.equity_with_loan_after)),
            commission: f(Number(self.commission)),
            min_commission: f(Number(self.min_commission)),
            max_commission: f(Number(self.max_commission)),
            commission_currency: self.commission_currency.clone(),
            warning_text: self.warning_text.clone(),
            completed_time: self.completed_time.clone(),
            completed_status: self.completed_status.clone(),
        }
    }

    /// The figures as numbers rounded to `digits` places, as Python's
    /// `round` rounds (a negative `digits` rounds to tens, hundreds and so
    /// on); `None` for a figure that does not parse or is unset. ib_async's
    /// `numeric`, whose `digits` defaults to 2.
    pub fn numeric(&self, digits: i32) -> OrderStateNumeric {
        self.transform(|v| float_or_none(v, digits))
    }

    /// The figures as text with thousands separators and `digits` places,
    /// from their values at 8 places: `"300,000.21"`; `None` stays `None`.
    /// ib_async's `formatted`, whose `digits` defaults to 2.
    pub fn formatted(&self, digits: usize) -> OrderStateNumeric<Option<String>> {
        self.transform(|v| float_or_none(v, 8).map(|x| grouped(x, digits)))
    }
}

/// ib_async's `floatOrNone` inside `numeric`: Python's `float()`, `None`
/// when that fails or gives `UNSET_DOUBLE`, else `round(x, digits)`.
fn float_or_none(v: OrderStateValue<'_>, digits: i32) -> Option<f64> {
    let x = match v {
        OrderStateValue::Text(s) => py_float(s)?,
        OrderStateValue::Number(n) => n?,
    };
    if x == UNSET_DOUBLE {
        return None;
    }
    py_round(x, digits)
}

/// Python's `round(x, digits)` for a float: to the nearest multiple of
/// `10^-digits`, ties to even, judged on the exact binary value. `None`
/// where Python raises `OverflowError`.
fn py_round(x: f64, digits: i32) -> Option<f64> {
    // CPython's bounds past which x rounds to itself or to a signed zero.
    if !x.is_finite() || digits > 323 {
        return Some(x);
    }
    if digits < -308 {
        return Some(0.0 * x);
    }
    let text = match usize::try_from(digits) {
        // Fixed-precision formatting rounds the exact value, ties to even.
        Ok(places) => format!("{x:.places$}"),
        Err(_) => round_left(x, digits.unsigned_abs() as usize),
    };
    text.parse::<f64>().ok().filter(|r| r.is_finite())
}

/// `x` rounded to a multiple of `10^k`, ties to even on the exact value, as
/// decimal text.
fn round_left(x: f64, k: usize) -> String {
    // A float's integer part prints exactly.
    let int = format!("{:.0}", x.abs().trunc());
    let (keep, rest) = int.split_at(int.len().saturating_sub(k));
    let rest = format!("{rest:0>k$}");
    let half = format!("{:0<k$}", "5");
    let odd = keep.ends_with(['1', '3', '5', '7', '9']);
    let up = match rest.cmp(&half) {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Equal => x.fract() != 0.0 || odd,
    };
    let mut digits = if keep.is_empty() {
        b"0".to_vec()
    } else {
        keep.as_bytes().to_vec()
    };
    if up {
        let mut i = digits.len();
        loop {
            if i == 0 {
                digits.insert(0, b'1');
                break;
            }
            i -= 1;
            if digits[i] == b'9' {
                digits[i] = b'0';
            } else {
                digits[i] += 1;
                break;
            }
        }
    }
    let sign = if x.is_sign_negative() { "-" } else { "" };
    format!("{sign}{}e{k}", String::from_utf8_lossy(&digits))
}

/// Python's `f"{x:,.{digits}f}"`: comma thousands, `digits` places, and
/// `nan`, `inf`, `-inf` spelled as Python spells them.
fn grouped(x: f64, digits: usize) -> String {
    if x.is_nan() {
        return "nan".into();
    }
    if x.is_infinite() {
        return if x > 0.0 { "inf" } else { "-inf" }.into();
    }
    let text = format!("{:.digits$}", x.abs());
    let (int, frac) = text.split_at(text.find('.').unwrap_or(text.len()));
    let mut out = String::with_capacity(text.len() + text.len() / 3 + 1);
    if x.is_sign_negative() {
        out.push('-');
    }
    for (i, c) in int.char_indices() {
        if i > 0 && (int.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out.push_str(frac);
    out
}

/// An order with its status, fills and log: ib_async's `Trade`.
///
/// Held as a [`Live<Trade>`], which carries its seven events. Two trades
/// are equal when their fields are, their orders compared by identity, as
/// ib_async's dataclass compares them.
#[derive(Clone, Debug, PartialEq)]
pub struct Trade {
    /// ib_async's `contract`.
    pub contract: Contract,
    /// ib_async's `order`: the handle given to `place_order`, updated in
    /// place.
    pub order: Live<Order>,
    /// ib_async's `orderStatus`.
    pub order_status: OrderStatus,
    /// ib_async's `fills`.
    pub fills: Vec<Fill>,
    /// ib_async's `log`.
    pub log: Vec<TradeLogEntry>,
    /// ib_async's `advancedError`.
    pub advanced_error: String,
}

/// ib_async's `Trade()`: a default contract and a new default order.
impl Default for Trade {
    fn default() -> Self {
        Trade {
            contract: Contract::default(),
            order: Live::new(Order::default()),
            order_status: OrderStatus::default(),
            fills: Vec::new(),
            log: Vec::new(),
            advanced_error: String::new(),
        }
    }
}

impl Trade {
    /// The names of a trade's events: ib_async's `Trade.events`.
    pub const EVENTS: [&'static str; 7] = [
        "statusEvent",
        "modifyEvent",
        "fillEvent",
        "commissionReportEvent",
        "filledEvent",
        "cancelEvent",
        "cancelledEvent",
    ];

    /// Sent but not yet working: the status is one of
    /// [`OrderStatus::WAITING_STATES`]. ib_async's `isWaiting`.
    pub fn is_waiting(&self) -> bool {
        OrderStatus::WAITING_STATES.contains(&self.order_status.status.as_str())
    }

    /// Working at the broker: the status is one of
    /// [`OrderStatus::WORKING_STATES`]. ib_async's `isWorking`.
    pub fn is_working(&self) -> bool {
        OrderStatus::WORKING_STATES.contains(&self.order_status.status.as_str())
    }

    /// Able to execute: the status is one of
    /// [`OrderStatus::ACTIVE_STATES`]. ib_async's `isActive`.
    pub fn is_active(&self) -> bool {
        OrderStatus::ACTIVE_STATES.contains(&self.order_status.status.as_str())
    }

    /// Filled or cancelled: the status is one of
    /// [`OrderStatus::DONE_STATES`]. ib_async's `isDone`.
    pub fn is_done(&self) -> bool {
        OrderStatus::DONE_STATES.contains(&self.order_status.status.as_str())
    }

    /// The shares filled, summed over the fills; for a `BAG` contract only
    /// the fills of the combo itself count, not those of its legs.
    /// ib_async's `filled`.
    pub fn filled(&self) -> f64 {
        let bag = self.contract.sec_type == "BAG";
        py_sum(
            self.fills
                .iter()
                .filter(|f| !bag || f.contract.sec_type == "BAG")
                .map(|f| f.execution.shares),
        )
    }

    /// The order's total quantity less [`filled`](Trade::filled):
    /// ib_async's `remaining`.
    pub fn remaining(&self) -> f64 {
        self.order.read().total_quantity - self.filled()
    }
}

/// Python's `sum` of floats, which since Python 3.12 carries the rounding
/// error along (Neumaier's method) and adds it back at the end.
fn py_sum(xs: impl Iterator<Item = f64>) -> f64 {
    let (mut sum, mut carry) = (0.0_f64, 0.0_f64);
    for x in xs {
        let t = sum + x;
        carry += if sum.abs() >= x.abs() {
            (sum - t) + x
        } else {
            (x - t) + sum
        };
        sum = t;
    }
    if carry != 0.0 && carry.is_finite() {
        sum + carry
    } else {
        sum
    }
}

mod events {
    use super::{CommissionReport, Fill, Live, Trade};
    use crate::event::Event;

    /// A trade's seven events. Nominally public, since the storage of a
    /// public type cannot name a private one; the module keeps it unnamed
    /// outside the crate.
    pub struct TradeEvents {
        pub status: Event<Live<Trade>>,
        pub modify: Event<Live<Trade>>,
        pub fill: Event<(Live<Trade>, Fill)>,
        pub commission_report: Event<(Live<Trade>, Fill, Live<CommissionReport>)>,
        pub filled: Event<Live<Trade>>,
        pub cancel: Event<Live<Trade>>,
        pub cancelled: Event<Live<Trade>>,
    }
}

impl sealed::Storage for Trade {
    type Events = events::TradeEvents;
    fn events(m: &sealed::Maker) -> events::TradeEvents {
        events::TradeEvents {
            status: m.event("statusEvent"),
            modify: m.event("modifyEvent"),
            fill: m.event("fillEvent"),
            commission_report: m.event("commissionReportEvent"),
            filled: m.event("filledEvent"),
            cancel: m.event("cancelEvent"),
            cancelled: m.event("cancelledEvent"),
        }
    }
}

impl Observed for Trade {}

impl Live<Trade> {
    /// A status change, a cancel, or a warning or error on the order:
    /// ib_async's `Trade.statusEvent`.
    pub fn status_event(&self) -> &Event<Live<Trade>> {
        &self.events().status
    }

    /// The order was modified: ib_async's `Trade.modifyEvent`.
    pub fn modify_event(&self) -> &Event<Live<Trade>> {
        &self.events().modify
    }

    /// A new fill: ib_async's `Trade.fillEvent`.
    pub fn fill_event(&self) -> &Event<(Live<Trade>, Fill)> {
        &self.events().fill
    }

    /// A fill's commission report, which is also the fill's own:
    /// ib_async's `Trade.commissionReportEvent`.
    pub fn commission_report_event(&self) -> &Event<(Live<Trade>, Fill, Live<CommissionReport>)> {
        &self.events().commission_report
    }

    /// The order became `Filled`: ib_async's `Trade.filledEvent`.
    pub fn filled_event(&self) -> &Event<Live<Trade>> {
        &self.events().filled
    }

    /// A cancel was sent: ib_async's `Trade.cancelEvent`.
    pub fn cancel_event(&self) -> &Event<Live<Trade>> {
        &self.events().cancel
    }

    /// The order became `Cancelled`: ib_async's `Trade.cancelledEvent`.
    pub fn cancelled_event(&self) -> &Event<Live<Trade>> {
        &self.events().cancelled
    }
}

/// By the trades' current values, as ib_async's dataclass compares them.
impl PartialEq for Live<Trade> {
    fn eq(&self, other: &Self) -> bool {
        *self.read() == *other.read()
    }
}

/// A parent order with its take-profit and stop-loss orders: ib_async's
/// `BracketOrder`.
#[derive(Clone, Debug, PartialEq)]
pub struct BracketOrder {
    /// ib_async's `parent`.
    pub parent: Live<Order>,
    /// ib_async's `takeProfit`.
    pub take_profit: Live<Order>,
    /// ib_async's `stopLoss`.
    pub stop_loss: Live<Order>,
}

/// A condition on an order: ib_async's `OrderCondition`, one variant per
/// subclass.
#[derive(Clone, Debug, PartialEq)]
pub enum OrderCondition {
    /// ib_async's `PriceCondition`.
    Price(PriceCondition),
    /// ib_async's `TimeCondition`.
    Time(TimeCondition),
    /// ib_async's `MarginCondition`.
    Margin(MarginCondition),
    /// ib_async's `ExecutionCondition`.
    Execution(ExecutionCondition),
    /// ib_async's `VolumeCondition`.
    Volume(VolumeCondition),
    /// ib_async's `PercentChangeCondition`.
    PercentChange(PercentChangeCondition),
}

impl OrderCondition {
    /// The default condition of type `cond_type` (1, 3, 4, 5, 6 or 7):
    /// ib_async's `createClass`. Another type is `Err(Value)`, where
    /// ib_async raises `KeyError`.
    pub fn create_class(cond_type: i32) -> Result<OrderCondition> {
        Ok(match cond_type {
            1 => OrderCondition::Price(PriceCondition::default()),
            3 => OrderCondition::Time(TimeCondition::default()),
            4 => OrderCondition::Margin(MarginCondition::default()),
            5 => OrderCondition::Execution(ExecutionCondition::default()),
            6 => OrderCondition::Volume(VolumeCondition::default()),
            7 => OrderCondition::PercentChange(PercentChangeCondition::default()),
            // KeyError's text is the missing key.
            other => return Err(Error::Value(other.to_string())),
        })
    }

    /// The condition joined to the next by and (`conjunction = "a"`):
    /// ib_async's `And`.
    #[must_use]
    pub fn and(mut self) -> Self {
        *self.conjunction_mut() = "a".into();
        self
    }

    /// The condition joined to the next by or (`conjunction = "o"`):
    /// ib_async's `Or`.
    #[must_use]
    pub fn or(mut self) -> Self {
        *self.conjunction_mut() = "o".into();
        self
    }

    fn conjunction_mut(&mut self) -> &mut String {
        match self {
            OrderCondition::Price(c) => &mut c.conjunction,
            OrderCondition::Time(c) => &mut c.conjunction,
            OrderCondition::Margin(c) => &mut c.conjunction,
            OrderCondition::Execution(c) => &mut c.conjunction,
            OrderCondition::Volume(c) => &mut c.conjunction,
            OrderCondition::PercentChange(c) => &mut c.conjunction,
        }
    }
}

/// A condition on a contract's price: ib_async's `PriceCondition`.
#[derive(Clone, Debug, PartialEq)]
pub struct PriceCondition {
    /// ib_async's `condType`: 1.
    pub cond_type: i32,
    /// ib_async's `conjunction`: `"a"` (and) or `"o"` (or).
    pub conjunction: String,
    /// ib_async's `isMore`.
    pub is_more: bool,
    /// ib_async's `price`.
    pub price: f64,
    /// ib_async's `conId`.
    pub con_id: i64,
    /// ib_async's `exch`.
    pub exch: String,
    /// ib_async's `triggerMethod`.
    pub trigger_method: i32,
}

impl Default for PriceCondition {
    fn default() -> Self {
        PriceCondition {
            cond_type: 1,
            conjunction: "a".into(),
            is_more: true,
            price: 0.0,
            con_id: 0,
            exch: String::new(),
            trigger_method: 0,
        }
    }
}

/// A condition on the time: ib_async's `TimeCondition`.
#[derive(Clone, Debug, PartialEq)]
pub struct TimeCondition {
    /// ib_async's `condType`: 3.
    pub cond_type: i32,
    /// ib_async's `conjunction`: `"a"` (and) or `"o"` (or).
    pub conjunction: String,
    /// ib_async's `isMore`.
    pub is_more: bool,
    /// ib_async's `time`.
    pub time: String,
}

impl Default for TimeCondition {
    fn default() -> Self {
        TimeCondition {
            cond_type: 3,
            conjunction: "a".into(),
            is_more: true,
            time: String::new(),
        }
    }
}

/// A condition on the account's margin cushion: ib_async's
/// `MarginCondition`.
#[derive(Clone, Debug, PartialEq)]
pub struct MarginCondition {
    /// ib_async's `condType`: 4.
    pub cond_type: i32,
    /// ib_async's `conjunction`: `"a"` (and) or `"o"` (or).
    pub conjunction: String,
    /// ib_async's `isMore`.
    pub is_more: bool,
    /// ib_async's `percent`.
    pub percent: i32,
}

impl Default for MarginCondition {
    fn default() -> Self {
        MarginCondition {
            cond_type: 4,
            conjunction: "a".into(),
            is_more: true,
            percent: 0,
        }
    }
}

/// A condition on an execution in a contract: ib_async's
/// `ExecutionCondition`.
#[derive(Clone, Debug, PartialEq)]
pub struct ExecutionCondition {
    /// ib_async's `condType`: 5.
    pub cond_type: i32,
    /// ib_async's `conjunction`: `"a"` (and) or `"o"` (or).
    pub conjunction: String,
    /// ib_async's `secType`.
    pub sec_type: String,
    /// ib_async's `exch`.
    pub exch: String,
    /// ib_async's `symbol`.
    pub symbol: String,
}

impl Default for ExecutionCondition {
    fn default() -> Self {
        ExecutionCondition {
            cond_type: 5,
            conjunction: "a".into(),
            sec_type: String::new(),
            exch: String::new(),
            symbol: String::new(),
        }
    }
}

/// A condition on a contract's traded volume: ib_async's
/// `VolumeCondition`.
#[derive(Clone, Debug, PartialEq)]
pub struct VolumeCondition {
    /// ib_async's `condType`: 6.
    pub cond_type: i32,
    /// ib_async's `conjunction`: `"a"` (and) or `"o"` (or).
    pub conjunction: String,
    /// ib_async's `isMore`.
    pub is_more: bool,
    /// ib_async's `volume`, at the engine's width: Python's `int` holds it
    /// whole.
    pub volume: i64,
    /// ib_async's `conId`.
    pub con_id: i64,
    /// ib_async's `exch`.
    pub exch: String,
}

impl Default for VolumeCondition {
    fn default() -> Self {
        VolumeCondition {
            cond_type: 6,
            conjunction: "a".into(),
            is_more: true,
            volume: 0,
            con_id: 0,
            exch: String::new(),
        }
    }
}

/// A condition on a contract's percent change: ib_async's
/// `PercentChangeCondition`.
#[derive(Clone, Debug, PartialEq)]
pub struct PercentChangeCondition {
    /// ib_async's `condType`: 7.
    pub cond_type: i32,
    /// ib_async's `conjunction`: `"a"` (and) or `"o"` (or).
    pub conjunction: String,
    /// ib_async's `isMore`.
    pub is_more: bool,
    /// ib_async's `changePercent`.
    pub change_percent: f64,
    /// ib_async's `conId`.
    pub con_id: i64,
    /// ib_async's `exch`.
    pub exch: String,
}

impl Default for PercentChangeCondition {
    fn default() -> Self {
        PercentChangeCondition {
            cond_type: 7,
            conjunction: "a".into(),
            is_more: true,
            change_percent: 0.0,
            con_id: 0,
            exch: String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::hash::{BuildHasher, RandomState};

    use super::*;
    use crate::objects::Execution;
    use crate::util::EPOCH;

    fn fill(sec_type: &str, shares: f64) -> Fill {
        Fill {
            contract: Contract {
                sec_type: sec_type.into(),
                ..Contract::default()
            },
            execution: Execution {
                shares,
                ..Execution::default()
            },
            commission_report: Live::new(CommissionReport::default()),
            time: EPOCH.clone(),
        }
    }

    fn in_status(status: &str) -> Trade {
        Trade {
            order_status: OrderStatus {
                status: status.into(),
                ..OrderStatus::default()
            },
            ..Trade::default()
        }
    }

    fn margin(text: &str) -> OrderState {
        OrderState {
            init_margin_before: text.into(),
            ..OrderState::default()
        }
    }

    #[test]
    fn predicates_follow_the_state_sets() {
        // (status, waiting, working, active, done), as ib_async's frozensets.
        let (t, f) = (true, false);
        for (status, waiting, working, active, done) in [
            ("PendingSubmit", t, f, t, f),
            ("ApiPending", t, f, t, f),
            ("PreSubmitted", t, f, t, f),
            ("Submitted", f, t, t, f),
            ("ValidationError", f, t, t, f),
            ("ApiUpdate", f, t, t, f),
            ("PendingCancel", f, f, f, f),
            ("Filled", f, f, f, t),
            ("Cancelled", f, f, f, t),
            ("ApiCancelled", f, f, f, t),
            ("Inactive", f, f, f, t),
            ("", f, f, f, f),
        ] {
            let trade = in_status(status);
            assert_eq!(trade.is_waiting(), waiting, "{status}");
            assert_eq!(trade.is_working(), working, "{status}");
            assert_eq!(trade.is_active(), active, "{status}");
            assert_eq!(trade.is_done(), done, "{status}");
        }
    }

    #[test]
    fn filled_sums_as_python_and_a_bag_counts_its_own_fills() {
        let mut trade = Trade::default();
        assert_eq!(trade.filled(), 0.0);
        trade.fills = vec![fill("STK", 0.1); 10];
        trade.order.update(|o| o.total_quantity = 1.0);
        // Python's compensated sum: exactly 1.0, where a plain one gives
        // 0.9999999999999999.
        assert_eq!(trade.filled(), 1.0);
        assert_eq!(trade.remaining(), 0.0);

        trade.fills = vec![fill("BAG", 2.0), fill("STK", 3.0), fill("OPT", 5.0)];
        assert_eq!(trade.filled(), 10.0);
        trade.contract.sec_type = "BAG".into();
        assert_eq!(trade.filled(), 2.0);
        trade.order.update(|o| o.total_quantity = 5.0);
        assert_eq!(trade.remaining(), 3.0);
    }

    #[test]
    fn python_sum_edges() {
        assert_eq!(py_sum([1e100, 1.0, -1e100].into_iter()), 1.0);
        assert_eq!(py_sum([f64::INFINITY, 1.0].into_iter()), f64::INFINITY);
        assert!(py_sum([f64::INFINITY, f64::NEG_INFINITY].into_iter()).is_nan());
        assert!(py_sum([-0.0].into_iter()).is_sign_positive());
    }

    #[test]
    fn order_status_names_and_sets() {
        assert_eq!(OrderStatus::PENDING_SUBMIT, "PendingSubmit");
        assert_eq!(OrderStatus::PENDING_CANCEL, "PendingCancel");
        assert_eq!(OrderStatus::PRE_SUBMITTED, "PreSubmitted");
        assert_eq!(OrderStatus::SUBMITTED, "Submitted");
        assert_eq!(OrderStatus::API_PENDING, "ApiPending");
        assert_eq!(OrderStatus::API_CANCELLED, "ApiCancelled");
        assert_eq!(OrderStatus::API_UPDATE, "ApiUpdate");
        assert_eq!(OrderStatus::CANCELLED, "Cancelled");
        assert_eq!(OrderStatus::FILLED, "Filled");
        assert_eq!(OrderStatus::INACTIVE, "Inactive");
        assert_eq!(OrderStatus::VALIDATION_ERROR, "ValidationError");
        assert_eq!(
            OrderStatus::DONE_STATES,
            ["Filled", "Cancelled", "ApiCancelled", "Inactive"]
        );
        assert_eq!(
            OrderStatus::ACTIVE_STATES,
            [
                "PendingSubmit",
                "ApiPending",
                "PreSubmitted",
                "Submitted",
                "ValidationError",
                "ApiUpdate"
            ]
        );
        assert_eq!(
            OrderStatus::WAITING_STATES,
            ["PendingSubmit", "ApiPending", "PreSubmitted"]
        );
        assert_eq!(
            OrderStatus::WORKING_STATES,
            ["Submitted", "ValidationError", "ApiUpdate"]
        );
        let s = OrderStatus {
            filled: 2.5,
            remaining: 7.5,
            ..OrderStatus::default()
        };
        assert_eq!(s.total(), 10.0);
    }

    #[test]
    fn transform_hands_margins_as_text_and_commissions_as_numbers() {
        let state = OrderState {
            status: "PreSubmitted".into(),
            init_margin_before: "1".into(),
            maint_margin_before: "2".into(),
            equity_with_loan_before: "3".into(),
            init_margin_change: "4".into(),
            maint_margin_change: "5".into(),
            equity_with_loan_change: "6".into(),
            init_margin_after: "7".into(),
            maint_margin_after: "8".into(),
            equity_with_loan_after: "9".into(),
            commission: Some(1.5),
            min_commission: None,
            max_commission: Some(2.5),
            commission_currency: "USD".into(),
            warning_text: "w".into(),
            completed_time: "t".into(),
            completed_status: "c".into(),
        };
        let mut seen = Vec::new();
        let out = state.transform(|v| {
            seen.push(format!("{v:?}"));
            seen.len()
        });
        let texts: Vec<String> = (1..=9).map(|n| format!("Text(\"{n}\")")).collect();
        assert_eq!(seen[..9], texts[..]);
        assert_eq!(
            seen[9..],
            ["Number(Some(1.5))", "Number(None)", "Number(Some(2.5))"]
        );
        assert_eq!(
            out,
            OrderStateNumeric {
                status: "PreSubmitted".into(),
                init_margin_before: 1,
                maint_margin_before: 2,
                equity_with_loan_before: 3,
                init_margin_change: 4,
                maint_margin_change: 5,
                equity_with_loan_change: 6,
                init_margin_after: 7,
                maint_margin_after: 8,
                equity_with_loan_after: 9,
                commission: 10,
                min_commission: 11,
                max_commission: 12,
                commission_currency: "USD".into(),
                warning_text: "w".into(),
                completed_time: "t".into(),
                completed_status: "c".into(),
            }
        );
    }

    #[test]
    fn numeric_rounds_as_python() {
        let n = |text: &str, digits| margin(text).numeric(digits).init_margin_before;
        // Each expected value is what Python gives.
        assert_eq!(n("0.125", 2), Some(0.12));
        assert_eq!(n("2.675", 2), Some(2.67));
        assert_eq!(n("1234.5", -2), Some(1200.0));
        // Python's float() syntax; what it refuses, and UNSET, give None.
        assert_eq!(n(" 1_000.125 ", 2), Some(1000.12));
        assert_eq!(n("", 2), None);
        assert_eq!(n("abc", 2), None);
        assert_eq!(n("1.7976931348623157e308", 2), None);
        assert!(n("nan", 2).is_some_and(f64::is_nan));
        assert_eq!(n("-inf", 2), Some(f64::NEG_INFINITY));
        // Ties to even, on the exact binary value, both sides of the point.
        assert_eq!(n("0.5", 0), Some(0.0));
        assert_eq!(n("1.5", 0), Some(2.0));
        assert_eq!(n("2.5", 0), Some(2.0));
        assert_eq!(n("1250", -2), Some(1200.0));
        assert_eq!(n("1350", -2), Some(1400.0));
        assert_eq!(n("1250.4", -2), Some(1300.0));
        assert_eq!(n("-1250", -2), Some(-1200.0));
        assert_eq!(n("999.9", -3), Some(1000.0));
        assert_eq!(n("99999", -2), Some(100000.0));
        assert!(n("-4", -2).is_some_and(|x| x == 0.0 && x.is_sign_negative()));
        // CPython's bounds, and its OverflowError, which floatOrNone
        // turns into None.
        assert_eq!(n("5e-324", 400), Some(5e-324));
        assert!(n("-123.456", -400).is_some_and(|x| x == 0.0 && x.is_sign_negative()));
        assert_eq!(n("1.7e308", -308), None);
        assert_eq!(n("1.7e308", i32::MAX), Some(1.7e308));
        assert_eq!(n("1.7e308", i32::MIN), Some(0.0));

        let state = OrderState {
            commission: Some(0.125),
            min_commission: Some(UNSET_DOUBLE),
            ..OrderState::default()
        };
        let numeric = state.numeric(2);
        assert_eq!(numeric.commission, Some(0.12));
        assert_eq!(numeric.min_commission, None);
        assert_eq!(numeric.max_commission, None);
    }

    #[test]
    fn formatted_writes_as_python() {
        let f = |text: &str, digits| margin(text).formatted(digits).init_margin_before;
        // ib_async's own examples, from its comments.
        assert_eq!(f("300000.21", 2).as_deref(), Some("300,000.21"));
        assert_eq!(f("0.0", 2).as_deref(), Some("0.00"));
        assert_eq!(f("431.342000000001", 2).as_deref(), Some("431.34"));
        assert_eq!(f("", 2), None);
        assert_eq!(f("-1234567.891", 2).as_deref(), Some("-1,234,567.89"));
        assert_eq!(f("-0.0", 2).as_deref(), Some("-0.00"));
        assert_eq!(f("1234.5", 0).as_deref(), Some("1,234"));
        assert_eq!(f("1e3", 2).as_deref(), Some("1,000.00"));
        assert_eq!(f("nan", 2).as_deref(), Some("nan"));
        assert_eq!(f("-nan", 2).as_deref(), Some("nan"));
        assert_eq!(f("inf", 2).as_deref(), Some("inf"));
        assert_eq!(f("-inf", 2).as_deref(), Some("-inf"));
        // Rounded to 8 places first, as ib_async's numeric(8) does.
        assert_eq!(f("0.0049999999999", 2).as_deref(), Some("0.01"));
        let state = OrderState {
            commission: Some(1.5),
            ..OrderState::default()
        };
        let text = state.formatted(2);
        assert_eq!(text.commission.as_deref(), Some("1.50"));
        assert_eq!(text.min_commission, None);
    }

    #[test]
    fn numeric_default_is_ib_asyncs() {
        let d = OrderStateNumeric::default();
        for v in [
            d.init_margin_before,
            d.maint_margin_before,
            d.equity_with_loan_before,
            d.init_margin_change,
            d.maint_margin_change,
            d.equity_with_loan_change,
            d.init_margin_after,
            d.maint_margin_after,
            d.equity_with_loan_after,
        ] {
            assert!(v.is_some_and(f64::is_nan));
        }
        assert_eq!(
            (d.commission, d.min_commission, d.max_commission),
            (None, None, None)
        );
    }

    #[test]
    fn combinators_set_the_conjunction() {
        let c = OrderCondition::Price(PriceCondition::default());
        let or = c.clone().or();
        assert!(matches!(&or, OrderCondition::Price(p) if p.conjunction == "o"));
        let and = or.and();
        assert_eq!(and, c);
        let v = OrderCondition::Volume(VolumeCondition::default()).or();
        assert!(matches!(v, OrderCondition::Volume(p) if p.conjunction == "o"));
    }

    #[test]
    fn create_class_gives_each_default_condition() {
        for (t, want) in [
            (1, OrderCondition::Price(PriceCondition::default())),
            (3, OrderCondition::Time(TimeCondition::default())),
            (4, OrderCondition::Margin(MarginCondition::default())),
            (5, OrderCondition::Execution(ExecutionCondition::default())),
            (6, OrderCondition::Volume(VolumeCondition::default())),
            (
                7,
                OrderCondition::PercentChange(PercentChangeCondition::default()),
            ),
        ] {
            assert_eq!(OrderCondition::create_class(t).unwrap(), want);
        }
        let p = PriceCondition::default();
        assert_eq!(
            (p.cond_type, p.conjunction.as_str(), p.is_more),
            (1, "a", true)
        );
        assert_eq!(TimeCondition::default().cond_type, 3);
        assert_eq!(MarginCondition::default().cond_type, 4);
        assert_eq!(ExecutionCondition::default().cond_type, 5);
        assert_eq!(VolumeCondition::default().cond_type, 6);
        assert_eq!(PercentChangeCondition::default().cond_type, 7);
        for t in [0, 2, 8] {
            let e = OrderCondition::create_class(t);
            assert!(matches!(e, Err(Error::Value(m)) if m == t.to_string()));
        }
    }

    #[test]
    fn trade_compares_its_fields_and_its_order_by_identity() {
        let a = Trade::default();
        let b = Trade::default();
        assert_ne!(a, b);
        let mut c = a.clone();
        assert_eq!(a, c);
        c.fills.push(fill("STK", 1.0));
        assert_ne!(a, c);

        let (x, y) = (Live::new(a.clone()), Live::new(a.clone()));
        assert!(x == y && !Live::ptr_eq(&x, &y));
        y.update(|t| t.advanced_error = "e".into());
        assert!(x != y);
        assert!(x != Live::new(b));
    }

    #[test]
    fn a_live_order_is_its_identity() {
        let a = Live::new(Order::limit("BUY", 1, 10.5));
        let b = Live::new(Order::limit("BUY", 1, 10.5));
        assert!(a == a.clone() && a != b);
        // Hashed by identity, whatever the value.
        let s = RandomState::new();
        assert_eq!(s.hash_one(&a), s.hash_one(a.clone()));
        a.update(|o| o.order_id = 7);
        assert_eq!(s.hash_one(&a), s.hash_one(a.clone()));
    }

    #[test]
    fn constructors_and_defaults() {
        let d = Order::default();
        assert!(d.transmit);
        assert_eq!((d.open_close.as_str(), d.exempt_code), ("O", -1));
        assert_eq!((d.lmt_price, d.aux_price, d.min_qty), (None, None, None));
        assert_eq!(d.soft_dollar_tier, SoftDollarTier::default());

        let l = Order::limit("BUY", 100, 10.5);
        assert_eq!(
            (l.order_type.as_str(), l.action.as_str(), l.total_quantity),
            ("LMT", "BUY", 100.0)
        );
        assert_eq!((l.lmt_price, l.aux_price), (Some(10.5), None));
        assert!(l.transmit);
        let m = Order::market("SELL", 2.5);
        assert_eq!((m.order_type.as_str(), m.total_quantity), ("MKT", 2.5));
        assert_eq!((m.lmt_price, m.aux_price), (None, None));
        let s = Order::stop("SELL", 1, 9);
        assert_eq!(s.order_type, "STP");
        assert_eq!((s.lmt_price, s.aux_price), (None, Some(9.0)));
        let sl = Order::stop_limit("BUY", 1, 10, 9.5);
        assert_eq!(sl.order_type, "STP LMT");
        assert_eq!((sl.lmt_price, sl.aux_price), (Some(10.0), Some(9.5)));
        assert_eq!(OrderComboLeg::default().price, None);
    }

    #[test]
    fn a_live_trade_carries_its_seven_events() {
        let t = Live::new(Trade::default());
        let names = [
            t.status_event().name(),
            t.modify_event().name(),
            t.fill_event().name(),
            t.commission_report_event().name(),
            t.filled_event().name(),
            t.cancel_event().name(),
            t.cancelled_event().name(),
        ];
        assert_eq!(names, Trade::EVENTS);

        let report = Live::new(CommissionReport::default());
        t.commission_report_event()
            .emit(&(t.clone(), fill("STK", 1.0), report.clone()));
        let (trade, f, r) = t.commission_report_event().value().unwrap();
        assert!(Live::ptr_eq(&trade, &t) && Live::ptr_eq(&r, &report));
        assert_eq!(f.execution.shares, 1.0);
        drop(trade);
        let fills = t.fill_event().clone();
        fills.emit(&(t.clone(), fill("STK", 2.0)));
        drop(t);
        assert!(fills.value().is_none());
    }

    #[test]
    fn a_bracket_compares_its_orders_by_identity() {
        let o = || Live::new(Order::default());
        let b = BracketOrder {
            parent: o(),
            take_profit: o(),
            stop_loss: o(),
        };
        assert_eq!(b, b.clone());
        let other = BracketOrder {
            parent: o(),
            ..b.clone()
        };
        assert_ne!(b, other);
    }
}

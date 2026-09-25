//! Conversions between this crate's types and the engine's.
//!
//! A request's values go to the engine by `From`/`TryFrom<&Ours>` for the
//! engine's type; a callback's come back by `From`/`TryFrom<&Engine>` for
//! ours, or by a function where ib_async's value needs more than the engine's
//! one value. The engine states "not set" as `f64::MAX` or `i32::MAX`, which
//! becomes `None` here; a `None` of ours goes to the engine as that field's
//! own default. A callback value that does not parse or narrow fails with
//! `Error::Value` naming the field, and the callback is dropped.

use jiff::Zoned;
use jiff::tz::TimeZone;

use crate::contract::{
    ComboLeg, Contract, ContractDescription, ContractDetails, DeltaNeutralContract, TagValue,
};
use crate::engine as e;
use crate::error::{Error, Result};
use crate::objects::{
    BarData, CommissionReport, CorporateAction, DepthMktDataDescription, Execution,
    ExecutionFilter, HistoricalNews, HistoricalTick, HistoricalTickAny, HistoricalTickBidAsk,
    HistoricalTickLast, NewsProvider, OptionComputation, OptionModel, PositionElsewhere,
    PriceIncrement, ScannerSubscription, SmartComponent, SoftDollarTier, TickAttrib,
    TickAttribBidAsk, TickAttribLast, TickerExtras, WshEventData,
};
use crate::order::{
    ExecutionCondition, MarginCondition, Order, OrderComboLeg, OrderCondition, OrderState,
    PercentChangeCondition, PriceCondition, TimeCondition, VolumeCondition,
};
use crate::util::{BarDate, parse_ib_datetime};

/// A value the engine cannot carry, refused as a gateway refuses it: the
/// code and text `EClient::refuse` pushes at the call. `pub` because it is
/// the error of the `TryFrom` impls on the engine's types; the module is
/// private.
#[derive(Clone, Debug, PartialEq)]
pub struct Refusal {
    /// The gateway's error code.
    pub code: i64,
    /// The gateway's text.
    pub msg: String,
}

/// A value this client cannot carry, refused as a gateway refuses a message
/// it cannot read: 320, `Error reading request:` and why. The code, the
/// prefix and the wording after it are the Python package's for the same
/// case (`bridge.py`, `_unreadable` and `_as_ours`); what a gateway writes for
/// these values is not established.
fn uncarried(field: &str, v: impl std::fmt::Display, why: &str) -> Refusal {
    Refusal {
        code: 320,
        msg: format!(
            "Error reading request:{field} was set to {v}, which this client cannot carry: {why}"
        ),
    }
}

/// An `Order` id the TWS API declares `int`, refused past `i32`, which a
/// gateway cannot read.
fn int(field: &str, v: i64) -> Result<i32, Refusal> {
    i32::try_from(v).map_err(|_| uncarried(&format!("Order.{field}"), v, "past the TWS API's int"))
}

/// A callback value that did not convert, naming the field.
fn bad(field: &str, value: &str, why: impl std::fmt::Display) -> Error {
    Error::Value(format!("{field} {value:?}: {why}"))
}

/// `None` where the engine states its unset double.
fn stated(v: f64) -> Option<f64> {
    (v != f64::MAX).then_some(v)
}

/// `None` where the engine states its unset integer.
fn stated_int(v: i32) -> Option<i32> {
    (v != i32::MAX).then_some(v)
}

/// Whether `s` is a stamp as the venue writes it, `YYYYMMDD-HH:MM:SS` with
/// any fraction, which the engine hands over as written and which is in UTC
/// (the engine's own reading, `protocol::datetime::ib_datetime_to_unix`).
fn venue_stamp(s: &str) -> bool {
    s.as_bytes().get(8) == Some(&b'-')
}

/// A time the engine states, parsed as `parseIBDatetime` parses it: an
/// instant as it is, a naive time in UTC as the venue stamps it, then in
/// `timezone`. A date, or a time that does not parse or that UTC cannot hold,
/// fails.
fn stamped(field: &str, s: &str, timezone: &TimeZone) -> Result<Zoned> {
    let t = match parse_ib_datetime(s) {
        Ok(BarDate::At(t)) => t,
        Ok(BarDate::Naive(t)) => t
            .to_zoned(TimeZone::UTC)
            .map_err(|why| bad(field, s, why))?,
        Ok(BarDate::Day(_)) => return Err(bad(field, s, "a date, not a time")),
        Err(why) => return Err(bad(field, s, why)),
    };
    Ok(t.with_time_zone(timezone.clone()))
}

fn tags(v: &[TagValue]) -> Vec<e::TagValue> {
    v.iter().map(Into::into).collect()
}

fn our_tags(v: &[e::TagValue]) -> Vec<TagValue> {
    v.iter().map(Into::into).collect()
}

impl From<&TagValue> for e::TagValue {
    fn from(t: &TagValue) -> Self {
        e::TagValue {
            tag: t.tag.clone(),
            value: t.value.clone(),
        }
    }
}

impl From<&e::TagValue> for TagValue {
    fn from(t: &e::TagValue) -> Self {
        TagValue {
            tag: t.tag.clone(),
            value: t.value.clone(),
        }
    }
}

impl From<&ComboLeg> for e::ComboLeg {
    fn from(l: &ComboLeg) -> Self {
        e::ComboLeg {
            con_id: l.con_id,
            ratio: l.ratio,
            action: l.action.clone(),
            exchange: l.exchange.clone(),
            open_close: l.open_close,
            shorting_policy: l.short_sale_slot,
            designated_location: l.designated_location.clone(),
            exempt_code: l.exempt_code,
        }
    }
}

impl From<&e::ComboLeg> for ComboLeg {
    fn from(l: &e::ComboLeg) -> Self {
        ComboLeg {
            con_id: l.con_id,
            ratio: l.ratio,
            action: l.action.clone(),
            exchange: l.exchange.clone(),
            open_close: l.open_close,
            short_sale_slot: l.shorting_policy,
            designated_location: l.designated_location.clone(),
            exempt_code: l.exempt_code,
        }
    }
}

impl From<&DeltaNeutralContract> for e::DeltaNeutralContract {
    fn from(d: &DeltaNeutralContract) -> Self {
        e::DeltaNeutralContract {
            con_id: d.con_id,
            delta: d.delta,
            price: d.price,
        }
    }
}

impl From<&e::DeltaNeutralContract> for DeltaNeutralContract {
    fn from(d: &e::DeltaNeutralContract) -> Self {
        DeltaNeutralContract {
            con_id: d.con_id,
            delta: d.delta,
            price: d.price,
        }
    }
}

impl From<&Contract> for e::Contract {
    fn from(c: &Contract) -> Self {
        e::Contract {
            con_id: c.con_id,
            symbol: c.symbol.clone(),
            sec_type: c.sec_type.clone(),
            exchange: c.exchange.clone(),
            currency: c.currency.clone(),
            last_trade_date_or_contract_month: c.last_trade_date_or_contract_month.clone(),
            strike: c.strike,
            right: c.right.clone(),
            multiplier: c.multiplier.clone(),
            local_symbol: c.local_symbol.clone(),
            primary_exchange: c.primary_exchange.clone(),
            trading_class: c.trading_class.clone(),
            // The engine's own; ib_async's Contract has no such field.
            last_trade_date: String::new(),
            include_expired: c.include_expired,
            sec_id_type: c.sec_id_type.clone(),
            sec_id: c.sec_id.clone(),
            description: c.description.clone(),
            issuer_id: c.issuer_id.clone(),
            combo_legs_descrip: c.combo_legs_descrip.clone(),
            combo_legs: c.combo_legs.iter().map(Into::into).collect(),
            delta_neutral_contract: c.delta_neutral_contract.as_ref().map(Into::into),
        }
    }
}

impl From<&e::Contract> for Contract {
    fn from(c: &e::Contract) -> Self {
        Contract {
            sec_type: c.sec_type.clone(),
            con_id: c.con_id,
            symbol: c.symbol.clone(),
            last_trade_date_or_contract_month: c.last_trade_date_or_contract_month.clone(),
            strike: c.strike,
            right: c.right.clone(),
            multiplier: c.multiplier.clone(),
            exchange: c.exchange.clone(),
            primary_exchange: c.primary_exchange.clone(),
            currency: c.currency.clone(),
            local_symbol: c.local_symbol.clone(),
            trading_class: c.trading_class.clone(),
            include_expired: c.include_expired,
            sec_id_type: c.sec_id_type.clone(),
            sec_id: c.sec_id.clone(),
            description: c.description.clone(),
            issuer_id: c.issuer_id.clone(),
            combo_legs_descrip: c.combo_legs_descrip.clone(),
            combo_legs: c.combo_legs.iter().map(Into::into).collect(),
            delta_neutral_contract: c.delta_neutral_contract.as_ref().map(Into::into),
        }
    }
}

/// Every value is sent as given: the engine holds `trigger_method` and
/// `percent` as the TWS API's `int`.
impl From<&OrderCondition> for e::OrderCondition {
    fn from(c: &OrderCondition) -> Self {
        let and = |conjunction: &str| conjunction == "a";
        match c {
            OrderCondition::Price(c) => e::OrderCondition::Price {
                con_id: c.con_id,
                exchange: c.exch.clone(),
                price: e::price_from_f64(c.price),
                is_more: c.is_more,
                trigger_method: c.trigger_method,
                is_conjunction_connection: and(&c.conjunction),
            },
            OrderCondition::Time(c) => e::OrderCondition::Time {
                time: c.time.clone(),
                is_more: c.is_more,
                is_conjunction_connection: and(&c.conjunction),
            },
            OrderCondition::Margin(c) => e::OrderCondition::Margin {
                percent: c.percent,
                is_more: c.is_more,
                is_conjunction_connection: and(&c.conjunction),
            },
            OrderCondition::Execution(c) => e::OrderCondition::Execution {
                symbol: c.symbol.clone(),
                exchange: c.exch.clone(),
                sec_type: c.sec_type.clone(),
                is_conjunction_connection: and(&c.conjunction),
            },
            OrderCondition::Volume(c) => e::OrderCondition::Volume {
                con_id: c.con_id,
                exchange: c.exch.clone(),
                volume: c.volume,
                is_more: c.is_more,
                is_conjunction_connection: and(&c.conjunction),
            },
            OrderCondition::PercentChange(c) => e::OrderCondition::PercentChange {
                con_id: c.con_id,
                exchange: c.exch.clone(),
                percent: c.change_percent,
                is_more: c.is_more,
                is_conjunction_connection: and(&c.conjunction),
            },
        }
    }
}

impl From<&e::OrderCondition> for OrderCondition {
    fn from(c: &e::OrderCondition) -> Self {
        let conjunction = |and: bool| if and { "a" } else { "o" }.to_owned();
        match c {
            e::OrderCondition::Price {
                con_id,
                exchange,
                price,
                is_more,
                trigger_method,
                is_conjunction_connection,
            } => OrderCondition::Price(PriceCondition {
                conjunction: conjunction(*is_conjunction_connection),
                is_more: *is_more,
                price: *price as f64 / e::PRICE_SCALE as f64,
                con_id: *con_id,
                exch: exchange.clone(),
                trigger_method: *trigger_method,
                ..PriceCondition::default()
            }),
            e::OrderCondition::Time {
                time,
                is_more,
                is_conjunction_connection,
            } => OrderCondition::Time(TimeCondition {
                conjunction: conjunction(*is_conjunction_connection),
                is_more: *is_more,
                time: time.clone(),
                ..TimeCondition::default()
            }),
            e::OrderCondition::Margin {
                percent,
                is_more,
                is_conjunction_connection,
            } => OrderCondition::Margin(MarginCondition {
                conjunction: conjunction(*is_conjunction_connection),
                is_more: *is_more,
                percent: *percent,
                ..MarginCondition::default()
            }),
            e::OrderCondition::Execution {
                symbol,
                exchange,
                sec_type,
                is_conjunction_connection,
            } => OrderCondition::Execution(ExecutionCondition {
                conjunction: conjunction(*is_conjunction_connection),
                sec_type: sec_type.clone(),
                exch: exchange.clone(),
                symbol: symbol.clone(),
                ..ExecutionCondition::default()
            }),
            e::OrderCondition::Volume {
                con_id,
                exchange,
                volume,
                is_more,
                is_conjunction_connection,
            } => OrderCondition::Volume(VolumeCondition {
                conjunction: conjunction(*is_conjunction_connection),
                is_more: *is_more,
                volume: *volume,
                con_id: *con_id,
                exch: exchange.clone(),
                ..VolumeCondition::default()
            }),
            e::OrderCondition::PercentChange {
                con_id,
                exchange,
                percent,
                is_more,
                is_conjunction_connection,
            } => OrderCondition::PercentChange(PercentChangeCondition {
                conjunction: conjunction(*is_conjunction_connection),
                is_more: *is_more,
                change_percent: *percent,
                con_id: *con_id,
                exch: exchange.clone(),
                ..PercentChangeCondition::default()
            }),
        }
    }
}

impl TryFrom<&Order> for e::Order {
    type Error = Refusal;

    /// The order as the engine takes it. What a gateway cannot read is
    /// refused under 320, and the three attributes the venue retired under
    /// the codes a gateway uses for them. `fa_profile` is taken and not applied, as
    /// ib_async sends it to no server at version 177 or later.
    fn try_from(o: &Order) -> Result<Self, Refusal> {
        let client_id = int("client_id", o.client_id)?;
        let delta_neutral_con_id = int("delta_neutral_con_id", o.delta_neutral_con_id)?;
        let reference_contract_id = int("reference_contract_id", o.reference_contract_id)?;
        let ref_futures_con_id = int("ref_futures_con_id", o.ref_futures_con_id)?;
        let conditions = o.conditions.iter().map(Into::into).collect();
        for (set, code, name) in [
            (o.e_trade_only, 10268, "EtradeOnly"),
            (o.firm_quote_only, 10269, "FirmQuoteOnly"),
            (o.nbbo_price_cap.is_some(), 10270, "NbboPriceCap"),
        ] {
            if set {
                return Err(Refusal {
                    code,
                    msg: format!("The '{name}' order attribute is not supported."),
                });
            }
        }
        let d = e::Order::default();
        Ok(e::Order {
            order_id: o.order_id,
            action: o.action.clone(),
            total_quantity: o.total_quantity,
            order_type: o.order_type.clone(),
            lmt_price: o.lmt_price.unwrap_or(d.lmt_price),
            aux_price: o.aux_price.unwrap_or(d.aux_price),
            tif: o.tif.clone(),
            outside_rth: o.outside_rth,
            display_size: o.display_size,
            min_qty: o.min_qty.unwrap_or(d.min_qty),
            hidden: o.hidden,
            good_after_time: o.good_after_time.clone(),
            good_till_date: o.good_till_date.clone(),
            oca_group: o.oca_group.clone(),
            trailing_percent: o.trailing_percent.unwrap_or(d.trailing_percent),
            algo_strategy: o.algo_strategy.clone(),
            algo_params: tags(&o.algo_params),
            what_if: o.what_if,
            cash_qty: o.cash_qty.unwrap_or(d.cash_qty),
            parent_id: o.parent_id,
            transmit: o.transmit,
            discretionary_amt: o.discretionary_amt,
            sweep_to_fill: o.sweep_to_fill,
            all_or_none: o.all_or_none,
            trigger_method: o.trigger_method,
            adjusted_order_type: o.adjusted_order_type.clone(),
            trigger_price: o.trigger_price.unwrap_or(d.trigger_price),
            adjusted_stop_price: o.adjusted_stop_price.unwrap_or(d.adjusted_stop_price),
            adjusted_stop_limit_price: o
                .adjusted_stop_limit_price
                .unwrap_or(d.adjusted_stop_limit_price),
            conditions,
            conditions_ignore_rth: o.conditions_ignore_rth,
            conditions_cancel_order: o.conditions_cancel_order,
            account: o.account.clone(),
            active_start_time: o.active_start_time.clone(),
            active_stop_time: o.active_stop_time.clone(),
            adjustable_trailing_unit: o.adjustable_trailing_unit,
            adjusted_trailing_amount: o
                .adjusted_trailing_amount
                .unwrap_or(d.adjusted_trailing_amount),
            advanced_error_override: o.advanced_error_override.clone(),
            algo_id: o.algo_id.clone(),
            auction_strategy: o.auction_strategy,
            auto_cancel_date: o.auto_cancel_date.clone(),
            auto_cancel_parent: o.auto_cancel_parent,
            basis_points: o.basis_points.unwrap_or(d.basis_points),
            basis_points_type: o.basis_points_type.unwrap_or(d.basis_points_type),
            block_order: o.block_order,
            clearing_account: o.clearing_account.clone(),
            clearing_intent: o.clearing_intent.clone(),
            client_id,
            compete_against_best_offset: o
                .compete_against_best_offset
                .unwrap_or(d.compete_against_best_offset),
            continuous_update: o.continuous_update,
            delta: o.delta.unwrap_or(d.delta),
            delta_neutral_aux_price: o
                .delta_neutral_aux_price
                .unwrap_or(d.delta_neutral_aux_price),
            delta_neutral_clearing_account: o.delta_neutral_clearing_account.clone(),
            delta_neutral_clearing_intent: o.delta_neutral_clearing_intent.clone(),
            delta_neutral_con_id,
            delta_neutral_designated_location: o.delta_neutral_designated_location.clone(),
            delta_neutral_open_close: o.delta_neutral_open_close.clone(),
            delta_neutral_order_type: o.delta_neutral_order_type.clone(),
            delta_neutral_settling_firm: o.delta_neutral_settling_firm.clone(),
            delta_neutral_short_sale: o.delta_neutral_short_sale,
            delta_neutral_short_sale_slot: o.delta_neutral_short_sale_slot,
            designated_location: o.designated_location.clone(),
            discretionary_up_to_limit_price: o.discretionary_up_to_limit_price,
            dont_use_auto_price_for_hedge: o.dont_use_auto_price_for_hedge,
            duration: o.duration.unwrap_or(d.duration),
            exempt_code: o.exempt_code,
            ext_operator: o.ext_operator.clone(),
            fa_group: o.fa_group.clone(),
            fa_method: o.fa_method.clone(),
            fa_percentage: o.fa_percentage.clone(),
            filled_quantity: o.filled_quantity.unwrap_or(d.filled_quantity),
            hedge_param: o.hedge_param.clone(),
            hedge_type: o.hedge_type.clone(),
            imbalance_only: o.imbalance_only,
            is_oms_container: o.is_oms_container,
            is_pegged_change_amount_decrease: o.is_pegged_change_amount_decrease,
            lmt_price_offset: o.lmt_price_offset.unwrap_or(d.lmt_price_offset),
            manual_order_time: o.manual_order_time.clone(),
            mid_offset_at_half: o.mid_offset_at_half.unwrap_or(d.mid_offset_at_half),
            mid_offset_at_whole: o.mid_offset_at_whole.unwrap_or(d.mid_offset_at_whole),
            mifid2_decision_algo: o.mifid_2_decision_algo.clone(),
            mifid2_decision_maker: o.mifid_2_decision_maker.clone(),
            mifid2_execution_algo: o.mifid_2_execution_algo.clone(),
            mifid2_execution_trader: o.mifid_2_execution_trader.clone(),
            min_compete_size: o.min_compete_size.unwrap_or(d.min_compete_size),
            min_trade_qty: o.min_trade_qty.unwrap_or(d.min_trade_qty),
            model_code: o.model_code.clone(),
            not_held: o.not_held,
            oca_type: o.oca_type,
            open_close: o.open_close.clone(),
            opt_out_smart_routing: o.opt_out_smart_routing,
            order_combo_legs: o
                .order_combo_legs
                .iter()
                .map(|l| l.price.unwrap_or(f64::MAX))
                .collect(),
            order_misc_options: tags(&o.order_misc_options),
            order_ref: o.order_ref.clone(),
            origin: o.origin,
            override_percentage_constraints: o.override_percentage_constraints,
            parent_perm_id: o.parent_perm_id,
            pegged_change_amount: o.pegged_change_amount,
            percent_offset: o.percent_offset.unwrap_or(d.percent_offset),
            perm_id: o.perm_id,
            post_to_ats: o.post_to_ats.unwrap_or(d.post_to_ats),
            randomize_price: o.randomize_price,
            randomize_size: o.randomize_size,
            ref_futures_con_id,
            reference_change_amount: o.reference_change_amount,
            reference_contract_id,
            reference_exchange_id: o.reference_exchange_id.clone(),
            reference_price_type: o.reference_price_type.unwrap_or(d.reference_price_type),
            // The stated value, false included: the engine sends `None` and
            // `Some(false)` alike.
            route_marketable_to_bbo: Some(o.route_marketable_to_bbo),
            rule80a: o.rule_80_a.clone(),
            scale_auto_reset: o.scale_auto_reset,
            scale_init_fill_qty: o.scale_init_fill_qty.unwrap_or(d.scale_init_fill_qty),
            scale_init_level_size: o.scale_init_level_size.unwrap_or(d.scale_init_level_size),
            scale_init_position: o.scale_init_position.unwrap_or(d.scale_init_position),
            scale_price_adjust_interval: o
                .scale_price_adjust_interval
                .unwrap_or(d.scale_price_adjust_interval),
            scale_price_adjust_value: o
                .scale_price_adjust_value
                .unwrap_or(d.scale_price_adjust_value),
            scale_price_increment: o.scale_price_increment.unwrap_or(d.scale_price_increment),
            scale_profit_offset: o.scale_profit_offset.unwrap_or(d.scale_profit_offset),
            scale_random_percent: o.scale_random_percent,
            scale_subs_level_size: o.scale_subs_level_size.unwrap_or(d.scale_subs_level_size),
            scale_table: o.scale_table.clone(),
            settling_firm: o.settling_firm.clone(),
            shareholder: o.shareholder.clone(),
            short_sale_slot: o.short_sale_slot,
            smart_combo_routing_params: tags(&o.smart_combo_routing_params),
            soft_dollar_tier_name: o.soft_dollar_tier.name.clone(),
            soft_dollar_tier_val: o.soft_dollar_tier.val.clone(),
            soft_dollar_tier_display_name: o.soft_dollar_tier.display_name.clone(),
            solicited: o.solicited,
            starting_price: o.starting_price.unwrap_or(d.starting_price),
            stock_range_lower: o.stock_range_lower.unwrap_or(d.stock_range_lower),
            stock_range_upper: o.stock_range_upper.unwrap_or(d.stock_range_upper),
            stock_ref_price: o.stock_ref_price.unwrap_or(d.stock_ref_price),
            trail_stop_price: o.trail_stop_price.unwrap_or(d.trail_stop_price),
            use_price_mgmt_algo: Some(i32::from(o.use_price_mgmt_algo)),
            volatility: o.volatility.unwrap_or(d.volatility),
            volatility_type: o.volatility_type.unwrap_or(d.volatility_type),
            // The engine's own fields, which ib_async's Order lacks.
            ..d
        })
    }
}

impl From<&e::Order> for Order {
    fn from(o: &e::Order) -> Self {
        // Where the engine's default for a field is not its unset number,
        // an order placed without the field carries that default, as
        // `TryFrom<&Order>` sends `None`: read back, it is ib_async's unset,
        // as a gateway reports the field.
        let d = e::Order::default();
        let given = |v: f64, default: f64| stated(v).filter(|v| *v != default);
        let given_int = |v: i32, default: i32| stated_int(v).filter(|v| *v != default);
        Order {
            order_id: o.order_id,
            client_id: i64::from(o.client_id),
            perm_id: o.perm_id,
            action: o.action.clone(),
            total_quantity: o.total_quantity,
            order_type: o.order_type.clone(),
            lmt_price: stated(o.lmt_price),
            aux_price: stated(o.aux_price),
            tif: o.tif.clone(),
            active_start_time: o.active_start_time.clone(),
            active_stop_time: o.active_stop_time.clone(),
            oca_group: o.oca_group.clone(),
            oca_type: o.oca_type,
            order_ref: o.order_ref.clone(),
            transmit: o.transmit,
            parent_id: o.parent_id,
            block_order: o.block_order,
            sweep_to_fill: o.sweep_to_fill,
            display_size: o.display_size,
            trigger_method: o.trigger_method,
            outside_rth: o.outside_rth,
            hidden: o.hidden,
            good_after_time: o.good_after_time.clone(),
            good_till_date: o.good_till_date.clone(),
            rule_80_a: o.rule80a.clone(),
            all_or_none: o.all_or_none,
            min_qty: given_int(o.min_qty, d.min_qty),
            percent_offset: stated(o.percent_offset),
            override_percentage_constraints: o.override_percentage_constraints,
            trail_stop_price: stated(o.trail_stop_price),
            trailing_percent: given(o.trailing_percent, d.trailing_percent),
            fa_group: o.fa_group.clone(),
            // ib_async's own four, which the engine does not carry: their
            // defaults.
            fa_profile: String::new(),
            fa_method: o.fa_method.clone(),
            fa_percentage: o.fa_percentage.clone(),
            designated_location: o.designated_location.clone(),
            open_close: o.open_close.clone(),
            origin: o.origin,
            short_sale_slot: o.short_sale_slot,
            exempt_code: o.exempt_code,
            discretionary_amt: o.discretionary_amt,
            e_trade_only: false,
            firm_quote_only: false,
            nbbo_price_cap: None,
            opt_out_smart_routing: o.opt_out_smart_routing,
            auction_strategy: o.auction_strategy,
            starting_price: stated(o.starting_price),
            stock_ref_price: stated(o.stock_ref_price),
            delta: stated(o.delta),
            stock_range_lower: stated(o.stock_range_lower),
            stock_range_upper: stated(o.stock_range_upper),
            randomize_price: o.randomize_price,
            randomize_size: o.randomize_size,
            volatility: stated(o.volatility),
            volatility_type: given_int(o.volatility_type, d.volatility_type),
            delta_neutral_order_type: o.delta_neutral_order_type.clone(),
            delta_neutral_aux_price: stated(o.delta_neutral_aux_price),
            delta_neutral_con_id: i64::from(o.delta_neutral_con_id),
            delta_neutral_settling_firm: o.delta_neutral_settling_firm.clone(),
            delta_neutral_clearing_account: o.delta_neutral_clearing_account.clone(),
            delta_neutral_clearing_intent: o.delta_neutral_clearing_intent.clone(),
            delta_neutral_open_close: o.delta_neutral_open_close.clone(),
            delta_neutral_short_sale: o.delta_neutral_short_sale,
            delta_neutral_short_sale_slot: o.delta_neutral_short_sale_slot,
            delta_neutral_designated_location: o.delta_neutral_designated_location.clone(),
            continuous_update: o.continuous_update,
            reference_price_type: given_int(o.reference_price_type, d.reference_price_type),
            basis_points: stated(o.basis_points),
            basis_points_type: stated_int(o.basis_points_type),
            scale_init_level_size: stated_int(o.scale_init_level_size),
            scale_subs_level_size: stated_int(o.scale_subs_level_size),
            scale_price_increment: stated(o.scale_price_increment),
            scale_price_adjust_value: stated(o.scale_price_adjust_value),
            scale_price_adjust_interval: stated_int(o.scale_price_adjust_interval),
            scale_profit_offset: stated(o.scale_profit_offset),
            scale_auto_reset: o.scale_auto_reset,
            scale_init_position: stated_int(o.scale_init_position),
            scale_init_fill_qty: stated_int(o.scale_init_fill_qty),
            scale_random_percent: o.scale_random_percent,
            scale_table: o.scale_table.clone(),
            hedge_type: o.hedge_type.clone(),
            hedge_param: o.hedge_param.clone(),
            account: o.account.clone(),
            settling_firm: o.settling_firm.clone(),
            clearing_account: o.clearing_account.clone(),
            clearing_intent: o.clearing_intent.clone(),
            algo_strategy: o.algo_strategy.clone(),
            algo_params: our_tags(&o.algo_params),
            smart_combo_routing_params: our_tags(&o.smart_combo_routing_params),
            algo_id: o.algo_id.clone(),
            what_if: o.what_if,
            not_held: o.not_held,
            solicited: o.solicited,
            model_code: o.model_code.clone(),
            order_combo_legs: o
                .order_combo_legs
                .iter()
                .map(|p| OrderComboLeg { price: stated(*p) })
                .collect(),
            order_misc_options: our_tags(&o.order_misc_options),
            reference_contract_id: i64::from(o.reference_contract_id),
            pegged_change_amount: o.pegged_change_amount,
            is_pegged_change_amount_decrease: o.is_pegged_change_amount_decrease,
            reference_change_amount: o.reference_change_amount,
            reference_exchange_id: o.reference_exchange_id.clone(),
            adjusted_order_type: o.adjusted_order_type.clone(),
            trigger_price: given(o.trigger_price, d.trigger_price),
            adjusted_stop_price: given(o.adjusted_stop_price, d.adjusted_stop_price),
            adjusted_stop_limit_price: given(
                o.adjusted_stop_limit_price,
                d.adjusted_stop_limit_price,
            ),
            adjusted_trailing_amount: stated(o.adjusted_trailing_amount),
            adjustable_trailing_unit: o.adjustable_trailing_unit,
            lmt_price_offset: stated(o.lmt_price_offset),
            conditions: o.conditions.iter().map(Into::into).collect(),
            conditions_cancel_order: o.conditions_cancel_order,
            conditions_ignore_rth: o.conditions_ignore_rth,
            ext_operator: o.ext_operator.clone(),
            soft_dollar_tier: SoftDollarTier {
                name: o.soft_dollar_tier_name.clone(),
                val: o.soft_dollar_tier_val.clone(),
                display_name: o.soft_dollar_tier_display_name.clone(),
            },
            cash_qty: given(o.cash_qty, d.cash_qty),
            mifid_2_decision_maker: o.mifid2_decision_maker.clone(),
            mifid_2_decision_algo: o.mifid2_decision_algo.clone(),
            mifid_2_execution_trader: o.mifid2_execution_trader.clone(),
            mifid_2_execution_algo: o.mifid2_execution_algo.clone(),
            dont_use_auto_price_for_hedge: o.dont_use_auto_price_for_hedge,
            is_oms_container: o.is_oms_container,
            discretionary_up_to_limit_price: o.discretionary_up_to_limit_price,
            auto_cancel_date: o.auto_cancel_date.clone(),
            filled_quantity: given(o.filled_quantity, d.filled_quantity),
            ref_futures_con_id: i64::from(o.ref_futures_con_id),
            auto_cancel_parent: o.auto_cancel_parent,
            shareholder: o.shareholder.clone(),
            imbalance_only: o.imbalance_only,
            route_marketable_to_bbo: o.route_marketable_to_bbo == Some(true),
            parent_perm_id: o.parent_perm_id,
            // Tag 8339 decodes to `Some(0 | 1)`.
            use_price_mgmt_algo: o.use_price_mgmt_algo.is_some_and(|n| n != 0),
            duration: stated_int(o.duration),
            post_to_ats: stated_int(o.post_to_ats),
            advanced_error_override: o.advanced_error_override.clone(),
            manual_order_time: o.manual_order_time.clone(),
            min_trade_qty: stated_int(o.min_trade_qty),
            min_compete_size: stated_int(o.min_compete_size),
            compete_against_best_offset: stated(o.compete_against_best_offset),
            mid_offset_at_whole: stated(o.mid_offset_at_whole),
            mid_offset_at_half: stated(o.mid_offset_at_half),
        }
    }
}

impl From<&e::OrderState> for OrderState {
    fn from(s: &e::OrderState) -> Self {
        OrderState {
            status: s.status.clone(),
            init_margin_before: s.init_margin_before.clone(),
            maint_margin_before: s.maint_margin_before.clone(),
            equity_with_loan_before: s.equity_with_loan_before.clone(),
            init_margin_change: s.init_margin_change.clone(),
            maint_margin_change: s.maint_margin_change.clone(),
            equity_with_loan_change: s.equity_with_loan_change.clone(),
            init_margin_after: s.init_margin_after.clone(),
            maint_margin_after: s.maint_margin_after.clone(),
            equity_with_loan_after: s.equity_with_loan_after.clone(),
            commission: stated(s.commission_and_fees),
            min_commission: stated(s.min_commission_and_fees),
            max_commission: stated(s.max_commission_and_fees),
            commission_currency: s.commission_and_fees_currency.clone(),
            warning_text: s.warning_text.clone(),
            completed_time: s.completed_time.clone(),
            completed_status: s.completed_status.clone(),
        }
    }
}

/// ib_async's `Execution` from the engine's. `time` is read as ib_async's
/// decoder reads it, then converted to `timezone`, the IB's
/// `IBDefaults.timezone`. The engine states the venue's transaction time,
/// `YYYYMMDD-HH:MM:SS`, which is UTC; another naive time, a gateway's local
/// stamp, is placed in `timezone_tws` (the system zone when there is none).
/// A date, or a time that does not parse or that the zone cannot hold,
/// fails.
pub(crate) fn execution(
    x: &e::Execution,
    timezone: &TimeZone,
    timezone_tws: Option<&TimeZone>,
) -> Result<Execution> {
    let field = "Execution.time";
    let time = match parse_ib_datetime(&x.time) {
        Ok(BarDate::Naive(t)) if !venue_stamp(&x.time) => t
            .to_zoned(timezone_tws.cloned().unwrap_or_else(TimeZone::system))
            .map_err(|why| bad(field, &x.time, why))?
            .with_time_zone(timezone.clone()),
        _ => stamped(field, &x.time, timezone)?,
    };
    Ok(Execution {
        exec_id: x.exec_id.clone(),
        time,
        acct_number: x.acct_number.clone(),
        exchange: x.exchange.clone(),
        side: x.side.clone(),
        shares: x.shares,
        price: x.price,
        perm_id: x.perm_id,
        client_id: x.client_id,
        order_id: x.order_id,
        liquidation: x.liquidation,
        cum_qty: x.cum_qty,
        avg_price: x.avg_price,
        order_ref: x.order_ref.clone(),
        ev_rule: x.ev_rule.clone(),
        ev_multiplier: x.ev_multiplier,
        model_code: x.model_code.clone(),
        last_liquidity: x.last_liquidity,
        pending_price_revision: x.pending_price_revision,
    })
}

impl TryFrom<&e::CommissionAndFeesReport> for CommissionReport {
    type Error = Error;

    /// An unset `realized_pnl` or `yield_` reads 0.0, as ib_async's wrapper
    /// sets it.
    fn try_from(r: &e::CommissionAndFeesReport) -> Result<Self> {
        let zero_if_unset = |v: f64| stated(v).unwrap_or(0.0);
        let date = r.yield_redemption_date;
        Ok(CommissionReport {
            exec_id: r.exec_id.clone(),
            commission: r.commission_and_fees,
            currency: r.currency.clone(),
            realized_pnl: zero_if_unset(r.realized_pnl),
            yield_: zero_if_unset(r.yield_amount),
            yield_redemption_date: i32::try_from(date).map_err(|why| {
                bad(
                    "CommissionReport.yield_redemption_date",
                    &date.to_string(),
                    why,
                )
            })?,
        })
    }
}

impl From<&e::ContractDetails> for ContractDetails {
    fn from(d: &e::ContractDetails) -> Self {
        ContractDetails {
            contract: Some((&d.contract).into()),
            market_name: d.market_name.clone(),
            min_tick: d.min_tick,
            order_types: d.order_types.clone(),
            valid_exchanges: d.valid_exchanges.clone(),
            price_magnifier: d.price_magnifier,
            under_con_id: i64::from(d.under_con_id),
            long_name: d.long_name.clone(),
            contract_month: d.contract_month.clone(),
            industry: d.industry.clone(),
            category: d.category.clone(),
            subcategory: d.subcategory.clone(),
            time_zone_id: d.time_zone_id.clone().unwrap_or_default(),
            trading_hours: d.trading_hours.clone().unwrap_or_default(),
            liquid_hours: d.liquid_hours.clone().unwrap_or_default(),
            ev_rule: d.ev_rule.clone(),
            ev_multiplier: d.ev_multiplier,
            // The engine carries none: ib_async's default.
            md_size_multiplier: 1,
            agg_group: d.agg_group,
            under_symbol: d.under_symbol.clone(),
            under_sec_type: d.under_sec_type.clone(),
            market_rule_ids: d.market_rule_ids.clone(),
            sec_id_list: d
                .sec_id_list
                .iter()
                .map(|(tag, value)| TagValue {
                    tag: tag.clone(),
                    value: value.clone(),
                })
                .collect(),
            real_expiration_date: d.real_expiration_date.clone(),
            last_trade_time: d.last_trade_time.clone(),
            stock_type: d.stock_type.clone(),
            min_size: d.min_size,
            size_increment: d.size_increment,
            suggested_size_increment: d.suggested_size_increment,
            cusip: d.cusip.clone(),
            ratings: d.ratings.clone(),
            desc_append: d.desc_append.clone(),
            bond_type: d.bond_type.clone(),
            coupon_type: d.coupon_type.clone(),
            callable: d.callable,
            putable: d.puttable,
            coupon: d.coupon,
            convertible: d.convertible,
            maturity: d.maturity.clone(),
            issue_date: d.issue_date.clone(),
            next_option_date: d.next_option_date.clone(),
            next_option_type: d.next_option_type.clone(),
            next_option_partial: d.next_option_partial,
            notes: d.bond_notes.clone(),
        }
    }
}

impl From<&e::ContractDescription> for ContractDescription {
    fn from(d: &e::ContractDescription) -> Self {
        ContractDescription {
            contract: Some(Contract {
                con_id: d.con_id,
                symbol: d.symbol.clone(),
                sec_type: d.sec_type.clone(),
                currency: d.currency.clone(),
                primary_exchange: d.primary_exchange.clone(),
                description: d.description.clone(),
                issuer_id: d.issuer_id.clone(),
                ..Contract::default()
            }),
            derivative_sec_types: d.derivative_sec_types.clone(),
        }
    }
}

impl TryFrom<&e::BarData> for BarData {
    type Error = Error;

    fn try_from(b: &e::BarData) -> Result<Self> {
        Ok(BarData {
            date: parse_ib_datetime(&b.date).map_err(|why| bad("BarData.date", &b.date, why))?,
            open: b.open,
            high: b.high,
            low: b.low,
            close: b.close,
            volume: b.volume as f64,
            average: b.wap,
            bar_count: b.bar_count,
        })
    }
}

/// ib_async's historical ticks from the engine's batch. The engine states
/// each time as the venue wrote it, `YYYYMMDD-HH:MM:SS` in UTC; it is read as
/// that instant and put in `timezone`, the IB's `IBDefaults.timezone`, as
/// ib_async stamps each tick with `fromtimestamp(time, defaultTimezone)`. A
/// date, or a time that does not parse, fails the batch. A midpoint carries
/// no size, and reads 0.0.
pub(crate) fn historical_ticks(
    ticks: &e::HistoricalTickData,
    timezone: &TimeZone,
) -> Result<Vec<HistoricalTickAny>> {
    match ticks {
        e::HistoricalTickData::Midpoint(v) => v
            .iter()
            .map(|t| {
                Ok(HistoricalTickAny::Midpoint(HistoricalTick {
                    time: stamped("HistoricalTick.time", &t.time, timezone)?,
                    price: t.price,
                    size: 0.0,
                }))
            })
            .collect(),
        e::HistoricalTickData::BidAsk(v) => v
            .iter()
            .map(|t| {
                Ok(HistoricalTickAny::BidAsk(HistoricalTickBidAsk {
                    time: stamped("HistoricalTickBidAsk.time", &t.time, timezone)?,
                    tick_attrib_bid_ask: TickAttribBidAsk {
                        bid_past_low: t.bid_past_low,
                        ask_past_high: t.ask_past_high,
                    },
                    price_bid: t.bid_price,
                    price_ask: t.ask_price,
                    size_bid: t.bid_size,
                    size_ask: t.ask_size,
                }))
            })
            .collect(),
        e::HistoricalTickData::Last(v) => v
            .iter()
            .map(|t| {
                Ok(HistoricalTickAny::Last(HistoricalTickLast {
                    time: stamped("HistoricalTickLast.time", &t.time, timezone)?,
                    tick_attrib_last: TickAttribLast {
                        past_limit: t.past_limit,
                        unreported: t.unreported,
                    },
                    price: t.price,
                    size: t.size,
                    exchange: t.exchange.clone(),
                    special_conditions: t.special_conditions.clone(),
                }))
            })
            .collect(),
    }
}

/// ib_async's `HistoricalNews` from the engine's `historical_news`
/// callback: `time` parsed as `parseIBDatetime` parses it, and not
/// localised.
pub(crate) fn historical_news(
    time: &str,
    provider_code: &str,
    article_id: &str,
    headline: &str,
) -> Result<HistoricalNews> {
    Ok(HistoricalNews {
        time: parse_ib_datetime(time).map_err(|why| bad("HistoricalNews.time", time, why))?,
        provider_code: provider_code.to_owned(),
        article_id: article_id.to_owned(),
        headline: headline.to_owned(),
    })
}

impl From<&ExecutionFilter> for e::ExecutionFilter {
    fn from(f: &ExecutionFilter) -> Self {
        e::ExecutionFilter {
            client_id: f.client_id,
            acct_code: f.acct_code.clone(),
            time: f.time.clone(),
            symbol: f.symbol.clone(),
            sec_type: f.sec_type.clone(),
            exchange: f.exchange.clone(),
            side: f.side.clone(),
            // The engine's own; ib_async's filter has neither.
            last_n_days: 0,
            specific_dates: Vec::new(),
        }
    }
}

impl From<&e::TickAttrib> for TickAttrib {
    fn from(a: &e::TickAttrib) -> Self {
        TickAttrib {
            can_auto_execute: a.can_auto_execute,
            past_limit: a.past_limit,
            pre_open: a.pre_open,
        }
    }
}

impl From<&e::TickAttribBidAsk> for TickAttribBidAsk {
    fn from(a: &e::TickAttribBidAsk) -> Self {
        TickAttribBidAsk {
            bid_past_low: a.bid_past_low,
            ask_past_high: a.ask_past_high,
        }
    }
}

impl From<&e::TickAttribLast> for TickAttribLast {
    fn from(a: &e::TickAttribLast) -> Self {
        TickAttribLast {
            past_limit: a.past_limit,
            unreported: a.unreported,
        }
    }
}

impl From<&e::DepthMktDataDescription> for DepthMktDataDescription {
    fn from(d: &e::DepthMktDataDescription) -> Self {
        DepthMktDataDescription {
            exchange: d.exchange.clone(),
            sec_type: d.sec_type.clone(),
            listing_exch: d.listing_exch.clone(),
            service_data_type: d.service_data_type.clone(),
            agg_group: stated_int(d.agg_group),
        }
    }
}

impl From<&e::SmartComponent> for SmartComponent {
    fn from(c: &e::SmartComponent) -> Self {
        SmartComponent {
            bit_number: c.bit_number,
            exchange: c.exchange.clone(),
            exchange_letter: c.exchange_letter.clone(),
        }
    }
}

impl From<&e::NewsProvider> for NewsProvider {
    fn from(p: &e::NewsProvider) -> Self {
        NewsProvider {
            code: p.code.clone(),
            name: p.name.clone(),
        }
    }
}

impl From<&e::PriceIncrement> for PriceIncrement {
    fn from(p: &e::PriceIncrement) -> Self {
        PriceIncrement {
            low_edge: p.low_edge,
            increment: p.increment,
        }
    }
}

/// The engine's `stkTypes` filter for ib_async's `stockTypeFilter`, or
/// none.
fn stk_types(name: &str) -> Option<String> {
    let code = match name.to_ascii_uppercase().as_str() {
        "STOCK" => "exc:ETF",
        "ETF" => "inc:ETF",
        "CORP" => "inc:CORP",
        "ADR" => "inc:ADR",
        "REIT" => "inc:REIT",
        "CEF" => "inc:CEF",
        // `ALL` and anything else filter nothing.
        _ => return None,
    };
    Some(code.to_owned())
}

/// The arguments of the engine's `req_scanner_subscription` for ib_async's
/// `reqScannerSubscription(subscription, …, scannerSubscriptionFilterOptions)`:
/// instrument, location code, scan code, rows and filter tags.
///
/// A negative `number_of_rows` asks for 50. Each field set becomes the
/// engine's filter tag for it, an unset number skipped; the filter options
/// follow, each replacing a filter of its tag, an empty tag skipped.
/// `scanner_setting_pairs` is taken and not carried: the engine's scan has
/// no field for it.
pub(crate) fn scanner_request(
    sub: &ScannerSubscription,
    filter_options: &[TagValue],
) -> (String, String, String, u32, Vec<e::TagValue>) {
    // The engine skips its unset double and unset integer as unset.
    let number = |v: Option<f64>| {
        v.filter(|n| *n != f64::MAX && *n != f64::from(i32::MAX))
            .map(|n| n.to_string())
    };
    let int = |v: Option<i32>| number(v.map(f64::from));
    let text = |s: &str| (!s.is_empty()).then(|| s.to_owned());
    let named = [
        ("priceAbove", number(sub.above_price)),
        ("priceBelow", number(sub.below_price)),
        ("volumeAbove", int(sub.above_volume)),
        ("marketCapAbove1e6", number(sub.market_cap_above)),
        ("marketCapBelow1e6", number(sub.market_cap_below)),
        ("moodyRatingAbove", text(&sub.moody_rating_above)),
        ("moodyRatingBelow", text(&sub.moody_rating_below)),
        ("spRatingAbove", text(&sub.sp_rating_above)),
        ("spRatingBelow", text(&sub.sp_rating_below)),
        ("maturityDateAbove", text(&sub.maturity_date_above)),
        ("maturityDateBelow", text(&sub.maturity_date_below)),
        ("couponRateAbove", number(sub.coupon_rate_above)),
        ("couponRateBelow", number(sub.coupon_rate_below)),
        ("avgOptVolumeAbove", int(sub.average_option_volume_above)),
        (
            "excludeConvertible",
            sub.exclude_convertible.then(|| "true".to_owned()),
        ),
        ("stkTypes", stk_types(&sub.stock_type_filter)),
    ];
    let mut filters: Vec<e::TagValue> = named
        .into_iter()
        .filter_map(|(tag, value)| {
            value.map(|value| e::TagValue {
                tag: tag.to_owned(),
                value,
            })
        })
        .collect();
    for option in filter_options.iter().filter(|o| !o.tag.is_empty()) {
        filters.retain(|f| f.tag != option.tag);
        filters.push(option.into());
    }
    (
        sub.instrument.clone(),
        sub.location_code.clone(),
        sub.scan_code.clone(),
        u32::try_from(sub.number_of_rows).unwrap_or(50),
        filters,
    )
}

impl From<&WshEventData> for e::CalendarQuery {
    fn from(w: &WshEventData) -> Self {
        e::CalendarQuery {
            con_id: w.con_id,
            filter: w.filter.clone(),
            start_date: w.start_date.clone(),
            end_date: w.end_date.clone(),
            total_limit: w.total_limit.map(i64::from),
            fill_watchlist: w.fill_watchlist,
            fill_portfolio: w.fill_portfolio,
            fill_competitors: w.fill_competitors,
        }
    }
}

impl From<&e::Adjustment> for CorporateAction {
    fn from(a: &e::Adjustment) -> Self {
        CorporateAction {
            kind: a
                .kind
                .map(e::AdjustmentKind::code)
                .unwrap_or_default()
                .to_owned(),
            date: a.date.clone(),
            value: a.value.clone(),
            currency: a.currency.clone(),
            announce_date: a.announce_date.clone(),
            record_date: a.record_date.clone(),
            pay_date: a.pay_date.clone(),
            payment_type: a.payment_type.clone(),
            distribution_type: a.distribution_type.clone(),
        }
    }
}

impl From<&e::PositionElsewhere> for PositionElsewhere {
    fn from(p: &e::PositionElsewhere) -> Self {
        PositionElsewhere {
            con_id: p.con_id,
            symbol: p.symbol.clone(),
            sec_type: p.sec_type.clone(),
            currency: p.currency.clone(),
            position: p.position,
            avg_cost: p.avg_cost as f64 / e::PRICE_SCALE as f64,
            held: p.held,
        }
    }
}

/// ib_async's `OptionComputation` from the engine's `tick_option_computation`
/// callback, whose figures come in ib_async's order: implied vol, delta,
/// option price, PV dividend, gamma, vega, theta, underlying price. A figure
/// that is `f64::MAX`, NaN or ib_async's sentinel for it (-1 for the
/// volatility and the prices, -2 for the greeks) is `None`; vega and theta
/// included, where ib_async keeps -2.
pub(crate) fn option_computation(tick_attrib: i32, figures: [f64; 8]) -> OptionComputation {
    const SENTINEL: [f64; 8] = [-1.0, -2.0, -1.0, -1.0, -2.0, -2.0, -2.0, -1.0];
    let mut f = figures
        .into_iter()
        .zip(SENTINEL)
        .map(|(v, s)| (v != f64::MAX && !v.is_nan() && v != s).then_some(v));
    let mut next = || f.next().flatten();
    OptionComputation {
        tick_attrib,
        implied_vol: next(),
        delta: next(),
        opt_price: next(),
        pv_dividend: next(),
        gamma: next(),
        vega: next(),
        theta: next(),
        und_price: next(),
    }
}

impl From<&e::OptionComputation> for OptionModel {
    fn from(m: &e::OptionComputation) -> Self {
        OptionModel {
            implied_vol: stated(m.implied_vol),
            delta: stated(m.delta),
            opt_price: stated(m.opt_price),
            pv_dividend: stated(m.pv_dividend),
            gamma: stated(m.gamma),
            vega: stated(m.vega),
            theta: stated(m.theta),
            und_price: stated(m.und_price),
            cal_days: stated(m.cal_days),
            rate: stated(m.rate),
            rho: stated(m.rho),
            fugit: stated(m.fugit),
            exercise_boundary: stated(m.exercise_boundary),
            forward_coeff: stated(m.forward_coeff),
            model_yield: stated(m.model_yield),
            bridge_yield: stated(m.bridge_yield),
            time_value: stated(m.time_value),
            price_based_vol: Some(m.price_based_vol),
        }
    }
}

/// A ticker's extras as the engine's figure getters state them, with every
/// figure the venue did not state (`f64::MAX`) made NaN.
pub(crate) fn unstated_as_nan(mut x: TickerExtras) -> TickerExtras {
    let nan = |v: &mut f64| {
        if *v == f64::MAX {
            *v = f64::NAN;
        }
    };
    nan(&mut x.shares_outstanding);
    nan(&mut x.open_a_year_ago);
    x.stated_figures.values_mut().flatten().for_each(nan);
    for (whole, fractional) in x.numbered_figures.values_mut() {
        whole
            .values_mut()
            .chain(fractional.values_mut())
            .for_each(nan);
    }
    for (a, b) in x.paired_figures.values_mut().flatten() {
        nan(a);
        nan(b);
    }
    for (a, b, c) in x.stated_rows.values_mut().flatten() {
        nan(a);
        nan(b);
        nan(c);
    }
    x
}

#[cfg(test)]
mod tests {
    use jiff::civil;

    use super::*;

    fn zone(name: &str) -> TimeZone {
        TimeZone::get(name).unwrap()
    }

    fn refusal(o: Order) -> (i64, String) {
        let r = e::Order::try_from(&o).unwrap_err();
        (r.code, r.msg)
    }

    #[test]
    fn price_management_and_bbo_routing_keep_what_was_stated() {
        let sent = |o: Order| e::Order::try_from(&o).unwrap();
        let o = sent(Order {
            use_price_mgmt_algo: true,
            route_marketable_to_bbo: true,
            ..Order::default()
        });
        assert_eq!(o.use_price_mgmt_algo, Some(1));
        assert_eq!(o.route_marketable_to_bbo, Some(true));
        let o = sent(Order::default());
        assert_eq!(o.use_price_mgmt_algo, Some(0));
        assert_eq!(o.route_marketable_to_bbo, Some(false));

        // As the engine decodes tag 8339 and the BBO flag: 0, 1 or unstated.
        for (algo, bbo, read) in [
            (Some(0), Some(false), false),
            (Some(1), Some(true), true),
            (None, None, false),
        ] {
            let back = Order::from(&e::Order {
                use_price_mgmt_algo: algo,
                route_marketable_to_bbo: bbo,
                ..e::Order::default()
            });
            assert_eq!(back.use_price_mgmt_algo, read);
            assert_eq!(back.route_marketable_to_bbo, read);
        }
    }

    #[test]
    fn unset_goes_as_the_engines_default_and_comes_back_none() {
        let o = e::Order::try_from(&Order::default()).unwrap();
        // The engine's own defaults: 0.0 for a limit, MAX for an offset.
        assert_eq!(o.lmt_price, 0.0);
        assert_eq!(o.percent_offset, f64::MAX);
        assert_eq!(o.min_trade_qty, i32::MAX);
        assert_eq!(o.open_close, "O");
        let o = e::Order::try_from(&Order {
            lmt_price: Some(1.5),
            order_combo_legs: vec![
                OrderComboLeg { price: None },
                OrderComboLeg { price: Some(2.0) },
            ],
            ..Order::default()
        })
        .unwrap();
        assert_eq!(o.lmt_price, 1.5);
        assert_eq!(o.order_combo_legs, vec![f64::MAX, 2.0]);

        let back = Order::from(&o);
        assert_eq!(back.lmt_price, Some(1.5));
        assert_eq!(back.percent_offset, None);
        assert_eq!(back.min_trade_qty, None);
        assert_eq!(
            back.order_combo_legs,
            vec![
                OrderComboLeg { price: None },
                OrderComboLeg { price: Some(2.0) }
            ]
        );
    }

    #[test]
    fn each_local_refusal_has_its_code() {
        let past = i64::from(i32::MAX) + 1;
        for (field, o) in [
            (
                "client_id",
                Order {
                    client_id: past,
                    ..Order::default()
                },
            ),
            (
                "delta_neutral_con_id",
                Order {
                    delta_neutral_con_id: past,
                    ..Order::default()
                },
            ),
            (
                "reference_contract_id",
                Order {
                    reference_contract_id: past,
                    ..Order::default()
                },
            ),
            (
                "ref_futures_con_id",
                Order {
                    ref_futures_con_id: -past - 1,
                    ..Order::default()
                },
            ),
        ] {
            let (code, msg) = refusal(o);
            assert_eq!(code, 320, "{field}");
            assert!(
                msg.starts_with(&format!("Error reading request:Order.{field} ")),
                "{msg}"
            );
        }
        assert_eq!(
            refusal(Order {
                e_trade_only: true,
                ..Order::default()
            }),
            (
                10268,
                "The 'EtradeOnly' order attribute is not supported.".to_owned()
            )
        );
        assert_eq!(
            refusal(Order {
                firm_quote_only: true,
                ..Order::default()
            }),
            (
                10269,
                "The 'FirmQuoteOnly' order attribute is not supported.".to_owned()
            )
        );
        assert_eq!(
            refusal(Order {
                nbbo_price_cap: Some(1.0),
                ..Order::default()
            }),
            (
                10270,
                "The 'NbboPriceCap' order attribute is not supported.".to_owned()
            )
        );
        // What the TWS API declares `int` is carried to its edge.
        let o = Order {
            client_id: i64::from(i32::MIN),
            fa_profile: "taken, not applied".into(),
            ..Order::default()
        };
        assert_eq!(e::Order::try_from(&o).unwrap().client_id, i32::MIN);
    }

    #[test]
    fn conditions_carry_the_price_scale_and_the_conjunction() {
        let ours = vec![
            OrderCondition::Price(PriceCondition {
                price: 0.29,
                con_id: 8314,
                exch: "SMART".into(),
                trigger_method: 2,
                ..PriceCondition::default()
            })
            .or(),
            OrderCondition::Margin(MarginCondition {
                percent: 30,
                ..MarginCondition::default()
            }),
            OrderCondition::Volume(VolumeCondition {
                volume: 1000,
                ..VolumeCondition::default()
            }),
        ];
        let sent: Vec<e::OrderCondition> = ours.iter().map(Into::into).collect();
        assert_eq!(
            sent[0],
            e::OrderCondition::Price {
                con_id: 8314,
                exchange: "SMART".into(),
                price: 29_000_000,
                is_more: true,
                trigger_method: 2,
                is_conjunction_connection: false,
            }
        );
        let back: Vec<OrderCondition> = sent.iter().map(Into::into).collect();
        assert_eq!(back, ours);
    }

    #[test]
    fn ev_multiplier_keeps_its_fraction() {
        let d = ContractDetails::from(&e::ContractDetails {
            ev_multiplier: 0.5,
            puttable: true,
            bond_notes: "notes".into(),
            under_con_id: 265598,
            sec_id_list: vec![("ISIN".into(), "US0378331005".into())],
            ..e::ContractDetails::default()
        });
        assert_eq!(d.ev_multiplier, 0.5);
        assert!(d.putable);
        assert_eq!(d.notes, "notes");
        assert_eq!(d.under_con_id, 265598);
        assert_eq!(d.md_size_multiplier, 1);
        assert_eq!(
            d.sec_id_list,
            vec![TagValue {
                tag: "ISIN".into(),
                value: "US0378331005".into()
            }]
        );
        let x = execution(
            &e::Execution {
                time: "1704205800".into(),
                ev_multiplier: 0.5,
                ..e::Execution::default()
            },
            &TimeZone::UTC,
            None,
        )
        .unwrap();
        assert_eq!(x.ev_multiplier, 0.5);
    }

    #[test]
    fn execution_times_in_each_form_land_in_the_default_zone() {
        let ny = zone("America/New_York");
        let ams = zone("Europe/Amsterdam");
        let at = |time: &str, tws: Option<&TimeZone>| {
            execution(
                &e::Execution {
                    time: time.into(),
                    ..e::Execution::default()
                },
                &ams,
                tws,
            )
        };
        // 2024-01-02 14:30 UTC, stated four ways.
        let want = civil::date(2024, 1, 2)
            .at(14, 30, 0, 0)
            .to_zoned(TimeZone::UTC)
            .unwrap();
        for (time, tws) in [
            ("20240102 14:30:00 UTC", None),
            ("1704205800", None),
            ("20240102 09:30:00 America/New_York", None),
            ("20240102 09:30:00", Some(&ny)),
            // The venue's own stamp, as the engine states it, is UTC
            // whatever the TWS zone.
            ("20240102-14:30:00", Some(&ny)),
            ("20240102-14:30:00", None),
            ("20240102-14:30:00.250", None),
        ] {
            let x = at(time, tws).unwrap();
            assert_eq!(x.time.timestamp(), want.timestamp(), "{time}");
            assert_eq!(x.time.time_zone(), &ams, "{time}");
        }
        // Naive with no TWS zone: the system zone, as Python's `astimezone`.
        let naive = civil::date(2024, 1, 2).at(9, 30, 0, 0);
        let x = at("20240102 09:30:00", None).unwrap();
        assert_eq!(
            x.time.timestamp(),
            naive.to_zoned(TimeZone::system()).unwrap().timestamp()
        );
        for bad in ["20240102", "yesterday", "20240102 09:30:00 Nowhere/Else"] {
            let Err(Error::Value(why)) = at(bad, None) else {
                panic!("{bad} converted")
            };
            assert!(why.starts_with("Execution.time"), "{why}");
        }
    }

    #[test]
    fn unset_commission_figures_read_zero() {
        let r =
            CommissionReport::try_from(&e::CommissionAndFeesReport::charged("0001", 1.25, "USD"))
                .unwrap();
        assert_eq!((r.commission, r.realized_pnl, r.yield_), (1.25, 0.0, 0.0));
        assert_eq!(r.yield_redemption_date, 0);
        let wide = e::CommissionAndFeesReport {
            yield_redemption_date: i64::from(i32::MAX) + 1,
            ..e::CommissionAndFeesReport::default()
        };
        assert!(CommissionReport::try_from(&wide).is_err());
    }

    #[test]
    fn each_option_computation_sentinel_is_none() {
        let all = |v: f64| option_computation(3, [v; 8]);
        let none = OptionComputation {
            tick_attrib: 3,
            implied_vol: None,
            delta: None,
            opt_price: None,
            pv_dividend: None,
            gamma: None,
            vega: None,
            theta: None,
            und_price: None,
        };
        assert_eq!(all(f64::MAX), none);
        assert_eq!(all(f64::NAN), none);
        // -1 is unset for the volatility and the prices, a value for a greek.
        let s = Some;
        assert_eq!(
            all(-1.0),
            OptionComputation {
                delta: s(-1.0),
                gamma: s(-1.0),
                vega: s(-1.0),
                theta: s(-1.0),
                ..none
            }
        );
        // -2 is unset for the greeks, vega and theta included.
        assert_eq!(
            all(-2.0),
            OptionComputation {
                implied_vol: s(-2.0),
                opt_price: s(-2.0),
                pv_dividend: s(-2.0),
                und_price: s(-2.0),
                ..none
            }
        );
    }

    #[test]
    fn a_scanner_subscription_becomes_the_engines_filters() {
        let (instrument, location, code, rows, filters) =
            scanner_request(&ScannerSubscription::default(), &[]);
        assert_eq!(
            (instrument, location, code, rows),
            (String::new(), String::new(), String::new(), 50)
        );
        assert!(filters.is_empty());

        let sub = ScannerSubscription {
            number_of_rows: 10,
            instrument: "STK".into(),
            location_code: "STK.US.MAJOR".into(),
            scan_code: "TOP_PERC_GAIN".into(),
            above_price: Some(5.0),
            below_price: Some(f64::MAX),
            above_volume: Some(100_000),
            average_option_volume_above: Some(i32::MAX),
            market_cap_above: Some(1.5),
            moody_rating_above: "A".into(),
            exclude_convertible: true,
            stock_type_filter: "etf".into(),
            scanner_setting_pairs: "Annual,true".into(),
            ..ScannerSubscription::default()
        };
        let options = [
            TagValue {
                tag: "priceAbove".into(),
                value: "7".into(),
            },
            TagValue {
                tag: String::new(),
                value: "skipped".into(),
            },
            TagValue {
                tag: "changePercAbove".into(),
                value: "2".into(),
            },
        ];
        let (instrument, location, code, rows, filters) = scanner_request(&sub, &options);
        assert_eq!(
            (instrument.as_str(), location.as_str(), code.as_str(), rows),
            ("STK", "STK.US.MAJOR", "TOP_PERC_GAIN", 10)
        );
        let filters: Vec<(&str, &str)> = filters
            .iter()
            .map(|t| (t.tag.as_str(), t.value.as_str()))
            .collect();
        assert_eq!(
            filters,
            [
                ("volumeAbove", "100000"),
                ("marketCapAbove1e6", "1.5"),
                ("moodyRatingAbove", "A"),
                ("excludeConvertible", "true"),
                ("stkTypes", "inc:ETF"),
                ("priceAbove", "7"),
                ("changePercAbove", "2"),
            ]
        );
    }

    #[test]
    fn bars_ticks_and_news_parse_their_times() {
        let bar = BarData::try_from(&e::BarData {
            date: "20240102".into(),
            volume: 1200,
            wap: 10.5,
            ..e::BarData::default()
        })
        .unwrap();
        assert_eq!(bar.date, BarDate::Day(civil::date(2024, 1, 2)));
        assert_eq!((bar.volume, bar.average), (1200.0, 10.5));
        assert!(
            BarData::try_from(&e::BarData {
                date: "soon".into(),
                ..e::BarData::default()
            })
            .is_err()
        );

        // A historical tick's time: the venue's UTC stamp, as the engine
        // states it, or epoch seconds; then in the default zone.
        let ny = zone("America/New_York");
        let want = "2024-01-02T09:30:00-05:00[America/New_York]"
            .parse::<Zoned>()
            .unwrap();
        for s in ["20240102-14:30:00", "20240102-14:30:00.5", "1704205800"] {
            let t = stamped("HistoricalTick.time", s, &ny).unwrap();
            assert_eq!(t, want, "{s}");
            assert_eq!(t.time_zone(), &ny, "{s}");
        }
        assert!(stamped("HistoricalTick.time", "20240102", &ny).is_err());
        assert!(
            historical_ticks(&e::HistoricalTickData::Last(Vec::new()), &ny)
                .unwrap()
                .is_empty()
        );

        let news = historical_news("2024-01-02 09:30:00.0", "BZ", "BZ$1", "Headline").unwrap();
        assert_eq!(
            news.time,
            BarDate::Naive(civil::date(2024, 1, 2).at(9, 30, 0, 0))
        );
    }

    #[test]
    fn engine_extras_state_unset_as_none_or_nan() {
        let m = OptionModel::from(&e::OptionComputation {
            implied_vol: 0.2,
            delta: f64::MAX,
            price_based_vol: true,
            ..e::OptionComputation::default()
        });
        assert_eq!(
            (m.implied_vol, m.delta, m.price_based_vol),
            (Some(0.2), None, Some(true))
        );

        let mut raw = TickerExtras {
            shares_outstanding: f64::MAX,
            open_a_year_ago: 101.0,
            ..TickerExtras::default()
        };
        raw.paired_figures.insert(7, vec![(f64::MAX, 2.0)]);
        let x = unstated_as_nan(raw);
        assert!(x.shares_outstanding.is_nan() && x.paired_figures[&7][0].0.is_nan());
        assert_eq!((x.open_a_year_ago, x.paired_figures[&7][0].1), (101.0, 2.0));

        let a = CorporateAction::from(&e::Adjustment {
            kind: Some(e::AdjustmentKind::Split),
            ..e::Adjustment::default()
        });
        assert_eq!(a.kind, "SS");
        assert_eq!(CorporateAction::from(&e::Adjustment::default()).kind, "");
    }
}

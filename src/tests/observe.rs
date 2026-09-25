//! What the replays observe of each type: its fields by their Rust names,
//! in ib_async's normal form (`scripts/oracle.py`). Each record is taken
//! apart field by field, so a field added to a type fails to compile here
//! until it is observed.

use jiff::Zoned;
use serde_json::{Map, Value, json};

use crate::contract::{ComboLeg, Contract, DeltaNeutralContract, TagValue};
use crate::live::{Live, Observed};
use crate::objects::{
    AccountValue, CommissionReport, Execution, Fill, NewsBulletin, NewsTick, PortfolioItem,
    Position, SoftDollarTier, TradeLogEntry,
};
use crate::order::{
    ExecutionCondition, MarginCondition, Order, OrderComboLeg, OrderCondition, OrderState,
    OrderStatus, PercentChangeCondition, PriceCondition, TimeCondition, Trade, VolumeCondition,
};

/// A value in the replays' normal form.
pub(crate) trait Observe {
    fn observe(&self) -> Value;
}

impl Observe for String {
    fn observe(&self) -> Value {
        Value::String(self.clone())
    }
}

impl Observe for bool {
    fn observe(&self) -> Value {
        Value::Bool(*self)
    }
}

impl Observe for i32 {
    fn observe(&self) -> Value {
        json!(self)
    }
}

impl Observe for i64 {
    fn observe(&self) -> Value {
        json!(self)
    }
}

/// A float, or `{"f": ...}` for one that is not finite.
impl Observe for f64 {
    fn observe(&self) -> Value {
        if self.is_nan() {
            json!({"f": "nan"})
        } else if self.is_infinite() {
            json!({"f": if *self > 0.0 { "inf" } else { "-inf" }})
        } else {
            json!(self)
        }
    }
}

/// Python's `isoformat()` and the zone's name.
impl Observe for Zoned {
    fn observe(&self) -> Value {
        let micros = self.subsec_nanosecond() / 1000;
        let frac = if micros == 0 {
            String::new()
        } else {
            format!(".{micros:06}")
        };
        let iso = format!(
            "{}{frac}{}",
            self.strftime("%Y-%m-%dT%H:%M:%S"),
            self.strftime("%:z")
        );
        let zone = self.time_zone().iana_name().unwrap_or("UTC");
        json!({"dt": iso, "tz": zone})
    }
}

impl<T: Observe> Observe for Option<T> {
    fn observe(&self) -> Value {
        self.as_ref().map_or(Value::Null, Observe::observe)
    }
}

impl<T: Observe> Observe for Vec<T> {
    fn observe(&self) -> Value {
        Value::Array(self.iter().map(Observe::observe).collect())
    }
}

impl<T: Observe + Observed> Observe for Live<T> {
    fn observe(&self) -> Value {
        self.read().observe()
    }
}

impl<T: Observe + ?Sized> Observe for &T {
    fn observe(&self) -> Value {
        (**self).observe()
    }
}

impl Observe for OrderCondition {
    fn observe(&self) -> Value {
        match self {
            OrderCondition::Price(c) => c.observe(),
            OrderCondition::Time(c) => c.observe(),
            OrderCondition::Margin(c) => c.observe(),
            OrderCondition::Execution(c) => c.observe(),
            OrderCondition::Volume(c) => c.observe(),
            OrderCondition::PercentChange(c) => c.observe(),
        }
    }
}

/// A record: `{"@": type, field: value, ...}` with every field.
macro_rules! record {
    ($t:ident { $($f:ident),* $(,)? }) => {
        impl Observe for $t {
            fn observe(&self) -> Value {
                let $t { $($f),* } = self;
                let mut m = Map::new();
                m.insert("@".into(), Value::String(stringify!($t).into()));
                $( m.insert(stringify!($f).into(), $f.observe()); )*
                Value::Object(m)
            }
        }
    };
}

record!(Contract {
    sec_type,
    con_id,
    symbol,
    last_trade_date_or_contract_month,
    strike,
    right,
    multiplier,
    exchange,
    primary_exchange,
    currency,
    local_symbol,
    trading_class,
    include_expired,
    sec_id_type,
    sec_id,
    description,
    issuer_id,
    combo_legs_descrip,
    combo_legs,
    delta_neutral_contract
});
record!(TagValue { tag, value });
record!(ComboLeg {
    con_id,
    ratio,
    action,
    exchange,
    open_close,
    short_sale_slot,
    designated_location,
    exempt_code
});
record!(DeltaNeutralContract {
    con_id,
    delta,
    price
});
record!(Order {
    order_id,
    client_id,
    perm_id,
    action,
    total_quantity,
    order_type,
    lmt_price,
    aux_price,
    tif,
    active_start_time,
    active_stop_time,
    oca_group,
    oca_type,
    order_ref,
    transmit,
    parent_id,
    block_order,
    sweep_to_fill,
    display_size,
    trigger_method,
    outside_rth,
    hidden,
    good_after_time,
    good_till_date,
    rule_80_a,
    all_or_none,
    min_qty,
    percent_offset,
    override_percentage_constraints,
    trail_stop_price,
    trailing_percent,
    fa_group,
    fa_profile,
    fa_method,
    fa_percentage,
    designated_location,
    open_close,
    origin,
    short_sale_slot,
    exempt_code,
    discretionary_amt,
    e_trade_only,
    firm_quote_only,
    nbbo_price_cap,
    opt_out_smart_routing,
    auction_strategy,
    starting_price,
    stock_ref_price,
    delta,
    stock_range_lower,
    stock_range_upper,
    randomize_price,
    randomize_size,
    volatility,
    volatility_type,
    delta_neutral_order_type,
    delta_neutral_aux_price,
    delta_neutral_con_id,
    delta_neutral_settling_firm,
    delta_neutral_clearing_account,
    delta_neutral_clearing_intent,
    delta_neutral_open_close,
    delta_neutral_short_sale,
    delta_neutral_short_sale_slot,
    delta_neutral_designated_location,
    continuous_update,
    reference_price_type,
    basis_points,
    basis_points_type,
    scale_init_level_size,
    scale_subs_level_size,
    scale_price_increment,
    scale_price_adjust_value,
    scale_price_adjust_interval,
    scale_profit_offset,
    scale_auto_reset,
    scale_init_position,
    scale_init_fill_qty,
    scale_random_percent,
    scale_table,
    hedge_type,
    hedge_param,
    account,
    settling_firm,
    clearing_account,
    clearing_intent,
    algo_strategy,
    algo_params,
    smart_combo_routing_params,
    algo_id,
    what_if,
    not_held,
    solicited,
    model_code,
    order_combo_legs,
    order_misc_options,
    reference_contract_id,
    pegged_change_amount,
    is_pegged_change_amount_decrease,
    reference_change_amount,
    reference_exchange_id,
    adjusted_order_type,
    trigger_price,
    adjusted_stop_price,
    adjusted_stop_limit_price,
    adjusted_trailing_amount,
    adjustable_trailing_unit,
    lmt_price_offset,
    conditions,
    conditions_cancel_order,
    conditions_ignore_rth,
    ext_operator,
    soft_dollar_tier,
    cash_qty,
    mifid_2_decision_maker,
    mifid_2_decision_algo,
    mifid_2_execution_trader,
    mifid_2_execution_algo,
    dont_use_auto_price_for_hedge,
    is_oms_container,
    discretionary_up_to_limit_price,
    auto_cancel_date,
    filled_quantity,
    ref_futures_con_id,
    auto_cancel_parent,
    shareholder,
    imbalance_only,
    route_marketable_to_bbo,
    parent_perm_id,
    use_price_mgmt_algo,
    duration,
    post_to_ats,
    advanced_error_override,
    manual_order_time,
    min_trade_qty,
    min_compete_size,
    compete_against_best_offset,
    mid_offset_at_whole,
    mid_offset_at_half
});
record!(OrderComboLeg { price });
record!(OrderStatus {
    order_id,
    status,
    filled,
    remaining,
    avg_fill_price,
    perm_id,
    parent_id,
    last_fill_price,
    client_id,
    why_held,
    mkt_cap_price
});
record!(OrderState {
    status,
    init_margin_before,
    maint_margin_before,
    equity_with_loan_before,
    init_margin_change,
    maint_margin_change,
    equity_with_loan_change,
    init_margin_after,
    maint_margin_after,
    equity_with_loan_after,
    commission,
    min_commission,
    max_commission,
    commission_currency,
    warning_text,
    completed_time,
    completed_status
});
record!(Trade {
    contract,
    order,
    order_status,
    fills,
    log,
    advanced_error
});
record!(PriceCondition {
    cond_type,
    conjunction,
    is_more,
    price,
    con_id,
    exch,
    trigger_method
});
record!(TimeCondition {
    cond_type,
    conjunction,
    is_more,
    time
});
record!(MarginCondition {
    cond_type,
    conjunction,
    is_more,
    percent
});
record!(ExecutionCondition {
    cond_type,
    conjunction,
    sec_type,
    exch,
    symbol
});
record!(VolumeCondition {
    cond_type,
    conjunction,
    is_more,
    volume,
    con_id,
    exch
});
record!(PercentChangeCondition {
    cond_type,
    conjunction,
    is_more,
    change_percent,
    con_id,
    exch
});
record!(SoftDollarTier {
    name,
    val,
    display_name
});
record!(Execution {
    exec_id,
    time,
    acct_number,
    exchange,
    side,
    shares,
    price,
    perm_id,
    client_id,
    order_id,
    liquidation,
    cum_qty,
    avg_price,
    order_ref,
    ev_rule,
    ev_multiplier,
    model_code,
    last_liquidity,
    pending_price_revision
});
record!(CommissionReport {
    exec_id,
    commission,
    currency,
    realized_pnl,
    yield_,
    yield_redemption_date
});
record!(TradeLogEntry {
    time,
    status,
    message,
    error_code
});
record!(Fill {
    contract,
    execution,
    commission_report,
    time
});
record!(Position {
    account,
    contract,
    position,
    avg_cost
});
record!(PortfolioItem {
    contract,
    position,
    market_price,
    market_value,
    average_cost,
    unrealized_pnl,
    realized_pnl,
    account
});
record!(AccountValue {
    account,
    tag,
    value,
    currency,
    model_code
});
record!(NewsBulletin {
    msg_id,
    msg_type,
    message,
    orig_exchange
});
record!(NewsTick {
    time_stamp,
    provider_code,
    article_id,
    headline,
    extra_data
});

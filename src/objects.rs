//! ib_async's `objects` module: the values requests and callbacks carry,
//! with `ConnectionStats`, the dynamic objects of fundamental ratios and
//! Flex reports, and the types of the calls beyond ib_async's API.

use std::collections::BTreeMap;
use std::ops::{Add, Mul, Sub};

use indexmap::IndexMap;
use jiff::tz::TimeZone;
use jiff::{Zoned, civil};

use crate::contract::{Contract, ScanData, TagValue};
use crate::engine::HeldElsewhere;
use crate::event::Event;
use crate::live::sealed::{Maker, Storage};
use crate::live::{Live, Observed};
use crate::util::{BarDate, DateTimeArg, EPOCH};

/// What a market scanner is asked for: ib_async's `ScannerSubscription`.
#[derive(Clone, Debug, PartialEq)]
pub struct ScannerSubscription {
    /// How many rows to return; a negative number asks for the default:
    /// `numberOfRows`.
    pub number_of_rows: i32,
    /// The instrument type scanned: `instrument`.
    pub instrument: String,
    /// The location scanned: `locationCode`.
    pub location_code: String,
    /// The scan to run: `scanCode`.
    pub scan_code: String,
    /// `abovePrice`.
    pub above_price: Option<f64>,
    /// `belowPrice`.
    pub below_price: Option<f64>,
    /// `aboveVolume`.
    pub above_volume: Option<i32>,
    /// `marketCapAbove`.
    pub market_cap_above: Option<f64>,
    /// `marketCapBelow`.
    pub market_cap_below: Option<f64>,
    /// `moodyRatingAbove`.
    pub moody_rating_above: String,
    /// `moodyRatingBelow`.
    pub moody_rating_below: String,
    /// `spRatingAbove`.
    pub sp_rating_above: String,
    /// `spRatingBelow`.
    pub sp_rating_below: String,
    /// `maturityDateAbove`.
    pub maturity_date_above: String,
    /// `maturityDateBelow`.
    pub maturity_date_below: String,
    /// `couponRateAbove`.
    pub coupon_rate_above: Option<f64>,
    /// `couponRateBelow`.
    pub coupon_rate_below: Option<f64>,
    /// `excludeConvertible`.
    pub exclude_convertible: bool,
    /// `averageOptionVolumeAbove`.
    pub average_option_volume_above: Option<i32>,
    /// `scannerSettingPairs`.
    pub scanner_setting_pairs: String,
    /// `stockTypeFilter`.
    pub stock_type_filter: String,
}

impl Default for ScannerSubscription {
    fn default() -> Self {
        ScannerSubscription {
            number_of_rows: -1,
            instrument: String::new(),
            location_code: String::new(),
            scan_code: String::new(),
            above_price: None,
            below_price: None,
            above_volume: None,
            market_cap_above: None,
            market_cap_below: None,
            moody_rating_above: String::new(),
            moody_rating_below: String::new(),
            sp_rating_above: String::new(),
            sp_rating_below: String::new(),
            maturity_date_above: String::new(),
            maturity_date_below: String::new(),
            coupon_rate_above: None,
            coupon_rate_below: None,
            exclude_convertible: false,
            average_option_volume_above: None,
            scanner_setting_pairs: String::new(),
            stock_type_filter: String::new(),
        }
    }
}

/// A soft dollar tier: ib_async's `SoftDollarTier`. ib_async's truth test
/// is `tier != SoftDollarTier::default()`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SoftDollarTier {
    /// `name`.
    pub name: String,
    /// `val`.
    pub val: String,
    /// `displayName`.
    pub display_name: String,
}

/// One execution of an order: ib_async's `Execution`.
#[derive(Clone, Debug, PartialEq)]
pub struct Execution {
    /// `execId`.
    pub exec_id: String,
    /// When it executed, in `IBDefaults.timezone`; `EPOCH` by default:
    /// `time`.
    pub time: Zoned,
    /// `acctNumber`.
    pub acct_number: String,
    /// `exchange`.
    pub exchange: String,
    /// `side`.
    pub side: String,
    /// `shares`.
    pub shares: f64,
    /// `price`.
    pub price: f64,
    /// `permId`.
    pub perm_id: i64,
    /// `clientId`.
    pub client_id: i64,
    /// `orderId`.
    pub order_id: i64,
    /// `liquidation`.
    pub liquidation: i32,
    /// `cumQty`.
    pub cum_qty: f64,
    /// `avgPrice`.
    pub avg_price: f64,
    /// `orderRef`.
    pub order_ref: String,
    /// `evRule`.
    pub ev_rule: String,
    /// `evMultiplier`.
    pub ev_multiplier: f64,
    /// `modelCode`.
    pub model_code: String,
    /// `lastLiquidity`.
    pub last_liquidity: i32,
    /// `pendingPriceRevision`.
    pub pending_price_revision: bool,
}

impl Default for Execution {
    fn default() -> Self {
        Execution {
            exec_id: String::new(),
            time: EPOCH.clone(),
            acct_number: String::new(),
            exchange: String::new(),
            side: String::new(),
            shares: 0.0,
            price: 0.0,
            perm_id: 0,
            client_id: 0,
            order_id: 0,
            liquidation: 0,
            cum_qty: 0.0,
            avg_price: 0.0,
            order_ref: String::new(),
            ev_rule: String::new(),
            ev_multiplier: 0.0,
            model_code: String::new(),
            last_liquidity: 0,
            pending_price_revision: false,
        }
    }
}

/// What an execution cost: ib_async's `CommissionReport`. Inside a [`Fill`]
/// it is one shared [`Live`] object, updated in place.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CommissionReport {
    /// `execId`.
    pub exec_id: String,
    /// `commission`.
    pub commission: f64,
    /// `currency`.
    pub currency: String,
    /// The realized P&L; 0.0 when not stated: `realizedPNL`.
    pub realized_pnl: f64,
    /// The yield; 0.0 when not stated: `yield_`.
    pub yield_: f64,
    /// `yieldRedemptionDate`.
    pub yield_redemption_date: i32,
}

/// Which executions to ask for: ib_async's `ExecutionFilter`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ExecutionFilter {
    /// `clientId`.
    pub client_id: i64,
    /// `acctCode`.
    pub acct_code: String,
    /// `time`.
    pub time: String,
    /// `symbol`.
    pub symbol: String,
    /// `secType`.
    pub sec_type: String,
    /// `exchange`.
    pub exchange: String,
    /// `side`.
    pub side: String,
}

/// One historical bar: ib_async's `BarData`.
#[derive(Clone, Debug, PartialEq)]
pub struct BarData {
    /// The bar's date or time; `EPOCH` by default: `date`.
    pub date: BarDate,
    /// `open`.
    pub open: f64,
    /// `high`.
    pub high: f64,
    /// `low`.
    pub low: f64,
    /// `close`.
    pub close: f64,
    /// `volume`.
    pub volume: f64,
    /// `average`.
    pub average: f64,
    /// `barCount`.
    pub bar_count: i32,
}

impl Default for BarData {
    fn default() -> Self {
        BarData {
            date: BarDate::At(EPOCH.clone()),
            open: 0.0,
            high: 0.0,
            low: 0.0,
            close: 0.0,
            volume: 0.0,
            average: 0.0,
            bar_count: 0,
        }
    }
}

/// One real-time bar: ib_async's `RealTimeBar`.
#[derive(Clone, Debug, PartialEq)]
pub struct RealTimeBar {
    /// `time`; `EPOCH` by default.
    pub time: Zoned,
    /// `endTime`; -1 by default.
    pub end_time: i32,
    /// `open_`.
    pub open_: f64,
    /// `high`.
    pub high: f64,
    /// `low`.
    pub low: f64,
    /// `close`.
    pub close: f64,
    /// `volume`.
    pub volume: f64,
    /// `wap`.
    pub wap: f64,
    /// `count`.
    pub count: i32,
}

impl Default for RealTimeBar {
    fn default() -> Self {
        RealTimeBar {
            time: EPOCH.clone(),
            end_time: -1,
            open_: 0.0,
            high: 0.0,
            low: 0.0,
            close: 0.0,
            volume: 0.0,
            wap: 0.0,
            count: 0,
        }
    }
}

/// A tick's attributes: ib_async's `TickAttrib`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TickAttrib {
    /// `canAutoExecute`.
    pub can_auto_execute: bool,
    /// `pastLimit`.
    pub past_limit: bool,
    /// `preOpen`.
    pub pre_open: bool,
}

/// A bid/ask tick's attributes: ib_async's `TickAttribBidAsk`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TickAttribBidAsk {
    /// `bidPastLow`.
    pub bid_past_low: bool,
    /// `askPastHigh`.
    pub ask_past_high: bool,
}

/// A trade tick's attributes: ib_async's `TickAttribLast`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TickAttribLast {
    /// `pastLimit`.
    pub past_limit: bool,
    /// `unreported`.
    pub unreported: bool,
}

/// One bucket of a histogram: ib_async's `HistogramData`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct HistogramData {
    /// `price`.
    pub price: f64,
    /// `count`.
    pub count: i64,
}

/// A news provider: ib_async's `NewsProvider`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct NewsProvider {
    /// `code`.
    pub code: String,
    /// `name`.
    pub name: String,
}

/// An exchange that offers market depth: ib_async's
/// `DepthMktDataDescription`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DepthMktDataDescription {
    /// `exchange`.
    pub exchange: String,
    /// `secType`.
    pub sec_type: String,
    /// `listingExch`.
    pub listing_exch: String,
    /// `serviceDataType`.
    pub service_data_type: String,
    /// `aggGroup`.
    pub agg_group: Option<i32>,
}

/// An account's P&L, updated in place and held as [`Live`]: ib_async's
/// `PnL`.
#[derive(Clone, Debug, PartialEq)]
pub struct PnL {
    /// `account`.
    pub account: String,
    /// `modelCode`.
    pub model_code: String,
    /// `dailyPnL`; NaN until stated.
    pub daily_pnl: f64,
    /// `unrealizedPnL`; NaN until stated.
    pub unrealized_pnl: f64,
    /// `realizedPnL`; NaN until stated.
    pub realized_pnl: f64,
}

impl Default for PnL {
    fn default() -> Self {
        PnL {
            account: String::new(),
            model_code: String::new(),
            daily_pnl: f64::NAN,
            unrealized_pnl: f64::NAN,
            realized_pnl: f64::NAN,
        }
    }
}

/// One entry of a trade's log: ib_async's `TradeLogEntry`.
#[derive(Clone, Debug, PartialEq)]
pub struct TradeLogEntry {
    /// `time`.
    pub time: Zoned,
    /// `status`.
    pub status: String,
    /// `message`.
    pub message: String,
    /// `errorCode`.
    pub error_code: i32,
}

/// One position's P&L, updated in place and held as [`Live`]: ib_async's
/// `PnLSingle`.
#[derive(Clone, Debug, PartialEq)]
pub struct PnLSingle {
    /// `account`.
    pub account: String,
    /// `modelCode`.
    pub model_code: String,
    /// `conId`.
    pub con_id: i64,
    /// `dailyPnL`; NaN until stated.
    pub daily_pnl: f64,
    /// `unrealizedPnL`; NaN until stated.
    pub unrealized_pnl: f64,
    /// `realizedPnL`; NaN until stated.
    pub realized_pnl: f64,
    /// `position`.
    pub position: f64,
    /// `value`; NaN until stated.
    pub value: f64,
}

impl Default for PnLSingle {
    fn default() -> Self {
        PnLSingle {
            account: String::new(),
            model_code: String::new(),
            con_id: 0,
            daily_pnl: f64::NAN,
            unrealized_pnl: f64::NAN,
            realized_pnl: f64::NAN,
            position: 0.0,
            value: f64::NAN,
        }
    }
}

/// One session of a historical schedule: ib_async's `HistoricalSession`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct HistoricalSession {
    /// `startDateTime`.
    pub start_date_time: String,
    /// `endDateTime`.
    pub end_date_time: String,
    /// `refDate`.
    pub ref_date: String,
}

/// A historical trading schedule: ib_async's `HistoricalSchedule`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct HistoricalSchedule {
    /// `startDateTime`.
    pub start_date_time: String,
    /// `endDateTime`.
    pub end_date_time: String,
    /// `timeZone`.
    pub time_zone: String,
    /// `sessions`.
    pub sessions: Vec<HistoricalSession>,
}

/// What a Wall Street Horizon event request asks for: ib_async's
/// `WshEventData`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct WshEventData {
    /// `conId`.
    pub con_id: Option<i64>,
    /// `filter`.
    pub filter: String,
    /// `fillWatchlist`.
    pub fill_watchlist: bool,
    /// `fillPortfolio`.
    pub fill_portfolio: bool,
    /// `fillCompetitors`.
    pub fill_competitors: bool,
    /// `startDate`.
    pub start_date: String,
    /// `endDate`.
    pub end_date: String,
    /// `totalLimit`.
    pub total_limit: Option<i32>,
}

/// One account value: ib_async's `AccountValue`.
#[derive(Clone, Debug, PartialEq)]
pub struct AccountValue {
    /// `account`.
    pub account: String,
    /// `tag`.
    pub tag: String,
    /// `value`.
    pub value: String,
    /// `currency`.
    pub currency: String,
    /// `modelCode`.
    pub model_code: String,
}

/// One price or size tick: ib_async's `TickData`.
#[derive(Clone, Debug, PartialEq)]
pub struct TickData {
    /// `time`.
    pub time: Zoned,
    /// `tickType`.
    pub tick_type: i32,
    /// `price`.
    pub price: f64,
    /// `size`.
    pub size: f64,
}

/// One historical midpoint tick: ib_async's `HistoricalTick`.
#[derive(Clone, Debug, PartialEq)]
pub struct HistoricalTick {
    /// `time`.
    pub time: Zoned,
    /// `price`.
    pub price: f64,
    /// `size`.
    pub size: f64,
}

/// One historical bid/ask tick: ib_async's `HistoricalTickBidAsk`.
#[derive(Clone, Debug, PartialEq)]
pub struct HistoricalTickBidAsk {
    /// `time`.
    pub time: Zoned,
    /// `tickAttribBidAsk`.
    pub tick_attrib_bid_ask: TickAttribBidAsk,
    /// `priceBid`.
    pub price_bid: f64,
    /// `priceAsk`.
    pub price_ask: f64,
    /// `sizeBid`.
    pub size_bid: f64,
    /// `sizeAsk`.
    pub size_ask: f64,
}

/// One historical trade tick: ib_async's `HistoricalTickLast`.
#[derive(Clone, Debug, PartialEq)]
pub struct HistoricalTickLast {
    /// `time`.
    pub time: Zoned,
    /// `tickAttribLast`.
    pub tick_attrib_last: TickAttribLast,
    /// `price`.
    pub price: f64,
    /// `size`.
    pub size: f64,
    /// `exchange`.
    pub exchange: String,
    /// `specialConditions`.
    pub special_conditions: String,
}

/// One historical tick of any kind: the element of ib_async's
/// `reqHistoricalTicks` list.
#[derive(Clone, Debug, PartialEq)]
pub enum HistoricalTickAny {
    /// A midpoint tick.
    Midpoint(HistoricalTick),
    /// A bid/ask tick.
    BidAsk(HistoricalTickBidAsk),
    /// A trade tick.
    Last(HistoricalTickLast),
}

/// One tick-by-tick trade: ib_async's `TickByTickAllLast`.
#[derive(Clone, Debug, PartialEq)]
pub struct TickByTickAllLast {
    /// `tickType`.
    pub tick_type: i32,
    /// `time`.
    pub time: Zoned,
    /// `price`.
    pub price: f64,
    /// `size`.
    pub size: f64,
    /// `tickAttribLast`.
    pub tick_attrib_last: TickAttribLast,
    /// `exchange`.
    pub exchange: String,
    /// `specialConditions`.
    pub special_conditions: String,
}

/// One tick-by-tick bid and ask: ib_async's `TickByTickBidAsk`.
#[derive(Clone, Debug, PartialEq)]
pub struct TickByTickBidAsk {
    /// `time`.
    pub time: Zoned,
    /// `bidPrice`.
    pub bid_price: f64,
    /// `askPrice`.
    pub ask_price: f64,
    /// `bidSize`.
    pub bid_size: f64,
    /// `askSize`.
    pub ask_size: f64,
    /// `tickAttribBidAsk`.
    pub tick_attrib_bid_ask: TickAttribBidAsk,
}

/// One tick-by-tick midpoint: ib_async's `TickByTickMidPoint`.
#[derive(Clone, Debug, PartialEq)]
pub struct TickByTickMidPoint {
    /// `time`.
    pub time: Zoned,
    /// `midPoint`.
    pub mid_point: f64,
}

/// One market depth change: ib_async's `MktDepthData`.
#[derive(Clone, Debug, PartialEq)]
pub struct MktDepthData {
    /// `time`.
    pub time: Zoned,
    /// `position`.
    pub position: i32,
    /// `marketMaker`.
    pub market_maker: String,
    /// `operation`.
    pub operation: i32,
    /// `side`.
    pub side: i32,
    /// `price`.
    pub price: f64,
    /// `size`.
    pub size: f64,
}

/// One level of a market depth book: ib_async's `DOMLevel`.
#[derive(Clone, Debug, PartialEq)]
pub struct DOMLevel {
    /// `price`.
    pub price: f64,
    /// `size`.
    pub size: f64,
    /// `marketMaker`.
    pub market_maker: String,
}

/// One step of a market rule: ib_async's `PriceIncrement`.
#[derive(Clone, Debug, PartialEq)]
pub struct PriceIncrement {
    /// `lowEdge`.
    pub low_edge: f64,
    /// `increment`.
    pub increment: f64,
}

/// One portfolio holding: ib_async's `PortfolioItem`.
#[derive(Clone, Debug, PartialEq)]
pub struct PortfolioItem {
    /// `contract`.
    pub contract: Contract,
    /// `position`.
    pub position: f64,
    /// `marketPrice`.
    pub market_price: f64,
    /// `marketValue`.
    pub market_value: f64,
    /// `averageCost`.
    pub average_cost: f64,
    /// `unrealizedPNL`.
    pub unrealized_pnl: f64,
    /// `realizedPNL`.
    pub realized_pnl: f64,
    /// `account`.
    pub account: String,
}

/// One position: ib_async's `Position`.
#[derive(Clone, Debug, PartialEq)]
pub struct Position {
    /// `account`.
    pub account: String,
    /// `contract`.
    pub contract: Contract,
    /// `position`.
    pub position: f64,
    /// `avgCost`.
    pub avg_cost: f64,
}

/// One fill: ib_async's `Fill`. Two fills are equal when their fields are,
/// the commission report compared by its current value.
#[derive(Clone, Debug, PartialEq)]
pub struct Fill {
    /// `contract`.
    pub contract: Contract,
    /// `execution`.
    pub execution: Execution,
    /// The fill's report, one object shared by every holder of the fill and
    /// updated in place: `commissionReport`.
    pub commission_report: Live<CommissionReport>,
    /// `time`.
    pub time: Zoned,
}

/// Exchange for physical futures data: ib_async's `EfpData`.
#[derive(Clone, Debug, PartialEq)]
pub struct EfpData {
    /// Annualized basis points: `basisPoints`.
    pub basis_points: f64,
    /// Basis points as a percentage string: `formattedBasisPoints`.
    pub formatted_basis_points: String,
    /// The implied futures price: `impliedFuture`.
    pub implied_future: f64,
    /// Days to the future's last trade date: `holdDays`.
    pub hold_days: i32,
    /// The future's expiration date: `futureLastTradeDate`.
    pub future_last_trade_date: String,
    /// The dividend impact on the annualized basis points:
    /// `dividendImpact`.
    pub dividend_impact: f64,
    /// Expected dividends to the future's expiration:
    /// `dividendsToLastTradeDate`.
    pub dividends_to_last_trade_date: f64,
}

/// An option's price, greeks and implied volatility: ib_async's
/// `OptionComputation`. A figure not stated is `None`.
///
/// `+`, `-` and `* f64` work as ib_async's: `tick_attrib` becomes 0,
/// `pv_dividend` `None` and `und_price` the left side's, and each other
/// figure is computed with `None` read as 0.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct OptionComputation {
    /// `tickAttrib`.
    pub tick_attrib: i32,
    /// `impliedVol`.
    pub implied_vol: Option<f64>,
    /// `delta`.
    pub delta: Option<f64>,
    /// `optPrice`.
    pub opt_price: Option<f64>,
    /// `pvDividend`.
    pub pv_dividend: Option<f64>,
    /// `gamma`.
    pub gamma: Option<f64>,
    /// `vega`.
    pub vega: Option<f64>,
    /// `theta`.
    pub theta: Option<f64>,
    /// `undPrice`.
    pub und_price: Option<f64>,
}

impl OptionComputation {
    /// ib_async's arithmetic: `f` on each figure pair, `None` read as 0.
    fn combine(self, other: Self, f: impl Fn(f64, f64) -> f64) -> Self {
        let op = |a: Option<f64>, b: Option<f64>| Some(f(a.unwrap_or(0.0), b.unwrap_or(0.0)));
        OptionComputation {
            tick_attrib: 0,
            implied_vol: op(self.implied_vol, other.implied_vol),
            delta: op(self.delta, other.delta),
            opt_price: op(self.opt_price, other.opt_price),
            pv_dividend: None,
            gamma: op(self.gamma, other.gamma),
            vega: op(self.vega, other.vega),
            theta: op(self.theta, other.theta),
            und_price: self.und_price,
        }
    }
}

impl Add for OptionComputation {
    type Output = OptionComputation;
    fn add(self, other: Self) -> Self {
        self.combine(other, |a, b| a + b)
    }
}

impl Sub for OptionComputation {
    type Output = OptionComputation;
    fn sub(self, other: Self) -> Self {
        self.combine(other, |a, b| a - b)
    }
}

impl Mul<f64> for OptionComputation {
    type Output = OptionComputation;
    fn mul(self, k: f64) -> Self {
        // Each figure times `k`; the second operand's figures are unused.
        self.combine(self, |a, _| a * k)
    }
}

/// An option chain's parameters: ib_async's `OptionChain`.
#[derive(Clone, Debug, PartialEq)]
pub struct OptionChain {
    /// `exchange`.
    pub exchange: String,
    /// `underlyingConId`.
    pub underlying_con_id: i64,
    /// `tradingClass`.
    pub trading_class: String,
    /// `multiplier`.
    pub multiplier: String,
    /// `expirations`.
    pub expirations: Vec<String>,
    /// `strikes`.
    pub strikes: Vec<f64>,
}

/// A contract's dividends: ib_async's `Dividends`.
#[derive(Clone, Debug, PartialEq)]
pub struct Dividends {
    /// `past12Months`.
    pub past_12_months: Option<f64>,
    /// `next12Months`.
    pub next_12_months: Option<f64>,
    /// `nextDate`.
    pub next_date: Option<civil::Date>,
    /// `nextAmount`.
    pub next_amount: Option<f64>,
}

/// A news article: ib_async's `NewsArticle`.
#[derive(Clone, Debug, PartialEq)]
pub struct NewsArticle {
    /// `articleType`.
    pub article_type: i32,
    /// `articleText`.
    pub article_text: String,
}

/// A historical news headline: ib_async's `HistoricalNews`.
#[derive(Clone, Debug, PartialEq)]
pub struct HistoricalNews {
    /// The headline's time, as parsed and not localised: `time`.
    pub time: BarDate,
    /// `providerCode`.
    pub provider_code: String,
    /// `articleId`.
    pub article_id: String,
    /// `headline`.
    pub headline: String,
}

/// A news headline from a market data subscription: ib_async's `NewsTick`.
#[derive(Clone, Debug, PartialEq)]
pub struct NewsTick {
    /// `timeStamp`.
    pub time_stamp: i64,
    /// `providerCode`.
    pub provider_code: String,
    /// `articleId`.
    pub article_id: String,
    /// `headline`.
    pub headline: String,
    /// `extraData`.
    pub extra_data: String,
}

/// A news bulletin: ib_async's `NewsBulletin`.
#[derive(Clone, Debug, PartialEq)]
pub struct NewsBulletin {
    /// `msgId`.
    pub msg_id: i64,
    /// `msgType`.
    pub msg_type: i32,
    /// `message`.
    pub message: String,
    /// `origExchange`.
    pub orig_exchange: String,
}

/// An account's family code: ib_async's `FamilyCode`.
#[derive(Clone, Debug, PartialEq)]
pub struct FamilyCode {
    /// `accountID`.
    pub account_id: String,
    /// `familyCodeStr`.
    pub family_code_str: String,
}

/// One exchange a smart route may use: ib_async's `SmartComponent`.
#[derive(Clone, Debug, PartialEq)]
pub struct SmartComponent {
    /// `bitNumber`.
    pub bit_number: i32,
    /// `exchange`.
    pub exchange: String,
    /// `exchangeLetter`.
    pub exchange_letter: String,
}

/// A session's traffic: ib_async's `ConnectionStats`.
#[derive(Clone, Debug, PartialEq)]
pub struct ConnectionStats {
    /// When the connection's statistics started, in seconds since the
    /// epoch: `startTime`.
    pub start_time: f64,
    /// Seconds since then: `duration`.
    pub duration: f64,
    /// `numBytesRecv`.
    pub num_bytes_recv: i64,
    /// `numBytesSent`.
    pub num_bytes_sent: i64,
    /// `numMsgRecv`.
    pub num_msg_recv: i64,
    /// `numMsgSent`.
    pub num_msg_sent: i64,
}

/// Historical bars with the request that fills them, updated in place and
/// held as [`Live`]: ib_async's `BarDataList`.
#[derive(Clone, Debug)]
pub struct BarDataList {
    /// The bars: the list itself in ib_async.
    pub bars: Vec<BarData>,
    /// `reqId`.
    pub req_id: i64,
    /// `contract`.
    pub contract: Contract,
    /// The end argument as given, `DateTimeArg::None` included:
    /// `endDateTime`.
    pub end_date_time: DateTimeArg,
    /// `durationStr`.
    pub duration_str: String,
    /// `barSizeSetting`.
    pub bar_size_setting: String,
    /// `whatToShow`.
    pub what_to_show: String,
    /// `useRTH`.
    pub use_rth: bool,
    /// `formatDate`.
    pub format_date: i32,
    /// `keepUpToDate`.
    pub keep_up_to_date: bool,
    /// `chartOptions`.
    pub chart_options: Vec<TagValue>,
}

impl Default for BarDataList {
    fn default() -> Self {
        BarDataList {
            bars: Vec::new(),
            req_id: 0,
            contract: Contract::default(),
            end_date_time: DateTimeArg::None,
            duration_str: String::new(),
            bar_size_setting: String::new(),
            what_to_show: String::new(),
            use_rth: false,
            format_date: 0,
            keep_up_to_date: false,
            chart_options: Vec::new(),
        }
    }
}

/// Real-time bars with the request that fills them, updated in place and
/// held as [`Live`]: ib_async's `RealTimeBarList`.
#[derive(Clone, Debug, Default)]
pub struct RealTimeBarList {
    /// The bars: the list itself in ib_async.
    pub bars: Vec<RealTimeBar>,
    /// `reqId`.
    pub req_id: i64,
    /// `contract`.
    pub contract: Contract,
    /// `barSize`.
    pub bar_size: i32,
    /// `whatToShow`.
    pub what_to_show: String,
    /// `useRTH`.
    pub use_rth: bool,
    /// `realTimeBarsOptions`.
    pub real_time_bars_options: Vec<TagValue>,
}

/// Scanner results with the subscription that fills them, updated in place
/// and held as [`Live`]: ib_async's `ScanDataList`.
#[derive(Clone, Debug, Default)]
pub struct ScanDataList {
    /// The results: the list itself in ib_async.
    pub data: Vec<ScanData>,
    /// `reqId`.
    pub req_id: i64,
    /// `subscription`.
    pub subscription: ScannerSubscription,
    /// `scannerSubscriptionOptions`.
    pub scanner_subscription_options: Vec<TagValue>,
    /// `scannerSubscriptionFilterOptions`.
    pub scanner_subscription_filter_options: Vec<TagValue>,
}

impl Storage for BarDataList {
    type Events = Event<(Live<BarDataList>, bool)>;
    fn events(maker: &Maker) -> Self::Events {
        maker.event("updateEvent")
    }
}
impl Observed for BarDataList {}

impl Storage for RealTimeBarList {
    type Events = Event<(Live<RealTimeBarList>, bool)>;
    fn events(maker: &Maker) -> Self::Events {
        maker.event("updateEvent")
    }
}
impl Observed for RealTimeBarList {}

impl Storage for ScanDataList {
    type Events = Event<Live<ScanDataList>>;
    fn events(maker: &Maker) -> Self::Events {
        maker.event("updateEvent")
    }
}
impl Observed for ScanDataList {}

impl Live<BarDataList> {
    /// Emits the list and whether a bar was added: ib_async's
    /// `BarDataList.updateEvent`.
    pub fn update_event(&self) -> &Event<(Live<BarDataList>, bool)> {
        self.events()
    }
}

impl Live<RealTimeBarList> {
    /// Emits the list and whether a bar was added: ib_async's
    /// `RealTimeBarList.updateEvent`.
    pub fn update_event(&self) -> &Event<(Live<RealTimeBarList>, bool)> {
        self.events()
    }
}

impl Live<ScanDataList> {
    /// Emits the list: ib_async's `ScanDataList.updateEvent`.
    pub fn update_event(&self) -> &Event<Live<ScanDataList>> {
        self.events()
    }
}

// PnL, PnLSingle and CommissionReport are shared in place and have no
// events.
impl Storage for PnL {
    type Events = ();
    fn events(_: &Maker) -> Self::Events {}
}
impl Observed for PnL {}

impl Storage for PnLSingle {
    type Events = ();
    fn events(_: &Maker) -> Self::Events {}
}
impl Observed for PnLSingle {}

impl Storage for CommissionReport {
    type Events = ();
    fn events(_: &Maker) -> Self::Events {}
}
impl Observed for CommissionReport {}

/// The same list: ib_async's lists compare by identity.
impl PartialEq for Live<BarDataList> {
    fn eq(&self, other: &Self) -> bool {
        Live::ptr_eq(self, other)
    }
}

/// The same list: ib_async's lists compare by identity.
impl PartialEq for Live<RealTimeBarList> {
    fn eq(&self, other: &Self) -> bool {
        Live::ptr_eq(self, other)
    }
}

/// The same list: ib_async's lists compare by identity.
impl PartialEq for Live<ScanDataList> {
    fn eq(&self, other: &Self) -> bool {
        Live::ptr_eq(self, other)
    }
}

/// Equal current values: ib_async's dataclass equality.
impl PartialEq for Live<PnL> {
    fn eq(&self, other: &Self) -> bool {
        *self.read() == *other.read()
    }
}

/// Equal current values: ib_async's dataclass equality.
impl PartialEq for Live<PnLSingle> {
    fn eq(&self, other: &Self) -> bool {
        *self.read() == *other.read()
    }
}

/// Equal current values: ib_async's dataclass equality.
impl PartialEq for Live<CommissionReport> {
    fn eq(&self, other: &Self) -> bool {
        *self.read() == *other.read()
    }
}

/// A value of a [`DynamicObject`]: a number when Python's `float()` reads
/// the text, an integer when `int()` also does, else the text.
#[derive(Clone, Debug, PartialEq)]
pub enum DynamicValue {
    /// Text `int()` reads, when it fits an `i64`.
    Int(i64),
    /// Text `float()` reads and `int()` does not, or an integer too wide
    /// for an `i64`.
    Float(f64),
    /// Text neither reads.
    Str(String),
}

/// Named values in the order given: ib_async's `DynamicObject`.
pub type DynamicObject = IndexMap<String, DynamicValue>;

/// A contract's fundamental ratios by tag: ib_async's `FundamentalRatios`.
pub type FundamentalRatios = DynamicObject;

/// Values used where the API states none: ib_async's `IBDefaults`.
#[derive(Clone, Debug, PartialEq)]
pub struct IBDefaults {
    /// The price for a quote that does not exist: `emptyPrice`.
    pub empty_price: f64,
    /// The size for a quote that does not exist: `emptySize`.
    pub empty_size: f64,
    /// What a ticker's figures start as: `unset`.
    pub unset: f64,
    /// The zone of the times stamped and logged: `timezone`.
    pub timezone: TimeZone,
}

impl Default for IBDefaults {
    fn default() -> Self {
        IBDefaults {
            empty_price: -1.0,
            empty_size: 0.0,
            unset: f64::NAN,
            timezone: TimeZone::UTC,
        }
    }
}

/// What the venue states for a ticker's contract beyond `Ticker`'s fields,
/// by the venue's own series numbers: `IB.tickerExtras`'s value. A figure
/// not stated is NaN.
#[derive(Clone, Debug, PartialEq)]
pub struct TickerExtras {
    /// `sharesOutstanding`.
    pub shares_outstanding: f64,
    /// `openAYearAgo`.
    pub open_a_year_ago: f64,
    /// A short-sale circuit breaker is on, not shortability:
    /// `shortSaleRestricted`.
    pub short_sale_restricted: bool,
    /// `statedFigures`.
    pub stated_figures: BTreeMap<u32, Vec<f64>>,
    /// Per series, the whole and the fractional table: `numberedFigures`.
    #[allow(clippy::type_complexity)] // ib_async's own shape, spelled out
    pub numbered_figures: BTreeMap<u32, (BTreeMap<i32, f64>, BTreeMap<i32, f64>)>,
    /// `pairedFigures`.
    pub paired_figures: BTreeMap<u32, Vec<(f64, f64)>>,
    /// The rows of each series the ticker's generic tick list names.
    pub stated_rows: BTreeMap<u32, Vec<(f64, f64, f64)>>,
}

impl Default for TickerExtras {
    fn default() -> Self {
        TickerExtras {
            shares_outstanding: f64::NAN,
            open_a_year_ago: f64::NAN,
            short_sale_restricted: false,
            stated_figures: BTreeMap::new(),
            numbered_figures: BTreeMap::new(),
            paired_figures: BTreeMap::new(),
            stated_rows: BTreeMap::new(),
        }
    }
}

/// The venue's option model, all its figures: `IB.optionModel`'s value. A
/// figure not stated is `None`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OptionModel {
    /// `impliedVol`.
    pub implied_vol: Option<f64>,
    /// `delta`.
    pub delta: Option<f64>,
    /// `optPrice`.
    pub opt_price: Option<f64>,
    /// `pvDividend`.
    pub pv_dividend: Option<f64>,
    /// `gamma`.
    pub gamma: Option<f64>,
    /// `vega`.
    pub vega: Option<f64>,
    /// `theta`.
    pub theta: Option<f64>,
    /// `undPrice`.
    pub und_price: Option<f64>,
    /// `calDays`.
    pub cal_days: Option<f64>,
    /// `rate`.
    pub rate: Option<f64>,
    /// `rho`.
    pub rho: Option<f64>,
    /// `fugit`.
    pub fugit: Option<f64>,
    /// `exerciseBoundary`.
    pub exercise_boundary: Option<f64>,
    /// `forwardCoeff`.
    pub forward_coeff: Option<f64>,
    /// `modelYield`.
    pub model_yield: Option<f64>,
    /// `bridgeYield`.
    pub bridge_yield: Option<f64>,
    /// `timeValue`.
    pub time_value: Option<f64>,
    /// `priceBasedVol`.
    pub price_based_vol: Option<bool>,
}

/// One of a contract's corporate actions: `IB.reqCorporateActions`'s
/// element. `kind` is the venue's two-letter code (CD, SD, SS, SO, RO, FR);
/// days are `YYYYMMDD`; a field the kind does not carry is empty.
#[derive(Clone, Debug, PartialEq)]
pub struct CorporateAction {
    /// `kind`.
    pub kind: String,
    /// `date`.
    pub date: String,
    /// `value`.
    pub value: String,
    /// `currency`.
    pub currency: String,
    /// `announceDate`.
    pub announce_date: String,
    /// `recordDate`.
    pub record_date: String,
    /// `payDate`.
    pub pay_date: String,
    /// `paymentType`.
    pub payment_type: String,
    /// `distributionType`.
    pub distribution_type: String,
}

/// One set of order defaults the account holds, without its values:
/// `IB.orderPresets`'s element.
#[derive(Clone, Debug, PartialEq)]
pub struct OrderPreset {
    /// `key`.
    pub key: String,
    /// `version`.
    pub version: String,
    /// `lastChanged`.
    pub last_changed: String,
}

/// A holding the venue reports that this broker does not hold itself:
/// `IB.positionsElsewhere`'s element.
#[derive(Clone, Debug, PartialEq)]
pub struct PositionElsewhere {
    /// `conId`.
    pub con_id: i64,
    /// `symbol`.
    pub symbol: String,
    /// `secType`.
    pub sec_type: String,
    /// `currency`.
    pub currency: String,
    /// `position`.
    pub position: f64,
    /// `avgCost`.
    pub avg_cost: f64,
    /// Where the venue says it sits: `held`.
    pub held: HeldElsewhere,
}

/// Another session that held the account when this one connected:
/// `IB.competingSession`'s value.
#[derive(Clone, Debug, PartialEq)]
pub struct CompetingSession {
    /// Where it came from: `origin`.
    pub origin: String,
    /// When it logged in, in UTC: `loggedInAt`.
    pub logged_in_at: Zoned,
    /// This session may read but not trade: `readOnly`.
    pub read_only: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oc(tick_attrib: i32, v: [Option<f64>; 8]) -> OptionComputation {
        let [implied_vol, delta, opt_price, pv_dividend] = [v[0], v[1], v[2], v[3]];
        let [gamma, vega, theta, und_price] = [v[4], v[5], v[6], v[7]];
        OptionComputation {
            tick_attrib,
            implied_vol,
            delta,
            opt_price,
            pv_dividend,
            gamma,
            vega,
            theta,
            und_price,
        }
    }

    #[test]
    fn option_computation_arithmetic_is_ib_asyncs() {
        // Each expectation is ib_async 2.1.0's result for the same operands.
        let s = Some;
        let a = oc(
            1,
            [
                s(0.25),
                s(0.5),
                None,
                s(1.5),
                s(0.125),
                None,
                s(-0.0625),
                s(100.0),
            ],
        );
        let b = oc(
            2,
            [
                s(0.125),
                None,
                s(3.0),
                s(2.0),
                s(0.25),
                s(0.5),
                s(0.25),
                s(90.0),
            ],
        );
        let sum = oc(
            0,
            [
                s(0.375),
                s(0.5),
                s(3.0),
                None,
                s(0.375),
                s(0.5),
                s(0.1875),
                s(100.0),
            ],
        );
        let diff = oc(
            0,
            [
                s(0.125),
                s(0.5),
                s(-3.0),
                None,
                s(-0.125),
                s(-0.5),
                s(-0.3125),
                s(100.0),
            ],
        );
        let twice = oc(
            0,
            [
                s(0.5),
                s(1.0),
                s(0.0),
                None,
                s(0.25),
                s(0.0),
                s(-0.125),
                s(100.0),
            ],
        );
        assert_eq!(a + b, sum);
        assert_eq!(a - b, diff);
        assert_eq!(a * 2.0, twice);
        let empty = oc(3, [None; 8]);
        let zero = oc(
            0,
            [s(0.0), s(0.0), s(0.0), None, s(0.0), s(0.0), s(0.0), None],
        );
        assert_eq!(empty * 0.5, zero);
        // NaN is a stated figure, not `None`.
        let nan = OptionComputation {
            implied_vol: s(f64::NAN),
            ..empty
        };
        assert!((nan + empty).implied_vol.is_some_and(f64::is_nan));
    }

    #[test]
    fn defaults_are_ib_asyncs() {
        let s = ScannerSubscription::default();
        assert_eq!(s.number_of_rows, -1);
        assert_eq!(
            (s.above_price, s.above_volume, s.average_option_volume_above),
            (None, None, None)
        );
        assert_eq!(s.stock_type_filter, "");

        let e = Execution::default();
        assert_eq!(e.time, *EPOCH);
        assert_eq!(e.time.time_zone(), &TimeZone::UTC);
        assert_eq!((e.shares, e.perm_id, e.liquidation), (0.0, 0, 0));
        assert!(!e.pending_price_revision);

        assert_eq!(BarData::default().date, BarDate::At(EPOCH.clone()));
        assert_eq!(BarData::default().bar_count, 0);
        let r = RealTimeBar::default();
        assert_eq!((r.time, r.end_time, r.count), (EPOCH.clone(), -1, 0));

        let h = HistogramData::default();
        assert_eq!((h.price, h.count), (0.0, 0));
        assert_eq!(DepthMktDataDescription::default().agg_group, None);
        let w = WshEventData::default();
        assert_eq!((w.con_id, w.total_limit), (None, None));
        assert_eq!(CommissionReport::default().yield_redemption_date, 0);
        assert_eq!(SoftDollarTier::default().display_name, "");
        assert_eq!(ExecutionFilter::default().client_id, 0);
        assert!(!TickAttrib::default().can_auto_execute);
        assert!(!TickAttribBidAsk::default().bid_past_low);
        assert!(!TickAttribLast::default().unreported);
        assert_eq!(NewsProvider::default().code, "");
        assert_eq!(HistoricalSession::default().ref_date, "");
        assert!(HistoricalSchedule::default().sessions.is_empty());

        let p = PnL::default();
        assert!(p.daily_pnl.is_nan() && p.unrealized_pnl.is_nan() && p.realized_pnl.is_nan());
        let p = PnLSingle::default();
        assert!(p.daily_pnl.is_nan() && p.unrealized_pnl.is_nan() && p.realized_pnl.is_nan());
        assert!(p.value.is_nan());
        assert_eq!((p.con_id, p.position), (0, 0.0));

        let d = IBDefaults::default();
        assert_eq!((d.empty_price, d.empty_size), (-1.0, 0.0));
        assert!(d.unset.is_nan());
        assert_eq!(d.timezone, TimeZone::UTC);

        let b = BarDataList::default();
        assert!(matches!(b.end_date_time, DateTimeArg::None));
        assert!(b.bars.is_empty() && b.chart_options.is_empty());
        assert_eq!(ScanDataList::default().subscription.number_of_rows, -1);
        assert_eq!(RealTimeBarList::default().bar_size, 0);

        let x = TickerExtras::default();
        assert!(x.shares_outstanding.is_nan() && x.open_a_year_ago.is_nan());
        assert!(!x.short_sale_restricted && x.stated_rows.is_empty());
        assert_eq!(OptionModel::default().price_based_vol, None);
    }

    #[test]
    fn live_values_have_their_events_and_equality() {
        let a = Live::new(BarDataList::default());
        assert_eq!(a.update_event().name(), "updateEvent");
        assert_eq!(a, a.clone());
        assert_ne!(a, Live::new(BarDataList::default()));
        let r = Live::new(RealTimeBarList::default());
        assert_eq!(r.update_event().name(), "updateEvent");
        assert_ne!(r, Live::new(RealTimeBarList::default()));
        let s = Live::new(ScanDataList::default());
        assert_eq!(s.update_event().name(), "updateEvent");
        assert_ne!(s, Live::new(ScanDataList::default()));

        let c = Live::new(CommissionReport::default());
        let d = Live::new(CommissionReport::default());
        assert_eq!(c, d);
        d.update(|v| v.commission = 1.0);
        assert_ne!(c, d);
        let pnl = PnLSingle {
            value: 1.0,
            daily_pnl: 1.0,
            unrealized_pnl: 1.0,
            realized_pnl: 1.0,
            ..PnLSingle::default()
        };
        assert_eq!(Live::new(pnl.clone()), Live::new(pnl));
        let pnl = PnL {
            daily_pnl: 1.0,
            unrealized_pnl: 2.0,
            realized_pnl: 3.0,
            ..PnL::default()
        };
        assert_eq!(Live::new(pnl.clone()), Live::new(pnl));
    }

    #[test]
    fn a_fill_compares_its_reports_current_value() {
        let report = Live::new(CommissionReport::default());
        let fill = Fill {
            contract: Contract::default(),
            execution: Execution::default(),
            commission_report: report.clone(),
            time: EPOCH.clone(),
        };
        let other = Fill {
            commission_report: Live::new(CommissionReport::default()),
            ..fill.clone()
        };
        assert_eq!(fill, other);
        report.update(|r| r.commission = 1.25);
        assert_eq!(fill.commission_report.read().commission, 1.25);
        assert_ne!(fill, other);
    }
}

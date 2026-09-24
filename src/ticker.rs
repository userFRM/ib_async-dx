//! ib_async's `ticker` module: `Ticker` and its bar helpers.
//!
//! The helpers are eventkit operators in ib_async. Here each is a stage that
//! runs inline with its source's emission. The source's slot holds the
//! stage, so a chain lives as long as the event it hangs on, with no handle
//! held; when the source is done, each stage lets go and is set done in turn.

use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::{Arc, Mutex, OnceLock};

use jiff::Zoned;

use crate::contract::Contract;
use crate::event::{Event, HandlerError, lock, on_owner};
use crate::live::sealed::{Maker, Storage};
use crate::live::{Live, Observed};
use crate::objects::{
    DOMLevel, Dividends, EfpData, FundamentalRatios, IBDefaults, MktDepthData, OptionComputation,
    TickByTickAllLast, TickByTickBidAsk, TickByTickMidPoint, TickData,
};

/// Current market data for a contract, updated in place and held as
/// [`Live`]: ib_async's `Ticker`.
///
/// Level-1 ticks of a pass are in `ticks`, tick-by-tick records in
/// `tick_by_ticks`, and depth changes in `dom_ticks`; the book is in
/// `dom_bids` and `dom_asks`. A figure not yet stated is `defaults.unset`.
/// `Live<Ticker>` handles are equal only when they are the same ticker, as
/// ib_async's `Ticker.__eq__` is `is`.
#[derive(Clone, Debug)]
pub struct Ticker {
    /// The contract the data is for: `contract`.
    pub contract: Option<Contract>,
    /// When the last pass that changed the ticker arrived: `time`.
    pub time: Option<Zoned>,
    /// That arrival as seconds since the epoch: `timestamp`.
    pub timestamp: Option<f64>,
    /// Live 1, frozen 2, delayed 3 or delayed-frozen 4: `marketDataType`.
    pub market_data_type: i32,
    /// `minTick`.
    pub min_tick: f64,
    /// `bid`.
    pub bid: f64,
    /// `bidSize`.
    pub bid_size: f64,
    /// `bidExchange`.
    pub bid_exchange: String,
    /// `ask`.
    pub ask: f64,
    /// `askSize`.
    pub ask_size: f64,
    /// `askExchange`.
    pub ask_exchange: String,
    /// `last`.
    pub last: f64,
    /// `lastSize`.
    pub last_size: f64,
    /// `lastExchange`.
    pub last_exchange: String,
    /// `lastTimestamp`.
    pub last_timestamp: Option<Zoned>,
    /// `prevBid`.
    pub prev_bid: f64,
    /// `prevBidSize`.
    pub prev_bid_size: f64,
    /// `prevAsk`.
    pub prev_ask: f64,
    /// `prevAskSize`.
    pub prev_ask_size: f64,
    /// `prevLast`.
    pub prev_last: f64,
    /// `prevLastSize`.
    pub prev_last_size: f64,
    /// `volume`.
    pub volume: f64,
    /// `open`.
    pub open: f64,
    /// `high`.
    pub high: f64,
    /// `low`.
    pub low: f64,
    /// `close`.
    pub close: f64,
    /// `vwap`.
    pub vwap: f64,
    /// `low13week`.
    pub low_13_week: f64,
    /// `high13week`.
    pub high_13_week: f64,
    /// `low26week`.
    pub low_26_week: f64,
    /// `high26week`.
    pub high_26_week: f64,
    /// `low52week`.
    pub low_52_week: f64,
    /// `high52week`.
    pub high_52_week: f64,
    /// `bidYield`.
    pub bid_yield: f64,
    /// `askYield`.
    pub ask_yield: f64,
    /// `lastYield`.
    pub last_yield: f64,
    /// `markPrice`.
    pub mark_price: f64,
    /// `halted`.
    pub halted: f64,
    /// `rtHistVolatility`.
    pub rt_hist_volatility: f64,
    /// `rtVolume`.
    pub rt_volume: f64,
    /// `rtTradeVolume`.
    pub rt_trade_volume: f64,
    /// `rtTime`.
    pub rt_time: Option<Zoned>,
    /// `avVolume`.
    pub av_volume: f64,
    /// `tradeCount`.
    pub trade_count: f64,
    /// `tradeRate`.
    pub trade_rate: f64,
    /// `volumeRate`.
    pub volume_rate: f64,
    /// `volumeRate3Min`.
    pub volume_rate_3_min: f64,
    /// `volumeRate5Min`.
    pub volume_rate_5_min: f64,
    /// `volumeRate10Min`.
    pub volume_rate_10_min: f64,
    /// `shortable`.
    pub shortable: f64,
    /// `shortableShares`.
    pub shortable_shares: f64,
    /// `indexFuturePremium`.
    pub index_future_premium: f64,
    /// `futuresOpenInterest`.
    pub futures_open_interest: f64,
    /// `putOpenInterest`.
    pub put_open_interest: f64,
    /// `callOpenInterest`.
    pub call_open_interest: f64,
    /// `putVolume`.
    pub put_volume: f64,
    /// `callVolume`.
    pub call_volume: f64,
    /// `avOptionVolume`.
    pub av_option_volume: f64,
    /// `histVolatility`.
    pub hist_volatility: f64,
    /// `impliedVolatility`.
    pub implied_volatility: f64,
    /// `openInterest`.
    pub open_interest: f64,
    /// `lastRthTrade`.
    pub last_rth_trade: f64,
    /// `lastRegTime`.
    pub last_reg_time: String,
    /// `optionBidExch`.
    pub option_bid_exch: String,
    /// `optionAskExch`.
    pub option_ask_exch: String,
    /// `bondFactorMultiplier`.
    pub bond_factor_multiplier: f64,
    /// `creditmanMarkPrice`.
    pub creditman_mark_price: f64,
    /// `creditmanSlowMarkPrice`.
    pub creditman_slow_mark_price: f64,
    /// `delayedLastTimestamp`.
    pub delayed_last_timestamp: Option<Zoned>,
    /// `delayedHalted`.
    pub delayed_halted: f64,
    /// `reutersMutualFunds`.
    pub reuters_mutual_funds: String,
    /// `etfNavClose`.
    pub etf_nav_close: f64,
    /// `etfNavPriorClose`.
    pub etf_nav_prior_close: f64,
    /// `etfNavBid`.
    pub etf_nav_bid: f64,
    /// `etfNavAsk`.
    pub etf_nav_ask: f64,
    /// `etfNavLast`.
    pub etf_nav_last: f64,
    /// `etfFrozenNavLast`.
    pub etf_frozen_nav_last: f64,
    /// `etfNavHigh`.
    pub etf_nav_high: f64,
    /// `etfNavLow`.
    pub etf_nav_low: f64,
    /// `socialMarketAnalytics`.
    pub social_market_analytics: String,
    /// `estimatedIpoMidpoint`.
    pub estimated_ipo_midpoint: f64,
    /// `finalIpoLast`.
    pub final_ipo_last: f64,
    /// `dividends`.
    pub dividends: Option<Dividends>,
    /// `fundamentalRatios`.
    pub fundamental_ratios: Option<FundamentalRatios>,
    /// The price, size and RT-volume ticks of the last pass: `ticks`.
    pub ticks: Vec<TickData>,
    /// The tick-by-tick records of the last pass: `tickByTicks`.
    pub tick_by_ticks: Vec<TickByTick>,
    /// The bid side of the book, by position: `domBids`.
    pub dom_bids: Vec<DOMLevel>,
    /// The bid side of the book, keyed by position: `domBidsDict`.
    pub dom_bids_dict: BTreeMap<i32, DOMLevel>,
    /// The ask side of the book, by position: `domAsks`.
    pub dom_asks: Vec<DOMLevel>,
    /// The ask side of the book, keyed by position: `domAsksDict`.
    pub dom_asks_dict: BTreeMap<i32, DOMLevel>,
    /// The depth changes of the last pass: `domTicks`.
    pub dom_ticks: Vec<MktDepthData>,
    /// `bidGreeks`.
    pub bid_greeks: Option<OptionComputation>,
    /// `askGreeks`.
    pub ask_greeks: Option<OptionComputation>,
    /// `lastGreeks`.
    pub last_greeks: Option<OptionComputation>,
    /// `modelGreeks`.
    pub model_greeks: Option<OptionComputation>,
    /// `custGreeks`.
    pub cust_greeks: Option<OptionComputation>,
    /// `bidEfp`.
    pub bid_efp: Option<EfpData>,
    /// `askEfp`.
    pub ask_efp: Option<EfpData>,
    /// `lastEfp`.
    pub last_efp: Option<EfpData>,
    /// `openEfp`.
    pub open_efp: Option<EfpData>,
    /// `highEfp`.
    pub high_efp: Option<EfpData>,
    /// `lowEfp`.
    pub low_efp: Option<EfpData>,
    /// `closeEfp`.
    pub close_efp: Option<EfpData>,
    /// `auctionVolume`.
    pub auction_volume: f64,
    /// `auctionPrice`.
    pub auction_price: f64,
    /// `auctionImbalance`.
    pub auction_imbalance: f64,
    /// `regulatoryImbalance`.
    pub regulatory_imbalance: f64,
    /// `bboExchange`.
    pub bbo_exchange: String,
    /// `snapshotPermissions`.
    pub snapshot_permissions: i32,
    /// The values a figure takes when none is stated: `defaults`.
    pub defaults: IBDefaults,
}

impl Ticker {
    /// The names of a ticker's events: ib_async's `Ticker.events`.
    pub const EVENTS: [&'static str; 1] = ["updateEvent"];

    /// A ticker for `contract` whose figures start as `defaults.unset`:
    /// ib_async's `Ticker(contract=..., defaults=...)`, which
    /// `__post_init__` seeds.
    pub fn new(contract: Option<Contract>, defaults: IBDefaults) -> Ticker {
        let unset = defaults.unset;
        Ticker {
            contract,
            time: None,
            timestamp: None,
            market_data_type: 1,
            min_tick: unset,
            bid: unset,
            bid_size: unset,
            bid_exchange: String::new(),
            ask: unset,
            ask_size: unset,
            ask_exchange: String::new(),
            last: unset,
            last_size: unset,
            last_exchange: String::new(),
            last_timestamp: None,
            prev_bid: unset,
            prev_bid_size: unset,
            prev_ask: unset,
            prev_ask_size: unset,
            prev_last: unset,
            prev_last_size: unset,
            volume: unset,
            open: unset,
            high: unset,
            low: unset,
            close: unset,
            vwap: unset,
            low_13_week: unset,
            high_13_week: unset,
            low_26_week: unset,
            high_26_week: unset,
            low_52_week: unset,
            high_52_week: unset,
            bid_yield: unset,
            ask_yield: unset,
            last_yield: unset,
            mark_price: unset,
            halted: unset,
            rt_hist_volatility: unset,
            rt_volume: unset,
            rt_trade_volume: unset,
            rt_time: None,
            av_volume: unset,
            trade_count: unset,
            trade_rate: unset,
            volume_rate: unset,
            volume_rate_3_min: unset,
            volume_rate_5_min: unset,
            volume_rate_10_min: unset,
            shortable: unset,
            shortable_shares: unset,
            index_future_premium: unset,
            futures_open_interest: unset,
            put_open_interest: unset,
            call_open_interest: unset,
            put_volume: unset,
            call_volume: unset,
            av_option_volume: unset,
            hist_volatility: unset,
            implied_volatility: unset,
            open_interest: unset,
            last_rth_trade: unset,
            last_reg_time: String::new(),
            option_bid_exch: String::new(),
            option_ask_exch: String::new(),
            bond_factor_multiplier: unset,
            creditman_mark_price: unset,
            creditman_slow_mark_price: unset,
            delayed_last_timestamp: None,
            delayed_halted: unset,
            reuters_mutual_funds: String::new(),
            etf_nav_close: unset,
            etf_nav_prior_close: unset,
            etf_nav_bid: unset,
            etf_nav_ask: unset,
            etf_nav_last: unset,
            etf_frozen_nav_last: unset,
            etf_nav_high: unset,
            etf_nav_low: unset,
            social_market_analytics: String::new(),
            estimated_ipo_midpoint: unset,
            final_ipo_last: unset,
            dividends: None,
            fundamental_ratios: None,
            ticks: Vec::new(),
            tick_by_ticks: Vec::new(),
            dom_bids: Vec::new(),
            dom_bids_dict: BTreeMap::new(),
            dom_asks: Vec::new(),
            dom_asks_dict: BTreeMap::new(),
            dom_ticks: Vec::new(),
            bid_greeks: None,
            ask_greeks: None,
            last_greeks: None,
            model_greeks: None,
            cust_greeks: None,
            bid_efp: None,
            ask_efp: None,
            last_efp: None,
            open_efp: None,
            high_efp: None,
            low_efp: None,
            close_efp: None,
            auction_volume: unset,
            auction_price: unset,
            auction_imbalance: unset,
            regulatory_imbalance: unset,
            bbo_exchange: String::new(),
            snapshot_permissions: 0,
            defaults,
        }
    }

    /// Whether `value` is the unset figure: equal to `defaults.unset`, or
    /// NaN when that is NaN. ib_async's `isUnset`.
    pub fn is_unset(&self, value: f64) -> bool {
        let dev = self.defaults.unset;
        (dev.is_nan() && value.is_nan()) || value == dev
    }

    /// Whether the ticker has a valid bid and ask: both stated and not -1,
    /// with sizes above 0. ib_async's `hasBidAsk`.
    pub fn has_bid_ask(&self) -> bool {
        self.bid != -1.0
            && !self.is_unset(self.bid)
            && self.bid_size > 0.0
            && self.ask != -1.0
            && !self.is_unset(self.ask)
            && self.ask_size > 0.0
    }

    /// The average of bid and ask, or `defaults.unset` without a valid bid
    /// and ask. ib_async's `midpoint`.
    pub fn midpoint(&self) -> f64 {
        if self.has_bid_ask() {
            (self.bid + self.ask) * 0.5
        } else {
            self.defaults.unset
        }
    }

    /// The last price when it lies within the bid and ask or there is no
    /// valid bid and ask, else the midpoint. ib_async's `marketPrice`.
    pub fn market_price(&self) -> f64 {
        if self.has_bid_ask() {
            if self.bid <= self.last && self.last <= self.ask {
                self.last
            } else {
                self.midpoint()
            }
        } else {
            self.last
        }
    }
}

/// A ticker for no contract, with the default `IBDefaults`: ib_async's
/// `Ticker()`.
impl Default for Ticker {
    fn default() -> Self {
        Ticker::new(None, IBDefaults::default())
    }
}

impl Storage for Ticker {
    type Events = Event<Live<Ticker>>;
    fn events(maker: &Maker) -> Self::Events {
        maker.event("updateEvent")
    }
}
impl Observed for Ticker {}

impl Live<Ticker> {
    /// Emits the ticker at the end of each pass that changed it: ib_async's
    /// `Ticker.updateEvent`. `trades()`, `bids()`, `asks()`, `bidasks()` and
    /// `midpoints()` build bar helpers on it.
    pub fn update_event(&self) -> &Event<Live<Ticker>> {
        self.events()
    }
}

/// The same ticker: ib_async's `Ticker.__eq__` is `is`.
impl PartialEq for Live<Ticker> {
    fn eq(&self, other: &Self) -> bool {
        Live::ptr_eq(self, other)
    }
}

impl Eq for Live<Ticker> {}

/// By identity, as ib_async's `Ticker.__hash__` is `id`.
impl Hash for Live<Ticker> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.addr().hash(state);
    }
}

/// One tick-by-tick record, the element of [`Ticker::tick_by_ticks`]:
/// ib_async's `TickByTickAllLast | TickByTickBidAsk | TickByTickMidPoint`.
#[derive(Clone, Debug, PartialEq)]
pub enum TickByTick {
    /// A trade: `TickByTickAllLast`.
    AllLast(TickByTickAllLast),
    /// A bid and ask: `TickByTickBidAsk`.
    BidAsk(TickByTickBidAsk),
    /// A midpoint: `TickByTickMidPoint`.
    MidPoint(TickByTickMidPoint),
}

/// A record appended to a ticker's `ticks`, `tick_by_ticks` or `dom_ticks`,
/// as the IB's `tick_event` carries it.
#[derive(Clone, Debug, PartialEq)]
pub enum Tick {
    /// Appended to `ticks`: a `TickData`.
    Data(TickData),
    /// Appended to `tick_by_ticks`: a `TickByTickAllLast`.
    AllLast(TickByTickAllLast),
    /// Appended to `tick_by_ticks`: a `TickByTickBidAsk`.
    BidAsk(TickByTickBidAsk),
    /// Appended to `tick_by_ticks`: a `TickByTickMidPoint`.
    MidPoint(TickByTickMidPoint),
    /// Appended to `dom_ticks`: an `MktDepthData`.
    Depth(MktDepthData),
}

/// A bar the ticker helpers aggregate from ticks: ib_async's `ticker.Bar`.
#[derive(Clone, Debug, PartialEq)]
pub struct Bar {
    /// When the bar starts: `time`.
    pub time: Option<Zoned>,
    /// The first price, NaN before any tick: `open`.
    pub open: f64,
    /// The highest price, NaN before any tick: `high`.
    pub high: f64,
    /// The lowest price, NaN before any tick: `low`.
    pub low: f64,
    /// The last price, NaN before any tick: `close`.
    pub close: f64,
    /// The sum of the ticks' sizes: `volume`.
    pub volume: f64,
    /// The number of ticks: `count`.
    pub count: i64,
}

impl Bar {
    /// A bar starting at `time` with no tick: `Bar(time)`.
    fn empty(time: Zoned) -> Bar {
        Bar {
            time: Some(time),
            open: f64::NAN,
            high: f64::NAN,
            low: f64::NAN,
            close: f64::NAN,
            volume: 0.0,
            count: 0,
        }
    }

    /// A bar opened by one tick: `Bar(time, price, price, price, price,
    /// size, 1)`.
    fn first(time: Zoned, price: f64, size: f64) -> Bar {
        Bar {
            time: Some(time),
            open: price,
            high: price,
            low: price,
            close: price,
            volume: size,
            count: 1,
        }
    }

    /// Adds a tick to the bar, as the helpers' `on_source` does, with
    /// Python's `max` and `min`, which keep a NaN on the left.
    fn add(&mut self, price: f64, size: f64) {
        if price > self.high {
            self.high = price;
        }
        if price < self.low {
            self.low = price;
        }
        self.close = price;
        self.volume += size;
        self.count += 1;
    }
}

/// The bars a helper has made, updated in place and held as [`Live`]:
/// ib_async's `BarList`. `Live<BarList>` handles are equal only when they
/// are the same list, as `BarList.__eq__` is `is`.
#[derive(Clone, Debug, Default)]
pub struct BarList {
    /// The bars: the list itself in ib_async.
    pub bars: Vec<Bar>,
}

impl Storage for BarList {
    type Events = Event<(Live<BarList>, bool)>;
    fn events(maker: &Maker) -> Self::Events {
        maker.event("updateEvent")
    }
}
impl Observed for BarList {}

impl Live<BarList> {
    /// Emits the list and whether a bar was completed: ib_async's
    /// `BarList.updateEvent`.
    pub fn update_event(&self) -> &Event<(Live<BarList>, bool)> {
        self.events()
    }
}

/// The same list: ib_async's `BarList.__eq__` is `is`.
impl PartialEq for Live<BarList> {
    fn eq(&self, other: &Self) -> bool {
        Live::ptr_eq(self, other)
    }
}

/// ib_async's `TickerUpdateEvent`: a ticker's `update_event` with its tick
/// filters.
impl Event<Live<Ticker>> {
    /// `(time, price, size)` of each trade tick of a pass, types 4, 5, 48,
    /// 68 and 71: ib_async's `trades`.
    pub fn trades(&self) -> TickFilter {
        self.filter(&[4, 5, 48, 68, 71])
    }

    /// `(time, price, size)` of each bid tick of a pass, types 0, 1, 66 and
    /// 69: ib_async's `bids`.
    pub fn bids(&self) -> TickFilter {
        self.filter(&[0, 1, 66, 69])
    }

    /// `(time, price, size)` of each ask tick of a pass, types 2, 3, 67 and
    /// 70: ib_async's `asks`.
    pub fn asks(&self) -> TickFilter {
        self.filter(&[2, 3, 67, 70])
    }

    /// `(time, price, size)` of each bid and ask tick of a pass, in the
    /// ticks' own order: ib_async's `bidasks`.
    pub fn bidasks(&self) -> TickFilter {
        self.filter(&[0, 1, 66, 69, 2, 3, 67, 70])
    }

    /// `(ticker.time, ticker.midpoint(), 0.0)` once per pass that appended
    /// ticks: ib_async's `midpoints`. A ticker with no `time` gives nothing,
    /// where ib_async emits `(None, midpoint, 0)`: the payload's time is a
    /// `Zoned`, which cannot be `None`. A pass always sets `time`, so only a
    /// program emitting the ticker's `update_event` itself meets this.
    pub fn midpoints(&self) -> TickFilter {
        let out = Event::follows("Midpoints", self);
        let o = out.clone();
        attach(self, &out, move |ticker: &Live<Ticker>| {
            let t = ticker.read();
            if !t.ticks.is_empty()
                && let Some(time) = &t.time
            {
                o.emit(&(time.clone(), t.midpoint(), 0.0));
            }
        });
        TickFilter { out }
    }

    fn filter(&self, types: &'static [i32]) -> TickFilter {
        let out = Event::follows("Tickfilter", self);
        let o = out.clone();
        attach(self, &out, move |ticker: &Live<Ticker>| {
            for t in ticker.read().ticks.iter() {
                if types.contains(&t.tick_type) {
                    o.emit(&(t.time.clone(), t.price, t.size));
                }
            }
        });
        TickFilter { out }
    }
}

/// `(time, price, size)` for each matching tick of a pass: ib_async's
/// `Tickfilter`, and `Midpoints`. It is the stage's output event, and
/// aggregates into bars with `timebars`, `tickbars` and `volumebars`.
#[derive(Clone, Debug)]
pub struct TickFilter {
    out: Event<(Zoned, f64, f64)>,
}

impl Deref for TickFilter {
    type Target = Event<(Zoned, f64, f64)>;
    fn deref(&self) -> &Self::Target {
        &self.out
    }
}

impl TickFilter {
    /// Bars timed by `timer`: each emission of `timer` completes the bar in
    /// progress and emits it, then starts a bar at the emitted time. A bar
    /// with no tick takes the previous bar's close. Done when `timer` is
    /// done. ib_async's `Tickfilter.timebars`.
    pub fn timebars(&self, timer: &Event<Zoned>) -> TimeBars {
        let stage = Arc::new(TimeStage {
            out: Event::follows("TimeBars", &self.out),
            bars: bars_of(&self.out),
            timer: Mutex::new(Some(timer.clone())),
        });
        let s = stage.clone();
        attach(
            &self.out,
            &stage.out,
            move |&(_, price, size): &(Zoned, f64, f64)| s.on_tick(price, size),
        );
        // The timer's slots hold the stage weakly, as eventkit connects
        // `_on_timer` without `keep_ref`.
        let w = Arc::downgrade(&stage);
        timer.connect(move |time| {
            if let Some(s) = w.upgrade() {
                s.on_timer(time);
            }
        });
        if let Some(done) = timer.done_event() {
            let w = Arc::downgrade(&stage);
            done.connect(move |_| {
                if let Some(s) = w.upgrade() {
                    s.on_timer_done();
                }
            });
        }
        TimeBars {
            bars: stage.bars.clone(),
            stage,
        }
    }

    /// Bars of `count` ticks each, emitting the list as each bar fills:
    /// ib_async's `Tickfilter.tickbars`.
    pub fn tickbars(&self, count: i64) -> TickBars {
        let (out, bars) = self.counted("TickBars", move |bar| bar.count == count);
        TickBars { bars, out }
    }

    /// Bars of at least `volume` each, emitting the list as each bar fills:
    /// ib_async's `Tickfilter.volumebars`.
    pub fn volumebars(&self, volume: f64) -> VolumeBars {
        let (out, bars) = self.counted("VolumeBars", move |bar| bar.volume >= volume);
        VolumeBars { bars, out }
    }

    /// A stage that starts a bar when there is none or the last is full,
    /// else adds the tick to the last, and emits the list when that bar is
    /// full: `TickBars.on_source` and `VolumeBars.on_source`.
    fn counted(
        &self,
        name: &'static str,
        full: impl Fn(&Bar) -> bool + Copy + Send + Sync + 'static,
    ) -> (Event<Live<BarList>>, Live<BarList>) {
        let (out, bars) = (Event::follows(name, &self.out), bars_of(&self.out));
        let (o, b) = (out.clone(), bars.clone());
        attach(&self.out, &out, move |(time, price, size)| {
            let (o, b2, time, price, size) = (o.clone(), b.clone(), time.clone(), *price, *size);
            on_bars(&b, move || {
                let filled = b2.update(|l| {
                    match l.bars.last_mut() {
                        Some(bar) if !full(bar) => bar.add(price, size),
                        _ => l.bars.push(Bar::first(time, price, size)),
                    }
                    l.bars.last().is_some_and(full)
                });
                if filled {
                    b2.update_event().emit(&(b2.clone(), true));
                    o.emit(&b2);
                }
            });
        });
        (out, bars)
    }
}

/// Bars timed by a timer event: ib_async's `TimeBars`. It emits each
/// completed [`Bar`].
#[derive(Clone, Debug)]
pub struct TimeBars {
    /// The bars made so far: ib_async's `TimeBars.bars`.
    pub bars: Live<BarList>,
    stage: Arc<TimeStage>,
}

impl Deref for TimeBars {
    type Target = Event<Bar>;
    fn deref(&self) -> &Self::Target {
        &self.stage.out
    }
}

/// Bars of a fixed number of ticks: ib_async's `TickBars`. It emits `bars`
/// each time a bar fills.
#[derive(Clone, Debug)]
pub struct TickBars {
    /// The bars made so far: ib_async's `TickBars.bars`.
    pub bars: Live<BarList>,
    out: Event<Live<BarList>>,
}

impl Deref for TickBars {
    type Target = Event<Live<BarList>>;
    fn deref(&self) -> &Self::Target {
        &self.out
    }
}

/// Bars of a minimum volume: ib_async's `VolumeBars`. It emits `bars` each
/// time a bar fills.
#[derive(Clone, Debug)]
pub struct VolumeBars {
    /// The bars made so far: ib_async's `VolumeBars.bars`.
    pub bars: Live<BarList>,
    out: Event<Live<BarList>>,
}

impl Deref for VolumeBars {
    type Target = Event<Live<BarList>>;
    fn deref(&self) -> &Self::Target {
        &self.out
    }
}

/// `TimeBars`'s state: its output event, its bars, and its timer until the
/// timer is done.
#[derive(Debug)]
struct TimeStage {
    out: Event<Bar>,
    bars: Live<BarList>,
    timer: Mutex<Option<Event<Zoned>>>,
}

impl TimeStage {
    /// A tick: `TimeBars.on_source`, which adds it to the bar in progress.
    fn on_tick(self: &Arc<Self>, price: f64, size: f64) {
        let s = self.clone();
        on_bars(&self.bars, move || {
            let added = s.bars.update(|l| match l.bars.last_mut() {
                Some(bar) => {
                    if bar.open.is_nan() {
                        (bar.open, bar.high, bar.low) = (price, price, price);
                    }
                    bar.add(price, size);
                    true
                }
                None => false,
            });
            if added {
                s.bars.update_event().emit(&(s.bars.clone(), false));
            }
        });
    }

    /// The timer: `TimeBars._on_timer`, which completes the bar in progress
    /// and starts one at `time`.
    fn on_timer(self: &Arc<Self>, time: &Zoned) {
        let (s, time) = (self.clone(), time.clone());
        on_bars(&self.bars, move || {
            let done = s.bars.update(|l| {
                if let [.., prev, last] = l.bars.as_mut_slice()
                    && last.close.is_nan()
                {
                    let close = prev.close;
                    (last.open, last.high, last.low, last.close) = (close, close, close, close);
                }
                l.bars.last().cloned()
            });
            if let Some(bar) = done {
                s.bars.update_event().emit(&(s.bars.clone(), true));
                s.out.emit(&bar);
            }
            s.bars.update(|l| l.bars.push(Bar::empty(time)));
        });
    }

    /// The timer is done: `TimeBars._on_timer_done`, which lets it go and
    /// sets the stage done.
    fn on_timer_done(&self) {
        let timer = lock(&self.timer).take();
        drop(timer);
        self.out.set_done();
    }
}

/// A stage's bars, bound to the IBs that hold what `source` belongs to, so
/// that a step triggered on another thread, as by a program's timer driving
/// `timebars`, runs on the owner, and an `edit` of the bars too.
// Bound to the holders the source has when the stage is made: a ticker an IB
// takes later leaves the bars the program's. A chain on an IB's ticker is made
// after the IB holds it.
fn bars_of<S>(source: &Event<S>) -> Live<BarList> {
    let bars = Live::new(BarList::default());
    for holder in source.holders() {
        bars.bind(holder);
    }
    bars
}

/// Connects a stage to `source` as eventkit's `Op` does: `step` runs on
/// each value (`on_source`), the source's handler errors are passed on
/// (`on_source_error`), and when the source is done the stage disconnects
/// its three slots from it and is set done (`on_source_done`). On a done
/// source the stage is set done at once.
///
/// The source's slots hold the stage, so the stage lives as long as the
/// source, or until the source is done; the stage holds the source only
/// weakly, for the disconnect.
fn attach<S, O>(source: &Event<S>, out: &Event<O>, step: impl Fn(&S) + Send + Sync + 'static)
where
    S: Clone + Send + Sync + 'static,
    O: Clone + Send + Sync + 'static,
{
    if source.done() {
        out.set_done();
        return;
    }
    let on_value = source.connect(step);
    let on_error = source.error_event().map(|errors| {
        let out = out.clone();
        errors.connect(move |e| forward(&out, e))
    });
    let Some(done) = source.done_event() else {
        return;
    };
    let (weak, out, own) = (source.downgrade(), out.clone(), Arc::new(OnceLock::new()));
    let this = own.clone();
    let id = done.connect(move |_| {
        if let Some(source) = weak.upgrade() {
            source.disconnect(on_value);
            if let (Some(id), Some(errors)) = (on_error, source.error_event()) {
                errors.disconnect(id);
            }
            if let (Some(id), Some(done)) = (this.get(), source.done_event()) {
                done.disconnect(*id);
            }
        }
        out.set_done();
    });
    let _ = own.set(id);
}

/// Passes a source's handler error on, as eventkit's `Op.on_source_error`:
/// on the stage's `error_event` when that has listeners, else to the log.
fn forward<O: Clone + Send + Sync + 'static>(out: &Event<O>, error: &HandlerError) {
    match out.error_event() {
        Some(errors) if !errors.is_empty() => errors.emit(error),
        _ => log::error!(target: "eventkit.event", "{}", error.message),
    }
}

/// Runs a step that changes a helper's bars on the owner while an IB holds
/// the list, as an IB event's `emit` runs there, and here otherwise.
fn on_bars(bars: &Live<BarList>, step: impl FnOnce() + Send + 'static) {
    if on_owner() {
        step();
    } else if let Some(step) = bars.post(Box::new(step)) {
        step();
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::thread;

    use jiff::Timestamp;
    use jiff::tz::TimeZone;

    use super::*;
    use crate::event::RecvError;
    use crate::live::tests::FakeIb;
    use crate::tests::errors_here;

    fn at(s: i64) -> Zoned {
        Timestamp::from_second(s).unwrap().to_zoned(TimeZone::UTC)
    }

    fn tick(tick_type: i32, price: f64, size: f64) -> TickData {
        TickData {
            time: at(1),
            tick_type,
            price,
            size,
        }
    }

    /// One pass: the ticker's ticks replaced, then its event emitted.
    fn pass(ticker: &Live<Ticker>, ticks: Vec<TickData>) {
        ticker.update(|t| t.ticks = ticks);
        ticker.update_event().emit(ticker);
    }

    /// What a test handler has collected.
    type Seen<T> = Arc<Mutex<Vec<T>>>;

    /// Everything `ev` emits from now on.
    fn collect<T: Clone + Send + Sync + 'static>(ev: &Event<T>) -> Seen<T> {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let s = seen.clone();
        ev.connect(move |v| lock(&s).push(v.clone()));
        seen
    }

    fn bar(time: i64, ohlc: [f64; 4], volume: f64, count: i64) -> Bar {
        let [open, high, low, close] = ohlc;
        Bar {
            time: Some(at(time)),
            open,
            high,
            low,
            close,
            volume,
            count,
        }
    }

    #[test]
    fn new_seeds_every_figure_with_unset_and_nothing_else() {
        let defaults = IBDefaults {
            unset: 7.0,
            ..IBDefaults::default()
        };
        let c = Contract::stock("AAPL", "SMART", "USD");
        let t = Ticker::new(Some(c.clone()), defaults.clone());
        // The 71 figures `__post_init__` seeds, and `defaults.unset` itself.
        assert_eq!(format!("{t:?}").matches(": 7.0,").count(), 72);
        assert_eq!((t.min_tick, t.bid, t.final_ipo_last), (7.0, 7.0, 7.0));
        assert_eq!(t.contract, Some(c));
        assert_eq!((t.market_data_type, t.snapshot_permissions), (1, 0));
        assert!(t.time.is_none() && t.timestamp.is_none() && t.rt_time.is_none());
        assert!(t.bid_exchange.is_empty() && t.ticks.is_empty() && t.dom_bids_dict.is_empty());
        assert!(t.bid_greeks.is_none() && t.close_efp.is_none() && t.dividends.is_none());
        assert_eq!(t.defaults, defaults);

        let d = Ticker::default();
        assert!(d.contract.is_none() && d.bid.is_nan() && d.low_13_week.is_nan());
        assert!(d.defaults.unset.is_nan() && d.defaults.empty_price == -1.0);
    }

    #[test]
    fn is_unset_reads_nan_as_unset_only_when_unset_is_nan() {
        let t = Ticker::default();
        assert!(t.is_unset(f64::NAN));
        assert!(!t.is_unset(0.0));
        let zero = Ticker::new(
            None,
            IBDefaults {
                unset: 0.0,
                ..IBDefaults::default()
            },
        );
        assert!(zero.is_unset(0.0));
        assert!(!zero.is_unset(f64::NAN));
    }

    #[test]
    fn midpoint_and_market_price_follow_the_bid_and_ask() {
        let quote = |bid, bid_size, ask, ask_size, last| Ticker {
            bid,
            bid_size,
            ask,
            ask_size,
            last,
            ..Ticker::default()
        };
        let t = quote(1.0, 5.0, 3.0, 5.0, 2.5);
        assert!(t.has_bid_ask());
        assert_eq!(t.midpoint(), 2.0);
        assert_eq!(t.market_price(), 2.5);
        // A last outside the spread, or unstated, gives the midpoint.
        assert_eq!(quote(1.0, 5.0, 3.0, 5.0, 4.0).market_price(), 2.0);
        assert_eq!(quote(1.0, 5.0, 3.0, 5.0, f64::NAN).market_price(), 2.0);
        // No valid bid and ask: the midpoint is unset, the price the last.
        for t in [
            quote(-1.0, 5.0, 3.0, 5.0, 2.5),
            quote(1.0, 0.0, 3.0, 5.0, 2.5),
            quote(1.0, 5.0, f64::NAN, 5.0, 2.5),
            quote(1.0, 5.0, 3.0, f64::NAN, 2.5),
        ] {
            assert!(!t.has_bid_ask());
            assert!(t.midpoint().is_nan());
            assert_eq!(t.market_price(), 2.5);
        }
        // A non-NaN unset counts as no bid.
        let t = Ticker {
            bid: 0.0,
            bid_size: 5.0,
            ask: 3.0,
            ask_size: 5.0,
            ..Ticker::new(
                None,
                IBDefaults {
                    unset: 0.0,
                    ..IBDefaults::default()
                },
            )
        };
        assert!(!t.has_bid_ask());
        assert_eq!(t.midpoint(), 0.0);
    }

    #[test]
    fn live_tickers_and_bar_lists_compare_by_identity() {
        let (a, b) = (Live::new(Ticker::default()), Live::new(Ticker::default()));
        assert!(a == a.clone() && a != b);
        assert_eq!(HashSet::from([a.clone(), a.clone(), b]).len(), 2);
        assert_eq!(a.update_event().name(), Ticker::EVENTS[0]);
        let (x, y) = (Live::new(BarList::default()), Live::new(BarList::default()));
        assert!(x == x.clone() && x != y);
        assert_eq!(x.update_event().name(), "updateEvent");
    }

    #[test]
    fn filters_emit_matching_ticks_in_tick_order() {
        let ticker = Live::new(Ticker::default());
        let ev = ticker.update_event();
        let trades = collect(&ev.trades());
        let bidasks = collect(&ev.bidasks());
        let asks = collect(&ev.asks());
        pass(
            &ticker,
            vec![
                tick(2, 10.0, 1.0),
                tick(4, 11.0, 2.0),
                tick(1, 9.0, 3.0),
                tick(71, 12.0, 4.0),
                tick(66, 8.0, 5.0),
                tick(70, 13.0, 6.0),
                tick(8, 0.0, 7.0),
            ],
        );
        let prices = |seen: &Seen<(Zoned, f64, f64)>| {
            lock(seen).iter().map(|v| (v.1, v.2)).collect::<Vec<_>>()
        };
        assert_eq!(prices(&trades), [(11.0, 2.0), (12.0, 4.0)]);
        assert_eq!(
            prices(&bidasks),
            [(10.0, 1.0), (9.0, 3.0), (8.0, 5.0), (13.0, 6.0)]
        );
        assert_eq!(prices(&asks), [(10.0, 1.0), (13.0, 6.0)]);
        assert_eq!(lock(&trades)[0].0, at(1));
    }

    #[test]
    fn midpoints_emit_once_per_pass_with_ticks() {
        let ticker = Live::new(Ticker::default());
        let mids = collect(&ticker.update_event().midpoints());
        ticker.update(|t| (t.bid, t.bid_size, t.ask, t.ask_size) = (1.0, 1.0, 2.0, 1.0));
        // No time yet: nothing.
        pass(&ticker, vec![tick(1, 1.0, 1.0)]);
        ticker.update(|t| t.time = Some(at(5)));
        pass(&ticker, vec![tick(1, 1.0, 1.0), tick(3, 2.0, 1.0)]);
        pass(&ticker, Vec::new());
        assert_eq!(*lock(&mids), [(at(5), 1.5, 0.0)]);
    }

    #[test]
    fn tickbars_and_volumebars_emit_the_list_as_each_bar_fills() {
        let ticker = Live::new(Ticker::default());
        let trades = ticker.update_event().trades();
        let tb = trades.tickbars(2);
        let vb = trades.volumebars(3.0);
        let tick_lists = collect(&tb);
        let tick_updates = collect(tb.bars.update_event());
        let volume_lists = collect(&vb);
        pass(
            &ticker,
            [(1.0, 1.0), (3.0, 2.0), (2.0, 1.0), (5.0, 5.0), (4.0, 1.0)]
                .map(|(p, s)| tick(4, p, s))
                .to_vec(),
        );
        assert_eq!(
            tb.bars.read().bars,
            [
                bar(1, [1.0, 3.0, 1.0, 3.0], 3.0, 2),
                bar(1, [2.0, 5.0, 2.0, 5.0], 6.0, 2),
                bar(1, [4.0, 4.0, 4.0, 4.0], 1.0, 1),
            ]
        );
        assert_eq!(lock(&tick_lists).len(), 2);
        assert!(lock(&tick_lists).iter().all(|l| *l == tb.bars));
        assert!(
            lock(&tick_updates)
                .iter()
                .all(|(l, new)| *l == tb.bars && *new)
        );
        assert_eq!(lock(&tick_updates).len(), 2);
        assert_eq!(
            vb.bars.read().bars,
            [
                bar(1, [1.0, 3.0, 1.0, 3.0], 3.0, 2),
                bar(1, [2.0, 5.0, 2.0, 5.0], 6.0, 2),
                bar(1, [4.0, 4.0, 4.0, 4.0], 1.0, 1),
            ]
        );
        assert_eq!(lock(&volume_lists).len(), 2);
    }

    #[test]
    fn timebars_complete_a_bar_at_each_timer_emission() {
        let ticker = Live::new(Ticker::default());
        let timer = Event::<Zoned>::new("timer");
        let tb = ticker.update_event().trades().timebars(&timer);
        let done = collect(&tb);
        let updates = collect(tb.bars.update_event());
        // Before the first timer emission there is no bar to add to.
        pass(&ticker, vec![tick(4, 10.0, 1.0)]);
        assert!(tb.bars.read().bars.is_empty() && lock(&updates).is_empty());
        timer.emit(&at(0));
        assert!(lock(&done).is_empty());
        pass(
            &ticker,
            vec![
                tick(4, 10.0, 1.0),
                tick(4, 12.0, 2.0),
                tick(1, 99.0, 1.0),
                tick(5, 9.0, 3.0),
            ],
        );
        timer.emit(&at(60));
        // A bar with no tick takes the previous close.
        timer.emit(&at(120));
        assert_eq!(
            *lock(&done),
            [
                bar(0, [10.0, 12.0, 9.0, 9.0], 6.0, 3),
                bar(60, [9.0, 9.0, 9.0, 9.0], 0.0, 0),
            ]
        );
        assert_eq!(tb.bars.read().bars.len(), 3);
        let flags: Vec<bool> = lock(&updates).iter().map(|u| u.1).collect();
        assert_eq!(flags, [false, false, false, true, true]);

        assert!(!tb.done());
        timer.set_done();
        assert!(tb.done());
    }

    #[test]
    fn a_chain_lives_with_its_source_and_no_handle() {
        let ticker = Live::new(Ticker::default());
        let ev = ticker.update_event();
        let prices = Arc::new(Mutex::new(Vec::new()));
        let p = prices.clone();
        ev.trades().connect(move |v| lock(&p).push(v.1));
        let mut sub = ev.trades().tickbars(2).subscribe();

        fn setup(ev: &Event<Live<Ticker>>, closes: Arc<Mutex<Vec<f64>>>) {
            ev.bids().volumebars(1.0).connect(move |l| {
                let last = l.read().bars.last().map(|b| b.close);
                lock(&closes).extend(last);
            });
        }
        let closes = Arc::new(Mutex::new(Vec::new()));
        setup(ev, closes.clone());

        pass(
            &ticker,
            vec![tick(4, 1.0, 1.0), tick(1, 7.0, 2.0), tick(4, 2.0, 1.0)],
        );
        assert_eq!(*lock(&prices), [1.0, 2.0]);
        let list = sub.try_recv().unwrap();
        assert_eq!(list.read().bars, [bar(1, [1.0, 2.0, 1.0, 2.0], 2.0, 2)]);
        assert_eq!(*lock(&closes), [7.0]);
    }

    #[test]
    fn a_done_source_sets_each_stage_done_and_frees_what_nothing_holds() {
        let ticker = Live::new(Ticker::default());
        let ev = ticker.update_event();
        let freed = ev.trades().tickbars(2).bars.downgrade();
        let mut sub = ev.asks().volumebars(1.0).subscribe();
        let kept = ev.bids().tickbars(1);
        let dones = Arc::new(Mutex::new(0));
        let d = dones.clone();
        kept.done_event().unwrap().connect(move |_| *lock(&d) += 1);
        assert!(freed.upgrade().is_some());

        pass(&ticker, vec![tick(1, 3.0, 1.0)]);
        ev.set_done();
        assert!(freed.upgrade().is_none());
        assert_eq!(sub.try_recv(), Err(RecvError::Done));
        assert!(kept.done() && *lock(&dones) == 1);
        assert_eq!(kept.bars.read().bars.len(), 1);
        // Each stage left the source, as eventkit's `on_source_done`
        // disconnects.
        assert!(ev.is_empty());
        assert!(ev.error_event().unwrap().is_empty());
        assert!(ev.done_event().unwrap().is_empty());
        // A later emission of the source no longer reaches a stage.
        pass(&ticker, vec![tick(1, 4.0, 1.0)]);
        assert_eq!(kept.bars.read().bars.len(), 1);
    }

    #[test]
    fn timebars_is_held_by_its_source_not_its_timer() {
        let ticker = Live::new(Ticker::default());
        let timer = Event::<Zoned>::new("timer");
        let freed = ticker
            .update_event()
            .trades()
            .timebars(&timer)
            .bars
            .downgrade();
        timer.emit(&at(0));
        assert_eq!(freed.upgrade().map(|b| b.read().bars.len()), Some(1));
        ticker.update_event().set_done();
        assert!(freed.upgrade().is_none());
        timer.emit(&at(60));
    }

    #[test]
    fn a_stage_on_a_done_source_is_done_at_once() {
        let ev = Event::<Live<Ticker>>::new("updateEvent");
        ev.set_done();
        let f = ev.midpoints();
        assert!(f.done() && f.tickbars(1).done());
        assert!(ev.is_empty());
    }

    #[test]
    fn a_stage_passes_its_sources_handler_errors_on() {
        let before = errors_here();
        let ticker = Live::new(Ticker::default());
        let ev = ticker.update_event();
        let mut sub = ev.trades().subscribe();
        let bars = ev.trades().tickbars(1);
        let heard = collect(bars.error_event().unwrap());
        let quiet = ev.asks();
        ev.connect(|_| panic!("boom"));
        pass(&ticker, vec![tick(4, 1.0, 2.0)]);

        let boom = HandlerError {
            event: "updateEvent".into(),
            message: "boom".into(),
        };
        assert_eq!(sub.try_recv(), Ok((at(1), 1.0, 2.0)));
        assert_eq!(sub.try_recv(), Err(RecvError::Handler(boom.clone())));
        assert_eq!(sub.try_recv(), Err(RecvError::Done));
        assert_eq!(*lock(&heard), [boom]);
        // No listener on `quiet`'s errors: logged, as eventkit logs it.
        let logged = [("eventkit.event".to_owned(), "boom".to_owned())];
        assert_eq!(errors_here()[before.len()..], logged);
        drop(quiet);
    }

    #[test]
    fn a_chain_on_an_ibs_ticker_changes_its_bars_on_the_owner() {
        let ticker = Live::new(Ticker::default());
        let ib = FakeIb::new();
        ticker.bind(ib.holder());
        let timer = Event::<Zoned>::new("timer");
        let tb = ticker.update_event().trades().timebars(&timer);
        let on = Arc::new(Mutex::new(Vec::new()));
        let o = on.clone();
        tb.bars
            .update_event()
            .connect(move |_| lock(&o).push(on_owner()));

        // The program's timer, emitted here, changes the bars on the owner.
        timer.emit(&at(0));
        assert!(tb.bars.read().bars.is_empty());
        assert_eq!(ib.run(), 1);
        assert_eq!(tb.bars.read().bars.len(), 1);
        // The ticker's event, emitted here, runs its chain on the owner.
        pass(&ticker, vec![tick(4, 1.0, 1.0)]);
        assert_eq!(tb.bars.read().bars[0].count, 0);
        assert_eq!(ib.run(), 1);
        assert_eq!(tb.bars.read().bars[0].count, 1);
        assert_eq!(*lock(&on), [true]);
        // The program's edit of the bars is the owner's too.
        let bars = tb.bars.clone();
        let job = thread::spawn(move || bars.edit(|l| l.bars.clear()));
        assert!(ib.serve(job).unwrap().is_ok());
        assert!(tb.bars.read().bars.is_empty());
    }
}

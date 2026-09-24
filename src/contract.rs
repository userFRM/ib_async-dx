//! ib_async's `contract` module: contracts, their details and constructors.

use jiff::Zoned;
use jiff::tz::TimeZone;

use crate::error::{Error, Result};
use crate::util::{strptime, zone};

/// A financial instrument: ib_async's `Contract`, and through its
/// constructors (`Contract::stock`, `Contract::option`, …) ib_async's
/// `Stock`, `Option` and the other contract classes. The kind is `sec_type`.
///
/// Two contracts are equal when they share a nonzero `con_id`, or when every
/// field is equal, as ib_async's `__eq__`.
#[derive(Clone, Debug, Default)]
pub struct Contract {
    /// `secType`: the security type, such as STK, OPT, FUT, CONTFUT, CASH,
    /// IND, CFD, CMDTY, CRYPTO, FOP, BOND, FUND, WAR, BAG or NEWS.
    pub sec_type: String,
    /// `conId`: IB's unique contract identifier.
    pub con_id: i64,
    /// `symbol`: the contract's symbol, or its underlying's.
    pub symbol: String,
    /// `lastTradeDateOrContractMonth`: `YYYYMM` is the contract month,
    /// `YYYYMMDD` the last trading day.
    pub last_trade_date_or_contract_month: String,
    /// `strike`: the option's strike price.
    pub strike: f64,
    /// `right`: `P`, `PUT`, `C`, `CALL`, or empty for a non-option.
    pub right: String,
    /// `multiplier`: the instrument's multiplier.
    pub multiplier: String,
    /// `exchange`: the destination exchange.
    pub exchange: String,
    /// `primaryExchange`: the contract's primary exchange.
    pub primary_exchange: String,
    /// `currency`: the underlying's currency.
    pub currency: String,
    /// `localSymbol`: the contract's symbol on its primary exchange.
    pub local_symbol: String,
    /// `tradingClass`: the trading class name.
    pub trading_class: String,
    /// `includeExpired`: whether details and history requests reach expired
    /// futures.
    pub include_expired: bool,
    /// `secIdType`: the security identifier's type, such as ISIN or CUSIP.
    pub sec_id_type: String,
    /// `secId`: the security identifier.
    pub sec_id: String,
    /// `description`.
    pub description: String,
    /// `issuerId`.
    pub issuer_id: String,
    /// `comboLegsDescrip`: the description of the combo legs.
    pub combo_legs_descrip: String,
    /// `comboLegs`: the legs of a combo.
    pub combo_legs: Vec<ComboLeg>,
    /// `deltaNeutralContract`: delta and underlying price for a
    /// delta-neutral combo order.
    pub delta_neutral_contract: Option<DeltaNeutralContract>,
}

impl PartialEq for Contract {
    /// ib_async's `__eq__`: a shared nonzero `con_id`, else every field.
    fn eq(&self, o: &Self) -> bool {
        if self.con_id != 0 && self.con_id == o.con_id {
            return true;
        }
        // Destructured so that a new field cannot be left out.
        let Contract {
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
            delta_neutral_contract,
        } = self;
        *sec_type == o.sec_type
            && *con_id == o.con_id
            && *symbol == o.symbol
            && *last_trade_date_or_contract_month == o.last_trade_date_or_contract_month
            && *strike == o.strike
            && *right == o.right
            && *multiplier == o.multiplier
            && *exchange == o.exchange
            && *primary_exchange == o.primary_exchange
            && *currency == o.currency
            && *local_symbol == o.local_symbol
            && *trading_class == o.trading_class
            && *include_expired == o.include_expired
            && *sec_id_type == o.sec_id_type
            && *sec_id == o.sec_id
            && *description == o.description
            && *issuer_id == o.issuer_id
            && *combo_legs_descrip == o.combo_legs_descrip
            && *combo_legs == o.combo_legs
            && *delta_neutral_contract == o.delta_neutral_contract
    }
}

/// A combo leg as a tuple of its fields in order, ib_async's
/// `dataclassAsTuple(leg)`.
type LegKey = (i64, i32, String, String, i32, i32, String, i32);

/// The key tickers are held under: ib_async's `hash(contract)`, the key of
/// its `Wrapper.tickers`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(not(test), expect(dead_code, reason = "the ticker state keys by it"))]
pub(crate) enum TickerKey {
    /// `con_id`, negated for a CONTFUT, which shares its front contract's
    /// `con_id`. Widened so that the negation is exact, as Python's is.
    Id(i128),
    /// A BAG: its legs sorted by `con_id`, then its symbol and exchange.
    Bag(Vec<LegKey>, String, String),
}

fn owned(s: impl Into<String>) -> String {
    s.into()
}

impl Contract {
    /// Whether the contract has a `con_id` to be keyed by: ib_async's
    /// `isHashable`.
    pub fn is_hashable(&self) -> bool {
        self.con_id != 0
    }

    /// The pair's short name, `symbol` then `currency`: ib_async's
    /// `Forex.pair`.
    pub fn pair(&self) -> String {
        format!("{}{}", self.symbol, self.currency)
    }

    /// The key the contract's ticker is held under: ib_async's `__hash__`.
    /// A BAG is keyed by its legs, sorted by `con_id`, with its symbol and
    /// exchange. Any other contract needs a `con_id`, and fails with
    /// `Err(Value)` without one, as ib_async raises.
    #[cfg_attr(not(test), expect(dead_code, reason = "the ticker state keys by it"))]
    pub(crate) fn ticker_key(&self) -> Result<TickerKey> {
        if self.sec_type == "BAG" {
            let mut legs: Vec<&ComboLeg> = self.combo_legs.iter().collect();
            // Stable, as Python's `sorted`.
            legs.sort_by_key(|leg| leg.con_id);
            let legs = legs
                .into_iter()
                .map(|l| {
                    (
                        l.con_id,
                        l.ratio,
                        l.action.clone(),
                        l.exchange.clone(),
                        l.open_close,
                        l.short_sale_slot,
                        l.designated_location.clone(),
                        l.exempt_code,
                    )
                })
                .collect();
            return Ok(TickerKey::Bag(
                legs,
                self.symbol.clone(),
                self.exchange.clone(),
            ));
        }
        if !self.is_hashable() {
            return Err(Error::Value(format!(
                "Contract {self:?} can't be hashed because no 'conId' value exists. \
                 Qualify contract to populate 'conId'."
            )));
        }
        let id = i128::from(self.con_id);
        Ok(TickerKey::Id(if self.sec_type == "CONTFUT" {
            -id
        } else {
            id
        }))
    }

    fn of(sec_type: &str, symbol: String, exchange: String, currency: String) -> Contract {
        Contract {
            sec_type: sec_type.into(),
            symbol,
            exchange,
            currency,
            ..Contract::default()
        }
    }

    /// A stock or ETF (STK): ib_async's `Stock`.
    pub fn stock(
        symbol: impl Into<String>,
        exchange: impl Into<String>,
        currency: impl Into<String>,
    ) -> Contract {
        Contract::of("STK", owned(symbol), owned(exchange), owned(currency))
    }

    /// An option (OPT): ib_async's `Option`.
    pub fn option(
        symbol: impl Into<String>,
        last_trade_date_or_contract_month: impl Into<String>,
        strike: f64,
        right: impl Into<String>,
        exchange: impl Into<String>,
    ) -> Contract {
        Contract {
            last_trade_date_or_contract_month: last_trade_date_or_contract_month.into(),
            strike,
            right: right.into(),
            ..Contract::of("OPT", owned(symbol), owned(exchange), String::new())
        }
    }

    /// A future (FUT): ib_async's `Future`.
    pub fn future(
        symbol: impl Into<String>,
        last_trade_date_or_contract_month: impl Into<String>,
        exchange: impl Into<String>,
    ) -> Contract {
        Contract {
            last_trade_date_or_contract_month: last_trade_date_or_contract_month.into(),
            ..Contract::of("FUT", owned(symbol), owned(exchange), String::new())
        }
    }

    /// A continuous future (CONTFUT): ib_async's `ContFuture`.
    pub fn cont_future(symbol: impl Into<String>, exchange: impl Into<String>) -> Contract {
        Contract::of("CONTFUT", owned(symbol), owned(exchange), String::new())
    }

    /// A currency pair (CASH) on IDEALPRO: ib_async's `Forex`. A pair of six
    /// characters, such as `EURUSD`, splits into `symbol` and `currency`; an
    /// empty pair sets neither. Any other length is `Err(Value)`, where
    /// ib_async's assertion fails.
    pub fn forex(pair: impl Into<String>) -> Result<Contract> {
        let pair = pair.into();
        let (symbol, currency) = match pair.chars().count() {
            0 => (String::new(), String::new()),
            6 => (
                pair.chars().take(3).collect(),
                pair.chars().skip(3).collect(),
            ),
            _ => {
                return Err(Error::Value(format!(
                    "a forex pair has 6 characters: {pair:?}"
                )));
            }
        };
        Ok(Contract::of("CASH", symbol, "IDEALPRO".into(), currency))
    }

    /// An index (IND): ib_async's `Index`.
    pub fn index(
        symbol: impl Into<String>,
        exchange: impl Into<String>,
        currency: impl Into<String>,
    ) -> Contract {
        Contract::of("IND", owned(symbol), owned(exchange), owned(currency))
    }

    /// A contract for difference (CFD): ib_async's `CFD`.
    pub fn cfd(
        symbol: impl Into<String>,
        exchange: impl Into<String>,
        currency: impl Into<String>,
    ) -> Contract {
        Contract::of("CFD", owned(symbol), owned(exchange), owned(currency))
    }

    /// A commodity (CMDTY): ib_async's `Commodity`.
    pub fn commodity(
        symbol: impl Into<String>,
        exchange: impl Into<String>,
        currency: impl Into<String>,
    ) -> Contract {
        Contract::of("CMDTY", owned(symbol), owned(exchange), owned(currency))
    }

    /// A crypto currency (CRYPTO): ib_async's `Crypto`.
    pub fn crypto(
        symbol: impl Into<String>,
        exchange: impl Into<String>,
        currency: impl Into<String>,
    ) -> Contract {
        Contract::of("CRYPTO", owned(symbol), owned(exchange), owned(currency))
    }

    /// An option on a future (FOP): ib_async's `FuturesOption`.
    pub fn futures_option(
        symbol: impl Into<String>,
        last_trade_date_or_contract_month: impl Into<String>,
        strike: f64,
        right: impl Into<String>,
        exchange: impl Into<String>,
    ) -> Contract {
        Contract {
            sec_type: "FOP".into(),
            ..Contract::option(
                symbol,
                last_trade_date_or_contract_month,
                strike,
                right,
                exchange,
            )
        }
    }

    /// A bond (BOND): ib_async's `Bond`; set the rest by struct update.
    pub fn bond() -> Contract {
        Contract::of("BOND", String::new(), String::new(), String::new())
    }

    /// A mutual fund (FUND): ib_async's `MutualFund`.
    pub fn mutual_fund() -> Contract {
        Contract::of("FUND", String::new(), String::new(), String::new())
    }

    /// A warrant (WAR): ib_async's `Warrant`.
    pub fn warrant() -> Contract {
        Contract::of("WAR", String::new(), String::new(), String::new())
    }

    /// A combo (BAG): ib_async's `Bag`; set `combo_legs` by struct update.
    pub fn bag() -> Contract {
        Contract::of("BAG", String::new(), String::new(), String::new())
    }

    /// A provider's news feed (NEWS): symbol `"{p}:{p}_ALL"` on exchange
    /// `p`, for `req_mkt_data` with the news ticks.
    pub fn news(provider: impl Into<String>) -> Contract {
        let p = provider.into();
        Contract::of("NEWS", format!("{p}:{p}_ALL"), p, String::new())
    }
}

/// A tag and its value: ib_async's `TagValue`.
#[derive(Clone, Debug, PartialEq)]
pub struct TagValue {
    /// `tag`.
    pub tag: String,
    /// `value`.
    pub value: String,
}

/// One leg of a combo: ib_async's `ComboLeg`.
#[derive(Clone, Debug, PartialEq)]
pub struct ComboLeg {
    /// `conId`: the leg's contract.
    pub con_id: i64,
    /// `ratio`: the leg's share of the combo.
    pub ratio: i32,
    /// `action`: BUY or SELL.
    pub action: String,
    /// `exchange`.
    pub exchange: String,
    /// `openClose`.
    pub open_close: i32,
    /// `shortSaleSlot`.
    pub short_sale_slot: i32,
    /// `designatedLocation`.
    pub designated_location: String,
    /// `exemptCode`; -1 by default.
    pub exempt_code: i32,
}

impl Default for ComboLeg {
    fn default() -> Self {
        ComboLeg {
            con_id: 0,
            ratio: 0,
            action: String::new(),
            exchange: String::new(),
            open_close: 0,
            short_sale_slot: 0,
            designated_location: String::new(),
            exempt_code: -1,
        }
    }
}

/// The underlying of a delta-neutral combo order: ib_async's
/// `DeltaNeutralContract`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DeltaNeutralContract {
    /// `conId`.
    pub con_id: i64,
    /// `delta`.
    pub delta: f64,
    /// `price`.
    pub price: f64,
}

/// A session's start and end, in the contract's zone: ib_async's
/// `TradingSession`.
#[derive(Clone, Debug, PartialEq)]
pub struct TradingSession {
    /// `start`.
    pub start: Zoned,
    /// `end`.
    pub end: Zoned,
}

/// A contract's full description: ib_async's `ContractDetails`.
#[derive(Clone, Debug, PartialEq)]
pub struct ContractDetails {
    /// `contract`.
    pub contract: Option<Contract>,
    /// `marketName`.
    pub market_name: String,
    /// `minTick`: the smallest price increment.
    pub min_tick: f64,
    /// `orderTypes`: the order types, comma-separated.
    pub order_types: String,
    /// `validExchanges`: the exchanges, comma-separated.
    pub valid_exchanges: String,
    /// `priceMagnifier`.
    pub price_magnifier: i32,
    /// `underConId`: the underlying's `con_id`.
    pub under_con_id: i64,
    /// `longName`.
    pub long_name: String,
    /// `contractMonth`.
    pub contract_month: String,
    /// `industry`.
    pub industry: String,
    /// `category`.
    pub category: String,
    /// `subcategory`.
    pub subcategory: String,
    /// `timeZoneId`: the zone `trading_hours` and `liquid_hours` are in.
    pub time_zone_id: String,
    /// `tradingHours`: the sessions, as `trading_sessions` reads them.
    pub trading_hours: String,
    /// `liquidHours`: the liquid sessions, as `liquid_sessions` reads them.
    pub liquid_hours: String,
    /// `evRule`.
    pub ev_rule: String,
    /// `evMultiplier`, at the width the engine delivers.
    pub ev_multiplier: f64,
    /// `mdSizeMultiplier`; 1 by default.
    pub md_size_multiplier: i32,
    /// `aggGroup`.
    pub agg_group: i32,
    /// `underSymbol`.
    pub under_symbol: String,
    /// `underSecType`.
    pub under_sec_type: String,
    /// `marketRuleIds`: the market rule ids, comma-separated.
    pub market_rule_ids: String,
    /// `secIdList`.
    pub sec_id_list: Vec<TagValue>,
    /// `realExpirationDate`.
    pub real_expiration_date: String,
    /// `lastTradeTime`.
    pub last_trade_time: String,
    /// `stockType`.
    pub stock_type: String,
    /// `minSize`.
    pub min_size: f64,
    /// `sizeIncrement`.
    pub size_increment: f64,
    /// `suggestedSizeIncrement`.
    pub suggested_size_increment: f64,
    /// `cusip`.
    pub cusip: String,
    /// `ratings`.
    pub ratings: String,
    /// `descAppend`.
    pub desc_append: String,
    /// `bondType`.
    pub bond_type: String,
    /// `couponType`.
    pub coupon_type: String,
    /// `callable`.
    pub callable: bool,
    /// `putable`.
    pub putable: bool,
    /// `coupon`.
    pub coupon: f64,
    /// `convertible`.
    pub convertible: bool,
    /// `maturity`.
    pub maturity: String,
    /// `issueDate`.
    pub issue_date: String,
    /// `nextOptionDate`.
    pub next_option_date: String,
    /// `nextOptionType`.
    pub next_option_type: String,
    /// `nextOptionPartial`.
    pub next_option_partial: bool,
    /// `notes`.
    pub notes: String,
}

impl Default for ContractDetails {
    fn default() -> Self {
        ContractDetails {
            contract: None,
            market_name: String::new(),
            min_tick: 0.0,
            order_types: String::new(),
            valid_exchanges: String::new(),
            price_magnifier: 0,
            under_con_id: 0,
            long_name: String::new(),
            contract_month: String::new(),
            industry: String::new(),
            category: String::new(),
            subcategory: String::new(),
            time_zone_id: String::new(),
            trading_hours: String::new(),
            liquid_hours: String::new(),
            ev_rule: String::new(),
            ev_multiplier: 0.0,
            md_size_multiplier: 1,
            agg_group: 0,
            under_symbol: String::new(),
            under_sec_type: String::new(),
            market_rule_ids: String::new(),
            sec_id_list: Vec::new(),
            real_expiration_date: String::new(),
            last_trade_time: String::new(),
            stock_type: String::new(),
            min_size: 0.0,
            size_increment: 0.0,
            suggested_size_increment: 0.0,
            cusip: String::new(),
            ratings: String::new(),
            desc_append: String::new(),
            bond_type: String::new(),
            coupon_type: String::new(),
            callable: false,
            putable: false,
            coupon: 0.0,
            convertible: false,
            maturity: String::new(),
            issue_date: String::new(),
            next_option_date: String::new(),
            next_option_type: String::new(),
            next_option_partial: false,
            notes: String::new(),
        }
    }
}

impl ContractDetails {
    /// `trading_hours` as sessions in `time_zone_id`: ib_async's
    /// `tradingSessions`.
    pub fn trading_sessions(&self) -> Result<Vec<TradingSession>> {
        self.parse_sessions(&self.trading_hours)
    }

    /// `liquid_hours` as sessions in `time_zone_id`: ib_async's
    /// `liquidSessions`.
    pub fn liquid_sessions(&self) -> Result<Vec<TradingSession>> {
        self.parse_sessions(&self.liquid_hours)
    }

    /// ib_async's `_parseSessions`: `start-end` pairs split on `;`, empty and
    /// CLOSED entries skipped, each stamp `%Y%m%d:%H%M` in `time_zone_id`.
    /// An unknown zone, a bad stamp or a time the zone cannot represent is
    /// `Err(Value)`.
    fn parse_sessions(&self, s: &str) -> Result<Vec<TradingSession>> {
        if s.is_empty() && self.time_zone_id.is_empty() {
            return Ok(Vec::new());
        }
        let tz = zone(&self.time_zone_id)?;
        let mut sessions = Vec::new();
        for sess in s.split(';') {
            if sess.is_empty() || sess.contains("CLOSED") {
                continue;
            }
            // Every stamp is parsed before the pair is counted, as ib_async
            // builds its list before `TradingSession(*list)`.
            let times = sess
                .split('-')
                .map(|t| session_time(t, &tz))
                .collect::<Result<Vec<_>>>()?;
            let [start, end] = <[Zoned; 2]>::try_from(times).map_err(|t| {
                Error::Value(match t.len() {
                    1 => "TradingSession.__new__() missing 1 required positional argument: 'end'"
                        .to_string(),
                    n => format!(
                        "TradingSession.__new__() takes 3 positional arguments but {} were given",
                        n + 1
                    ),
                })
            })?;
            sessions.push(TradingSession { start, end });
        }
        Ok(sessions)
    }
}

/// `datetime.strptime(t, "%Y%m%d:%H%M").replace(tzinfo=tz)`. A wall time
/// that falls in a gap or a repeated hour names the instant Python's
/// `fold=0` does (jiff's `compatible`).
fn session_time(t: &str, tz: &TimeZone) -> Result<Zoned> {
    strptime(t, "%Y%m%d:%H%M")?
        .to_zoned(tz.clone())
        .map_err(|e| Error::Value(e.to_string()))
}

/// One row of a contract search: ib_async's `ContractDescription`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ContractDescription {
    /// `contract`.
    pub contract: Option<Contract>,
    /// `derivativeSecTypes`: the security types of its derivatives.
    pub derivative_sec_types: Vec<String>,
}

/// One row of a scanner's result: ib_async's `ScanData`.
#[derive(Clone, Debug, PartialEq)]
pub struct ScanData {
    /// `rank`.
    pub rank: i32,
    /// `contractDetails`.
    pub contract_details: ContractDetails,
    /// `distance`.
    pub distance: String,
    /// `benchmark`.
    pub benchmark: String,
    /// `projection`.
    pub projection: String,
    /// `legsStr`.
    pub legs_str: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn value_text(r: Result<impl std::fmt::Debug>) -> String {
        match r {
            Err(Error::Value(m)) => m,
            other => panic!("expected Err(Value), got {other:?}"),
        }
    }

    #[test]
    fn eq_by_con_id_else_by_every_field() {
        let a = Contract {
            con_id: 265598,
            ..Contract::stock("AAPL", "SMART", "USD")
        };
        let b = Contract {
            con_id: 265598,
            ..Contract::default()
        };
        assert_eq!(a, b);
        assert_eq!(b, a);
        // Unqualified: every field decides, legs and delta-neutral included.
        assert_eq!(
            Contract::stock("AAPL", "SMART", "USD"),
            Contract::stock("AAPL", "SMART", "USD")
        );
        assert_ne!(
            Contract::stock("AAPL", "SMART", "USD"),
            Contract::stock("AAPL", "SMART", "EUR")
        );
        let leg = ComboLeg {
            con_id: 1,
            ratio: 1,
            ..ComboLeg::default()
        };
        let bag = Contract {
            combo_legs: vec![leg.clone()],
            ..Contract::bag()
        };
        let other = Contract {
            combo_legs: vec![ComboLeg { ratio: 2, ..leg }],
            ..Contract::bag()
        };
        assert_ne!(bag, other);
        let dn = Contract {
            delta_neutral_contract: Some(DeltaNeutralContract::default()),
            ..Contract::bag()
        };
        assert_ne!(dn, Contract::bag());
        // Different nonzero ids fall back to the fields, which differ in
        // `con_id` itself.
        assert_ne!(
            Contract {
                con_id: 2,
                ..b.clone()
            },
            b
        );
    }

    #[test]
    fn ticker_key_mirrors_hash() {
        let stk = Contract {
            con_id: 5,
            ..Contract::stock("X", "SMART", "USD")
        };
        assert_eq!(stk.ticker_key().unwrap(), TickerKey::Id(5));
        assert!(stk.is_hashable());
        // CONTFUT shares its front month's con_id, so it is negated.
        let cf = Contract {
            con_id: 5,
            ..Contract::cont_future("ES", "CME")
        };
        assert_eq!(cf.ticker_key().unwrap(), TickerKey::Id(-5));
        let edge = Contract {
            con_id: i64::MIN,
            ..Contract::cont_future("ES", "CME")
        };
        assert_eq!(
            edge.ticker_key().unwrap(),
            TickerKey::Id(-i128::from(i64::MIN))
        );

        let unqualified = Contract::stock("AMD", "SMART", "USD");
        assert!(!unqualified.is_hashable());
        let m = value_text(unqualified.ticker_key());
        assert!(m.starts_with("Contract Contract {"), "{m}");
        assert!(
            m.ends_with(
                " can't be hashed because no 'conId' value exists. \
                 Qualify contract to populate 'conId'."
            ),
            "{m}"
        );

        // A BAG is keyed by its legs in con_id order, whatever its con_id.
        let leg = |con_id, ratio| ComboLeg {
            con_id,
            ratio,
            action: "BUY".into(),
            exchange: "SMART".into(),
            ..ComboLeg::default()
        };
        let bag = |legs, con_id| Contract {
            con_id,
            symbol: "SPY".into(),
            exchange: "SMART".into(),
            combo_legs: legs,
            ..Contract::bag()
        };
        let k1 = bag(vec![leg(2, 1), leg(1, 1)], 0).ticker_key().unwrap();
        let k2 = bag(vec![leg(1, 1), leg(2, 1)], 28812380)
            .ticker_key()
            .unwrap();
        assert_eq!(k1, k2);
        let TickerKey::Bag(legs, symbol, exchange) = &k1 else {
            panic!("{k1:?}")
        };
        assert_eq!(
            legs[0],
            (1, 1, "BUY".into(), "SMART".into(), 0, 0, String::new(), -1)
        );
        assert_eq!((symbol.as_str(), exchange.as_str()), ("SPY", "SMART"));
        assert_ne!(k1, bag(vec![leg(1, 1), leg(2, 2)], 0).ticker_key().unwrap());
        assert!(bag(Vec::new(), 0).ticker_key().is_ok());
    }

    #[test]
    fn constructors() {
        let s = Contract::stock("AAPL", "SMART", "USD");
        assert_eq!(
            (s.sec_type.as_str(), s.symbol.as_str(), s.exchange.as_str()),
            ("STK", "AAPL", "SMART")
        );
        assert_eq!(s.currency, "USD");
        let o = Contract::option("SPY", "20261218", 600.0, "C", "SMART");
        assert_eq!(
            o,
            Contract {
                sec_type: "OPT".into(),
                symbol: "SPY".into(),
                last_trade_date_or_contract_month: "20261218".into(),
                strike: 600.0,
                right: "C".into(),
                exchange: "SMART".into(),
                ..Contract::default()
            }
        );
        let fop = Contract::futures_option("ES", "202612", 6000.0, "P", "CME");
        assert_eq!(
            fop,
            Contract {
                sec_type: "FOP".into(),
                ..Contract::option("ES", "202612", 6000.0, "P", "CME")
            }
        );
        let f = Contract::future("ES", "202612", "CME");
        assert_eq!(
            (
                f.sec_type.as_str(),
                f.last_trade_date_or_contract_month.as_str()
            ),
            ("FUT", "202612")
        );
        let cf = Contract::cont_future("ES", "CME");
        assert_eq!(
            (cf.sec_type.as_str(), cf.exchange.as_str()),
            ("CONTFUT", "CME")
        );
        for (c, t) in [
            (Contract::index("SPX", "CBOE", "USD"), "IND"),
            (Contract::cfd("IBUS30", "SMART", "USD"), "CFD"),
            (Contract::commodity("XAUUSD", "SMART", "USD"), "CMDTY"),
            (Contract::crypto("BTC", "PAXOS", "USD"), "CRYPTO"),
        ] {
            assert_eq!(c.sec_type, t);
            assert_eq!(c.currency, "USD");
        }
        for (c, t) in [
            (Contract::bond(), "BOND"),
            (Contract::mutual_fund(), "FUND"),
            (Contract::warrant(), "WAR"),
            (Contract::bag(), "BAG"),
        ] {
            assert_eq!(
                c,
                Contract {
                    sec_type: t.into(),
                    ..Contract::default()
                }
            );
        }
        let isin = Contract {
            sec_id_type: "ISIN".into(),
            sec_id: "US03076KAA60".into(),
            ..Contract::bond()
        };
        assert_eq!(isin.sec_type, "BOND");

        let fx = Contract::forex("EURUSD").unwrap();
        assert_eq!(
            fx,
            Contract {
                sec_type: "CASH".into(),
                symbol: "EUR".into(),
                exchange: "IDEALPRO".into(),
                currency: "USD".into(),
                ..Contract::default()
            }
        );
        assert_eq!(fx.pair(), "EURUSD");
        let empty = Contract::forex("").unwrap();
        assert_eq!(
            (
                empty.sec_type.as_str(),
                empty.symbol.as_str(),
                empty.exchange.as_str()
            ),
            ("CASH", "", "IDEALPRO")
        );
        // Split by character, as Python slices a str.
        let wide = Contract::forex("€€€ÜÜÜ").unwrap();
        assert_eq!(
            (wide.symbol.as_str(), wide.currency.as_str()),
            ("€€€", "ÜÜÜ")
        );
        value_text(Contract::forex("EURUSDX"));
        value_text(Contract::forex("EUR"));

        let n = Contract::news("BRFG");
        assert_eq!(
            (n.sec_type.as_str(), n.symbol.as_str(), n.exchange.as_str()),
            ("NEWS", "BRFG:BRFG_ALL", "BRFG")
        );
    }

    #[test]
    fn defaults() {
        assert_eq!(ComboLeg::default().exempt_code, -1);
        assert_eq!(ContractDetails::default().md_size_multiplier, 1);
        assert_eq!(ContractDetails::default().contract, None);
        assert_eq!(ContractDescription::default().derivative_sec_types.len(), 0);
    }

    fn details(zone: &str, hours: &str) -> ContractDetails {
        ContractDetails {
            time_zone_id: zone.into(),
            trading_hours: hours.into(),
            liquid_hours: hours.replace("0400", "0930").replace("2000", "1600"),
            ..ContractDetails::default()
        }
    }

    #[test]
    fn sessions_in_zoned() {
        // ib_async's own example.
        let hours = "20240721:CLOSED;20240722:0400-20240722:2000;\
                     20240723:0400-20240723:2000";
        let d = details("US/Eastern", hours);
        let s = d.trading_sessions().unwrap();
        assert_eq!(s.len(), 2);
        assert_eq!(s[0].start.time_zone().iana_name(), Some("US/Eastern"));
        assert_eq!(
            s[0].start.to_string(),
            "2024-07-22T04:00:00-04:00[US/Eastern]"
        );
        assert_eq!(s[0].end.timestamp().as_second(), 1721692800);
        assert_eq!(
            s[1].end.to_string(),
            "2024-07-23T20:00:00-04:00[US/Eastern]"
        );
        let l = d.liquid_sessions().unwrap();
        assert_eq!(
            l[0].start.to_string(),
            "2024-07-22T09:30:00-04:00[US/Eastern]"
        );

        // Empty entries are skipped, as is anything naming CLOSED.
        let s = details(
            "America/New_York",
            ";20240722:0400-20240722:2000;;x CLOSED y",
        )
        .trading_sessions()
        .unwrap();
        assert_eq!(s.len(), 1);

        // A wall time in the spring gap names the instant Python's fold=0
        // does: 1710055800, 07:30 UTC.
        let s = details("America/New_York", "20240310:0230-20240310:0300")
            .trading_sessions()
            .unwrap();
        assert_eq!(s[0].start.timestamp().as_second(), 1710055800);
        // In the repeated hour, the first occurrence.
        let s = details("America/New_York", "20241103:0130-20241103:0200")
            .trading_sessions()
            .unwrap();
        assert_eq!(s[0].start.offset().seconds(), -4 * 3600);

        // The short forms CPython's strptime accepts.
        let s = details("UTC", "2024722:400-2024111:0400")
            .trading_sessions()
            .unwrap();
        assert_eq!(s[0].start.to_string(), "2024-07-22T04:00:00+00:00[UTC]");
        assert_eq!(s[0].end.to_string(), "2024-11-01T04:00:00+00:00[UTC]");
        let s = details("UTC", "20240722: 400-20240722:04")
            .trading_sessions()
            .unwrap();
        assert_eq!(s[0].start.to_string(), "2024-07-22T04:00:00+00:00[UTC]");
        assert_eq!(s[0].end.to_string(), "2024-07-22T00:04:00+00:00[UTC]");
    }

    #[test]
    fn sessions_empty_and_errors() {
        // Nothing to read only when both the hours and the zone are empty.
        assert_eq!(details("", "").trading_sessions().unwrap(), Vec::new());
        assert_eq!(details("UTC", "").trading_sessions().unwrap(), Vec::new());
        value_text(details("Bogus/Zone", "").trading_sessions());
        value_text(details("", "20240722:0400-20240722:2000").trading_sessions());

        let err = |hours: &str| value_text(details("UTC", hours).trading_sessions());
        assert_eq!(
            err("20240722:0400x-20240722:2000"),
            "unconverted data remains: x"
        );
        assert_eq!(
            err("20240722:2400-20240722:2000"),
            "unconverted data remains: 0"
        );
        assert_eq!(
            err("2024-07-22"),
            "time data '2024' does not match format '%Y%m%d:%H%M'"
        );
        assert_eq!(
            err("20240230:0400-20240722:2000"),
            "day 30 must be in range 1..29 for month 2 in year 2024"
        );
        assert_eq!(
            err("00000722:0400-20240722:2000"),
            "year must be in 1..9999, not 0"
        );
        // A stamp fails before the pair is counted.
        assert_eq!(
            err("20240722:0400-bad-x"),
            "time data 'bad' does not match format '%Y%m%d:%H%M'"
        );
        assert_eq!(
            err("20240723:0930"),
            "TradingSession.__new__() missing 1 required positional argument: 'end'"
        );
        assert_eq!(
            err("20240722:0400-20240722:2000-20240722:2100"),
            "TradingSession.__new__() takes 3 positional arguments but 4 were given"
        );
        // A time the zone cannot represent.
        value_text(details("Asia/Tokyo", "99991231:2359-99991231:2359").trading_sessions());
    }
}

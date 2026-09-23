# Beyond ib_async

`ib_async_dx.IB` is ib_async's `IB` with three kinds of addition: two of
ib_async's bugs fixed, the engine's calls beyond the documented API, and a
warning at connect. No field is added to an ib_async class, so ib_async's
objects, their reprs, `util.df`'s columns and equality stay ib_async's.

## ib_async's bugs, fixed

Each fix is a small override on `IB`, and each has a test that runs ib_async's
own `IB` beside it, so the difference is shown rather than described.

| In ib_async 2.1 | Here |
| --- | --- |
| `reqUserInfo()` returns `[]`. Its wrapper's `userInfo` ends the request without the White Branding ID it was answered with | `reqUserInfo()` returns the White Branding ID, as its docstring says |
| `connect` asks for positions whatever `fetchFields` says | Left out of `fetchFields`, `StartupFetch.POSITIONS` means that request is not made, so `positions()` stays empty until `reqPositions()`, which asks as usual |

The tests are `test_reqUserInfo_answers_the_white_branding_id` and
`test_fetchFields_without_positions_asks_for_no_positions`, in
`tests/python/test_ib_runs_on_the_engine.py`.

## The engine's calls beyond the documented API

The venue states more than the documented API has messages for. These methods
read it from the engine, and return types of this package's own:
`TickerExtras`, `OptionModel`, `OrderPreset` and `CompetingSession`, all
importable from `ib_async_dx` and none of them in `__all__`. Called while not
connected, before `connect` or after `disconnect`, each raises
`ConnectionError("Not connected")`, as ib_async's client does.

| Method | Returns | What it answers |
| --- | --- | --- |
| `reqMktDataEx(..., marketDataType=None)` | `Ticker` | `reqMktData`'s arguments, and a market data type for this request only, numbered as `reqMarketDataType` numbers them: 1 live, 2 frozen, 3 delayed, 4 delayed frozen. `None` keeps the session's. A contract holds one subscription: asked again while subscribed, it follows the one that is up, so cancel between two types |
| `reqCurrentTimeInMillis()`, `reqCurrentTimeInMillisAsync()` | `int` | The venue's clock in milliseconds since the epoch: a call in the documented API that ib_async does not have. It is the local clock corrected by the venue's, so it is given to the millisecond and accurate to about a second |
| `tickerExtras(ticker)` | `TickerExtras` | What the venue has stated for the ticker's market data request beyond ib_async's `Ticker`, read when called: `sharesOutstanding`, `openAYearAgo`, `shortSaleRestricted` (a short-sale circuit breaker is on, not whether the contract can be shorted), and `statedFigures`, `numberedFigures` and `pairedFigures`, series by the venue's own numbering. A series is asked for by its number in `genericTickList`. A figure not stated is `nan` |
| `optionModel(ticker)` | `OptionModel` or `None` | The venue's model of the ticker's option: the eight figures of `OptionComputation` and the ten it has no field for — `calDays`, `rate`, `rho`, `fugit`, `exerciseBoundary`, `forwardCoeff`, `modelYield`, `bridgeYield`, `timeValue` and `priceBasedVol`. A figure not stated is `None`; `priceBasedVol` is always a bool, `False` when the venue did not state it |
| `closingOptionModel(ticker)` | `OptionModel` or `None` | The same model as it stood at the close |
| `companyData(contract)` | `dict[int, list[tuple[str, str]]]` | What the venue states about the contract's company or terms, as its own key and value pairs, by series. Asked for by number in `genericTickList`, and kept after the cancel. Empty means not entitled or nothing stated; the two cannot be told apart |
| `enabledFeatures()` | `list[str]` | The capability tokens the venue granted this account at logon |
| `orderPermissions()` | `dict[str, list[str]]` | The order types the venue permits, by security type |
| `permittedOrderTypes(secType)` | `list[str]` or `None` | The order types permitted for one security type, or `None` if it is not permitted. An order the account may not place comes back `Inactive` with no text |
| `algorithms()` | `dict[str, list[str]]` | The algorithms the venue offers, keyed `PROVIDER/SECTYPE` |
| `algorithmsFor(secType)` | `list[str]` | The algorithms offered for one security type, across every provider |
| `orderPresets()` | `list[OrderPreset]` | The sets of order defaults the account holds, as `key`, `version` and `lastChanged`. Their values are not carried |
| `competingSession()` | `CompetingSession` or `None` | Another session that held the account when this one connected: its `origin`, when it logged in (`loggedInAt`, in UTC), and `readOnly`, true when this session may read but not trade because the other holds the account |
| `reqPing()`, `lastRtt()` | `None`, `float` or `None` | `reqPing` measures the round trip to the venue; `lastRtt` reads the latest, in milliseconds |

**A warning at connect.** When another session held the account as this one
connected, `connect` logs it at `WARNING` on ib_async's own `ib_async.ib`
logger, with what `competingSession()` answers.

> [!TIP]
> These calls are one-way. A program that uses one cannot move back to a
> gateway, because a gateway has no message to carry it. Everything ib_async
> itself names moves both ways.

## Coming

Each of these waits on an addition to the engine. `reqSpreadScan`,
`positionsElsewhere` and `accountValuesElsewhere` have no call on its Python
client yet, `reqCorporateActions` needs it to hold the answer by the request
that asked, and `statedRows` needs a lister for its series, as the engine has
for the other series. None is on `IB` until then, rather than there as a name
that only raises.

| Call | What it will answer |
| --- | --- |
| `reqCorporateActions`, and its `…Async` twin | A contract's corporate actions over a range of days |
| `reqSpreadScan`, and its `…Async` twin | An underlying, scanned by the venue for strategies worth putting on, in the venue's own words |
| `positionsElsewhere` | Holdings the venue reports that this broker does not hold itself: positions held away at another broker, and rows shown but not held. Kept out of `positions()` |
| `accountValuesElsewhere` | The account figures for those holdings, kept apart so that the account is not overstated |
| `TickerExtras.statedRows` | The series the venue states as rows of three figures |

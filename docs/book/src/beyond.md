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
`TickerExtras`, `OptionModel`, `OrderPreset`, `CompetingSession`,
`CorporateAction`, `ScannedStrategy` and `PositionElsewhere`, with the
engine's own `SpreadScan` for what a spread scan looks for. All are importable
from `ib_async_dx` and none is in `__all__`. Called while not connected, before
`connect` or after `disconnect`, each raises `ConnectionError("Not
connected")`, as ib_async's client does.

| Method | Returns | What it answers |
| --- | --- | --- |
| `reqMktDataEx(..., marketDataType=None)` | `Ticker` | `reqMktData`'s arguments, and a market data type for this request only, numbered as `reqMarketDataType` numbers them: 1 live, 2 frozen, 3 delayed, 4 delayed frozen. `None` keeps the session's. A contract holds one subscription: asked again while subscribed, it follows the one that is up, so cancel between two types |
| `reqCurrentTimeInMillis()`, `reqCurrentTimeInMillisAsync()` | `int` | The time in milliseconds since the epoch: a call in the documented API that ib_async does not have. It is read off the session's clock, the local clock set by the venue's to within about a second, as a gateway answers it from its own clock |
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
| `reqCorporateActions(contract, startDate, endDate)`, `reqCorporateActionsAsync(...)` | `list[CorporateAction]` | A contract's corporate actions over a range of days, `YYYYMMDD`, asked for by its `conId`. Each states its `kind` as the venue names it — CD a cash dividend, SD a dividend in shares, SS a split, SO a spin-off, RO a rights offer, FR a future rolling into the next month — with its `date`, `value` and `currency`, and the `announceDate`, `recordDate`, `payDate`, `paymentType` and `distributionType` the kind carries; one it does not carry is empty. A request given up before its answer, on a `RequestTimeout` among other ways, is withdrawn; one dropped by `disconnect()` raises `ConnectionError` |
| `reqSpreadScan(contract, scan, timeout=10)`, `reqSpreadScanAsync(...)` | `list[ScannedStrategy]` | An underlying, by its `conId`, scanned by the venue for strategies worth putting on. `scan` is the engine's `SpreadScan`, whose fields are the venue's own words. As `reqScannerData` does, it subscribes, takes the first answer and cancels; the subscription's quotes reach the underlying's ticker meanwhile. Each strategy states its `legs` as contract ids and sizes, a leg sold negative, its `kind` and `aggression` as the venue numbers them, its thirteen `figures` in the venue's order, its `breakEvens` and its `lastFigure`; a figure not stated is `nan`. Unanswered within `timeout` seconds of being asked, 0 for no limit, it found nothing: `[]`; refused, it cancels nothing. The engine keeps a scan's answer for as long as the underlying's market data is subscribed, so a scan made while it still is — by the program's own `reqMktData`, or by a scan just ended — can be answered with the strategies the last scan found. A scan dropped by `disconnect()` raises `ConnectionError` |
| `positionsElsewhere()` | `list[PositionElsewhere]` | Holdings the venue reports that this broker does not hold itself, as `conId`, `symbol`, `secType`, `currency`, `position`, `avgCost` and `held`: `'Away'` for a position held at another broker, `'DisplayOnly'` for a row shown but not held, `'Aside'` for one reported apart without saying why. Kept out of `positions()`, so the account is not overstated |
| `accountValuesElsewhere(held)` | `list[AccountValue]` | The account figures for one of those sets, `'Away'`, `'DisplayOnly'` or `'Aside'`, as ib_async's own `AccountValue` with an empty `modelCode`. A figure stated in two currencies is two rows. Kept out of `accountValues()` and `accountValueEvent` |

**A warning at connect.** When another session held the account as this one
connected, `connect` logs it at `WARNING` on this package's `ib_async_dx.ib`
logger, with what `competingSession()` answers. ib_async's own loggers carry
only what ib_async says.

> [!TIP]
> These calls are one-way. A program that uses one cannot move back to a
> gateway, because a gateway has no message to carry it. Everything ib_async
> itself names moves both ways.

## Coming

This waits on an addition to the engine: `statedRows` needs a lister for its
series, as the engine has for the other series. It is not on `TickerExtras`
until then.

| Call | What it will answer |
| --- | --- |
| `TickerExtras.statedRows` | The series the venue states as rows of three figures |

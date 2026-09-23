# Limits

What a caller has to know before writing against ib_async-dx, sorted by what
kind of thing it is: where ib_async on the engine behaves differently from
ib_async over a gateway, what is taken and not applied, and what is the
venue's answer rather than this package's.

ib_async runs as itself, so its calls, its arguments and its answers are its
own. What differs is what its `IB` reads off a transport with no socket
underneath, and what the engine does that a gateway does not. What a gateway
refuses is refused as a gateway refuses it, on `errorEvent`. What is taken and
not applied is named below, where it happens. Where the engine answers as a gateway does, and
a program might not expect it to, [Running ib_async itself](./bridge.md) says
so.

# Where it differs from a gateway

* **`connect` addresses nothing.** `host` and `port` are accepted and not used,
  and the port does not choose paper or live: the session is paper unless
  `connect` is given `paper=False`. `clientId` is carried into the login.
* **One program per login.** Each program is its own session on the login, and
  a second program on the same login, or a gateway, takes the session from the
  first; the venue says which host took it. Programs that share one gateway
  login under their own client ids each need a login of their own here.
* **Every session hears every order on the account**, whatever its
  `clientId`: `trades()`, `openTrades()`, `reqOpenOrders()` and
  `openOrderEvent` include the account's orders placed elsewhere — in TWS, on
  the phone, or by a program under another client id. Over a gateway, a
  program is told of its own orders only, unless it connects as client 0 or as
  the gateway's master client, or asks with `reqAllOpenOrders()`.
* **A request a gateway reroutes is refused.** Asked for a quote or a book on
  a contract the venue serves under another, a contract for difference
  standing for a share among them, a gateway tells the program where to ask
  instead, and ib_async, which has no handler for that, hears nothing. Here
  the request is refused in the venue's words, on `errorEvent`.
* **A pass's ticks reach a ticker in the engine's order.** The engine states
  a quote's prices before its sizes, and a price is handed over with the size
  that goes with it, so the bid, ask and last a pass states land in
  `ticker.ticks` after the other ticks of that pass. The ticker's fields end
  the pass as they would over a gateway.
* **`connectionStats()`** counts the messages each way, as ib_async's client
  does: a request is one sent, and what reaches ib_async's wrapper one
  received. Its byte counts are zero: the engine does not count the bytes of
  its connections.
* **An option list is checked as a gateway checks one, and `manual` is not
  carried.** On nine of the eleven requests a gateway reads a list on —
  `reqMktData`, `placeOrder` (the order's `orderMiscOptions`), `reqMktDepth`,
  `reqHistoricalData`, `reqScannerSubscription`, `reqRealTimeBars`,
  `reqNewsArticle`, `reqHistoricalNews` and `reqHistoricalTicks` — the one key
  taken is `manual`, valued 0 or 1. The other two,
  `calculateImpliedVolatility` and `calculateOptionPrice`, take no key at
  all. Another key is refused with 10337 and another value with 10338, on
  `errorEvent` under the request's number, and the request is not sent. Where
  the venue exempts the account from the check (`NOAPIMISCVLD` among its
  `enabledFeatures()`), nothing is refused. `manual` itself is taken and not
  applied: the engine has no field for it.
* **Some options are taken and not applied**: `fundamentalDataOptions`, and
  `ignoreSize` on `reqHistoricalTicks`. The request has nowhere to put them,
  and goes out without them.
* **Some arguments are taken and not applied**, because the venue answers the
  request the same way whatever they name:
  * `groupName` on `reqAccountSummary`, the account on `reqAccountUpdates`, and
    `modelCode` on `reqPnL` and `reqPnLSingle`: a session holds one account,
    and the venue states its figures without being asked which. Another
    account named on `reqPnL` or `reqPnLSingle` is refused.
  * `ledgerAndNLV` on `reqAccountUpdatesMulti`: the venue states the ledger and
    the net liquidation among the account's figures without being asked.
  * `bboExchange` on `reqSmartComponents`: the venue states one table of
    routing components for the session, and the whole table comes back.
* **`IBC` launches nothing, and holds the login it names.** `ib_async_dx.IBC`
  is ib_async's with no gateway to launch: the engine logs in on `connect`,
  and rebuilds a dropped connection on the session it already holds. Starting
  it holds its `userid` and `password`, on a live session where `tradingMode`
  is `'live'` and on paper otherwise, as the gateway it would launch holds a
  login; a connect in the same context that names no login of its own logs in
  with it, and an empty `userid` or `password` is read from `IB_USERNAME` or
  `IB_PASSWORD`. A login named on `connect`, or given to `attach`, comes first.
  Terminating it ends every session that login opened, as stopping a gateway
  ends the sessions connected to it, and from whichever context it is
  terminated, no connect logs in with it afterwards. ib_async's own `Watchdog`, handed one, is
  a reconnect loop: it starts the IBC and connects its `IB` in a task of its
  own, so each `Watchdog` logs in with its own IBC's login, and when the
  session ends it connects again. Its probes and timeouts are ib_async's own.
  A session that ends because another program or a gateway logged in on the
  same login is one the `Watchdog` connects again, which takes it back. The
  paths, the Java settings and the FIX login are a gateway's, and nothing
  reads them. Nor is IBC's own `config.ini`: a login or a `TradingMode` kept
  there is not read, so `userid` and `password` are given to the `IBC` or
  left to the environment, and a `tradingMode` left empty is paper. Where the
  `IBC`'s login is used, its `tradingMode`, not `connect`'s `paper`, decides
  whether the session is live.

## A restart is a new login

The session lives in the program's process and ends with it. A gateway held
its login while a program restarted; here each start logs in, and on a live
account that login waits on the second factor. The session is kept in
`~/.ibkr_dx/session-<user>-<paper|live>` unless `connect` is given another
`sessionFile`, and a start naming a session the venue still holds is answered
with a challenge rather than a full login. The venue lets a session go soon
after its process ends, so that covers a quick restart only.

# What ib_async itself does not have

* **`reqCurrentTimeInMillis`** is in the documented API and not in ib_async.
  `ib_async_dx.IB` adds it, with its `…Async` twin; see
  [Beyond ib_async](./beyond.md).

# The venue's answer, not this package's

These are the venue's answers, as the engine's sessions have met them, and
none is something this package decides. They are written down because a
program meeting one for the first time reads it as a fault here.

* **Market depth depends on the entitlement.** A venue the account is not
  entitled to refuses by name. A book asked for on no particular venue is
  acknowledged and then produces nothing, which is what an account with no
  aggregate entitlement is told. It does not error.
* **News headlines and corporate events need subscriptions.** Without a news
  subscription the providers list comes back and every query is empty; without
  a Wall Street Horizon subscription the calendar's schema arrives and its
  events are empty.
* **The protocol is not published.** The venue can change it without notice.

The engine's own limits — what it measures, and what no session has settled
yet — are set out in [ibkr-dx's documentation](https://userfrm.github.io/ibkr-dx/).

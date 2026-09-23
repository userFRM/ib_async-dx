# Limits

What a caller has to know before writing against ib_async-dx, sorted by what
kind of thing it is: where ib_async on the engine behaves differently from
ib_async over a gateway, what is taken and not applied, and what is the
venue's answer rather than this package's.

ib_async runs as itself, so its calls, its arguments and its answers are its
own. What differs is what its `IB` reads off a transport with no socket
underneath, and what the engine does that a gateway does not. Most of what
cannot be carried is refused by name. What is taken and not applied instead is
named below, where it happens. Where the engine answers as a gateway does, and
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
* **A non-empty `mktDataOptions` or `chartOptions` is refused** with
  `NotImplementedError` naming it, rather than the request going out without
  it and answering something other than what was asked.
* **Every other option list is taken and not applied**: `mktDepthOptions`,
  `realTimeBarsOptions`, `newsArticleOptions`, `historicalNewsOptions`,
  `fundamentalDataOptions`, `miscOptions`, `scannerSubscriptionOptions`,
  `implVolOptions` and `optPrcOptions`, and `ignoreSize` on
  `reqHistoricalTicks`. The request has nowhere to put them, and goes out
  without them.
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
* **`IBC` starts nothing.** `ib_async_dx.IBC` is ib_async's with no gateway to
  start or stop: the engine logs in on `connect`, and rebuilds a dropped
  connection on the session it already holds. ib_async's own `Watchdog`, handed
  one, is a reconnect loop: it connects its `IB`, and when the session ends it
  connects again. Its login is the one `connect` takes with no login arguments,
  `IB_USERNAME` and `IB_PASSWORD` on a paper session. `IBC` given a `userid`, a
  `password` or `tradingMode="live"` raises `ValueError`, rather than open a
  session on another login or on paper. A `Watchdog` keeps another login, or a
  live session, across its reconnects on an `ib_async.IB` handed it by
  `ib_async_dx.attach(ib_async.IB(), username=..., password=..., paper=False)`.
  A session that ends because another program or a gateway logged in on the
  same login is one the `Watchdog` connects again, which takes it back.

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

A gateway answers every one of these the same way. They are written down
because a program meeting one for the first time reads it as a fault here.

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

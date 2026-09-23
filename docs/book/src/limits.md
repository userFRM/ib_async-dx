# Limits

What a caller has to know before writing against ib_async-dx, sorted by what
kind of thing it is: where ib_async on the engine behaves differently from
ib_async over a gateway, what is not carried and why, and what is the venue's
answer rather than this package's.

ib_async runs as itself, so its calls, its arguments and its answers are its
own. What differs is what its `IB` reads off a transport with no socket
underneath, and what the engine does that a gateway does not. Most of what
cannot be carried is refused by name. What is taken and not applied instead is
named below, where it happens.

# Where it differs from a gateway

* **`connect` addresses nothing.** `host` and `port` are accepted and not used,
  and the port does not choose paper or live: the session is paper unless
  `connect` is given `paper=False`. `clientId` is carried into the login.
* **`timeout` bounds what ib_async asks once the session is open** — positions,
  orders, account updates, executions — as it does against a gateway. The login
  itself is not cut short by it: a live login waits on a person.
* **`readonly=True` reaches the session.** ib_async's own `readonly` only skips
  the order requests it makes as it connects. Here the session also refuses to
  send anything that places, changes or withdraws an order.
* **A new order the engine refuses before sending it stays `PendingSubmit`.**
  The refusal — from a read-only session, for one — reaches ib_async's wrapper
  as error 321 while `placeOrder` runs, before ib_async has made the `Trade`.
  With no trade yet to mark, the wrapper leaves the `Trade` it hands back
  `PendingSubmit`, whatever the code. The refusal, with its reason, is on
  `errorEvent`.
* **`serverVersion()`** is a fixed 178, and **`connectionStats()`** states when
  the session started and how long it has run, with its byte and message
  counts at zero.
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
  * `numIds` on `reqIds`: ids are handed out one at a time.
  * `bAutoBind` on `reqAutoOpenOrders`, which ib_async sends as it connects
    with `clientId` 0: the session hears about every order on the account
    either way.
* **`FlexReport`** is not routed through the engine. It fetches a report over
  the web with a token of its own, never touches a session, and runs as it
  always has.
* **IBC and `Watchdog`** start and restart a gateway. There is none to start:
  the engine logs in itself, and rebuilds a dropped connection on the session
  it already holds.

## A restart is a new login

The session lives in the program's process and ends with it. A gateway held
its login while a program restarted; here each start logs in, and on a live
account that login waits on the second factor. The session is kept in
`~/.ibkr_dx/session-<user>-<paper|live>` unless `connect` is given another
`sessionFile`, and a start naming a session the venue still holds is answered
with a challenge rather than a full login. The venue lets a session go soon
after its process ends, so that covers a quick restart only.

# What is not carried, and why

The documented API names a few callbacks that nothing here delivers. None of
them costs an ib_async program anything, because ib_async has no handler for
most of them either.

| Callbacks | Why |
| --- | --- |
| `displayGroupList`, `displayGroupUpdated` | A display group is what a Trader Workstation window is showing, and there is no window here. ib_async's `IB` has no call for them and its wrapper no handler; the two are dropped |
| `verifyMessageAPI`, `verifyCompleted`, `verifyAndAuthMessageAPI`, `verifyAndAuthCompleted` | A handshake a program makes with the gateway it connects to. There is no gateway to make it with, and the engine never fires them. ib_async's wrapper has none of the four |
| `rerouteMktDataReq`, `rerouteMktDepthReq` | Nothing on this connection states a reroute, so nothing fires them. ib_async's wrapper has neither |
| `winError` | An error from the reference client's own socket layer, and there is none here. ib_async's wrapper has no handler for it |
| `tickEFP`, `deltaNeutralValidation` | ib_async's wrapper handles both. The venue states neither an exchange-for-physical quote nor a delta-neutral pairing on this connection, so they never fire, and a `Ticker`'s EFP fields keep their defaults |

ib_async's raw wire — its `Connection`, and its client's `send` and `sendMsg` —
has nothing to write to: there is no socket.

## What ib_async itself does not have

* **`reqCurrentTimeInMillis`** is in the documented API and not in ib_async.
  `ib_async_dx.IB` adds it, with its `…Async` twin; see
  [Beyond ib_async](./beyond.md).
* **`replaceFAEnd`** has no handler in ib_async's wrapper, so the completion of
  `replaceFA` is not reported through ib_async — here, as against a gateway.
* **`tickPrice`** is not a gap. ib_async's wrapper takes a price and its size
  together, as `priceSizeTick`, and the engine's client pairs the two before
  handing them over.

# The venue's answer, not this package's

A gateway answers every one of these the same way. They are written down
because a program meeting one for the first time reads it as a fault here.

* **One session per login.** Opening a second takes the first away, and the
  venue says which host took it. A gateway on the same login is a second
  session.
* **A live login waits on a person.** It enters the venue's second-factor
  approval, which waits on a device. A paper login presents no second factor.
* **Executions are the day's, not the account's history.** `reqExecutions()`
  answers with the executions this session has seen and those the venue
  restated when it opened — the day's, fills on orders already completed among
  them. Anything before today is not available: an empty answer means the venue
  restated none and this session has seen none. `reqCompletedOrders()` asks the
  venue.
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

# Running ib_async itself

`ib_async` is layered. `IB`, `Wrapper`, `Ticker`, `Trade` and everything above
them are transport-agnostic; only its `Client` and `Connection` know there is a
socket to a gateway on localhost. Replacing that one layer runs the rest of the
library unchanged.

That is what `ib_async_dx.IB` does. Its `connect` gives the instance a client
that answers from the engine, `IbkrDxClient`, in place of ib_async's, and then
runs ib_async's own connect. Nothing of `ib_async` is copied or modified: it is
installed as ib_async publishes it, and its classes and modules are left as
they are.

```python
from ib_async_dx import IB, Stock

ib = IB()
ib.connect(username="your_user", password="your_pass")

spy = Stock("SPY", "SMART", "USD")
ib.qualifyContracts(spy)
bars = ib.reqHistoricalData(spy, "", "2 D", "1 hour", "TRADES", useRTH=True)

ib.pendingTickersEvent += lambda tickers: print(len(tickers), "updates")
ib.reqMktData(spy)
ib.sleep(5)
ib.disconnect()
```

Their `IB`, their `Wrapper`, their events, their types — this engine
underneath, and no gateway process.

**An `ib_async.IB` built elsewhere** gets the same client from
`ib_async_dx.attach(ib, username=..., password=...)`, which hands the instance
back ready to connect. A session the instance holds is ended first, as its
`disconnect()` ends one, and the client it replaces is no longer tied to it.
`ib_async_dx.IB` calls it on every connect. An attached `ib_async.IB` is
ib_async's own, so the [bugs fixed](./beyond.md#ib_asyncs-bugs-fixed) on
`ib_async_dx.IB` are still there.

## The session's life

- **Callbacks run on the loop, a pass at a time.** A pass hands their wrapper
  what the engine holds, as one batch: `tcpDataArrived` before its first
  message, `tcpDataProcessed` after its last, as their transport marks a packet.
  The loop makes each pass itself, 10 ms after the last, so a program away from
  its loop — in `time.sleep`, say — queues nothing, and one whose loop ends
  without a `disconnect()` leaves no pass running. A refusal stated inside a
  request call waits for the next pass, as a gateway's comes back on the socket
  once the call has returned. A handler that ends the session ends the pass:
  nothing more of it reaches their wrapper, as their client drops what it had
  read once its socket is closed.
- **The login runs off the loop**, which keeps turning while it waits. What the
  engine announces from inside it — the connect acknowledgement, the accounts
  and the next id — reaches nothing there, on the login's thread; the same
  three are said on the loop once it returns. Their wrapper is called on the
  loop's thread and no other.
- **`timeout`** bounds each request of ib_async's startup sync, as it does
  against a gateway. The login is the engine's to bound, the engine's wait of
  up to three seconds for the venue to name the working orders among it: a
  paper login presents no second factor, and a live one waits on it for as
  long as the engine allows, as a gateway's login is made before a program
  connects.
- **A connect ends at once when it is cancelled** — by `wait_for`, a task
  cancel or an interrupt — **or overtaken** by `disconnect()` or by another
  connect. The engine is told, and drops the session its login opens instead
  of keeping it. The login itself runs on inside the engine until the engine
  returns — on a live account, until the second factor is answered or its wait
  runs out — on a thread that does not hold the program open. An overtaken
  connect raises `ConnectionError`. Of two connects on one `ib_async_dx.IB`,
  the later one's session is the one left open, whenever the later one is
  made. An attached `ib_async.IB` runs ib_async's own connect, which does not
  stand down for another: there a connect overtaken during ib_async's startup
  sync, once the engine has logged in, can still report success on the later
  session, or end it.
- **A login that fails raises `ConnectionError`**, `apiError` says why, and the
  client reads disconnected, as their client fails a connect.
- **A session the engine ends as it opens** fails the connect, with nothing
  said on `disconnectedEvent`: their client says nothing of a socket that
  closed before the API was ready either.
- **A session the engine ends once open** is handled as their client handles a
  dropped socket, once: every waiting request fails, `disconnectedEvent`
  fires, and delivery stops. A pass that raises ends the session the same way,
  as their transport closes a socket whose data it could not handle.
- **A `connect` on an `IB` that is connected** ends that session first, as
  `disconnect()` ends one, and opens the next, as their client closes its
  socket before it opens another.

## What is carried

Their `IB` calls 67 methods on the transport layer, counted from their own
source at 2.1.0. All of them are carried here. A test reads that list out of
their source on every run rather than checking a list kept by hand, so a name
they add is a failure here rather than a program that stops.

**Their types are theirs.** A callback carrying an object hands over one of
this engine's, and their wrapper reads one of theirs, so every argument is
rebuilt on the way through — by its own type name, from their dataclass. A
field they have and this engine does not keeps its default, and neither side
needs editing when the other gains one. A fill's cost reaches them as their
`CommissionReport`, a histogram as their `HistogramData`, a historical tick as
their own record.

**Their requests are carried whole.** A contract or an order going the other
way is rebuilt as the engine's, field by field, whatever the field: a
combination keeps its legs, an algo its parameters, an order its soft-dollar
tier and its conditions, joined by and or by or. A field is carried as their
client sends it, one at their own default among them; what their client sends
as an empty field (None, an empty string or list, their unset number) is left to
the engine's own default. A value its field cannot take — text where a number
goes — is refused with 320 on `errorEvent`, under the request's number, as a
gateway refuses a message it cannot read, rather than the request going out on
terms nobody stated.

**An order is what their client writes.** Their `placeOrder` writes the order
as its message, and the message is read back as a gateway reads it, so what
reaches the engine is what a gateway would be sent: a field their client does
not write is not carried, and `volatility`, which their client clears on any
order but a volatility order, is cleared. Their client also writes
`eTradeOnly`, `firmQuoteOnly` and `nbboPriceCap`, which the venue no longer
takes, and an order stating one is answered as a gateway answers it: where the
venue has retired them for the account, refused with 10268, 10269 or 10270
under the order's number; otherwise placed without it, with the notice 2168,
2169 or 2170.

**Orders and requests are numbered from one counter**, as their client numbers
them, so an order never takes the number of a request still waiting. The
counter starts past every id the account has used that a request can carry,
which the venue names at every connect, and is kept past every one it names
after: a new order never takes an id a fill has already spent, and an order
placed elsewhere under an id wider than a request can carry leaves every
request numberable.

**Their client's own messages are the requests they name.** `send` is their
client's own, writing the fields as one message, and `sendMsg` reads a
message back, as a gateway reads one, into the request their client writes it
for, and answers it as a gateway does. A message naming no request their
client writes is logged on `ib_async_dx.bridge`, and nothing answers it. One
that does not read as the request it names is refused with 320 on
`errorEvent`, under that request's number where it was read before the field
that failed, and under -1 before it. Either way the session carries on.

A few details are worth knowing:

- A bar's date is handed over in the spelling their own parser reads. Their
  `parseIBDatetime` decides the shape from the string — eight digits is a day,
  a date and a time and a zone separated by single spaces is an aware moment —
  and the frame example in their own tests calls `tz_convert` on the date
  column, which refuses a naive datetime.
- A price and the size that goes with it reach them together, as their
  `priceSizeTick`, and a size that changed on its own as their `tickSize`, as
  a gateway sends them.
- A refusal reaches their `error` in the four-argument shape their wrapper
  declares, as a gateway's does, and after the call that caused it has
  returned. So a new order refused before it is sent reaches the `Trade`
  `placeOrder` handed back, which their wrapper marks as it marks one a gateway
  refused: `Cancelled` for an error, and, on `ib_async_dx.IB`, for 321, which
  ib_async counts as a warning ([why](./beyond.md#ib_asyncs-bugs-fixed)).
- An option list is checked as a gateway checks one: the one key taken is
  `manual`, valued 0 or 1, none is taken on the two option computations, and
  another key or value is refused with 10337 or 10338, under the request's
  number, with nothing sent. `manual` is not carried: see
  [Limits](./limits.md).
- An order's status names the client that placed it, as a gateway's does, so
  their wrapper finds the `Trade` an order another client placed is kept under.
- A record their wrapper builds whole arrives whole: a routing component, a
  family code. A bar's average price arrives as its `average`, and a
  condition says how it joins the next. A callback that cannot be rebuilt, or
  that their wrapper raises on, is logged on `ib_async_dx.bridge` and passed
  over, as their decoder treats a message it cannot handle, and the session
  carries on.
- Every account the login holds is listed by `managedAccounts()`, not only the
  first.
- `isConnected()` answers as their client does. An outage the engine is still
  mending (1100 until 1102) leaves the session connected.
- `disconnect()` does not call their `connectionClosed`, as their own client's
  does not. Their wrapper treats that as a session that went away underneath
  them: it fails every request still waiting and raises on their global error
  event. That is right for a socket that dropped and wrong for a caller who
  asked to stop.
- An `IB` connects again after it disconnected. Each session and each connect
  is counted, and nothing kept for an earlier one — a pass, a login, a held
  refusal — reaches the next.
- A request made while not connected raises `ConnectionError("Not connected")`,
  as their client's does. A request reached by name on `ib.client` takes its
  arguments as their client's method does, by position or by keyword; `connect`
  and `run` on it are their client's own, and `reset` ends the session.
- Request ids stop at the widest a request can carry: the rest of the range
  is the engine's own. An account whose ids reach it has `getReqId()` raise
  `OverflowError` rather than number a request the engine refuses.
- `updateEvent` fires, and their wrapper's `lastTime` moves, only on a pass
  of the engine that delivered something, as they do over a socket only when
  data arrives. A session with nothing arriving stays quiet, so `setTimeout`
  fires `timeoutEvent`.

## Their own test suite

The strongest available statement about whether their library runs here is
their own tests. They are not copied here; point a run at a checkout of theirs:

```bash
git clone https://github.com/ib-api-reloaded/ib_async /tmp/ib_async
git -C /tmp/ib_async checkout ab629f34c1    # 2.1.0
cp tests/ib_async_upstream/conftest.py /tmp/ib_async/tests/
IB_USERNAME=… IB_PASSWORD=… pytest /tmp/ib_async/tests \
    -o asyncio_mode=auto \
    -o asyncio_default_fixture_loop_scope=session \
    -o asyncio_default_test_loop_scope=session
```

Both loop scopes are needed: their session-scoped connection fixture and their
tests must share one event loop, or callbacks land on a loop that is not
running while the test waits on them. pandas has to be installed too: their
`test_contract.py` imports it, and without it the run stops at collection.

The conftest replaces their shared `ib` fixture with an `ib_async_dx.IB`,
connected with the login in the environment, and makes `ib_async.IB` this
package's, so every test runs on the engine. At 2.1.0 their suite is three
tests, taken from their commit `ab629f34c1`; none has been run against the venue
at this revision:

| Test | What it asks, and what holds offline |
| --- | --- |
| `test_account_summary` | The account summary, through their `ib` fixture |
| `test_request_error_raised` | A `RequestError` carrying 321 from a refused what-if. ib_async 2.1 counts 321 as a warning, which never ends a request, so against any server that answers 321 its own `IB` waits for good; `ib_async_dx.IB` ends the request with the refusal, and `test_a_what_if_refused_with_321_ends_with_the_refusal` holds that offline |
| `test_contract_format_data_pd` | It builds its own `ib_async.IB()` and connects it to `127.0.0.1:4001` rather than taking the fixture. The conftest makes `ib_async.IB` this package's before their tests are collected, in that run only, so that `IB` connects to the engine whatever host and port it names |

## What it does not carry

Some of what their `IB` reads off its client answers for a transport that has
no socket. `connectionStats()` counts the messages each way, as their client
does: a request is one sent, and what reaches their wrapper one received, over
the session since it opened. Its byte counts are zero: the engine does not
count the bytes of its connections. `throttleStart` and `throttleEnd` never
fire, and `MaxRequests` and `RequestsInterval` are theirs and set nothing:
their client paces what it writes to a gateway's socket, and nothing between
the program and the venue paces requests here. And their client's `conn`, the
socket connection, is not there.

Everything on their `IB` is routed. The rest of what differs is in
[Limits](./limits.md).

## Where it answers as a gateway does

A program meeting one of these for the first time may read it as a difference.
Each is what ib_async over a gateway does too.

- `readonly=True` makes a read-only session, which refuses to send anything that
  places, changes or withdraws an order, as a gateway set to read-only does.
- `serverVersion()` is 178 once connected, the version a current gateway
  settles on with ib_async 2.1, and 0 until then, as their client answers it.
- `reqExecutions()` answers with the day's executions: those this session has
  seen and those the venue restated when it opened, fills on orders already
  completed among them. ib_async 2.1's request cannot ask for more.
  `reqCompletedOrders()` asks the venue.
- `numIds` on `reqIds` changes nothing: ids are handed out one at a time.
- A request their client makes that the engine does not carry — `verifyRequest`
  and the three after it, the handshake a program makes with the gateway it
  connects to — is taken, and nothing answers it, as a gateway answers
  nothing. A name their client does not have is a missing attribute, as on
  theirs.
- Callbacks that reach nothing in ib_async over a gateway reach nothing here.
  Their wrapper has no handler for `displayGroupList`, `displayGroupUpdated`,
  `rerouteMktDataReq`, `rerouteMktDepthReq` or `replaceFAEnd`, discards
  `deltaNeutralValidation`, and is never sent `verifyMessageAPI`,
  `verifyCompleted`, `verifyAndAuthMessageAPI`, `verifyAndAuthCompleted` or
  `tickEFP`, so a `Ticker`'s EFP fields keep their defaults. `winError` is no
  message at all. A request a gateway would reroute is itself answered
  differently: see [Limits](./limits.md).
- `FlexReport` fetches a report over the web with a token of its own and never
  touches a session, here or over a gateway.

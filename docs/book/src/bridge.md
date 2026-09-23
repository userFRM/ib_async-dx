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
back ready to connect. `ib_async_dx.IB` calls it on every connect.

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
tier. A field still at their default is left to the engine's own. A field set to
something the engine cannot carry raises a `ValueError` that names it, rather
than the order going out on terms nobody stated.

**Orders are numbered from the account.** Their client numbers orders and
requests out of one counter. Here the two are counted apart: an order they leave
unnumbered is numbered from what the account has used, since the venue refuses
an id a fill has already spent, and an account whose order ids have grown past
what a request id can carry still leaves every request numberable.

A few details are worth knowing:

- A bar's date is handed over in the spelling their own parser reads. Their
  `parseIBDatetime` decides the shape from the string — eight digits is a day,
  a date and a time and a zone separated by single spaces is an aware moment —
  and the frame example in their own tests calls `tz_convert` on the date
  column, which refuses a naive datetime.
- A price and its size reach them together, as their `priceSizeTick`. A size
  stated before any price is sent with their own "no price", because a zero
  there is a market quoted at nothing.
- A refusal reaches their `error` in the four-argument shape their wrapper
  declares. Handed five, it would raise on the first notice of the session.
- Every account the login holds is listed by `managedAccounts()`, not only the
  first.
- `isConnected()` answers as their client does. An outage the engine is still
  mending (1100 until 1102) leaves the session connected. A session the engine
  ends is handled as their client handles a dropped socket: every waiting
  request fails, `disconnectedEvent` fires, and the session reads as not
  connected.
- `disconnect()` does not call their `connectionClosed`, as their own client's
  does not. Their wrapper treats that as a session that went away underneath
  them: it fails every request still waiting and raises on their global error
  event. That is right for a socket that dropped and wrong for a caller who
  asked to stop.
- An `IB` connects again after it disconnected. Delivery still queued from
  the first session is skipped once the program has disconnected, so it cannot
  reach their wrapper during the next connect and cancel it. A `connect` on an
  `ib_async_dx.IB` that is already connected closes that session first, as
  their client closes its socket.
- A request made while not connected raises `ConnectionError("Not connected")`,
  as their client's does.
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
connected with the login in the environment, so every test that takes that
fixture runs on the engine. At 2.1.0 their suite is three tests:

| Test | Here |
| --- | --- |
| `test_account_summary` | Passes on the engine |
| `test_request_error_raised` | Cannot pass against any server. Its last line asserts a `RequestError` carrying 321, and 321 is in their own `warningCodes` frozenset, where a warning never ends the request it belongs to, so the error it waits for is never raised |
| `test_contract_format_data_pd` | Never reaches the engine. It builds its own `IB` and connects it to `127.0.0.1:4001` rather than taking the fixture, so the conftest cannot replace it; with no gateway on that port it fails to connect |

## What it does not carry

`FlexReport` reads a report over the web. It is a class of its own, not a method
on their `IB`, and it never touches a session, so it runs as it always has.
Everything on their `IB` is routed.

Two things their `IB` reads off its client answer for a transport that has no
socket. `serverVersion()` is a fixed 178. `connectionStats()` states when the
session started and how long it has run, and its byte and message counts are
zero: there is no socket to count.

A request their client carries and the engine does not raises
`NotImplementedError` naming it, rather than failing as a missing attribute.
None of the 67 their `IB` makes is one of those. The rest — display groups,
callbacks the venue never sends — is in [Limits](./limits.md).

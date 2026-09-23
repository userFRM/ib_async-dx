# What drop-in means

The promise is the one an ib_async user would ask for: **an existing program
changes its import and its connect line, and nothing else.** The connect line
gains credentials, or keeps its host and port where `IB_USERNAME` and
`IB_PASSWORD` are set, because nothing local is addressed.

```diff
- from ib_async import IB, Stock
+ from ib_async_dx import IB, Stock

  ib = IB()
- ib.connect("127.0.0.1", 4001, clientId=1)    # a gateway on localhost
+ ib.connect(username="...", password="...")   # a paper session; paper=False for live
```

## What the import gives

`ib_async_dx` is ib_async. It exports ib_async's `__all__`, all 103 names, and
every one of them is ib_async's own object — the same class, the same function,
the same enum — except two:

* **`IB`**, a subclass of `ib_async.IB`. Its `connect` and `connectAsync` put
  the engine in place of ib_async's `Client` and then run ib_async's own
  connect. Everything else on it is ib_async's, apart from the
  [two bug fixes and the added calls](./beyond.md).
* **`__version__`**, which is this package's version. `__ib_async_version__`
  names the ib_async it runs, and `__version_info__` stays ib_async's, so a
  program that checks the API level reads what it always did.

ib_async's submodules resolve here as themselves, so both import forms work:
`import ib_async_dx.order` and `from ib_async_dx.util import df`.
`ib_async_dx.ib` is ib_async's `ib` module with `IB` replaced. Because the
objects are ib_async's own, `isinstance` checks, pickles and a module that
still imports `Stock` from `ib_async` all agree with the rest of the program.

## The connect line

`connect` is ib_async's signature, with its parameters and defaults, and four
keyword-only parameters after them: `username`, `password`, `paper` and
`sessionFile`. [Getting started](./getting-started.md#what-connect-takes)
writes it out.

* `host` and `port` are accepted and not used. The port does not choose paper
  or live: the session is paper unless `paper=False`.
* `clientId` is carried into the login, where it keys this session's orders.
* `username` and `password`, left empty, are read from `IB_USERNAME` and
  `IB_PASSWORD`.
* `readonly=True` also reaches the session, which then refuses to send
  anything that places, changes or withdraws an order.
* `timeout` bounds the requests ib_async makes once the session is open, not
  the login.

The rest is ib_async's own connect: as it does against a gateway, it asks for
the account's values, its positions, its open and completed orders and its
executions, as `fetchFields` says.

## How it is proven

**The package against ib_async, on every run.**
`test_the_package_is_ib_async.py` checks that `ib_async_dx.__all__` equals
ib_async's and that every name in it is ib_async's own object but `IB` and
`__version__`; that all 13 of ib_async's submodules resolve under both import
forms; that `IB` is a subclass of ib_async's; and that its `connect` and
`connectAsync` are ib_async's parameters, defaults and kinds with the four
keyword-only ones after them. That is the parity check: a name ib_async adds
to `__all__`, or a change to its `connect`, fails here before it reaches a
program.

**Against ib_async's own source, on every run.**
`test_every_call_their_library_makes_is_carried` reads every call ib_async's
`IB` makes on its client out of ib_async's installed source — 67 at 2.1.0 — and
fails if any one of them does not land on the engine. It is not a list kept by
hand, so a call ib_async adds is a failing test here rather than a program that
stops.

**Against ib_async's own tests.** `tests/ib_async_upstream/conftest.py` runs
ib_async's test suite against the engine instead of against a gateway: every
test that uses their shared `ib` fixture gets an `ib_async_dx.IB`, connected
with the login in the environment. The suite is theirs, fetched from their
repository rather than copied into this one. At 2.1.0 one of its three tests
passes here, one cannot pass on any server, and one opens its own connection to
a gateway and never reaches the engine; the detail, and how to run it, is in
[Running ib_async itself](./bridge.md#their-own-test-suite).

**Against the venue.** Two tests take a live login, each with ib_async's own
`IB` attached. One runs an unmodified ib_async program — connect, qualify,
bars, quotes through `pendingTickersEvent`. The other takes an order through
its whole life in ib_async's objects: priced as a what-if, placed, changed and
withdrawn. Both are skipped where `IB_USERNAME` and `IB_PASSWORD` are not set.
The scripts under `scripts/` do the same through `ib_async_dx.IB` against a
paper account, and the [notebooks](./notebooks.md) do it a cell at a time.
Neither has been run against the venue through `ib_async_dx.IB` yet, so
[Evidence](./evidence.md) rates that path ✅ Offline.

**What crosses, offline.** The other 49 tests need no session: that a
combination keeps its legs on every request path, that a fill's cost reaches
ib_async as its own `CommissionReport`, that `connect` takes its login from the
environment and is paper unless told otherwise, that each bug fix answers what
ib_async 2.1 does not. [Evidence](./evidence.md) lists what they cover.

## In Rust

Coming: ib_async's model and names, in Rust spelling, on the engine's public
API. It is not in this repository yet.

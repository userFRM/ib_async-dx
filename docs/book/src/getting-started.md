# Getting started

## What you need

* An Interactive Brokers account, paper or live. No IB software: no IB
  Gateway, no Trader Workstation, and not the `ibapi` package either.
* Python 3.11 or newer.
* A Rust toolchain, 1.89 or newer. The engine installs from its repository,
  and installing it compiles it.

ib_async 2.1 is a dependency of this package, and pip installs it with it.
Nothing is on PyPI yet: ib_async-dx and the engine both install from their
repositories.

## Install

```bash
pip install "git+https://github.com/userFRM/ibkr-dx"
pip install "ib_async-dx @ git+https://github.com/userFRM/ib_async-dx"
```

The engine goes first. ib_async-dx names it as a dependency, and with nothing
on PyPI, pip has to find it already installed.

## Credentials

The credentials go to the session, through `connect`. There is no
configuration file, and no process holding a login on your behalf.

```python
from ib_async_dx import IB

ib = IB()
ib.connect(username="your_user", password="your_pass")
```

Left out, they are read from `IB_USERNAME` and `IB_PASSWORD`, and the scripts
under `scripts/` read the same two:

```bash
export IB_USERNAME="your_username"
export IB_PASSWORD="your_password"
```

**There is no host to name.** `connect` still takes a host, a port and a client
id, because ib_async's does. The host and port are not used: the engine finds
the server the account lives on by itself. The client id is carried into the
login, where it keys this session's orders.

**`paper`** is `True` unless the program says otherwise; the port does not
choose. A live session is asked for with `paper=False`, and that enters the
venue's second-factor approval, which waits on a device: `connect` returns once
it has been answered. A paper session presents no second factor. Use a paper
account while you are writing something; a live account is a live account.

That wait is why `connect`'s `timeout` does not bound the login. The login runs
off ib_async's event loop, so the loop keeps turning while it waits, and
`timeout` bounds the requests ib_async makes once the session is open, as it
does against a gateway.

> [!IMPORTANT]
> One session per login. Opening a second takes the first away, and the venue
> says which host took it. A program here and a gateway on the same login take
> the session from each other; use a second login to run both.

> [!CAUTION]
> Credentials are the account. Never commit them, never paste them into an
> issue, and never put them in a file the repository tracks.

## A first program

An ib_async program, as ib_async writes one, with the import and the connect
line changed:

```python
from ib_async_dx import IB, Stock

ib = IB()
ib.connect()                                 # credentials from the environment, paper

spy = Stock("SPY", "SMART", "USD")
ib.qualifyContracts(spy)
bars = ib.reqHistoricalData(spy, "", "2 D", "1 hour", "TRADES", useRTH=True)
print(bars[-1])

ib.pendingTickersEvent += lambda tickers: print(len(tickers), "updates")
ib.reqMktData(spy)
ib.sleep(5)
ib.disconnect()
```

`ib.sleep()`, never `time.sleep()`. ib_async runs its event loop on the calling
thread, so a plain sleep stops it: quotes stop arriving, and every stream reads
as dead when it is only unattended.

## What `connect` takes

ib_async's parameters, with its defaults, and four keyword-only ones after
them:

```python
ib.connect(
    host="127.0.0.1", port=7497, clientId=1,    # host and port: accepted, not used
    timeout=4, readonly=False, account="",
    raiseSyncErrors=False, fetchFields=StartupFetchALL,
    *,
    username="", password="",                   # left empty: IB_USERNAME, IB_PASSWORD
    paper=True,                                 # False asks for a live session
    sessionFile=None,                           # where the session is kept between runs
)
```

`connectAsync` takes the same. `readonly=True` also makes the session refuse to
send anything that places, changes or withdraws an order; ib_async's own
`readonly` only skips the order requests it makes as it connects.

**The session is kept between runs.** With `sessionFile=None` it is kept in
`~/.ibkr_dx/session-<user>-<paper|live>`: readable by its owner only, sealed
with the password, and refused if it names another account. A start that names
a session the venue still holds is answered with a challenge rather than a full
login. The venue lets a session go soon after the process holding it ends, so
this covers a quick restart; a program started again later logs in afresh, and
on a live account that login waits on the second factor. A path moves the file,
and `sessionFile=False` keeps nothing.

## An `ib_async.IB` you already hold

Code that builds `ib_async.IB()` itself can keep doing so, and hand the
instance to `attach`:

```python
import ib_async
import ib_async_dx

ib = ib_async_dx.attach(ib_async.IB(), username="your_user", password="your_pass")
ib.connect()                      # names no host: there is no gateway
```

`attach` puts the engine under that one instance and hands it back; nothing of
ib_async's classes or modules is patched. It takes the same login as `connect`,
spelled `username`, `password`, `paper`, `session_file`, `client_id` and
`readonly`. It is what `ib_async_dx.IB` does on every connect.

## Next steps

* [What drop-in means](./drop-in.md) — the promise, and how it is proven
* [Running ib_async itself](./bridge.md) — what the engine changes, what it
  carries, and how ib_async's own tests run here
* [Beyond ib_async](./beyond.md) — the two bugs fixed, and the calls ib_async
  has no name for
* [Notebooks](./notebooks.md) — seven of ib_async's eight notebook subjects,
  with no gateway
* [Limits](./limits.md) — read this before you depend on a call

# ib_async, without a gateway

Eight notebooks, one on each of ib_async's own subjects, written against
[`ib_async`](https://github.com/ib-api-reloaded/ib_async) itself — its `IB`, its
contracts, its events, its `util.df` — and run on the ibkr-dx engine instead of
on a gateway.

`ib_async` (BSD-2-Clause) is not copied or modified here: it is a dependency,
and its own code runs. ib_async-dx is not affiliated with ib_async or with
Interactive Brokers. ib_async is layered: everything above
`Client`/`Connection` is transport-agnostic, and only that layer knows there is
a socket to a local process. `ib_async_dx.IB` is ib_async's `IB` with the
engine in that layer's place, so the import and the connect call are what
differ from the library's own notebooks:

```python
from ib_async_dx import IB, util

ib = IB()
ib.connect(username="...", password="...")   # paper unless paper=False
```

`IB.connect` still takes a host, a port and a client id, because it was written
for a gateway. The host and port are never used; the client id is carried into
the login.

## Running them

```bash
pip install "git+https://github.com/userFRM/ibkr-dx@335fa89d259cde347881de88460c85d0e5670608"
pip install "ib_async-dx @ git+https://github.com/userFRM/ib_async-dx" \
    jupyter python-dotenv pandas
git clone https://github.com/userFRM/ib_async-dx
jupyter lab ib_async-dx/notebooks
```

The engine compiles from source, so it needs a Rust toolchain, 1.89 or newer.
The notebooks need Python 3.11 to 3.13: `util.startLoop()` applies
nest_asyncio, which cannot run asyncio's timeouts on 3.14.
`util.df` needs pandas, which ib_async does not install on its own.
Credentials come from a `.env` file at the repository root, as `IB_USERNAME`
and `IB_PASSWORD`; keep it out of version control. Every notebook opens a paper
session, and `ordering` places a real order on it: a buy at half the last
price, which a later cell withdraws.

## One thing to keep

`ib.sleep()`, never `time.sleep()`. The library runs its event loop on the
calling thread, so a plain sleep stops it: quotes stop arriving, and every
stream reads as dead when it is only unattended.

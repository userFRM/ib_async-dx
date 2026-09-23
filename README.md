# ib-async-dx

**ib_async without the gateway.** The same API a program written for
[ib_async](https://github.com/ib-api-reloaded/ib_async) already uses, running on
the [IBKR-DX](https://github.com/userFRM/ibkr-dx) engine, which talks to
Interactive Brokers' servers directly: no IB Gateway, no Trader Workstation,
no JVM. In Python, and with the same model in Rust.

```diff
- from ib_async import IB
- ib.connect("127.0.0.1", 4001, clientId=1)     # needs a gateway running
+ from ib_async_dx import IB
+ ib.connect(username="...", password="...")    # no external process
```

> [!NOTE]
> Early. The API is being brought to exact parity with ib_async, measured
> method by method. Nothing is published to PyPI or crates.io yet.

## Two ways in

| | What runs | When |
| --- | --- | --- |
| `ib_async_dx.IB` | ib_async's API, implemented on the engine | The default. Being brought to exact parity with ib_async, method by method. |
| `ib_async_dx.attach(ib_async.IB(), ...)` | The real ib_async library, unmodified, with the engine in place of its socket | When you want ib_async itself. Needs the `bridge` extra. |

```python
import ib_async
import ib_async_dx

ib = ib_async_dx.attach(ib_async.IB(), username="...", password="...", paper=True)
ib.connect()
```

## Install

Neither package is on PyPI yet. Both build from their repositories:

```bash
pip install "git+https://github.com/userFRM/ibkr-dx"
pip install "ib-async-dx[bridge] @ git+https://github.com/userFRM/ib-async-dx"
```

## Layout

| Path | What |
| --- | --- |
| `python/ib_async_dx` | The Python package |
| `tests/python` | Its tests |
| `tests/ib_async_upstream` | Runs ib_async's own test suite against the engine (see [docs/ib_async.md](docs/ib_async.md)) |
| `notebooks` | ib_async's notebook subjects, without a gateway |
| `scripts` | Checks against a paper account |

The Rust client, the same model in Rust, is being rebuilt on IBKR-DX's public
API and lands here when it is.

## Not affiliated

An independent project. It is not affiliated with ib_async or its
maintainers, nor with Interactive Brokers. ib_async's public API is the
specification it is held to; its code is not copied here.

## License

[AGPL-3.0](LICENSE), the same as IBKR-DX.

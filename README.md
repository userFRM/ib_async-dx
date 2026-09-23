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

## Layout

| Path | What |
| --- | --- |
| `python/ib_async_dx` | The Python package (`pip install` from this repository) |
| `src/` | The Rust crate |

Both depend on [`ibkr-dx`](https://github.com/userFRM/ibkr-dx), the engine.

## Not affiliated

An independent project. It is not affiliated with ib_async or its
maintainers, nor with Interactive Brokers. ib_async's public API is the
specification it is held to; its code is not copied here.

## License

[AGPL-3.0](LICENSE), the same as IBKR-DX.

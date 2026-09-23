<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/userFRM/ib_async-dx/main/docs/book/src/banner-dark.svg">
    <img src="https://raw.githubusercontent.com/userFRM/ib_async-dx/main/docs/book/src/banner-light.svg" alt="ib_async-dx: ib_async, without the gateway" width="100%">
  </picture>
</p>

<p align="center">
  <strong>ib_async, with no gateway. No JVM, no window, no process to keep alive.</strong>
</p>

<p align="center">
  <a href="https://github.com/userFRM/ib_async-dx/actions/workflows/tests.yml"><img src="https://github.com/userFRM/ib_async-dx/actions/workflows/tests.yml/badge.svg" alt="Build"></a>
  <img src="https://img.shields.io/badge/python-3.11+-blue.svg" alt="Python version">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-AGPL--3.0-blue.svg" alt="License"></a>
  <a href="https://userfrm.github.io/ib_async-dx/"><img src="https://img.shields.io/badge/docs-book-green.svg" alt="Docs"></a>
  <a href="https://github.com/userFRM/ibkr-dx"><img src="https://img.shields.io/badge/engine-ibkr--dx-red.svg" alt="Engine: ibkr-dx"></a>
</p>

## Contents

**Start here** — [The two lines that change](#the-two-lines-that-change) · [Why this exists](#why-this-exists) · [Installation](#installation) · [Quick start](#quick-start)

**Moving across** — [Drop-in, and how it is proven](#drop-in-and-how-it-is-proven) · [Same, better, left out](#same-better-left-out) · [Beyond ib_async](#beyond-ib_async) · [Rust](#rust) · [Notebooks](#notebooks)

**The honest parts** — [Questions](#questions) · [Testing](#testing)

**Around it** — [Relationship to ibkr-dx](#relationship-to-ibkr-dx) · [Documentation](#documentation) · [Contributing](#contributing) · [Security](#security) · [Not affiliated](#not-affiliated) · [License and credits](#license-and-credits)

## The two lines that change

A program written against [ib_async](https://github.com/ib-api-reloaded/ib_async)
talks to IB Gateway or Trader Workstation over a socket on localhost, and that
process talks to the venue. ib_async-dx puts the
[ibkr-dx](https://github.com/userFRM/ibkr-dx) engine where the socket was: it
logs in itself, holds the session inside your process, and the program above it
stays as it is.

```diff
- from ib_async import IB, Stock
+ from ib_async_dx import IB, Stock

  ib = IB()
- ib.connect("127.0.0.1", 4001, clientId=1)    # a gateway on localhost
+ ib.connect(username="...", password="...")   # a paper session; paper=False for live
```

> [!TIP]
> `ib_async_dx` is ib_async: the same 103 names, the same objects and the same
> submodules, from the copy of ib_async installed with it. The one class that
> differs is `IB`, a subclass of ib_async's whose `connect` takes a login instead
> of a gateway's address. Credentials left out of `connect` are read from
> `IB_USERNAME` and `IB_PASSWORD`, so where those are set the connect line can
> stay as it was for a paper account: the host and port are not used, and the
> port does not choose paper or live. Add `paper=False` for a live account.

## Why this exists

ib_async gives a Python program a clean API to Interactive Brokers, and every
program written with it depends on a process it does not own: IB Gateway or
Trader Workstation, a Java application that has to be installed, logged in, and
kept alive for as long as the program is.

That arrangement costs four things:

1. **An operational dependency.** Something has to start it, watch it, restart
   it, and log in again when it drops. In a container or over ssh that is work
   you did not want.
2. **A second failure mode.** Your program can be healthy while the thing it
   depends on is wedged, and the two do not agree about it.
3. **A ceiling.** The heap is finite, and bulk historical data is what finds the
   edge of it.
4. **A narrower view than the terminal's.** The gateway receives far more from
   the venue than it forwards. Whatever has no message in the documented API
   never reaches you.

The engine underneath this package speaks the venue's protocol itself. The first
three go with the gateway. The fourth goes as far as you want it to: ib_async's
API stays what it was, and what the venue states beyond it is
[a method away](#beyond-ib_async). ib_async-dx keeps the API you already write
against and removes the process behind it.

### What this removes

* **The gateway process** — nothing to install, launch, log into, or restart
* **The JVM** — no heap to size
* **The localhost socket** — the session lives in your process
* **The window** — runs headless, in a container, over ssh
* **The login automation** — nothing drives a login window; the program logs in with its own credentials

### Requirements

* An Interactive Brokers account, paper or live
* Python 3.11 or newer
* A Rust toolchain, 1.89 or newer, while the engine is installed from source
* ib_async 2.1, which pip installs with this package

No IB software is required, and not the `ibapi` package either.

> [!IMPORTANT]
> A live login enters the venue's second-factor approval, which waits on a
> device. Paper logins do not. One session per login: opening a second takes
> the first away, and the venue says which host took it.

## Installation

The engine first, then this package:

```bash
# The engine. It compiles from source, so it needs the Rust toolchain.
pip install "git+https://github.com/userFRM/ibkr-dx"

# This package, which brings ib_async 2.1 with it.
pip install "ib_async-dx @ git+https://github.com/userFRM/ib_async-dx"
```

> [!NOTE]
> Neither ib_async-dx nor the engine is on PyPI or crates.io yet, so both
> install from their repositories.
> The order matters: ib_async-dx names the engine (`ibkr-dx`) as a dependency,
> and pip finds it only once the first line has installed it.

## Quick start

```python
from ib_async_dx import IB, Stock

ib = IB()
ib.connect(username="your_user", password="your_pass")   # paper unless paper=False

spy = Stock("SPY", "SMART", "USD")
ib.qualifyContracts(spy)
bars = ib.reqHistoricalData(spy, "", "2 D", "1 hour", "TRADES", useRTH=True)
print(bars[-1])

ib.pendingTickersEvent += lambda tickers: print(len(tickers), "updated")
ib.reqMktData(spy)
ib.sleep(5)                       # ib.sleep, not time.sleep: the loop runs on this thread

ib.disconnect()
```

ib_async's `IB`, its `Wrapper`, its events, its types — this engine underneath,
and no gateway process. How that works, and what it carries, is on
[Running ib_async itself](https://userfrm.github.io/ib_async-dx/bridge.html).

**Code that already holds an `ib_async.IB`** can keep it:
`ib_async_dx.attach(ib, username="...", password="...")` puts the engine under
that instance and hands it back, and its `connect()` then names no host. It is
what `ib_async_dx.IB` does on every connect.

## Drop-in, and how it is proven

**Drop-in means one thing here:** a program written for ib_async runs with the
two lines above changed and nothing else, and does what it did over a gateway.
That is a claim about behaviour, so it rests on running things rather than on
reading code:

| What is run | What it shows | Where |
| --- | --- | --- |
| The package, against ib_async | `ib_async_dx.__all__` is ib_async's, and every name in it is ib_async's own object except `IB` and `__version__`. All 13 of its submodules resolve here, as `import ib_async_dx.contract` and as `from ib_async_dx.util import df`. `connect` is ib_async's signature with four keyword-only parameters after it | [`tests/python/test_the_package_is_ib_async.py`](tests/python/test_the_package_is_ib_async.py) |
| Their transport, read from their source | Their `IB` makes 67 distinct calls on its transport at 2.1.0, and every one lands here. The list is read out of their installed source on every run, so a call they add fails here before it fails a program | [`tests/python/test_ib_async_transport.py`](tests/python/test_ib_async_transport.py) |
| ib_async's own test suite | Their tests, from their own checkout and unvendored. Those that use their shared `ib` fixture run against an `ib_async_dx.IB` | [`tests/ib_async_upstream/conftest.py`](tests/ib_async_upstream/conftest.py) |
| An unmodified program, live | Their `IB`, attached, connects, names its account, reads bars and quotes, and takes an order through its whole life | [`tests/python/test_ib_async_transport.py`](tests/python/test_ib_async_transport.py), with a login |
| A paper account | Every read asked of the venue, and an order placed, changed and withdrawn, through `ib_async_dx.IB`; not yet run against the venue | [`scripts/`](scripts/) |

Their suite is not vendored. Point a run at a checkout of theirs:

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
tests must share one event loop, or callbacks land on a loop that is not running
while the test waits on them. pandas has to be installed too: their
`test_contract.py` imports it, and without it the run stops at collection.

> [!NOTE]
> At 2.1.0 their suite is three tests. `test_account_summary` passes on the
> engine. `test_request_error_raised` cannot pass against any server: it
> asserts a `RequestError` carrying code 321, and 321 is in their own
> `warningCodes`, where a warning never ends the request it belongs to.
> `test_contract_format_data_pd` builds its own `IB` and connects it to
> `127.0.0.1:4001` rather than using their `ib` fixture, so this conftest cannot
> give it an `ib_async_dx.IB`: without a gateway on that port it fails to
> connect, and never reaches the engine.

The whole account of what "drop-in" covers, and what each claim rests on, is on
[Drop-in](https://userfrm.github.io/ib_async-dx/drop-in.html) and
[Evidence](https://userfrm.github.io/ib_async-dx/evidence.html).

## Same, better, left out

### What is the same

**All of ib_async.** ib_async is layered: `IB`, `Wrapper`, `Ticker`, `Trade`
and everything above them do not know there is a socket; only its `Client` and
`Connection` do. `ib_async_dx.IB` replaces that one layer when it connects, and
nothing else, so ib_async's events, its `…Async` methods, its types and its
`util` are the library you installed. Every callback argument is rebuilt
as ib_async's own dataclass, by type name, so a field it has and the engine does
not keeps its default. A bar's date arrives in the spelling ib_async's own
parser reads.

### What is better underneath

* **No process in the middle.** No gateway, no JVM, no localhost socket, no
  window.
* **Connections mend themselves.** Each connection a session runs on — trading,
  market data, historical, contract definitions — is rebuilt on its own if it
  drops, and what it was serving is asked for again under the caller's request.
* **Order ids come from the account.** An order left unnumbered is numbered
  from what the account has used, and order ids are counted apart from request
  ids, so an account whose order ids have grown wide still has request ids to
  give.
* **`readonly` holds.** `connect(readonly=True)` makes the session itself
  refuse to send anything that places, changes or withdraws an order. ib_async's
  own `readonly` only skips the order requests it makes as it connects.
* **Refused rather than dropped.** A contract or order field set to a value the
  engine cannot carry raises a `ValueError` naming it, rather than the order
  going out on terms nobody stated.
* **Two of ib_async's bugs, fixed.** In ib_async 2.1, `reqUserInfo()` returns
  `[]`: its wrapper ends the request without the White Branding ID it was
  answered with. Here it returns the ID. And ib_async 2.1 asks for positions as
  it connects whatever `fetchFields` says; here `StartupFetch.POSITIONS` left
  out means that request is not made, and a later `reqPositions()` asks as
  usual. Each fix is a small override on `IB`, with a test that shows ib_async's
  own answer beside it.

### What is left out

| | Today |
| --- | --- |
| A new order the engine refuses before sending it | Reported to ib_async while `placeOrder` runs, before ib_async has made the `Trade`, so the `Trade` stays `PendingSubmit`. The refusal is on `errorEvent`. |
| Option lists other than `mktDataOptions` and `chartOptions` | Taken and not applied: the request has nowhere to put them. A non-empty `mktDataOptions` or `chartOptions` raises `NotImplementedError`. |
| `bboExchange`, `modelCode`, `groupName`, the account on `reqAccountUpdates`, `ledgerAndNLV`, `numIds`, `bAutoBind` | Taken and not applied: the venue answers these requests the same way whatever they name. [Limits](https://userfrm.github.io/ib_async-dx/limits.html) says why for each. |
| Five of the calls [beyond ib_async](#beyond-ib_async) | Coming; each needs an addition to the engine first. |
| The Rust client | [Coming](#rust). |
| Published packages | None yet; ib_async-dx and the engine both install from git. |

The full list, including what the venue rather than this package decides, is
on [Limits](https://userfrm.github.io/ib_async-dx/limits.html).

## Beyond ib_async

The engine's calls beyond the documented API are methods on `ib_async_dx.IB`,
each returning a type of its own. No field is added to an ib_async class, so
ib_async's objects, reprs, `util.df` columns and equality stay ib_async's.

| Method | What it answers |
| --- | --- |
| `reqMktDataEx(..., marketDataType=None)` | `reqMktData`, with a market data type (1 live, 2 frozen, 3 delayed, 4 delayed frozen) for this request only. A contract holds one subscription: asked again while subscribed, it follows the one that is up |
| `reqCurrentTimeInMillis()`, and its `…Async` twin | The venue's clock in milliseconds. It is in the documented API and not in ib_async; accurate to about a second |
| `tickerExtras(ticker)` | What the venue states for a ticker's contract beyond ib_async's `Ticker`: shares outstanding, the open a year ago, whether a short-sale circuit breaker is on, and numbered series as the venue states them |
| `optionModel(ticker)`, `closingOptionModel(ticker)` | The venue's option model: `OptionComputation`'s eight figures and the ten it has no field for |
| `companyData(contract)` | What the venue states about a contract's company or terms, by series |
| `enabledFeatures()` | The capabilities the venue granted this account at logon |
| `orderPermissions()`, `permittedOrderTypes(secType)` | The order types the account may place, by security type. An order it may not place comes back `Inactive` with no text |
| `algorithms()`, `algorithmsFor(secType)` | The algorithms the venue offers the account |
| `orderPresets()` | The sets of order defaults the account holds, by key; their values are not carried |
| `competingSession()` | Another session that held the account when this one connected. `connect` logs a warning when there is one |
| `reqPing()`, `lastRtt()` | The round trip to the venue, in milliseconds |

Coming, each once the engine exposes what it needs: `reqCorporateActions` (a
contract's corporate actions over a range of days), `reqSpreadScan` (an
underlying scanned for strategies), `positionsElsewhere` and
`accountValuesElsewhere` (holdings the venue reports that this broker does not
hold, kept apart from `positions()` so the account is not overstated), and
`TickerExtras.statedRows`. More is on
[Beyond ib_async](https://userfrm.github.io/ib_async-dx/beyond.html).

> [!TIP]
> These calls are one-way. A program that uses one cannot move back to a
> gateway, because a gateway has no message to carry it. Everything ib_async
> itself names moves both ways.

## Rust

Coming: ib_async's model in Rust spelling — the same `IB`, the same calls and
the same objects, named the way Rust names things — built on the engine's public
API, so a call answers the same in both languages. It is not in this repository
yet, and nothing above depends on it. Until it lands, a Rust program reaches the
engine through [ibkr-dx](https://github.com/userFRM/ibkr-dx) directly.

## Notebooks

Seven of ib_async's eight notebook subjects (all but `option_chain`), run
without a gateway. Each connects an `ib_async_dx.IB`, so the code in them is
ib_async's own, and each opens a paper session.

| Notebook | What it covers |
| --- | --- |
| [`basics`](notebooks/basics.ipynb) | Account values, positions, and one quote |
| [`bar_data`](notebooks/bar_data.ipynb) | How far back the venue holds a series, the bars themselves, a frame, and a series kept up to date |
| [`contract_details`](notebooks/contract_details.ipynb) | What the venue knows about a contract, and how it answers a description that matches more than one |
| [`market_depth`](notebooks/market_depth.ipynb) | The book, and which venues will answer for it |
| [`ordering`](notebooks/ordering.ipynb) | Placing an order, watching it, moving it, withdrawing it, and a preview that sends nothing |
| [`scanners`](notebooks/scanners.ipynb) | What can be scanned for, and one scan run |
| [`tick_data`](notebooks/tick_data.ipynb) | Top of book as it changes, and every print as it happens |

```bash
git clone https://github.com/userFRM/ib_async-dx && cd ib_async-dx
pip install "git+https://github.com/userFRM/ibkr-dx"
pip install -e . jupyter python-dotenv pandas
jupyter lab notebooks
```

Credentials come from a `.env` file at the repository root, as `IB_USERNAME` and
`IB_PASSWORD`. The repository ignores that file; keep it that way. More on each
notebook is on
[Notebooks](https://userfrm.github.io/ib_async-dx/notebooks.html).

## Questions

<details>
<summary><b>Do I still need IB Gateway or TWS installed?</b></summary>

No. Nothing is installed, launched or logged into on your behalf. The engine
logs in with the credentials the program gives it.
</details>

<details>
<summary><b>Do I still need ib_async installed?</b></summary>

Yes, and pip installs it with this package: ib_async-dx runs ib_async's own
code. A program imports from `ib_async_dx`, and what it gets are ib_async's own
objects, so a module that still imports `Stock` from `ib_async` gets the same
class.
</details>

<details>
<summary><b>Will my existing program run unchanged?</b></summary>

Apart from two lines. The import becomes `ib_async_dx`, and the connect call
gains credentials — or keeps its host and port where `IB_USERNAME` and
`IB_PASSWORD` are set, because neither is used. The client id is carried into
the login. The port does not choose paper or live: the session is paper unless
`connect` is given `paper=False`.
</details>

<details>
<summary><b>Why <code>ib.sleep()</code> and not <code>time.sleep()</code>?</b></summary>

ib_async runs its event loop on the calling thread, so a plain sleep stops it:
quotes stop arriving, and every stream reads as dead when it is only unattended.
</details>

<details>
<summary><b>Can I run this and a gateway at the same time?</b></summary>

Not on the same login. One session per login — opening a second takes the first
away, and the venue names the host that took it. Use a second login if you need
both at once.
</details>

<details>
<summary><b>Does restarting my program log in again?</b></summary>

Yes. The session lives in your process, so it ends with it; a gateway held its
login while a program restarted, and here each start is a login — on a live
account, one that waits on the second factor. The session is kept in
`~/.ibkr_dx/session-<user>-<paper|live>`, owner only and sealed with the
password, and a start naming a session the venue still holds is answered with a
challenge rather than a full login. The venue lets a session go soon after its
process ends, so that covers a quick restart only. `connect(sessionFile=...)`
moves the file, and `sessionFile=False` keeps nothing.
</details>

<details>
<summary><b>Is paper different from live?</b></summary>

Not in what arrives. The same wire, the same API, the same entitlements — the
money is what differs. A live login additionally enters the second-factor
approval, which paper does not.
</details>

<details>
<summary><b>What happens when a connection drops?</b></summary>

It is rebuilt on its own, and the subscriptions it was serving are asked for
again under the request the caller made. Connections are independent: a quote
feed reconnecting does not disturb an order in flight.
</details>

<details>
<summary><b>Is it on PyPI or crates.io?</b></summary>

Not yet. Both this package and the engine install from their repositories; see
[Installation](#installation).
</details>

<details>
<summary><b>What about Rust?</b></summary>

Coming, as ib_async's model in Rust spelling on the same engine. It is not in
this repository yet. See [Rust](#rust).
</details>

<details>
<summary><b>Is this part of ib_async?</b></summary>

No. It is an independent project, not affiliated with ib_async or its
maintainers. It runs ib_async's own code as a dependency and does not copy it.
See [Not affiliated](#not-affiliated).
</details>

## Testing

Claims here rest on tests, and the tests are counted rather than described:

| Suite | Count | Needs a session |
| --- | ---: | :---: |
| Python | 49 | No |
| Python, live | 2 | Yes |
| ib_async's own suite, at 2.1.0 | 3 | Yes |
| Paper-account scripts | 3 | Yes |

The offline suite drives a session with no venue behind it, which needs the
engine built with its test hooks. A wheel built for use leaves them out on
purpose, so the tests build their own — the same steps the workflow runs on
every push:

```bash
git clone https://github.com/userFRM/ibkr-dx
pip install maturin
maturin build -m ibkr-dx/Cargo.toml --features python,extension-module,test-helpers -o dist
pip install dist/*.whl
pip install -e . pytest pytest-asyncio
pytest tests/python -q
```

Of ib_async's three tests, two run on the engine: `test_account_summary` passes,
and `test_request_error_raised` fails as it does against any server. The third
opens its own connection to a gateway on port 4001 and never reaches the engine
([why](#drop-in-and-how-it-is-proven)).

The two live tests run when `IB_USERNAME` and `IB_PASSWORD` are set, and are
skipped otherwise; the workflow sets neither. The scripts under [`scripts/`](scripts/) run against a paper
account: `sdk_sweep.py` asks for every read and places nothing, `sdk_lifecycle.py`
places, changes and withdraws a limit far from the market, and
`order_round_trip.py` does the same on a contract that trades nearly around the
clock.

## Relationship to ibkr-dx

[ibkr-dx](https://github.com/userFRM/ibkr-dx) is the engine: it logs in, holds
the connections a session runs on, speaks the venue's protocol, and exposes the
TWS API's `EClient` and `EWrapper` in Rust and Python — plus the calls beyond the
documented API. ib_async-dx is ib_async's API on top of it, and holds no
protocol code of its own.

| Your program is written against | Use |
| --- | --- |
| ib_async | ib_async-dx |
| The TWS API (`ibapi`, `EClient` / `EWrapper`) | [ibkr-dx](https://github.com/userFRM/ibkr-dx) |

A fix to the engine reaches this package by reinstalling the engine.

## Documentation

* [The book](https://userfrm.github.io/ib_async-dx/) — the guide, from install to limits
* [Getting started](https://userfrm.github.io/ib_async-dx/getting-started.html) — install, credentials, and a program that connects
* [Drop-in](https://userfrm.github.io/ib_async-dx/drop-in.html) — what "drop-in" means here, and how it is proven
* [Running ib_async itself](https://userfrm.github.io/ib_async-dx/bridge.html) — how the engine takes the socket's place, and what it carries
* [Beyond ib_async](https://userfrm.github.io/ib_async-dx/beyond.html) — the two bugs fixed, and the calls ib_async has no name for
* [Notebooks](https://userfrm.github.io/ib_async-dx/notebooks.html) — seven of ib_async's notebook subjects, without a gateway
* [Limits](https://userfrm.github.io/ib_async-dx/limits.html) — what differs from ib_async over a gateway, and what is not carried
* [Evidence](https://userfrm.github.io/ib_async-dx/evidence.html) — what each claim rests on

## Contributing

Issues and pull requests are welcome.

> [!TIP]
> The most useful report is a difference: a call, its arguments, what ib_async
> answered over a gateway, and what came back here. Drop-in is only as good as
> the programs it has been held against.

Before opening a pull request, run the offline suite as [Testing](#testing)
shows; the workflow runs the same steps.

## Security

> [!CAUTION]
> Credentials are the account. Never commit them, never paste them into an
> issue, and never put them in a file the repository tracks.

Pass them from the environment or a secret store. The session kept between runs
is sealed with the password, readable by its owner only, and never written in
the clear. The engine's test hooks, which can fabricate a connected session,
are a build feature that is off by default; only the test build asks for them.

If you find a security problem, please keep the details out of public issues:
open an issue saying only that you have a security report, and a private
channel will be arranged.

## Not affiliated

ib_async-dx is an independent project. It is **not affiliated with, endorsed
by, or supported by ib_async or its maintainers, or by Interactive Brokers**.

Interactive Brokers®, IBKR®, Trader Workstation® and IB Gateway® are
registered trademarks of Interactive Brokers Group, Inc.

> [!CAUTION]
> This package places orders against a real account. Test against a paper login
> first, and satisfy yourself that an order reads the way you meant it before
> pointing it at money.

- **No warranty.** Provided "as is", without warranty of any kind. See [LICENSE](LICENSE) for full terms.
- **Use at your own risk.** Users are solely responsible for ensuring their use complies with Interactive Brokers' Terms of Service, Customer Agreement, and any applicable laws or regulations. Use may carry risks including account restriction or termination by IB.
- **Not financial software.** An experimental project, not a replacement for officially supported IB software in production trading. The authors accept no liability for financial losses, missed trades, account issues, or any other damages arising from its use.
- **Protocol stability.** The engine speaks a protocol IB does not publish, and IB may change it at any time without notice. There is no guarantee of continued functionality.

## License and credits

[AGPL-3.0](LICENSE), the same as ibkr-dx.

ib_async's code is not copied, vendored or modified here: ib_async-dx runs the
copy pip installs, as a dependency, under its own BSD-2-Clause licence. ib_async
continues [ib_insync](https://github.com/erdewit/ib_insync) by Ewald de Wit and
is maintained by [ib-api-reloaded](https://github.com/ib-api-reloaded); thank
you to all of them for the API worth keeping.

Built on [ibkr-dx](https://github.com/userFRM/ibkr-dx), which began as a fork of
[ibx](https://github.com/deepentropy/ibx) by DeepEntropy and Odyssée.

- ib_async-dx: Copyright (C) 2026 userFRM

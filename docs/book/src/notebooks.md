# Notebooks

[`notebooks/`](https://github.com/userFRM/ib_async-dx/tree/main/notebooks)
holds seven notebooks on ib_async's own subjects, written against ib_async
itself — its `IB`, its contracts, its events, its `util.df` — and run on the
engine through `ib_async_dx.IB`. None of them starts a gateway, and every one
opens a paper session.

Each begins the same way. It is the one cell that differs from a notebook
written for a gateway: the import names `ib_async_dx`, and `connect` takes the
login.

```python
import os
from dotenv import load_dotenv
from ib_async_dx import IB, util

util.startLoop()
load_dotenv()

ib = IB()
ib.connect(
    username=os.environ["IB_USERNAME"],
    password=os.environ["IB_PASSWORD"],
    paper=True,
)
```

`util.startLoop()` is ib_async's own: it applies nest_asyncio so that
ib_async's blocking calls can run inside the notebook's loop, as it does over a
gateway.

## The seven

| Notebook | What it shows | ib_async calls |
| --- | --- | --- |
| `basics` | What the account is worth, what it holds, and one quote | `accountSummary`, `positions`, `qualifyContracts`, `reqMktData`, `cancelMktData` |
| `contract_details` | What the venue knows about a contract, how it answers a description that matches more than one, and a search by name | `reqContractDetails`, `reqMatchingSymbols` |
| `bar_data` | How far back the venue holds a series, the bars themselves as a frame, and a series kept up to date | `reqHeadTimeStamp`, `reqHistoricalData`, `util.df`, `keepUpToDate=True`, `cancelHistoricalData` |
| `tick_data` | Top of book on three contracts as it changes, and every print as it happens | `reqMktData`, `pendingTickersEvent`, `reqTickByTickData`, `ticker.tickByTicks` |
| `market_depth` | Which venues will answer for a book, a ten-level book, and the book moving | `reqMktDepthExchanges`, `reqMktDepth`, `ticker.domBids`, `ticker.domAsks`, `ticker.updateEvent` |
| `scanners` | What the venue can scan for, one scan run, and its rows as a frame | `reqScannerParameters`, `ScannerSubscription`, `reqScannerData`, `util.df` |
| `ordering` | An order placed, watched, moved and withdrawn, and one previewed rather than sent | `placeOrder`, `LimitOrder`, `trade.log`, `cancelOrder`, `openTrades`, `whatIfOrder` |

A few things they show along the way:

- **A quote wants a qualified contract.** ib_async keys a `Ticker` by the
  contract, and a contract without its `conId` cannot be a key, so
  `qualifyContracts` comes first.
- **One `Ticker` per contract, amended in place.** `pendingTickersEvent` fires
  with the tickers that changed, rather than on a timer; tick-by-tick is the
  trade stream itself rather than a summary of it.
- **A kept-up-to-date series amends its last bar** as it forms, rather than
  repeating it.
- **A book asked for at a named venue is answered by that venue.** Asked for at
  none, it is acknowledged and may produce nothing, which is what an account
  with no aggregate entitlement is told.
- **The venue states its scanner parameters as one XML document**, and the
  notebook reads the scan codes out of it.
- **A `Trade` is amended as the venue reports on it**, and its log is the whole
  history of the order. A modification is the same order under the same id, at
  a new price; `whatIfOrder` asks what an order would cost the account and
  sends nothing.

> [!NOTE]
> `ordering` places a real order on the paper account: a buy at half the last
> price, so it rests and does not fill, and a later cell withdraws it.
> Run the notebook through, or withdraw it yourself.

## Running them

```bash
pip install "git+https://github.com/userFRM/ibkr-dx"
pip install "ib_async-dx @ git+https://github.com/userFRM/ib_async-dx" \
    jupyter python-dotenv pandas
git clone https://github.com/userFRM/ib_async-dx
jupyter lab ib_async-dx/notebooks
```

`util.df` needs pandas, which ib_async does not install on its own. The
credentials come from a `.env` file at the repository root, read by
`python-dotenv`, as `IB_USERNAME` and `IB_PASSWORD`. The repository ignores
that file; keep it that way.

## One thing to keep

`ib.sleep()`, never `time.sleep()`. The library runs its event loop on the
calling thread, so a plain sleep stops it: quotes stop arriving, and every
stream reads as dead when it is only unattended.

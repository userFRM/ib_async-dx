#!/usr/bin/env python3
"""Call every read on the Python client against the venue, and say what came back.

The offline suites prove each call is carried. They cannot prove the venue
answers it, and an answer of nothing looks the same as a market with nothing in
it. This runs one session, asks for everything a program written against
ib_async asks for, and prints what arrived.

Nothing here places an order. The two order calls it does make are previews,
which the venue prices and does not place.

    IB_USERNAME=… IB_PASSWORD=… python3 scripts/sdk_sweep.py
"""

import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "python"))

import ib_async_dx  # noqa: E402


def main() -> int:
    ib = ib_async_dx.IB()
    ib.connect(
        username=os.environ["IB_USERNAME"],
        password=os.environ["IB_PASSWORD"],
        paper=True,
    )
    said: list[tuple[int, str]] = []
    ib.errorEvent += lambda r, code, m, contract: said.append((code, m[:70]))

    stock = ib_async_dx.Contract(symbol="SPY", secType="STK", exchange="SMART", currency="USD")
    spy = ib.reqContractDetails(stock)[0].contract
    fx = ib.reqContractDetails(
        ib_async_dx.Contract(symbol="EUR", secType="CASH", exchange="IDEALPRO", currency="USD")
    )[0].contract

    def ask(what, call, shape=len):
        said.clear()
        try:
            answer = call()
        except Exception as e:  # noqa: BLE001 — the point is to report it
            print(f"  {what:26} raised {type(e).__name__}: {str(e).splitlines()[0][:60]}")
            return
        heard = [s for s in said if s[0] not in (2104, 2106, 2107, 2119, 2158, 2100)]
        size = "—" if answer is None else shape(answer)
        print(f"  {what:26} {size} {heard[:1] if heard else ''}")

    print("\nreference data")
    ask("contract details", lambda: ib.reqContractDetails(stock))
    ask("option chains", lambda: ib.reqSecDefOptParams("SPY", "", "STK", spy.conId))
    ask("symbol search", lambda: ib.reqMatchingSymbols("APP"))
    ask("head timestamp", lambda: ib.reqHeadTimeStamp(spy, "TRADES", True), str)
    ask("histogram", lambda: ib.reqHistogramData(spy, True, "3 days"))
    ask("fundamentals", lambda: ib.reqFundamentalData(spy, "ReportsFinSummary"), len)
    ask("trading schedule", lambda: ib.reqHistoricalSchedule(spy, 7), lambda s: len(s.sessions))
    ask("headlines", lambda: ib.reqHistoricalNews(spy.conId, "BRFG", "", "", 5))

    print("\nmarket data")
    ask("bars", lambda: ib.reqHistoricalData(stock, "", "2 D", "1 hour", "TRADES", True))
    ask("bars, unqualified", lambda: ib.reqHistoricalData(
        ib_async_dx.Contract(symbol="AAPL", secType="STK", exchange="SMART", currency="USD"),
        "", "1 D", "1 hour", "TRADES", True))
    # Bounded, so a snapshot that never ends is reported rather than waited on.
    ask("tickers", lambda: ib.run(ib.reqTickersAsync(spy), timeout=8),
        lambda t: f"bid {t[0].bid}")
    ask("currency tickers", lambda: ib.run(ib.reqTickersAsync(fx), timeout=8),
        lambda t: f"bid {t[0].bid}")

    def stream(start, stop, read, secs=8):
        """What a subscription's ticker heard: `read` takes one pass's worth."""
        got: list = []

        def heard(ticker):
            got.extend(read(ticker))

        ticker = start()
        ticker.updateEvent += heard
        ib.sleep(secs)
        ticker.updateEvent -= heard
        stop()
        return got

    def ticks(kind, contract):
        return stream(
            lambda: ib.reqTickByTickData(contract, kind, 0, False),
            lambda: ib.cancelTickByTickData(contract, kind),
            lambda ticker: ticker.tickByTicks,
        )

    ask("every trade", lambda: ticks("AllLast", spy))
    ask("the exchange's trades", lambda: ticks("Last", spy))
    ask("quote changes", lambda: ticks("BidAsk", fx))

    print("\nsubscriptions")

    def gather(event, start, stop, secs=8):
        got: list = []

        def heard(*args):
            got.append(args)

        event += heard
        held = start()
        ib.sleep(secs)
        stop(held)
        event -= heard
        return got

    ask("depth of book", lambda: stream(
        lambda: ib.reqMktDepth(spy, 5), lambda: ib.cancelMktDepth(spy),
        lambda ticker: ticker.domTicks))
    ask("five-second bars", lambda: gather(
        ib.barUpdateEvent, lambda: ib.reqRealTimeBars(spy, 5, "TRADES", False),
        ib.cancelRealTimeBars, 12))
    ask("profit and loss", lambda: gather(
        ib.pnlEvent, lambda: ib.reqPnL(ib.managedAccounts()[0]), lambda pnl: None, 5))

    print("\naccount")
    ask("account values", lambda: ib.accountSummary())
    ask("positions", lambda: ib.positions())
    ask("managed accounts", lambda: ib.managedAccounts())
    ask("open orders", lambda: ib.reqAllOpenOrders() or ib.openOrders())
    ask("completed orders", lambda: ib.reqCompletedOrders(False) or ib.trades())

    print("\norders the venue prices and does not place")
    preview = ib_async_dx.Order(action="BUY", orderType="LMT", totalQuantity=1, lmtPrice=1.0)
    ask("a share", lambda: ib.whatIfOrder(spy, preview), lambda s: f"{s.status} {s.commission}")
    ask("a currency pair", lambda: ib.whatIfOrder(fx, ib_async_dx.Order(
        action="BUY", orderType="LMT", totalQuantity=20000, lmtPrice=0.5)),
        lambda s: f"{s.status} {s.commission}")

    ib.disconnect()
    return 0


if __name__ == "__main__":
    sys.exit(main())

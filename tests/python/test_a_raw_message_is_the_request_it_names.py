"""A message sent with their client's `send` or `sendMsg` is the request it names.

ib_async's client writes each request as one message and sends it with `send`,
which a program can also call itself. There is no socket here, so the message
is read back into the request their client names for it. Each request their
client makes is run here as their own code writes it, through `send`, and must
reach the engine exactly as the same request made by name does.
"""

import copy
import dataclasses
import inspect
import logging
import math

import ib_async
import ibkr_dx
import pytest
from ib_async.client import Client

from ib_async_dx._messages import read
from ib_async_dx.bridge import IbkrDxClient


class Recording:
    """Stands in for the engine: every request it carries, recorded."""

    def __init__(self):
        self.calls = []

    def __getattr__(self, name):
        if not hasattr(ibkr_dx.EClient, name):
            raise AttributeError(name)
        return lambda *args: self.calls.append((name, [_seen(a) for a in args]))


def _seen(value):
    """An engine object as the fields it holds, so two can be compared."""
    if isinstance(value, (str, bytes, int, float, bool, type(None))):
        return value
    if isinstance(value, (list, tuple)):
        return [_seen(v) for v in value]
    return {
        name: _seen(getattr(value, name))
        for name in dir(value)
        if not name.startswith("_") and not callable(getattr(value, name))
    }


def _client():
    client = IbkrDxClient(ib_async.IB().wrapper)
    client.connState = client.CONNECTED
    client._client = Recording()
    return client


CONTRACT = ib_async.Contract(
    conId=756733, symbol="SPY", secType="STK", exchange="SMART", currency="USD",
    localSymbol="SPY",
)


def _order():
    order = ib_async.LimitOrder("BUY", 10.0, 101.25, tif="GTC", outsideRth=True)
    order.volatility = 0.3  # stated on a limit order, which their client clears
    order.algoStrategy = "Adaptive"
    order.algoParams = [ib_async.TagValue("adaptivePriority", "Normal")]
    order.conditions = [
        ib_async.PriceCondition(conjunction="o", price=100.0, conId=756733, exch="SMART"),
        ib_async.TimeCondition(time="20261218 15:00:00 US/Eastern"),
    ]
    order.softDollarTier = ib_async.SoftDollarTier("T", "v")
    return order


def _combo():
    combo = ib_async.Contract(secType="BAG", symbol="SPY", exchange="SMART", currency="USD")
    combo.comboLegs = [
        ib_async.ComboLeg(conId=1, ratio=1, action="BUY", exchange="SMART"),
        ib_async.ComboLeg(conId=2, ratio=2, action="SELL", exchange="SMART"),
    ]
    return combo


#: A value for every argument their client's requests take, by its name.
SAMPLES = {
    "reqId": 7, "tickerId": 7, "orderId": 7, "contract": CONTRACT,
    "genericTickList": "233", "snapshot": False, "regulatorySnapshot": True,
    "mktDataOptions": [], "order": _order(), "manualCancelOrderTime": "",
    "subscribe": True, "acctCode": "DU000000", "execFilter": ib_async.ExecutionFilter(
        clientId=3, acctCode="DU000000", time="20260918 09:30:00", symbol="SPY",
        secType="STK", exchange="SMART", side="BUY",
    ),
    "numIds": 1, "numRows": 10, "isSmartDepth": True, "mktDepthOptions": [],
    "allMsgs": True, "logLevel": 3, "bAutoBind": True, "faData": 1, "cxml": "<xml/>",
    "endDateTime": "20260918 16:00:00 US/Eastern", "durationStr": "2 D",
    "barSizeSetting": "1 hour", "whatToShow": "TRADES", "useRTH": True, "formatDate": 2,
    "keepUpToDate": False, "chartOptions": [], "exerciseAction": 1,
    "exerciseQuantity": 2, "account": "DU000000", "override": 0,
    "subscription": ib_async.ScannerSubscription(
        numberOfRows=5, instrument="STK", locationCode="STK.US.MAJOR",
        scanCode="TOP_PERC_GAIN", abovePrice=5.0, excludeConvertible=True,
    ),
    "scannerSubscriptionOptions": [],
    "scannerSubscriptionFilterOptions": [ib_async.TagValue("marketCapAbove1e6", "10000")],
    "barSize": 5, "realTimeBarsOptions": [], "reportType": "ReportSnapshot",
    "fundamentalDataOptions": [], "optionPrice": 3.5, "underPrice": 450.25,
    "implVolOptions": [], "volatility": 0.25, "optPrcOptions": [],
    "marketDataType": 3, "groupName": "All", "tags": "NetLiquidation",
    "apiName": "app", "apiVersion": "1", "apiData": "data", "groupId": 4,
    "contractInfo": "756733@SMART", "opaqueIsvKey": "key", "xyzResponse": "response",
    "modelCode": "", "ledgerAndNLV": True, "underlyingSymbol": "SPY",
    "futFopExchange": "", "underlyingSecType": "STK", "underlyingConId": 756733,
    "pattern": "SP", "bboExchange": "a6", "providerCode": "BRFG",
    "articleId": "BRFG$1", "newsArticleOptions": [], "conId": 756733,
    "providerCodes": "BRFG+DJNL", "startDateTime": "20260901 00:00:00",
    "totalResults": 10, "historicalNewsOptions": [], "timePeriod": "3 days",
    "marketRuleId": 26, "conid": 756733, "numberOfTicks": 100, "useRth": True,
    "ignoreSize": False, "miscOptions": [], "tickType": "Last", "apiOnly": True,
    "data": ib_async.WshEventData(conId=756733, startDate="20260901", totalLimit=5),
}

#: Every request their client writes as a message, read from their source.
REQUESTS = sorted(
    name for name, method in inspect.getmembers(Client, inspect.isfunction)
    if "self.send(" in inspect.getsource(method) and name != "connectAsync"
)


def _args(request):
    """Its arguments, each a copy: their client changes an order it writes."""
    return [
        copy.deepcopy(SAMPLES[name])
        for name in list(inspect.signature(getattr(Client, request)).parameters)[1:]
    ]


class Writing:
    """Their client's own writer, keeping each message it would send."""

    send = Client.send
    clientId, optCapab = 1, ""

    def __init__(self):
        self.sent = []

    def isConnected(self):
        return True

    def serverVersion(self):
        return IbkrDxClient.MaxClientVersion

    def sendMsg(self, msg):
        self.sent.append(msg)


def _written(request, *args):
    writer = Writing()
    getattr(Client, request)(writer, *args)
    [msg] = writer.sent
    return msg


def _distinct(obj):
    """``obj`` with every number, string and flag it holds set to a value its
    neighbours do not hold, so a field read into the wrong place shows."""
    for at, field in enumerate(dataclasses.fields(obj)):
        held = getattr(obj, field.name)
        if isinstance(held, bool):
            setattr(obj, field.name, at % 2 == 0)
        elif isinstance(held, int):
            setattr(obj, field.name, 1000 + at)
        elif isinstance(held, float):
            setattr(obj, field.name, 1000 + at + 0.5)
        elif isinstance(held, str):
            setattr(obj, field.name, field.name)
    return obj


def test_every_request_their_client_writes_is_read():
    from ib_async_dx._messages import REQUESTS as READ

    assert sorted(name for name, _ in READ.values()) == REQUESTS


@pytest.mark.parametrize("request_name", REQUESTS)
def test_a_message_reads_back_into_the_request_that_wrote_it(request_name):
    """Read back and written again by their own client, a message is the
    message it was: every field read is the field written."""
    msg = _written(request_name, *_args(request_name))
    request, args = read(msg)
    assert request == request_name
    assert _written(request, *args) == msg


def test_an_order_reads_back_field_for_field():
    """With every field its own value, a field read into its neighbour's
    place would be written back there."""
    contract = _distinct(ib_async.Contract())
    order = _distinct(ib_async.Order())
    _distinct(order.softDollarTier)
    msg = _written("placeOrder", 7, contract, order)
    assert _written("placeOrder", *read(msg)[1]) == msg


@pytest.mark.parametrize("request_name", REQUESTS)
def test_a_message_their_client_writes_is_the_request_it_names(request_name):
    by_name, through_send = _client(), _client()
    getattr(Client, request_name)(through_send, *_args(request_name))
    getattr(by_name, request_name)(*_args(request_name))
    assert through_send._client.calls == by_name._client.calls
    assert through_send._callbacks.refused == by_name._callbacks.refused
    assert through_send._sent == by_name._sent


def test_a_combination_and_its_hedge_are_read_whole():
    by_name, through_send = _client(), _client()
    combo = _combo()
    combo.deltaNeutralContract = ib_async.DeltaNeutralContract(756733, 0.5, 450.0)
    order = _order()
    order.orderComboLegs = [ib_async.OrderComboLeg(1.5), ib_async.OrderComboLeg(2.5)]
    order.smartComboRoutingParams = [ib_async.TagValue("NonGuaranteed", "1")]
    Client.placeOrder(through_send, 7, combo, order)
    Client.reqMktData(through_send, 8, combo, "", False, False, [])
    by_name.placeOrder(7, combo, order)
    by_name.reqMktData(8, combo, "", False, False, [])
    assert through_send._client.calls == by_name._client.calls
    [(_, (_, contract, placed)), _] = by_name._client.calls
    assert [leg["conId"] for leg in contract["comboLegs"]] == [1, 2]
    assert [leg["price"] for leg in placed["orderComboLegs"]] == [1.5, 2.5]


def _pegged(orderType, **fields):
    order = ib_async.Order(action="SELL", totalQuantity=3, orderType=orderType, **fields)
    return order


#: Orders reaching each part of their client's message that only some orders
#: write.
ORDERS = {
    "pegged to the best": _pegged(
        "PEG BEST", minCompeteSize=100, competeAgainstBestOffset=math.inf,
        midOffsetAtWhole=0.01, midOffsetAtHalf=0.005,
    ),
    "pegged to the midpoint": _pegged("PEG MID", midOffsetAtWhole=0.02, midOffsetAtHalf=0.01),
    "pegged to a benchmark": _pegged(
        "PEG BENCH", referenceContractId=756733, isPeggedChangeAmountDecrease=True,
        peggedChangeAmount=0.5, referenceChangeAmount=0.25, referenceExchangeId="SMART",
    ),
    "hedged and scaled": _pegged(
        "LMT", lmtPrice=10.0, hedgeType="D", hedgeParam="0.5",
        deltaNeutralOrderType="MKT", deltaNeutralConId=756733,
        deltaNeutralOpenClose="O", deltaNeutralShortSale=True,
        scaleInitLevelSize=100, scaleSubsLevelSize=50, scalePriceIncrement=0.05,
        scalePriceAdjustValue=0.01, scalePriceAdjustInterval=60,
        scaleAutoReset=True, scaleInitPosition=10, scaleInitFillQty=5,
    ),
}


@pytest.mark.parametrize("kind", ORDERS)
def test_an_order_is_read_whole_whatever_parts_it_writes(kind):
    by_name, through_send = _client(), _client()
    ats = ib_async.Contract(conId=756733, secType="STK", exchange="IBKRATS", currency="USD")
    order = ORDERS[kind]
    order.minTradeQty = 100
    Client.placeOrder(through_send, 7, ats, order)
    by_name.placeOrder(7, ats, order)
    assert by_name._client.calls, "the order reached the engine"
    assert through_send._client.calls == by_name._client.calls


def test_an_order_carries_what_their_client_writes_and_nothing_else():
    """What a gateway reads off their client's message is what reaches the
    engine. Their client clears `volatility` on any order but a volatility
    order, writes no `faProfile` at this version, and never writes
    `imbalanceOnly`."""
    client = _client()
    order = _order()
    order.faProfile = "profile"
    order.imbalanceOnly = True
    client.placeOrder(7, CONTRACT, order)
    [(name, (orderId, contract, placed))] = client._client.calls
    unset = ibkr_dx.Order()
    assert (name, orderId) == ("place_order", 7)
    assert placed["volatility"] == unset.volatility
    assert order.volatility is None, "cleared on the program's order, as their client clears it"
    assert placed["imbalanceOnly"] == unset.imbalanceOnly
    assert not client._callbacks.refused

    vol = ib_async.Order(action="BUY", totalQuantity=1.0, orderType="VOL", volatility=0.3)
    client.placeOrder(8, CONTRACT, vol)
    assert client._client.calls[-1][1][2]["volatility"] == 0.3


def test_a_message_that_does_not_read_is_refused_as_320(caplog):
    """As a gateway answers a message it cannot read: 320, once the call has
    returned, under the request's number where it was read before the field
    that failed, and under -1 before it."""
    client = _client()
    heard = []
    client.wrapper.ib.errorEvent += lambda *args: heard.append(args[:2])
    client.sendMsg("92\0" "7\0" "DU000000\0")    # one field short of reqPnL
    client.sendMsg("92\0" "x\0" "DU000000\0\0")  # its number is not one
    client.sendMsg("49\0" "1\0" "extra\0")       # a field past reqCurrentTime
    assert client._client.calls == [] and heard == []
    client._callbacks.begin_pass()
    assert heard == [(7, 320), (-1, 320), (-1, 320)]
    assert client._sent == 3


def test_a_message_naming_no_request_is_logged_and_unanswered(caplog):
    """As a gateway treats a request type it does not know: said in its log,
    and nothing answers it. An empty message is nothing to send."""
    client = _client()
    with caplog.at_level(logging.ERROR, logger="ib_async_dx"):
        client.sendMsg("105\0")
        client.sendMsg("")
    client._callbacks.begin_pass()
    assert client._client.calls == [] and not client._callbacks.refused
    assert [r.getMessage() for r in caplog.records] == ["Invalid incoming request type - 105"]
    assert client._sent == 1


def test_a_message_is_sent_only_while_connected():
    """As on their client: `send` raises, and `sendMsg` sends nothing."""
    client = _client()
    client.connState = client.DISCONNECTED
    with pytest.raises(ConnectionError, match="Not connected"):
        client.send(49, 1)
    client.sendMsg("49\0" "1\0")
    assert client._client.calls == [] and not client._callbacks.refused

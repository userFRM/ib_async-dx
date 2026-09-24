"""`ib_async_dx.IB`, connected, on the engine's test session.

Needs no venue: the engine's own test session stands in for a logon, and what
`connect` was given is recorded where the logon would have taken it.
"""

import asyncio
import contextlib
import datetime
import math
import pathlib
import time

import ib_async
import ibkr_dx
import pytest

import ib_async_dx
from ib_async_dx import StartupFetch, StartupFetchNONE


class OfflineEngine(ibkr_dx.EClient):
    """The engine, with its test session in place of a logon."""

    def connect(self, **logon):
        self.logon = logon
        self.asked = []
        self._test_connect("DU000000", logon["readonly"])
        self._test_finish_account_download()

    def req_positions(self):
        self.asked.append("positions")
        super().req_positions()

    # Recorded and not sent: a subscription on the test session waits out the
    # engine's registration. The core's own tests follow it to the venue.
    def req_mkt_data(self, *args):
        self.asked.append(("req_mkt_data", args))

    def req_mkt_data_ex(self, *args):
        self.asked.append(("req_mkt_data_ex", args))


@pytest.fixture
def connect(monkeypatch):
    """Connect an IB (a new ib_async_dx.IB unless one is given) offline."""
    monkeypatch.setattr(ibkr_dx, "EClient", OfflineEngine)
    monkeypatch.delenv("IB_USERNAME", raising=False)
    monkeypatch.delenv("IB_PASSWORD", raising=False)
    opened = []

    def connect(*args, ib=None, **kwargs):
        ib = ib or ib_async_dx.IB()
        kwargs.setdefault("fetchFields", StartupFetchNONE)
        ib.connect(*args, **kwargs)
        opened.append(ib)
        return ib

    yield connect
    for ib in opened:
        ib.disconnect()


SPY = ib_async.Stock("SPY", "SMART", "USD", conId=756733)


def _heard(ib):
    """What reaches ib_async's errorEvent, as (reqId, code)."""
    heard = []
    ib.errorEvent += lambda reqId, code, text, contract: heard.append((reqId, code))
    return heard


# ── connect ──


def test_the_login_comes_from_the_environment_when_connect_names_none(connect, monkeypatch):
    monkeypatch.setenv("IB_USERNAME", "envuser")
    monkeypatch.setenv("IB_PASSWORD", "envpass")
    logon = connect().client._client.logon
    assert (logon["username"], logon["password"]) == ("envuser", "envpass")
    # The session file is the bridge's, named for the account.
    home = pathlib.Path.home() / ".ibkr_dx"
    assert logon["session_file"] == str(home / "session-envuser-paper")


def test_a_login_named_on_connect_is_the_one_used(connect, monkeypatch):
    monkeypatch.setenv("IB_USERNAME", "envuser")
    monkeypatch.setenv("IB_PASSWORD", "envpass")
    # ib_async's positional form, host and port and all.
    ib = connect("127.0.0.1", 7497, 7, username="me", password="pw", sessionFile=False)
    logon = ib.client._client.logon
    assert (logon["username"], logon["password"]) == ("me", "pw")
    assert logon["client_id"] == 7, "the client id keys this session's orders"
    assert logon["session_file"] is None, "False keeps nothing"
    assert ib.isConnected()


def test_a_session_is_paper_unless_the_program_says_live(connect):
    assert connect().client._client.logon["paper"] is True
    live = connect(username="me", paper=False).client._client.logon
    assert live["paper"] is False
    assert live["session_file"].endswith("session-me-live")


def test_an_ib_connects_again_after_it_disconnected(connect):
    """A pass left queued by the first session must not end the second.

    The first session's pump had passes queued on the loop when it stopped;
    run by the second connect, one told ib_async's wrapper the session had
    dropped, and ib_async's global error event cancelled the connect.
    """
    ib = connect()
    ib.disconnect()
    assert connect(ib=ib) is ib
    assert ib.isConnected()
    # Each connect attaches again, and leaves ib_async's own placeOrder in place.
    assert "placeOrder" not in vars(ib)


def test_a_second_connect_closes_the_first_session(connect):
    ib = connect()
    first = ib.client
    connect(ib=ib)
    assert first.connState == first.DISCONNECTED and first._pass is None
    assert not first._client.is_connected(), "the first session is logged out"
    assert ib.isConnected()


def test_an_order_status_names_the_client_that_placed_the_order(connect):
    """As a gateway states it. ib_async keys an order by the client that placed
    it, so a status stamped with this session's client instead reached no
    trade for an order another client placed."""
    ib = connect(clientId=1)
    engine = ib.client._client
    engine._test_set_instrument_count(1)
    engine._test_set_client_id(9)
    engine._test_track_order(42, 0, "SPY", "BUY", 1.0, 1.0, 0, "USD")
    engine._test_set_client_id(1)
    engine._test_push_order_update(42, 0, "Submitted", 0.0, 1.0)
    ib.client._pass_once()
    trade = ib.wrapper.trades[(9, 42)]
    assert (trade.orderStatus.clientId, trade.orderStatus.remaining) == (9, 1.0)
    assert (1, 42) not in ib.wrapper.trades


def test_a_price_carries_its_size_and_a_size_alone_reaches_tickSize(connect):
    """As a gateway sends them: a price and the size that goes with it as one
    `priceSizeTick`, and a size that changed on its own as a `tickSize`."""
    ib = connect()
    engine = ib.client._client
    reqId = ib.client.getReqId()
    ticker = ib.wrapper.startTicker(reqId, SPY, "mktData")
    engine._test_set_instrument_count(1)
    engine._test_map_instrument(reqId, 0)

    engine._test_push_quote(0, bid=100.25, bid_size=300)
    ib.client._pass_once()
    assert [(t.tickType, t.price, t.size) for t in ticker.ticks] == [(1, 100.25, 300.0)]

    engine._test_push_quote(0, bid=100.25, bid_size=500)
    ib.client._pass_once()
    assert [(t.tickType, t.price, t.size) for t in ticker.ticks] == [(0, 100.25, 500.0)]
    assert (ticker.bid, ticker.bidSize, ticker.prevBidSize) == (100.25, 500.0, 300.0)

    engine._test_push_quote(0, bid=100.5, bid_size=500)
    ib.client._pass_once()
    assert [(t.tickType, t.price, t.size) for t in ticker.ticks] == [(1, 100.5, 500.0)], \
        "a price stated alone carries the size standing"


def test_a_refused_new_order_reaches_its_trade(connect, caplog):
    """As a gateway's refusal does: after `placeOrder` has made the `Trade`.
    Delivered inside the call, before the `Trade` existed, the refusal left it
    PendingSubmit. And a new order refused with 321 is over: ib_async 2.1
    counts 321 as a warning and left the order `ValidationError`, open for
    good; ib_async's own rule for 110 on a new order cancels it, and says so
    on its wrapper's logger."""
    import logging

    ib = connect(readonly=True)
    heard = _heard(ib)
    trade = ib.placeOrder(SPY, ib_async.LimitOrder("BUY", 1, 1.0))
    assert trade.orderStatus.status == "PendingSubmit", "nothing has come back yet"
    with caplog.at_level(logging.WARNING, logger="ib_async.wrapper"):
        ib.client._pass_once()
    said = [(r.name, r.getMessage().split(":")[0]) for r in caplog.records]
    assert said == [
        ("ib_async.wrapper", f"Error 321, reqId {trade.order.orderId}"),
        ("ib_async.wrapper", "Canceled order"),
    ], said
    assert (trade.log[-1].status, trade.log[-1].errorCode) == ("Cancelled", 321)
    assert trade.isDone() and trade not in ib.openTrades()
    assert heard == [(trade.order.orderId, 321)]


def test_a_what_if_refused_with_321_ends_with_the_refusal(connect):
    """ib_async 2.1's `whatIfOrder` waited for good on a 321, a warning to
    it, which never ends a request; its own suite expects a `RequestError`
    carrying 321."""
    ib = connect(readonly=True)
    ib.RaiseRequestErrors = True
    with pytest.raises(ib_async.RequestError) as refused:
        ib.run(asyncio.wait_for(ib.whatIfOrderAsync(SPY, ib_async.LimitOrder("BUY", 1, 1.0)), 2))
    assert refused.value.code == 321
    ib.RaiseRequestErrors = False
    assert ib.run(asyncio.wait_for(ib.whatIfOrderAsync(SPY, ib_async.LimitOrder("BUY", 1, 1.0)), 2)) == []


def test_321_on_an_order_already_working_stays_a_warning(connect, monkeypatch):
    """A modification refused leaves the order live at the venue, which is
    why ib_async counts 321 as a warning: the trade is marked, and kept."""
    monkeypatch.setattr(ibkr_dx, "EClient", Placing)
    ib = connect()
    heard = _heard(ib)
    trade = ib.placeOrder(SPY, ib_async.LimitOrder("BUY", 1, 1.0))
    trade.orderStatus.status = "Submitted"
    ib.wrapper.error(trade.order.orderId, 321, "the change was not accepted", "")
    assert trade.orderStatus.status == "ValidationError"
    assert not trade.isDone()
    assert heard[-1] == (trade.order.orderId, 321)


class Placing(OfflineEngine):
    """Records the orders that reach it, and grants what the test names."""

    features = []

    def place_order(self, *args):
        self.asked.append(("place_order", args))

    def enabled_features(self):
        return self.features


class Retired(Placing):
    features = ["DEPRETFQNC"]


def test_an_order_stating_a_retired_attribute_goes_out_without_it(connect, monkeypatch):
    """As a gateway places one where the venue has not retired them for the
    account: it says so, under the order's number, and places the order
    without the attribute. ib_async counts the notice as a warning."""
    monkeypatch.setattr(ibkr_dx, "EClient", Placing)
    ib = connect()
    heard = _heard(ib)
    order = ib_async.LimitOrder("BUY", 1, 1.0)
    order.eTradeOnly, order.nbboPriceCap = True, 1.5
    trade = ib.placeOrder(SPY, order)
    ib.client._pass_once()
    assert heard == [(order.orderId, 2168), (order.orderId, 2170)]
    [(name, (orderId, contract, placed))] = ib.client._client.asked
    assert (name, orderId) == ("place_order", order.orderId)
    assert trade.orderStatus.status == "ValidationError"
    assert "EtradeOnly" in trade.log[1].message


def test_where_the_venue_has_retired_them_the_order_is_refused(connect, monkeypatch):
    """As a gateway refuses one: 10269 under the order's number, and nothing
    placed. ib_async cancels the order's `Trade`."""
    monkeypatch.setattr(ibkr_dx, "EClient", Retired)
    ib = connect()
    heard = _heard(ib)
    order = ib_async.LimitOrder("BUY", 1, 1.0)
    order.firmQuoteOnly = True
    trade = ib.placeOrder(SPY, order)
    ib.client._pass_once()
    assert heard == [(order.orderId, 10269)]
    assert ib.client._client.asked == []
    assert trade.orderStatus.status == "Cancelled"
    assert ib.placeOrder(SPY, ib_async.LimitOrder("BUY", 1, 1.0)) and ib.client._client.asked, \
        "an order stating none of them is placed"


def test_a_handler_that_asks_again_on_every_refusal_does_not_hold_the_loop(connect):
    """A refusal of a request a handler makes while refusals are delivered
    comes on the next pass, as a gateway's answer comes back on the socket.
    Delivered in the same pass, a handler placing the order again on every
    refusal held the loop for as long as it kept asking."""
    ib = connect(readonly=True)
    heard = _heard(ib)

    def again(*args):
        if len(heard) < 3:
            ib.placeOrder(SPY, ib_async.LimitOrder("BUY", 1, 1.0))

    ib.errorEvent += again
    ib.placeOrder(SPY, ib_async.LimitOrder("BUY", 1, 1.0))
    for passes in (1, 2, 3):
        ib.client._pass_once()
        assert len(heard) == passes, "one refusal a pass"


def test_a_wrapper_that_raises_is_logged_and_the_session_carries_on(connect, monkeypatch, caplog):
    """As their decoder treats a message their wrapper raises on. Raised into
    the engine, it closed the session."""
    import logging

    ib = connect()
    engine = ib.client._client
    engine._test_set_instrument_count(1)
    engine._test_track_order(42, 0, "SPY", "BUY", 1.0, 1.0, 0, "USD")
    engine._test_push_order_update(42, 0, "Submitted", 0.0, 1.0)

    def raising(*args):
        raise RuntimeError("their wrapper failed")

    monkeypatch.setattr(ib.wrapper, "orderStatus", raising)
    with caplog.at_level(logging.ERROR, logger="ib_async_dx"):
        ib.client._pass_once()
    assert ib.isConnected()
    assert any("their wrapper failed" in r.exc_text for r in caplog.records if r.exc_text)


def test_an_order_id_the_venue_names_after_the_connect_is_not_handed_out(connect):
    """The history of an order that filled can be named later than the wait
    at connect, and the venue refuses an id a fill has spent. The counter is
    kept past it; their wrapper raises it only on an open order."""
    ib = connect(readonly=True)
    ib.client._client._test_push_venue_order(5000, "SPY", "BUY", 1.0, 1.0, "Filled")
    trade = ib.placeOrder(SPY, ib_async.LimitOrder("BUY", 1, 1.0))
    assert trade.order.orderId == 5001


def test_prices_stated_as_the_session_ends_reach_the_ticker(connect, caplog):
    """What a pass stated before the session ended reaches the ticker before
    the close, as a socket's last data is read before it closes. Held to the
    end of the pass, it reached a wrapper the close had cleared, which logged
    each price as a request it did not know."""
    import logging

    ib = connect()
    engine = ib.client._client
    reqId = ib.client.getReqId()
    ticker = ib.wrapper.startTicker(reqId, SPY, "mktData")
    engine._test_set_instrument_count(1)
    engine._test_map_instrument(reqId, 0)
    engine._test_push_quote(0, bid=100.0, bid_size=300)
    ib.client._pass_once()
    # A price whose size did not change waits for the end of the pass.
    engine._test_push_quote(0, bid=100.25, bid_size=300)
    engine._test_end_session()
    with caplog.at_level(logging.ERROR):
        ib.client._pass_once()
    assert not ib.isConnected()
    assert (ticker.bid, ticker.bidSize) == (100.25, 300.0)
    assert not [r for r in caplog.records if "Unknown reqId" in r.getMessage()]


def test_a_refusal_held_as_the_program_disconnects_does_not_reach_the_next_session(connect):
    """The client `attach` gives is kept across connects, and what it held
    for one session is not the next one's: a refusal delivered there named a
    request of the session before."""
    ib = connect(ib=ib_async_dx.attach(ib_async.IB(), readonly=True))
    client = ib.client
    ib.placeOrder(SPY, ib_async.LimitOrder("BUY", 1, 1.0))
    ib.disconnect()
    heard = _heard(ib)
    connect(ib=ib)
    ib.client._pass_once()
    assert ib.client is client and heard == []


def test_orders_and_requests_are_numbered_from_one_counter(connect):
    """As ib_async's client numbers them, so an order never takes the number
    of a request still waiting. ib_async's `error` ends a waiting request
    under the number before it looks for a trade: with two counters, an
    order's refusal ended somebody's request and never reached the order."""
    ib = connect()
    heard = _heard(ib)
    reqId = ib.client.getReqId()
    waiting = ib.wrapper.startReq(reqId)
    combo = ib_async.Contract(secType="BAG", symbol="SPY", exchange="SMART", currency="USD")
    trade = ib.placeOrder(combo, ib_async.LimitOrder("BUY", 1, 1.0))
    assert trade.order.orderId == reqId + 1, "the next number on the same counter"
    ib.client._pass_once()
    assert trade.orderStatus.status == "Cancelled", "314, a combination with no legs"
    assert not waiting.done(), "the request waiting under its own number is untouched"
    assert heard == [(trade.order.orderId, 314)]


def test_updateEvent_and_timeoutEvent_follow_what_arrives(connect):
    """As over a socket: `updateEvent` when a batch arrives, and
    `timeoutEvent` when nothing has for the time set."""
    ib = connect()
    ib.sleep(0.1)
    updates, idle = [], []
    ib.updateEvent += lambda: updates.append(1)
    ib.timeoutEvent += idle.append
    ib.setTimeout(0.1)
    ib.sleep(0.3)
    assert updates == [] and len(idle) == 1, (len(updates), idle)
    ib.reqCurrentTime()
    assert updates, "an answer is a batch"


# ── ib_async 2.1's bugs, fixed ──


def test_fetchFields_without_positions_asks_for_no_positions(connect):
    theirs = connect(ib=ib_async_dx.attach(ib_async.IB()))
    assert theirs.client._client.asked == ["positions"], "ib_async 2.1 asks anyway"

    ours = connect()
    assert ours.client._client.asked == []
    ours.reqPositions()
    assert ours.client._client.asked == ["positions"], "only the startup ask is skipped"

    named = connect(fetchFields=StartupFetch.POSITIONS)
    assert named.client._client.asked == ["positions"]


def test_reqUserInfo_answers_the_white_branding_id(connect):
    theirs = connect(ib=ib_async_dx.attach(ib_async.IB()))
    ours = connect()
    for ib in (theirs, ours):
        ib.client._client._test_note_reference_data(0, "", "", "", "", "WB1")
    assert theirs.reqUserInfo() == [], "ib_async 2.1 drops the answer"
    assert ours.reqUserInfo() == "WB1"


# ── the extras ──


def test_reqMktDataEx_asks_with_the_market_data_type_named(connect):
    ib = connect()
    spy = ib_async.Stock("SPY", "SMART", "USD", conId=756733)
    for tws, engine in {1: 0, 2: 2, 3: 1, 4: 3}.items():
        ticker = ib.reqMktDataEx(spy, "233", marketDataType=tws)
        name, (reqId, contract, ticks, snapshot, regulatory, mode) = ib.client._client.asked[-1]
        assert (name, ticks, mode) == ("req_mkt_data_ex", "233", engine), tws
        assert contract.conId == 756733
        assert ib.wrapper.reqId2Ticker[reqId] is ticker, "their ticker, filled as reqMktData's"

    ib.reqMktDataEx(spy)
    assert ib.client._client.asked[-1][0] == "req_mkt_data", "None keeps the session's type"
    with pytest.raises(ValueError, match="marketDataType"):
        ib.reqMktDataEx(spy, marketDataType=5)


def test_reqCurrentTimeInMillis_is_the_venues_clock(connect):
    millis = connect().reqCurrentTimeInMillis()
    assert isinstance(millis, int)
    assert abs(millis - time.time() * 1000) < 2000, "accurate to about a second"


def test_the_option_model_is_read_by_the_tickers_request(connect):
    ib = connect()
    engine = ib.client._client
    option = ib_async.Option("SPY", "20261218", 450, "C", "SMART", conId=1)
    # What reqMktData registers, without the subscription (see OfflineEngine).
    reqId = ib.client.getReqId()
    ticker = ib.wrapper.startTicker(reqId, option, "mktData")
    engine._test_map_instrument(reqId, 7)
    engine._test_push_option_model(7, 0.25, 3.5, 450.0)

    model = ib.optionModel(ticker)
    assert (model.impliedVol, model.optPrice, model.undPrice) == (0.25, 3.5, 450.0)
    assert model.rho is None and model.fugit is None, "the engine's unset is None"
    assert ib.closingOptionModel(ticker) is None, "none stated"
    assert ib.optionModel(ib_async.Ticker()) is None, "a ticker nobody asked for"


class Stating(OfflineEngine):
    """The engine's answers where the test session has no hook to set them."""

    def contract_figures(self, reqId):
        return {"sharesOutstanding": 1.5e9, "openAYearAgo": 1.7976931348623157e308}

    def short_sale_restricted(self, reqId):
        return True

    def stated_figures_series(self, reqId):
        return [310]

    def stated_figures(self, reqId, series):
        return [1.0, 1.7976931348623157e308]

    def numbered_figures_series(self, reqId):
        return [612]

    def numbered_figures(self, reqId, series, fractional=False):
        return [(1, 0.5)] if fractional else [(1, 42.0)]

    def paired_figures_series(self, reqId):
        return [293]

    def paired_figures(self, reqId, series):
        return [(0.1, 0.2)]

    def order_presets(self):
        return [("STK", "3", "20260901-12:00:00")]

    def competing_session(self):
        return ("10.0.0.4", "20260813-09:30:00", True)


def test_what_the_engine_states_is_carried_in_this_packages_types(connect, monkeypatch):
    monkeypatch.setattr(ibkr_dx, "EClient", Stating)
    ib = connect()
    extras = ib.tickerExtras(ib_async.Ticker())
    assert extras.sharesOutstanding == 1.5e9
    assert math.isnan(extras.openAYearAgo), "the engine's unset is nan"
    assert extras.shortSaleRestricted is True
    assert extras.statedFigures[310][0] == 1.0 and math.isnan(extras.statedFigures[310][1])
    assert extras.numberedFigures == {612: ({1: 42.0}, {1: 0.5})}, "whole, then fractional"
    assert extras.pairedFigures == {293: [(0.1, 0.2)]}

    assert ib.orderPresets() == [ib_async_dx.OrderPreset("STK", "3", "20260901-12:00:00")]
    session = ib.competingSession()
    assert session.origin == "10.0.0.4" and session.readOnly is True
    assert session.loggedInAt == datetime.datetime(2026, 8, 13, 9, 30, tzinfo=datetime.UTC)


def test_the_accounts_grants_are_read_from_the_engine(connect):
    """A test session holds no grants; each reads as the engine's empty answer."""
    ib = connect()
    assert ib.enabledFeatures() == []
    assert ib.orderPermissions() == {}
    assert ib.permittedOrderTypes("STK") is None, "not permitted"
    assert ib.algorithms() == {}
    assert ib.algorithmsFor("STK") == []
    assert ib.orderPresets() == []
    assert ib.competingSession() is None
    assert ib.companyData(ib_async.Stock(conId=756733)) == {}
    extras = ib.tickerExtras(ib_async.Ticker())
    assert math.isnan(extras.sharesOutstanding) and extras.statedFigures == {}


def test_priceBasedVol_is_a_bool_when_unstated():
    """As its docstring says: False when the venue did not state it."""
    from ib_async_dx.ib import _optionModel

    assert _optionModel({}).priceBasedVol is False
    assert ib_async_dx.OptionModel().priceBasedVol is False
    assert _optionModel({"priceBasedVol": True}).priceBasedVol is True


def test_a_subscription_over_leaves_no_size_behind(connect):
    """The size kept for a quote's price stays only as long as the
    subscription: a program opening and closing many kept every one."""
    ib = connect()
    engine = ib.client._client
    engine._test_set_instrument_count(2)
    held = ib.client._callbacks._sizes

    def kept(reqId):
        return [k for k in held if (k[0] if isinstance(k, tuple) else k) == reqId]

    reqIds = []
    for slot, contract in enumerate((SPY, ib_async.Stock("QQQ", "SMART", "USD", conId=320227571))):
        reqId = ib.client.getReqId()
        ib.wrapper.startTicker(reqId, contract, "mktData")
        engine._test_map_instrument(reqId, slot)
        engine._test_push_quote(slot, bid=100.25, bid_size=300)
        reqIds.append(reqId)
    ib.client._pass_once()
    assert all(kept(reqId) for reqId in reqIds)
    ib.cancelMktData(SPY)
    assert not kept(reqIds[0]), "cancelled"
    ib.client._callbacks.tickSnapshotEnd(reqIds[1])
    assert not kept(reqIds[1]), "a snapshot answered"


def test_an_option_list_is_checked_as_a_gateway_checks_one(connect):
    """The engine checks a request's option list as a gateway checks it: one
    key, `manual`, valued 0 or 1, and none at all on the two option
    computations. A request stating another key is refused with 10337, and
    another value with 10338, in the gateway's words, on errorEvent under the
    request's number once the call has returned, and nothing goes out."""
    ib = connect()
    heard = []
    ib.errorEvent += lambda reqId, code, text, contract: heard.append((reqId, code, text))
    engine = ib.client._client
    engine._test_take_commands()
    TagValue = ib_async.TagValue
    bars = ib.reqRealTimeBars(SPY, 5, "TRADES", False, [TagValue("manual", "2")])
    order = ib_async.LimitOrder("BUY", 1, 1.0)
    order.orderMiscOptions = [TagValue("rth", "1")]
    trade = ib.placeOrder(SPY, order)
    ib.client.calculateOptionPrice(91, SPY, 0.2, 100.0, [TagValue("manual", "0")])
    ib.client._pass_once()
    assert heard == [
        (bars.reqId, 10338, "Misc options value=2 is invalid for key=manual in "
                            "ReqRealTimeBars(50) request. Valid values are: 0, 1"),
        (order.orderId, 10337, "Misc options key=rth is invalid in PlaceOrder(3) "
                               "request. Valid keys are: manual"),
        (91, 10337, "Misc options key=manual is invalid in ReqCalcOptionPrice(55) "
                    "request. Valid keys are: "),
    ]
    assert engine._test_take_commands() == [], "nothing went out"
    assert trade.orderStatus.status == "Cancelled"

    # Handed to the engine as its own, which takes `manual`.
    ib.reqMktData(SPY, mktDataOptions=[TagValue("manual", "1")])
    name, args = engine.asked[-1]
    assert name == "req_mkt_data"
    assert [(o.tag, o.value) for o in args[-1]] == [("manual", "1")]


def test_implVolOptions_takes_no_key(connect):
    """ib_async's calculateImpliedVolatility, stating a key: the engine
    refuses it with 10337 under the request's number, as a gateway does, and
    nothing is computed."""
    ib = connect()
    heard = []
    ib.errorEvent += lambda reqId, code, text, contract: heard.append((reqId, code, text))
    engine = ib.client._client
    engine._test_take_commands()
    ib.RaiseRequestErrors = True
    with pytest.raises(ib_async.RequestError) as refused:
        ib.calculateImpliedVolatility(SPY, 1.5, 100.0, [ib_async.TagValue("manual", "1")])
    text = (
        "Misc options key=manual is invalid in ReqCalcImpliedVolatility(54) "
        "request. Valid keys are: "
    )
    assert heard == [(refused.value.reqId, 10337, text)]
    assert refused.value.code == 10337
    assert engine._test_take_commands() == [], "nothing went out"


def test_reqMktDataEx_hands_its_option_list_to_the_engine(connect):
    """With a market data type named, the list is checked by the engine's
    reqMktData, asked under that type for this request alone."""
    ib = connect()
    engine = ib.client._client
    TagValue = ib_async.TagValue
    ib.reqMarketDataType(2)
    set_to = []
    engine.req_market_data_type = set_to.append
    ib.reqMktDataEx(SPY, mktDataOptions=[TagValue("manual", "1")], marketDataType=3)
    name, args = engine.asked[-1]
    assert name == "req_mkt_data"
    assert [(o.tag, o.value) for o in args[-1]] == [("manual", "1")]
    assert set_to == [3, 2], "this request's type, then the session's again"


def test_a_ping_reaches_the_engine(connect):
    ib = connect()
    engine = ib.client._client
    engine._test_take_commands()
    ib.reqPing()
    assert engine._test_take_commands() == ["Ping"]
    assert ib.lastRtt() is None, "no venue answered it"


def test_an_extra_while_not_connected_raises(connect):
    with pytest.raises(ConnectionError):
        ib_async_dx.IB().orderPresets()
    ib = connect()
    ib.disconnect()
    for call in (ib.orderPresets, ib.reqCurrentTimeInMillis, ib.competingSession):
        with pytest.raises(ConnectionError, match="Not connected"):
            call()


def test_the_routing_components_arrive_as_their_records(connect):
    """A record ib_async makes whole, answered from the engine. Built empty
    and filled a field at a time, it could not be made: the callback raised,
    and the session closed.

    Asked by the BBO exchange a quote's tickReqParams names, as a gateway is
    asked; a name no quote was acknowledged under is refused as a gateway
    refuses it, and the session stays up."""
    ib = connect()
    ib.RequestTimeout = 2
    ib.client._client._test_note_reference_data(4, "ARCA", "P", "", "", "")
    assert ib.reqSmartComponents("a60001") == [ib_async.SmartComponent(4, "ARCA", "P")]
    assert ib.reqSmartComponents("SMART") == []
    assert ib.isConnected()


def test_connectionStats_counts_the_messages_each_way(connect):
    """As their client counts them: a request is a message sent, and what
    reaches their wrapper a message received. The byte counts stay at nought:
    the engine does not count the bytes of its connections."""
    ib = connect()
    before = ib.client.connectionStats()
    ib.reqCurrentTime()
    after = ib.client.connectionStats()
    assert after.numMsgSent == before.numMsgSent + 1
    assert after.numMsgRecv > before.numMsgRecv
    assert (after.numBytesSent, after.numBytesRecv) == (0, 0)
    assert after.startTime == before.startTime and after.duration > before.duration
    ib.disconnect()
    with pytest.raises(ConnectionError, match="Not connected"):
        ib.client.connectionStats()


class Malformed(Stating):
    def competing_session(self):
        return ("10.0.0.4", "20261399-99:99:99", True)


def test_a_completed_connect_is_not_failed_by_what_follows_it(connect, monkeypatch):
    """ib_async's connect returns once it has emitted connectedEvent. The
    competing-session warning that followed failed it: a stamp that did not
    parse raised, and so did a handler that had disconnected."""
    monkeypatch.setattr(ibkr_dx, "EClient", Malformed)
    assert connect().isConnected(), "a stamp that does not parse"
    monkeypatch.setattr(ibkr_dx, "EClient", Stating)
    ib = ib_async_dx.IB()
    ib.connectedEvent += ib.disconnect
    assert connect(ib=ib) is ib
    assert not ib.isConnected()


def test_the_competing_session_warning_is_this_packages_own(connect, monkeypatch, caplog):
    """Said on this package's logger, `ib_async_dx.ib`: ib_async's `ib_async.ib`
    carries only what ib_async itself says."""
    import logging

    monkeypatch.setattr(ibkr_dx, "EClient", Stating)
    with caplog.at_level(logging.WARNING):
        connect()
    [warning] = [r for r in caplog.records if "Another session" in r.getMessage()]
    assert (warning.name, warning.levelno) == ("ib_async_dx.ib", logging.WARNING)
    assert "10.0.0.4" in warning.getMessage()


def test_an_unmodified_watchdog_keeps_the_session_up(connect):
    """ib_async's own Watchdog, handed this package's IBC: there is no gateway
    to launch, so it connects, and when the session ends it connects again.
    ib_async's IBC tried to launch a gateway on every turn and never
    connected."""
    ib = ib_async_dx.IB()
    watchdog = ib_async_dx.Watchdog(
        ib_async_dx.IBC(1012, gateway=True, tradingMode="paper"), ib,
        appStartupTime=0, retryDelay=0, readonly=True, connectTimeout=0.5,
    )
    started, stopped = [], []
    watchdog.startedEvent += lambda w: started.append(ib.client)
    watchdog.stoppedEvent += lambda w: stopped.append(w)

    def until(done):
        for _ in range(100):
            if done():
                return
            # A session that ends cancels what ib_async is running, a sleep
            # included, as a dropped socket does.
            with contextlib.suppress(asyncio.CancelledError, ConnectionError):
                ib.sleep(0.1)
        raise AssertionError("never happened")

    # The loop the Watchdog schedules itself on, as ib.run() would find it.
    ib_async.util.getLoop()
    watchdog.start()
    try:
        until(lambda: started)
        assert ib.isConnected()
        started[0]._client._test_end_session()
        until(lambda: len(started) == 2)
        assert stopped == [watchdog]
        assert ib.isConnected() and ib.client is not started[0]
    finally:
        watchdog.stop()
    assert not ib.isConnected()


def test_an_ibc_carries_its_login_to_the_connect_and_terminates_its_session(connect):
    """ib_async documents `IBC(976, gateway=True, tradingMode='live',
    userid=..., password=...)` as how a gateway is given its login. It
    raised here. Its login is the one a connect naming none logs in with, and
    terminating it ends the session that login opened, as stopping a gateway
    ends the sessions of the programs connected to it."""
    for mode, paper in (("live", False), ("paper", True), ("", True)):
        ibc = ib_async_dx.IBC(1012, gateway=True, tradingMode=mode, userid="u", password="p")
        ibc.start()
        ib = connect()
        logon = ib.client._client.logon
        assert (logon["username"], logon["password"], logon["paper"]) == ("u", "p", paper), mode
        assert logon["session_file"].endswith("session-u-" + ("paper" if paper else "live"))
        named = connect(username="me", password="pw")
        assert named.client._client.logon["username"] == "me", "a login named on connect"
        ended = []
        ib.disconnectedEvent += lambda: ended.append(1)
        ibc.terminate()
        assert not ib.isConnected() and ended == [1], mode
        assert named.isConnected(), "only the session its login opened"
        assert connect().client._client.logon["username"] == "", "and it holds it no longer"


def test_a_terminated_ibc_ends_every_session_of_its_login_and_lends_it_to_none(connect):
    """Terminated from another context — a task's copy of the program's own —
    its login stayed held where it was started, and the next connect there
    logged in with it, where no gateway was left to connect to. And it ended
    only the last session its login had opened."""
    import contextvars

    ibc = ib_async_dx.IBC(1012, gateway=True, tradingMode="paper", userid="u", password="p")
    ibc.start()
    first, second = connect(), connect()
    contextvars.copy_context().run(ibc.terminate)
    assert not first.isConnected() and not second.isConnected()
    assert connect().client._client.logon["username"] == ""


def test_an_unmodified_watchdog_logs_in_with_its_ibcs_login(connect):
    """Each Watchdog runs in a task of its own, so each connects with its
    own IBC's login, and connects again with it when the session ends."""
    ibs = [ib_async_dx.IB(), ib_async_dx.IB()]
    ibcs = [
        ib_async_dx.IBC(1012, gateway=True, tradingMode="live", userid="u1", password="p1"),
        ib_async_dx.IBC(1012, gateway=True, tradingMode="paper", userid="u2", password="p2"),
    ]
    watchdogs = [
        ib_async_dx.Watchdog(ibc, ib, appStartupTime=0, retryDelay=0, readonly=True,
                             connectTimeout=0.5)
        for ibc, ib in zip(ibcs, ibs)
    ]
    logons = {id(ib): [] for ib in ibs}
    for watchdog, ib in zip(watchdogs, ibs):
        watchdog.startedEvent += lambda w, ib=ib: logons[id(ib)].append(ib.client._client.logon)

    def until(done):
        for _ in range(100):
            if done():
                return
            with contextlib.suppress(asyncio.CancelledError, ConnectionError):
                ibs[0].sleep(0.1)
        raise AssertionError("never happened")

    ib_async.util.getLoop()
    for watchdog in watchdogs:
        watchdog.start()
    try:
        until(lambda: all(logons.values()))
        ibs[0].client._client._test_end_session()
        until(lambda: len(logons[id(ibs[0])]) == 2)
    finally:
        for watchdog in watchdogs:
            watchdog.stop()
    first, again = logons[id(ibs[0])]
    [second] = logons[id(ibs[1])]
    assert (first["username"], first["password"], first["paper"]) == ("u1", "p1", False)
    assert (again["username"], again["paper"]) == ("u1", False), "the same login again"
    assert (second["username"], second["password"], second["paper"]) == ("u2", "p2", True)
    assert not any(ib.isConnected() for ib in ibs)


class Answering(OfflineEngine):
    """The engine's answers where the test session has no hook to give them."""

    actions = [{
        "kind": "CD", "date": "20260915", "value": "0.25", "currency": "USD",
        "announce_date": "20260801", "record_date": "20260910",
        "pay_date": "20260915", "payment_type": "", "distribution_type": "",
    }]
    strategies = [{
        "legs": [(1001, 1), (1002, -1)], "kind": 3, "aggression": 2,
        "figures": [0.5, 1.7976931348623157e308], "breakEvens": [99.5],
        "lastFigure": 1.7976931348623157e308,
    }]

    def req_adjustments(self, *args):
        self.asked.append(("req_adjustments", args))

    def adjustments_for(self, reqId):
        return self.actions

    def cancel_adjustments(self, reqId):
        self.asked.append(("cancel_adjustments", reqId))

    def req_spread_scan(self, *args):
        self.asked.append(("req_spread_scan", args))

    def scanned_strategies(self, reqId):
        return self.strategies

    def cancel_mkt_data(self, reqId):
        self.asked.append(("cancel_mkt_data", reqId))

    def positions_elsewhere(self):
        return [{
            "con_id": 265598, "symbol": "AAPL", "sec_type": "STK", "currency": "USD",
            "position": 10.0, "avg_cost": 150.5, "held": "Away",
        }]

    def values_elsewhere(self, held):
        return [("NetLiquidation", "1505.00", "USD")] if held == "Away" else []


class Unanswered(Answering):
    actions = None
    strategies = []


def test_reqCorporateActions_asks_by_the_contracts_id_and_takes_the_answer(connect, monkeypatch):
    monkeypatch.setattr(ibkr_dx, "EClient", Answering)
    ib = connect()
    [action] = ib.reqCorporateActions(SPY, "20260101", "20261231")
    assert action == ib_async_dx.CorporateAction(
        "CD", "20260915", "0.25", "USD", "20260801", "20260910", "20260915", "", "",
    )
    [(name, (reqId, *asked))] = ib.client._client.asked
    assert (name, asked) == ("req_adjustments", [756733, "STK", "SMART", "20260101", "20261231"])
    assert reqId not in ib.wrapper._futures, "the request is over once answered"


def test_reqCorporateActions_given_up_withdraws_the_query(connect, monkeypatch):
    monkeypatch.setattr(ibkr_dx, "EClient", Unanswered)
    ib = connect()
    ib.RequestTimeout = 0.1
    with pytest.raises(TimeoutError):
        ib.reqCorporateActions(SPY, "20260101", "20261231")
    engine = ib.client._client
    reqId = engine.asked[0][1][0]
    assert engine.asked[1:] == [("cancel_adjustments", reqId)]


def test_reqCorporateActions_needs_the_contracts_id(connect):
    """The engine's own refusal, as it states it."""
    with pytest.raises(ValueError, match="qualify the contract"):
        connect().reqCorporateActions(ib_async.Stock("SPY", "SMART", "USD"), "20260101", "20261231")


def test_reqSpreadScan_takes_the_first_answer_and_cancels(connect, monkeypatch):
    monkeypatch.setattr(ibkr_dx, "EClient", Answering)
    ib = connect()
    scan = ib_async_dx.SpreadScan()
    [found] = ib.reqSpreadScan(SPY, scan)
    assert (found.legs, found.kind, found.aggression) == ([(1001, 1), (1002, -1)], 3, 2)
    assert found.figures[0] == 0.5 and math.isnan(found.figures[1]), "unstated is nan"
    assert found.breakEvens == [99.5] and math.isnan(found.lastFigure)
    engine = ib.client._client
    [(name, (reqId, contract, sent)), cancel] = engine.asked
    assert (name, contract.conId, sent) == ("req_spread_scan", 756733, scan)
    assert cancel == ("cancel_mkt_data", reqId)
    assert ib.wrapper.reqId2Ticker[reqId] is ib.ticker(SPY), "its quotes go to the ticker"
    assert reqId not in ib.wrapper.ticker2ReqId["spreadScan"].values()


def test_reqSpreadScan_unanswered_in_time_found_nothing(connect, monkeypatch):
    monkeypatch.setattr(ibkr_dx, "EClient", Unanswered)
    ib = connect()
    assert ib.reqSpreadScan(SPY, ib_async_dx.SpreadScan(), timeout=0.1) == []
    assert ib.client._client.asked[-1][0] == "cancel_mkt_data"


class Refusing(Answering):
    """Refuses the scan inside the call, as the engine refuses a request."""

    strategies = []

    def __init__(self, callbacks):
        super().__init__(callbacks)
        self.callbacks = callbacks

    def req_spread_scan(self, reqId, *args):
        self.asked.append(("req_spread_scan", (reqId, *args)))
        self.callbacks.error(reqId, 0, 200, "No security definition has been found", "")


def test_reqSpreadScan_refused_cancels_nothing(connect, monkeypatch):
    """A subscription the venue refused is not up, so nothing is cancelled:
    the cancel would only be refused in turn."""
    monkeypatch.setattr(ibkr_dx, "EClient", Refusing)
    ib = connect()
    heard = _heard(ib)
    assert ib.reqSpreadScan(SPY, ib_async_dx.SpreadScan()) == []
    [(name, (reqId, *_))] = ib.client._client.asked
    assert name == "req_spread_scan" and heard == [(reqId, 200)]


@pytest.mark.parametrize("asking", [
    lambda ib: ib.reqCorporateActionsAsync(SPY, "20260101", "20261231"),
    lambda ib: ib.reqSpreadScanAsync(SPY, ib_async_dx.SpreadScan(), timeout=0),
], ids=["reqCorporateActions", "reqSpreadScan"])
def test_a_request_the_program_disconnects_under_ends_with_the_session(
    connect, monkeypatch, asking,
):
    """ib_async's disconnect drops every request its wrapper holds. One of
    these waiting then ends as a request made while not connected does, and
    withdraws nothing: the session it was asked on is gone, and the number
    may already be the next session's."""
    monkeypatch.setattr(ibkr_dx, "EClient", Unanswered)
    ib = connect()
    engine = ib.client._client

    async def disconnecting():
        waiting = asyncio.ensure_future(asking(ib))
        await asyncio.sleep(0.05)
        ib.disconnect()
        with pytest.raises(ConnectionError, match="Not connected"):
            await asyncio.wait_for(waiting, 1)

    ib.run(disconnecting())
    assert [name for name, *_ in engine.asked] in (
        ["req_adjustments"], ["req_spread_scan"],
    )


def test_holdings_elsewhere_are_kept_apart(connect, monkeypatch):
    monkeypatch.setattr(ibkr_dx, "EClient", Answering)
    ib = connect()
    assert ib.positionsElsewhere() == [
        ib_async_dx.PositionElsewhere(265598, "AAPL", "STK", "USD", 10.0, 150.5, "Away"),
    ]
    assert ib.accountValuesElsewhere("Away") == [
        ib_async.AccountValue("DU000000", "NetLiquidation", "1505.00", "USD", ""),
    ]


def test_accountValuesElsewhere_names_one_of_three_sets(connect):
    """The engine's own reads, on a session holding nothing elsewhere."""
    ib = connect()
    assert ib.positionsElsewhere() == []
    assert ib.accountValuesElsewhere("DisplayOnly") == []
    with pytest.raises(ValueError, match="Away"):
        ib.accountValuesElsewhere("Nowhere")

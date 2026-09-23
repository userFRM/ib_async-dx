"""`ib_async_dx.IB`, connected, on the engine's test session.

Needs no venue: the engine's own test session stands in for a logon, and what
`connect` was given is recorded where the logon would have taken it.
"""

import datetime
import inspect
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
    # Each connect attaches again; placeOrder is wrapped once, not once per connect.
    placing = inspect.getclosurevars(ib.placeOrder).nonlocals["placing"]
    assert placing is ib_async.IB.placeOrder


def test_a_second_connect_closes_the_first_session(connect):
    ib = connect()
    first = ib.client
    connect(ib=ib)
    assert first.connState == first.DISCONNECTED and first._stop.is_set()
    assert not first._client.is_connected(), "the first session is logged out"
    assert ib.isConnected()


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

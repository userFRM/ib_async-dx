"""A session's life, from the login to the end, on the program's own loop.

Needs no venue: the engine's test session stands in for a logon, and where a
test has to hold the login open, the engine's own rule for a disconnect that
lands during one is kept — the session that opens is dropped, not installed.
"""

import asyncio
import inspect
import logging
import pathlib
import subprocess
import sys
import threading
import time

import ib_async
import ibkr_dx
import pytest
from test_ib_runs_on_the_engine import SPY, OfflineEngine, connect  # noqa: F401  (a fixture)

import ib_async_dx
from ib_async_dx import StartupFetch, StartupFetchNONE
from ib_async_dx.bridge import IbkrDxClient


class Gated(OfflineEngine):
    """A login that waits until the test lets it through, and keeps the
    engine's rule: a disconnect counted while it ran drops the session."""

    def __init__(self, callbacks):
        super().__init__(callbacks)
        self.entered = threading.Event()
        self.release = threading.Event()

    def connect(self, **logon):
        before = self._test_disconnects()
        self.entered.set()
        self.release.wait(5)
        if self._test_disconnects() != before:
            raise RuntimeError(
                "Connection abandoned: disconnect() was called while it was still logging in"
            )
        super().connect(**logon)


async def _logging_in(ib, **kwargs):
    """A connect whose login is under way, and the engine it runs on."""
    kwargs.setdefault("fetchFields", StartupFetchNONE)
    task = asyncio.ensure_future(ib.connectAsync(**kwargs))
    for _ in range(500):
        await asyncio.sleep(0.01)
        engine = getattr(ib.client, "_client", None)
        if isinstance(engine, Gated) and engine.entered.is_set():
            return task, engine
    raise AssertionError("the login never started")


@pytest.fixture
def gated(monkeypatch):
    monkeypatch.setattr(ibkr_dx, "EClient", Gated)
    monkeypatch.delenv("IB_USERNAME", raising=False)
    monkeypatch.delenv("IB_PASSWORD", raising=False)
    engines = []
    yield engines
    for engine in engines:
        engine.release.set()


def test_a_connect_cancelled_during_its_login_leaves_no_session(gated):
    """The login goes on in the engine after the caller gave up on it, and
    the engine drops what it opens only if it is told. Never told, it kept a
    session logged in that nothing could reach."""
    ib = ib_async_dx.IB()

    async def cancelled():
        task, engine = await _logging_in(ib)
        gated.append(engine)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        engine.release.set()
        await asyncio.sleep(0.2)
        return engine

    engine = asyncio.run(cancelled())
    assert engine._test_disconnects() == 1, "the engine was told"
    assert not engine.is_connected(), "and opened nothing"
    assert ib.client.connState == IbkrDxClient.DISCONNECTED


def test_a_disconnect_during_the_login_ends_it(gated):
    """ib_async's own disconnect does nothing while its client is connecting,
    so the connect went on and opened the session the program had asked to
    close. The connect ends with ConnectionError, and nothing stays open."""
    ib = ib_async_dx.IB()

    async def disconnected():
        task, engine = await _logging_in(ib)
        gated.append(engine)
        ib.disconnect()
        with pytest.raises(ConnectionError):
            await asyncio.wait_for(task, 1)
        engine.release.set()
        await asyncio.sleep(0.2)
        return engine

    engine = asyncio.run(disconnected())
    assert not engine.is_connected()
    assert not ib.isConnected()


@pytest.mark.parametrize("theirs", [False, True], ids=["IB", "attached ib_async.IB"])
def test_a_later_connect_retires_an_earlier_one_still_logging_in(gated, theirs):
    """Two connects on one IB: the later one's session is the one left open.
    The earlier ends with ConnectionError at once, rather than when its login
    returns, and what its login opens is dropped by the engine."""
    ib = ib_async_dx.attach(ib_async.IB()) if theirs else ib_async_dx.IB()

    async def twice():
        first, engine = await _logging_in(ib)
        gated.append(engine)
        second = asyncio.ensure_future(ib.connectAsync(fetchFields=StartupFetchNONE))
        await asyncio.sleep(0.05)
        latest = ib.client._client
        gated.append(latest)
        with pytest.raises(ConnectionError):
            await asyncio.wait_for(first, 1)
        latest.release.set()
        engine.release.set()
        await asyncio.wait_for(second, 2)
        assert ib.isConnected(), "the later connect's session"
        await asyncio.sleep(0.1)
        assert ib.isConnected(), "and nothing the earlier one did afterwards closed it"
        connected = ib.client._client.is_connected()
        ib.disconnect()
        return engine, connected

    engine, connected = asyncio.run(twice())
    assert connected
    assert engine._test_disconnects() >= 1


class SlowToNameTheOrders(OfflineEngine):
    """The first session's venue slow to name the orders working, once
    logged in."""

    named = []

    def next_shared_id(self):
        if not self.named:
            self.named.append(self)
            time.sleep(0.5)
        return super().next_shared_id()


@pytest.mark.parametrize("theirs", [False, True], ids=["IB", "attached ib_async.IB"])
def test_a_connect_overtaken_after_its_login_leaves_the_later_session_open(monkeypatch, theirs):
    """The earlier connect, logged in and waiting on the venue, went on
    waiting after a later connect had overtaken it, failed once the later
    one had opened, and ib_async's disconnect on its way out closed the
    later one's session."""
    monkeypatch.setattr(ibkr_dx, "EClient", SlowToNameTheOrders)
    monkeypatch.setattr(SlowToNameTheOrders, "named", [])
    ib = ib_async_dx.attach(ib_async.IB()) if theirs else ib_async_dx.IB()
    ended = []
    ib.disconnectedEvent += lambda: ended.append(1)

    async def twice():
        first = asyncio.ensure_future(ib.connectAsync(fetchFields=StartupFetchNONE))
        await asyncio.sleep(0.1)
        await asyncio.wait_for(ib.connectAsync(fetchFields=StartupFetchNONE), 2)
        with pytest.raises(ConnectionError):
            await first
        await asyncio.sleep(0.1)
        held = ib.isConnected(), ib.client._client.is_connected()
        ib.disconnect()
        return held

    assert asyncio.run(twice()) == (True, True)
    assert ended == [1], "only the disconnect at the end"


class SlowFirstSync(OfflineEngine):
    """The first session's venue never answers its open orders."""

    opened = []

    def connect(self, **logon):
        self.opened.append(self)
        super().connect(**logon)

    def req_open_orders(self):
        if self is not self.opened[0]:
            super().req_open_orders()


@pytest.mark.parametrize("raiseSyncErrors", [False, True])
def test_a_connect_overtaken_in_its_startup_sync_fails_and_leaves_the_later_open(
        monkeypatch, raiseSyncErrors):
    """The earlier connect waited out its timeout on requests the later
    one's disconnect had dropped, then reported success on the later
    session, or failed and closed it."""
    monkeypatch.setattr(ibkr_dx, "EClient", SlowFirstSync)
    monkeypatch.setattr(SlowFirstSync, "opened", [])
    ib = ib_async_dx.IB()
    connected = []
    ib.connectedEvent += lambda: connected.append(1)

    async def twice():
        first = asyncio.ensure_future(ib.connectAsync(
            timeout=0.5, fetchFields=StartupFetch.ORDERS_OPEN, raiseSyncErrors=raiseSyncErrors,
        ))
        await asyncio.sleep(0.1)
        await asyncio.wait_for(
            ib.connectAsync(timeout=0.5, fetchFields=StartupFetch.ORDERS_OPEN), 2
        )
        with pytest.raises(ConnectionError):
            await first
        await asyncio.sleep(0.1)
        held = ib.isConnected()
        ib.disconnect()
        return held

    assert asyncio.run(twice())
    assert connected == [1], "said of the later session only"


def test_of_three_connects_at_once_the_last_is_left_open(monkeypatch):
    monkeypatch.setattr(ibkr_dx, "EClient", SlowLogin)
    ib = ib_async_dx.IB()

    async def thrice():
        connects = [
            asyncio.ensure_future(ib.connectAsync(fetchFields=StartupFetchNONE)) for _ in range(3)
        ]
        done = await asyncio.gather(*connects, return_exceptions=True)
        held = ib.isConnected()
        ib.disconnect()
        return [type(d).__name__ for d in done], held

    assert asyncio.run(thrice()) == (["ConnectionError", "ConnectionError", "IB"], True)


class NoticeOnTheFirstPass(OfflineEngine):
    def poll(self):
        if not getattr(self, "noticed", False):
            self.noticed = True
            self.callbacks.error(-1, 0, 10197, "No market data during competing live session", "")
        super().poll()

    def __init__(self, callbacks):
        super().__init__(callbacks)
        self.callbacks = callbacks


def test_a_disconnect_from_a_handler_during_the_connect_ends_it(monkeypatch):
    """A notice the connect delivers, and a handler that disconnects on it:
    the disconnect was taken for ib_async's own on a failed connect, which
    does nothing while connecting, and the session opened anyway."""
    monkeypatch.setattr(ibkr_dx, "EClient", NoticeOnTheFirstPass)
    ib = ib_async_dx.IB()
    ib.errorEvent += lambda reqId, code, text, contract: code == 10197 and ib.disconnect()

    async def main():
        with pytest.raises(ConnectionError):
            await ib.connectAsync(fetchFields=StartupFetchNONE)
        await asyncio.sleep(0.05)
        return ib.isConnected(), ib.client._client.is_connected()

    assert asyncio.run(main()) == (False, False)


def test_a_login_given_up_on_does_not_hold_the_program_open():
    """A live login waits on its second factor for as long as the engine
    allows, eighteen minutes by default. Given up on, it held a worker of the
    loop's executor, which the program waits for as it exits."""
    child = f"""
import asyncio, sys, time
sys.path.insert(0, {str(pathlib.Path(__file__).parent)!r})
import ibkr_dx
from test_ib_runs_on_the_engine import OfflineEngine
import ib_async_dx

class SecondFactor(OfflineEngine):
    def connect(self, **logon):
        time.sleep(20)

ibkr_dx.EClient = SecondFactor

async def main():
    try:
        await asyncio.wait_for(
            ib_async_dx.IB().connectAsync(fetchFields=ib_async_dx.StartupFetchNONE), 0.2
        )
    except TimeoutError:
        pass

asyncio.run(main())
"""
    started = time.monotonic()
    subprocess.run([sys.executable, "-c", child], check=True, timeout=60)
    assert time.monotonic() - started < 10


class InstallsLate(OfflineEngine):
    """The first login installs its session and announces it after a
    disconnect has reached the engine, as the engine can when the disconnect
    lands between its check and the install."""

    first = []

    def __init__(self, callbacks):
        super().__init__(callbacks)
        self.callbacks = callbacks
        self.release = threading.Event()

    def connect(self, **logon):
        if self.first:
            return super().connect(**logon)
        self.first.append(self)
        self.release.wait(5)
        super().connect(**logon)
        self.callbacks.connect_ack()
        self.callbacks.managed_accounts("DU_EARLIER")
        self.callbacks.next_valid_id(1)


@pytest.mark.parametrize("theirs", [False, True], ids=["IB", "attached ib_async.IB"])
def test_a_login_given_up_on_reaches_nothing_and_keeps_nothing(monkeypatch, theirs):
    """Given up on, a login went on inside the engine, and what it announced
    from its thread reached the wrapper the later session shares. Its session
    is closed by the login that opened it, on an engine of its own: on the
    later session's engine, that closed the later session."""
    monkeypatch.setattr(ibkr_dx, "EClient", InstallsLate)
    monkeypatch.setattr(InstallsLate, "first", [])
    ib = ib_async_dx.attach(ib_async.IB()) if theirs else ib_async_dx.IB()
    heard = []
    for name in ("connectAck", "managedAccounts", "nextValidId"):
        theirs_ = getattr(ib.wrapper, name)

        def recording(*args, theirs_=theirs_, name=name):
            heard.append((name, threading.get_ident()))
            return theirs_(*args)

        setattr(ib.wrapper, name, recording)

    async def main():
        first = asyncio.ensure_future(ib.connectAsync(fetchFields=StartupFetchNONE))
        while not InstallsLate.first:
            await asyncio.sleep(0.01)
        earlier = InstallsLate.first[0]
        await asyncio.wait_for(ib.connectAsync(fetchFields=StartupFetchNONE), 2)
        with pytest.raises(ConnectionError):
            await first
        heard.clear()
        earlier.release.set()
        await asyncio.sleep(0.3)
        held = ib.isConnected(), ib.client._client.is_connected(), ib.wrapper.accounts
        kept = earlier.is_connected()
        ib.disconnect()
        return held, kept

    held, kept = asyncio.run(main())
    assert heard == [], "nothing from the login's thread"
    assert held == (True, True, ["DU000000"]), "the later session untouched"
    assert not kept, "and the earlier login's session closed"


def test_only_the_connects_own_startup_request_for_positions_is_skipped(gated):
    """ib_async 2.1 asks for positions as it connects whatever fetchFields
    says, and that one request is answered from the cache. Any request made
    by the program meanwhile is its own: one made during the login took the
    skip, was answered an empty list rather than refused as not connected,
    and the startup then asked for positions after all."""
    ib = ib_async_dx.IB()

    async def meanwhile():
        task, engine = await _logging_in(ib)
        gated.append(engine)
        with pytest.raises(ConnectionError):
            await ib.reqPositionsAsync()
        engine.release.set()
        await asyncio.wait_for(task, 2)
        return engine

    engine = asyncio.run(meanwhile())
    assert engine.asked == [], "the startup asked for none"
    ib.disconnect()


class Announcing(OfflineEngine):
    """Announces the new session from inside the login, as the engine does:
    on whatever thread the login runs on."""

    def __init__(self, callbacks):
        super().__init__(callbacks)
        self.callbacks = callbacks

    def connect(self, **logon):
        super().connect(**logon)
        self.callbacks.connect_ack()
        self.callbacks.managed_accounts("DU000000")
        self.callbacks.next_valid_id(1000)


def test_their_wrapper_is_called_on_the_loops_thread_only(connect, monkeypatch):
    """The login runs off the loop, and the engine announces the session from
    it. Delivered there, their wrapper was called on another thread, beside
    the loop that owns it."""
    monkeypatch.setattr(ibkr_dx, "EClient", Announcing)
    ib = ib_async_dx.IB()
    threads = []
    for name in ("connectAck", "managedAccounts", "nextValidId", "tcpDataArrived"):
        theirs = getattr(ib.wrapper, name)

        def recording(*args, theirs=theirs, name=name):
            threads.append((name, threading.get_ident()))
            return theirs(*args)

        setattr(ib.wrapper, name, recording)
    connect(ib=ib)
    assert threads, "their wrapper heard the session open"
    assert {thread for _, thread in threads} == {threading.get_ident()}, threads
    assert [name for name, _ in threads if name != "tcpDataArrived"] == [
        "connectAck", "managedAccounts", "nextValidId",
    ], "all three, as their client's handshake says them"
    assert ib.managedAccounts() == ["DU000000"]


def _counting_passes(monkeypatch):
    passes = []
    theirs = IbkrDxClient._pass_once

    def counted(self):
        passes.append(1)
        return theirs(self)

    monkeypatch.setattr(IbkrDxClient, "_pass_once", counted)
    return passes


def test_a_program_that_leaves_the_loop_queues_no_passes(connect, monkeypatch):
    """A pass runs when the loop does. Queued from a thread every 10 ms
    whether the loop ran or not, a program sleeping outside it came back to a
    hundred passes for every second it slept."""
    passes = _counting_passes(monkeypatch)
    ib = connect()
    time.sleep(0.3)
    passes.clear()
    ib.sleep(0)
    assert len(passes) <= 1, len(passes)
    ib.sleep(0.1)
    assert passes, "and passes go on while the loop runs"


def test_a_loop_closed_without_disconnect_leaves_nothing_running(monkeypatch):
    """A program whose loop ends without a disconnect, as many scripts end.
    A thread kept feeding the closed loop, and said so on every turn."""
    monkeypatch.setattr(ibkr_dx, "EClient", OfflineEngine)
    raised = []
    monkeypatch.setattr(threading, "excepthook", raised.append)
    before = set(threading.enumerate())

    async def main():
        await ib_async_dx.IB().connectAsync(fetchFields=StartupFetchNONE)

    asyncio.run(main())
    time.sleep(0.1)
    assert raised == []
    assert not [t for t in set(threading.enumerate()) - before if t.is_alive() and t.daemon]


class EndsAtOnce(OfflineEngine):
    """A session the engine gives up before the first pass is made."""

    def connect(self, **logon):
        super().connect(**logon)
        self._test_end_session()


def test_a_session_that_ends_as_it_opens_fails_the_connect_and_stops(monkeypatch):
    """The connect raises, ib_async's apiError says why, and nothing more is
    done for the session: no passes, and no disconnectedEvent for a session
    that never opened, as ib_async's client says nothing of one either."""
    monkeypatch.setattr(ibkr_dx, "EClient", EndsAtOnce)
    passes = _counting_passes(monkeypatch)
    ib = ib_async_dx.attach(ib_async.IB())
    said, ended = [], []
    ib.client.apiError += said.append
    ib.disconnectedEvent += lambda: ended.append(1)

    async def main():
        with pytest.raises(ConnectionError):
            await ib.connectAsync(fetchFields=StartupFetchNONE)
        passes.clear()
        await asyncio.sleep(0.1)

    asyncio.run(main())
    assert passes == [], "nothing is done for a session that has ended"
    assert said and "API connection failed" in said[-1]
    assert ended == []
    assert not ib.isConnected()


class Refused(OfflineEngine):
    def connect(self, **logon):
        raise RuntimeError("Connection failed: the password was not accepted")


def test_a_login_refused_raises_ConnectionError_and_says_so(monkeypatch):
    """As ib_async's client fails a connect: a ConnectionError a program
    retrying on one catches, apiError said, and the client disconnected. It
    raised the engine's RuntimeError and stayed connecting."""
    monkeypatch.setattr(ibkr_dx, "EClient", Refused)
    ib = ib_async_dx.attach(ib_async.IB())
    said = []
    ib.client.apiError += said.append
    with pytest.raises(ConnectionError, match="password was not accepted"):
        ib.connect(fetchFields=StartupFetchNONE)
    assert said and "password was not accepted" in said[0]
    assert ib.client.connState == IbkrDxClient.DISCONNECTED


class SlowLogin(OfflineEngine):
    def connect(self, **logon):
        time.sleep(0.3)
        super().connect(**logon)


class SlowAfter(OfflineEngine):
    """A venue slow to name the orders already working once logged in."""

    def next_shared_id(self):
        time.sleep(1)
        return super().next_shared_id()


def test_timeout_bounds_the_wait_after_the_login_not_the_login(connect, monkeypatch):
    """A login is the engine's to bound — a live one waits on its second
    factor — as a gateway's login is made before a program connects. The
    engine answers the next id at once after its login, which has already
    waited for the venue to name the working orders; were it to wait, the
    wait would be bounded by `timeout`, which is what this guards."""
    monkeypatch.setattr(ibkr_dx, "EClient", SlowLogin)
    assert connect(timeout=0.1).isConnected(), "a login slower than timeout"

    monkeypatch.setattr(ibkr_dx, "EClient", SlowAfter)
    ib = ib_async_dx.IB()
    started = time.monotonic()
    with pytest.raises(TimeoutError):
        ib.connect(timeout=0.2, fetchFields=StartupFetchNONE)
    assert time.monotonic() - started < 0.9, "the wait was bounded"
    assert not ib.isConnected()
    assert not ib.client._client.is_connected(), "and the session it opened is closed"


def test_connecting_an_attached_ib_that_is_connected_opens_a_new_session(connect):
    """ib_async's own client closes the session it holds before it opens
    another. Here the engine refused the second login as already connected,
    and the client was left connecting."""
    ib = connect(ib=ib_async_dx.attach(ib_async.IB()))
    ended = []
    ib.disconnectedEvent += lambda: ended.append(1)
    connect(ib=ib)
    assert ib.isConnected()
    assert ended == [1], "the first session ended as disconnect() ends one"
    assert ib.wrapper.clientId == 1, "and the new one is keyed by its client id"


def test_serverVersion_is_nought_until_connected(connect):
    """As ib_async's client answers it: 0 until the handshake."""
    ib = ib_async_dx.attach(ib_async.IB())
    assert ib.client.serverVersion() == 0
    connect(ib=ib)
    assert ib.client.serverVersion() == 178
    ib.disconnect()
    assert ib.client.serverVersion() == 0


def test_attach_to_a_connected_ib_ends_its_session_first(connect):
    """The client replaced went on holding its session, and shared the
    wrapper with the new one: its later end failed the new session's
    requests."""
    ib = connect(ib=ib_async_dx.attach(ib_async.IB()))
    old = ib.client
    ended = []
    ib.disconnectedEvent += lambda: ended.append(1)
    ib_async_dx.attach(ib)
    assert ib.client is not old
    assert not old._client.is_connected(), "its session is closed"
    assert ended == [1], "as disconnect() closes one"
    old.apiEnd.emit()
    assert ended == [1], "and the old client is no longer tied to the IB"


def test_attach_names_no_client_id():
    """The client id is connect's: every connect states one, so one given to
    attach was overwritten before it was ever used."""
    assert "client_id" not in inspect.signature(ib_async_dx.attach).parameters
    assert "client_id" not in inspect.signature(IbkrDxClient).parameters


def test_the_client_level_names_mean_what_they_mean_in_ib_async(connect):
    """`connect`, `run` and `reset` reached engine methods of the same names
    that do something else: `connect` took the timeout as a username, `run`
    blocked the loop on the engine's own dispatch, and `reset` logged the
    session out while the client still read connected."""
    theirs = ib_async.client.Client
    for name in ("connect", "run"):
        assert getattr(IbkrDxClient, name) is getattr(theirs, name), name
    for name in ("MaxRequests", "RequestsInterval", "events"):
        assert getattr(IbkrDxClient, name) == getattr(theirs, name), name

    ib = connect()
    engine = ib.client._client
    ib.client.reset()
    assert not ib.client.isConnected() and not engine.is_connected()

    ib = ib_async_dx.attach(ib_async.IB())
    ib.client.connect("127.0.0.1", 7497, 1, 2.0)
    try:
        assert ib.client.isConnected()
        assert ib.client._client.logon["username"] == ""
    finally:
        ib.client.disconnect()


def test_a_request_reached_by_name_takes_keywords(connect):
    """As their client's own methods do. Forwarded as positions only, a
    program calling `ib.client.reqIds(numIds=1)` was refused a TypeError."""
    ib = connect()
    engine = ib.client._client
    engine._test_take_commands()
    ib.client.reqIds(numIds=1)
    ib.client.reqMatchingSymbols(reqId=7, pattern="SPY")
    ib.client.reqMatchingSymbols(8, pattern="SPY")
    with pytest.raises(TypeError):
        ib.client.reqMatchingSymbols(9, "SPY", pattern="SPY")


def test_a_pass_that_raises_ends_the_session_once(connect, caplog):
    """As ib_async's transport closes a socket whose data it could not
    handle: the session ends once, what was waiting fails, and delivery
    stops. Raised on the loop every 10 ms instead, it was logged a hundred
    times a second and the session read as connected throughout."""
    import contextlib
    import logging

    ib = connect()
    engine = ib.client._client
    ended = []
    ib.disconnectedEvent += lambda: ended.append(1)

    def failing():
        raise RuntimeError("the engine failed")

    engine.poll = failing
    with caplog.at_level(logging.ERROR, logger="ib_async_dx"):
        for _ in range(3):
            with contextlib.suppress(ConnectionError):
                ib.sleep(0.05)
    assert ended == [1]
    assert not ib.isConnected() and not engine.is_connected()
    assert len([r for r in caplog.records if "delivery failed" in r.getMessage()]) == 1


def test_what_one_pass_delivers_is_one_batch(connect):
    """As ib_async's transport marks a packet: one `tcpDataArrived` before
    the first message of a pass and one `tcpDataProcessed` after the last,
    both on the loop, so tickers a pass updates are announced together."""
    from test_ib_runs_on_the_engine import SPY

    ib = connect()
    engine = ib.client._client
    engine._test_set_instrument_count(2)
    tickers = []
    for slot, contract in enumerate((SPY, ib_async.Stock("QQQ", "SMART", "USD", conId=320227571))):
        reqId = ib.client.getReqId()
        tickers.append(ib.wrapper.startTicker(reqId, contract, "mktData"))
        engine._test_map_instrument(reqId, slot)
        engine._test_push_quote(slot, bid=100.0 + slot, bid_size=300)
    marks = []
    for name in ("tcpDataArrived", "tcpDataProcessed"):
        theirs = getattr(ib.wrapper, name)

        def marking(theirs=theirs, name=name):
            marks.append((name, threading.get_ident()))
            theirs()

        setattr(ib.wrapper, name, marking)
    ib.pendingTickersEvent += lambda updated: marks.append(set(updated))
    ib.client._pass_once()
    here = threading.get_ident()
    assert marks == [("tcpDataArrived", here), ("tcpDataProcessed", here), set(tickers)]


class NoticeAfterQuotes(OfflineEngine):
    def __init__(self, callbacks):
        super().__init__(callbacks)
        self.callbacks = callbacks
        self.notice = False

    def poll(self):
        super().poll()
        if self.notice:
            self.notice = False
            self.callbacks.error(
                -1, 0, 1100, "Connectivity between IBKR and the API has been lost.", ""
            )


def test_a_handler_that_ends_the_session_mid_pass_ends_the_pass(monkeypatch, caplog):
    """As their client drops what it had read once a handler disconnects:
    a price held for the end of the pass reached a wrapper the disconnect
    had cleared, which logged it as a request it did not know."""
    monkeypatch.setattr(ibkr_dx, "EClient", NoticeAfterQuotes)
    ib = ib_async_dx.IB()
    ib.connect(fetchFields=StartupFetchNONE)
    engine = ib.client._client
    engine._test_set_instrument_count(1)
    reqId = ib.client.getReqId()
    ib.wrapper.startTicker(reqId, SPY, "mktData")
    engine._test_map_instrument(reqId, 0)
    engine._test_push_quote(0, bid=100.0, bid_size=300)
    ib.client._pass_once()
    engine._test_push_quote(0, bid=101.0, bid_size=300)
    engine.notice = True
    ib.errorEvent += lambda r, code, t, c: code == 1100 and ib.disconnect()
    with caplog.at_level(logging.ERROR, logger="ib_async"):
        ib.client._pass_once()
    assert not ib.isConnected()
    assert not [r for r in caplog.records if "Unknown reqId" in r.getMessage()]


def test_an_interrupted_connect_opens_nothing_later(monkeypatch):
    """An interrupt leaves ib_async's loop with the connect still on it, and
    the next call that ran the loop finished it: the session the program had
    been stopped from opening opened."""
    monkeypatch.setattr(ibkr_dx, "EClient", SlowLogin)
    ib = ib_async_dx.IB()

    def interrupt():
        raise KeyboardInterrupt

    ib_async.util.getLoop().call_later(0.1, interrupt)
    with pytest.raises(KeyboardInterrupt):
        ib.connect(fetchFields=StartupFetchNONE)
    ib.sleep(0.6)
    assert not ib.isConnected()
    assert not ib.client._client.is_connected()


def test_a_handler_that_ends_the_session_leaves_the_refusals_behind_it(connect):
    """Refused inside their calls, two requests are answered on the next
    pass. A handler that ends the session on the first: the second reached a
    wrapper the disconnect had cleared, as if the session were still open."""
    ib = connect()
    heard = []

    def on_error(reqId, code, text, contract):
        heard.append(reqId)
        ib.disconnect()

    ib.errorEvent += on_error
    ib.client._callbacks.refused.extend([(7, 200, "No security definition", ""),
                                         (8, 200, "No security definition", "")])
    ib.client._pass_once()
    assert heard == [7]

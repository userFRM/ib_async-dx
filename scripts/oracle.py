#!/usr/bin/env python3
"""ib_async 2.1.0's own answer to each scenario in tests/oracle: what ib_async-dx is held to.

Run it with the interpreter that has ib_async 2.1.0 and aeventkit installed:

    python scripts/oracle.py            # every scenario; then each departure must have one
    python scripts/oracle.py NAME ...   # only the named scenarios

Each ``tests/oracle/NAME.json`` is replayed through ib_async's own ``IB``, ``Wrapper`` and
``Client``. The ``Client`` keeps its code, but its ``Connection`` opens no socket, and the clock
moves only when a step says so. What ib_async does, with the scenario's stated departures applied,
is written to ``NAME.expected.json``; ``defaults.expected.json`` holds every type's field defaults.

Scenario (input, in ib_async's spelling)
    ``about``      what the scenario shows.
    ``delta``      the stated departures it tests, by the design's names (``DEPARTURES``). A step
                   that meets a departure the scenario does not name stops the run.
    ``connected``  optional ``{"client_id", "next_id", "accounts"}``: a session already logged on.
    ``steps``      run in order; the loop settles after each. A step with ``"delta": NAME`` runs
                   only when that departure is applied: what the engine says, a gateway does not.

    {"read": [CALLBACK, ...]}
        One pass: tcpDataArrived, each Wrapper callback, tcpDataProcessed. A callback is
        ``[name, *args]`` or ``{"cb", "args", "origin"}``, with its arguments as ib_async's decoder
        hands them to the Wrapper. ``origin`` is what the engine says an error is about:
        ``{"Order": {"id", "op"}}``, ``{"Request": {"id", "ends"}}``,
        ``{"Question": {"q", "ends"}}`` or ``"Session"``. A close inside a read drops the read's
        later callbacks.
    {"call": NAME, "args", "kwargs", "on", "as"}
        An IB method, or a method of the object stored as ``on``. A method whose body is
        ``self._run(self.NAMEAsync(..))`` is its blocking face: util.run of the twin, which a
        global error cancels. An awaitable runs as a task. ``as`` stores the value and records it
        in the step where it is known. ``run`` with no argument never returns.
    {"get": PATH, "on", "as"}, {"set": PATH, "on", "value"}   read or write an attribute path.
    {"let": {NAME: VALUE}}   store values.     {"show": NAME, "as"}   record a stored one again.
    {"eq": [A, B], "as"}   Python's ``==``.    {"advance": SECONDS}   move the clock.
    {"logon": {"next_id", "accounts"}}   the gateway's handshake answer, as one packet.
    {"connected": {...}}   as the top-level key, mid-scenario.   {"peer_close": true}
    {"handler": NAME, "on": EVENT_PATH, "do": [STEP, ...]}   a handler: logged, then runs ``do``.
    {"clear": EVENT_PATH}   Event.clear().     {"drop": true}   the IB is collected.
    {"helper": NAME, "on": REF, "args", "as"}   a Ticker helper stage (bidasks, tickbars, ...).

    Values: ``{"@": Class, field: value}`` builds an ib_async object and ``{"ref": NAME}`` is a
    stored one. ``{"id": NAME, "n": i}`` is the i-th number (reqId, orderId) sent by the call
    stored as NAME; an origin's ids are written the same way. ``{"f": "nan"}``,
    ``{"dt": ISO, "tz": ZONE}``, ``{"naive": ISO}`` and ``{"date": ISO}`` as in the expectation.

Expectation (output, in Rust spelling: every name goes through the parity mapping ``snake()``)
    ``steps[i].log``      everything in order. ``{"emit", "on", "args"}`` is an event, where
                          ``on`` is ``IB``, ``Client``, ``util``, ``helper`` or the type owning it;
                          ``{"done", "on", "key"}`` an event set done; ``{"send", "args"}`` a
                          request the Client sends, with its ib_async parameters (the handshake's
                          startApi is not one); ``{"handler": NAME}`` a handler's call.
                          ``tick_event`` is ib_async-dx's added event: one per record appended to
                          a ticker's ticks, tickByTicks or domTicks, at the append.
    ``steps[i].results``  the values of calls that completed in the step. An exception is
                          ``{"raise": {"@": type, "message"}}``, with ``req_id`` and ``code`` for a
                          RequestError; ConnectionError("Not connected") is NotConnected.
    ``steps[i].state``    trades, tickers, positions, portfolio, account_values, fills, pnl,
                          pnl_single and realtime_bars after the step; an empty one is left out.
    ``pending``           the calls that never completed.
    ``ib_async``          for a scenario with departures: the steps where ib_async itself differs,
                          and its pending calls.

    An object is ``{"@": type, field: value}`` with the fields that differ from the type's
    default in ``defaults.expected.json``. A field whose default is UNSET_DOUBLE or UNSET_INTEGER
    reads null when unset, as the Rust ``Option`` does. A bar or scan list is its type with its
    items under ``bars`` or ``data`` and its request's attributes; a DynamicObject is a plain
    object of its values; a dict's keys are strings. Floats that are not finite are
    ``{"f": "nan" | "inf" | "-inf"}``, and numbers compare by value, since ib_async keeps an int in
    some float fields. Datetimes are ``{"dt": isoformat, "tz": zone}``, ``{"naive": ..}`` or
    ``{"date": ..}``. Ticker.created is not observed (X8), nor are ib_async's log lines.

Two things are fixed so that a replay is deterministic: the pending tickers keep the order they
were added in, as ib_async-dx keeps them (ib_async iterates a set, in the order of the tickers'
ids), and the pass end and every ``now()`` read the scenario clock.
"""

import asyncio
import builtins
import contextlib
import contextvars
import dataclasses
import datetime as dt
import functools
import inspect
import json
import logging
import math
import re
import struct
import sys
import types
from collections import defaultdict, deque
from pathlib import Path
from zoneinfo import ZoneInfo

import eventkit
import ib_async
from eventkit.util import NO_VALUE
from ib_async import IB, StartupFetch, util
from ib_async import client as ib_client
from ib_async import ib as ib_ib
from ib_async import wrapper as ib_wrapper
from ib_async.client import Client
from ib_async.connection import Connection
from ib_async.contract import Contract
from ib_async.objects import BarDataList, DynamicObject, RealTimeBarList, ScanDataList
from ib_async.order import Order, Trade
from ib_async.ticker import BarList, Ticker
from ib_async.wrapper import RequestError

SCENARIOS = Path(__file__).resolve().parent.parent / "tests" / "oracle"

# The stated departures every one of which a scenario must test (design §9.2).
DEPARTURES = (
    "X11",
    "X12",
    "X13",
    "X20 (1)",
    "X20 (2)",
    "X20 (3)",
    "X20 (4)",
    "X20 (6)",
    "X20 (8)",
    "X20 (9)",
    "X20 (10)",
    "X20 (11)",
    "X20 (12)",
    "X22",
    "§3.3 single-value Err",
    "§3.4a manual cancel time",
    "req_user_info",
)

START = 1704207600.0  # 2024-01-02 15:00:00 UTC
SERVER_VERSION = Client.MaxClientVersion
WARNING_CODES = frozenset({105, 110, 165, 321, 329, 399, 404, 434, 492, 10167})  # wrapper.py:1609
OBJECT_EVENTS = (*Trade.events, *Ticker.events)  # the lists' updateEvent shares Ticker's name
TICK_LISTS = ("ticks", "tickByTicks", "domTicks")
LIST_ITEMS = {BarDataList: "bars", RealTimeBarList: "bars", ScanDataList: "data", BarList: "bars"}
RUST_KEYWORDS = frozenset(
    "as async await break const continue crate dyn else enum extern false fn for gen if impl in "
    "let loop match mod move mut pub ref return self static struct super trait true type unsafe "
    "use where while abstract become box do final macro override priv try typeof unsized virtual "
    "yield".split()
)
MISSING = object()
OPEN = object()  # the transport of a connection that has no socket
CALL = contextvars.ContextVar("call", default=None)  # the label of the call whose sends run now
RUN = None  # the replay that records what is emitted


def snake(name):
    """The parity mapping from an ib_async name to its Rust name."""
    s = name.replace("PnL", "Pnl").replace("PNL", "Pnl")
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", s)
    s = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", "_", s)
    s = re.sub(r"(?<=[A-Za-z])(?=[0-9])", "_", s)
    s = re.sub(r"(?<=[0-9])(?=[A-Za-z])", "_", s).lower()
    return s + "_" if s in RUST_KEYWORDS else s


# ------------------------------------------------------ the clock, the socket, the observers


class Clock:
    t = START


class FrozenDatetime(dt.datetime):
    """datetime whose now() reads the scenario clock."""

    @classmethod
    def now(cls, tz=None):
        return dt.datetime.fromtimestamp(Clock.t, tz)


class VirtualLoop(asyncio.SelectorEventLoop):
    """An event loop whose timers run on the scenario clock, counted from its start: at an epoch's
    magnitude the loop's clock resolution vanishes in rounding and a due timer never runs."""

    def time(self):
        return Clock.t - START


class StubConnection(Connection):
    """ib_async's Connection with no socket. Sends are counted, and a close reports the loss of the
    connection on the next loop turn, as an asyncio transport's close does."""

    def open(self):
        self.transport, self.closing = OPEN, False

    async def connectAsync(self, host, port):
        if self.transport:
            self.disconnect()
            await self.disconnected
        self.reset()
        self.open()

    def disconnect(self):
        if self.transport and not self.closing:
            self.closing = True
            asyncio.get_running_loop().call_soon(self.connection_lost, None)

    def connection_lost(self, exc):
        if self.transport:
            super().connection_lost(exc)

    def sendMsg(self, msg):
        if self.transport:
            self.numBytesSent += len(msg)
            self.numMsgSent += 1


class OrderedSet(dict):
    """The wrapper's pending tickers, in the order they were added."""

    def add(self, item):
        self[item] = None


class PositionOrder(dict):
    """A depth side whose values come in position order (X20 (2))."""

    def values(self):
        return [self[k] for k in sorted(self)]


class TickList(list):
    """A ticker's ticks, tickByTicks or domTicks, which reports each record appended to it."""

    ticker = None

    def append(self, item):
        super().append(item)
        if RUN is not None:
            RUN.log.append(
                {"emit": "tick_event", "on": "IB", "args": [norm(self.ticker), norm(item)]}
            )

    def __iadd__(self, items):
        for item in items:
            self.append(item)
        return self


_emit, _set_done = eventkit.Event.emit, eventkit.Event.set_done


def emit(self, *args):
    if RUN is not None:
        RUN.emitted(self, args)
    return _emit(self, *args)


def set_done(self):
    if RUN is not None and not self._done:
        RUN.ended(self)
    return _set_done(self)


def ticker_setattr(self, name, value):
    if name in TICK_LISTS and type(value) is list:
        value = TickList(value)
        value.ticker = self
    object.__setattr__(self, name, value)


def instrument():
    datetime_module = types.ModuleType("datetime")
    datetime_module.__dict__.update(dt.__dict__)
    datetime_module.datetime = FrozenDatetime
    clock = types.SimpleNamespace(time=lambda: Clock.t)
    ib_client.Connection = StubConnection
    ib_client.time = clock
    ib_wrapper.datetime = FrozenDatetime
    ib_wrapper.time = clock
    # only the pending tickers are built with set() there (wrapper.py:324, 1733)
    ib_wrapper.set = OrderedSet
    ib_ib.datetime = datetime_module
    eventkit.Event.emit = eventkit.Event.__call__ = emit
    eventkit.Event.set_done = set_done
    Ticker.__setattr__ = ticker_setattr


# ---------------------------------------------------------------- normal form


def tag_of(cls):
    if issubclass(cls, Contract):
        return "Contract"
    if issubclass(cls, Order):
        return "Order"
    return cls.__name__


def is_record(cls):
    if issubclass(cls, eventkit.Event):
        return False  # the Ticker helper stages are events, not values
    return dataclasses.is_dataclass(cls) or (issubclass(cls, tuple) and hasattr(cls, "_fields"))


def unset(value):
    return type(value) in (int, float) and value in (util.UNSET_DOUBLE, util.UNSET_INTEGER)


@functools.cache
def fields_of(cls):
    """(name, normal default or MISSING, whether UNSET reads as null) for each observed field."""
    out = []
    if dataclasses.is_dataclass(cls):
        for f in dataclasses.fields(cls):
            if issubclass(cls, Ticker) and f.name == "created":
                continue  # X8
            if f.default is not dataclasses.MISSING:
                default = f.default
            elif f.default_factory is not dataclasses.MISSING:
                default = f.default_factory()
            else:
                out.append((f.name, MISSING, False))
                continue
            out.append((f.name, None if unset(default) else norm(default), unset(default)))
    else:
        for name in cls._fields:
            default = cls._field_defaults.get(name, MISSING)
            out.append((name, default if default is MISSING else norm(default), False))
    return tuple(out)


def norm(v):
    if v is None or isinstance(v, (bool, int, str)):
        return v
    if isinstance(v, float):
        return (
            v
            if math.isfinite(v)
            else {"f": "nan" if math.isnan(v) else "inf" if v > 0 else "-inf"}
        )
    if isinstance(v, dt.datetime):
        return {"dt": v.isoformat(), "tz": str(v.tzinfo)} if v.tzinfo else {"naive": v.isoformat()}
    if isinstance(v, dt.date):
        return {"date": v.isoformat()}
    if isinstance(v, dt.tzinfo):
        return str(v)
    if isinstance(v, BaseException):
        out = {"@": type(v).__name__, "message": str(v)}
        if isinstance(v, RequestError):
            out.update(req_id=v.reqId, code=v.code, message=v.message)
        return out
    if isinstance(v, IB):
        return {"@": "IB"}
    if isinstance(v, DynamicObject):
        return {k: norm(x) for k, x in vars(v).items()}  # data keys, not field names
    if isinstance(v, OrderedSet):
        return [norm(x) for x in v]
    if type(v) in LIST_ITEMS:
        out = {"@": type(v).__name__, LIST_ITEMS[type(v)]: [norm(x) for x in v]}
        for name in inspect.get_annotations(type(v)):
            if hasattr(v, name):
                out[snake(name)] = norm(getattr(v, name))
        return out
    if is_record(type(v)):
        out = {"@": tag_of(type(v))}
        for name, default, nullable in fields_of(type(v)):
            x = getattr(v, name)
            x = None if nullable and unset(x) else norm(x)
            if default is MISSING or x != default:
                out[snake(name)] = x
        return out
    if isinstance(v, dict):
        return {str(k): norm(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [norm(x) for x in v]
    raise TypeError(f"no normal form for {type(v).__name__}")


def defaults_table():
    table = {}
    for module in (ib_async.contract, ib_async.order, ib_async.objects, ib_async.ticker):
        for cls in vars(module).values():
            if isinstance(cls, type) and cls.__module__ == module.__name__ and is_record(cls):
                table.setdefault(
                    tag_of(cls), {snake(n): d for n, d, _ in fields_of(cls) if d is not MISSING}
                )
    return dict(sorted(table.items()))


def key_of(owner):
    if isinstance(owner, Trade):
        return owner.order.orderId if owner.order.orderId > 0 else owner.order.permId
    if isinstance(owner, Ticker):
        return owner.contract.conId
    return getattr(owner, "reqId", None)


def zone(name):
    return dt.UTC if name == "UTC" else ZoneInfo(name)


def find_class(name):
    for module in (ib_async, ib_async.contract, ib_async.order, ib_async.objects, ib_async.ticker):
        if isinstance(getattr(module, name, None), type):
            return getattr(module, name)
    raise KeyError(f"no ib_async class {name}")


def resolve(target, path):
    for part in path.split("."):
        target = getattr(target, part)
    return target


@contextlib.contextmanager
def attr(obj, name, value):
    """Set obj.name for the block, then put back what was there."""
    had, old = name in vars(obj), vars(obj).get(name)
    setattr(obj, name, value)
    try:
        yield
    finally:
        if had:
            setattr(obj, name, old)
        else:
            delattr(obj, name)


@functools.cache
def blocking(name):
    method = getattr(IB, name, None)
    return hasattr(IB, name + "Async") and "self._run(" in inspect.getsource(method)


def chain(source, target):
    def done(f):
        if target.done():
            return
        if f.cancelled():
            target.cancel()
        elif f.exception() is not None:
            target.set_exception(f.exception())
        else:
            target.set_result(f.result())

    asyncio.ensure_future(source).add_done_callback(done)


# ---------------------------------------------------------------- the stated departures


def x11(run):
    """X11: a user disconnect fails every outstanding request with NotConnected; ib_async leaves it
    pending (client.py:238-243)."""
    w, disconnect = run.ib.wrapper, run.ib.client.disconnect

    def patched():
        waiting = [f for f in w._futures.values() if not f.done()]
        disconnect()
        for f in waiting:
            if not f.done():
                f.set_exception(ConnectionError("Not connected"))

    run.ib.client.disconnect = patched


def x12(run):
    """X12: the startup sync asks for positions only when fetchFields holds POSITIONS
    (ib.py:2056)."""
    ib, connect = run.ib, run.ib.connectAsync
    signature = inspect.signature(IB.connectAsync)

    async def patched(*args, **kwargs):
        bound = signature.bind(ib, *args, **kwargs)
        bound.apply_defaults()
        if bound.arguments["fetchFields"] & StartupFetch.POSITIONS:
            return await connect(*args, **kwargs)
        skipped = asyncio.get_running_loop().create_future()
        skipped.set_result([])
        with attr(ib, "reqPositionsAsync", lambda: skipped):
            return await connect(*args, **kwargs)

    ib.connectAsync = patched


QUESTIONS = {
    "reqOpenOrdersAsync": "openOrders",
    "reqAllOpenOrdersAsync": "openOrders",
    "reqCompletedOrdersAsync": "completedOrders",
    "reqPositionsAsync": "positions",
    "reqAccountUpdatesAsync": "accountValues",
    "reqCurrentTimeAsync": "currentTime",
    "reqNewsProvidersAsync": "newsProviders",
    "reqMktDepthExchangesAsync": "mktDepthExchanges",
    "reqScannerParametersAsync": "scannerParams",
}


def x13(run):
    """X13: one exchange of a keyed question is in flight; a later call waits in its lane and is
    sent at the exchange's terminal callback. ib_async's startReq replaces the keyed future
    (wrapper.py:370-382)."""
    ib, w = run.ib, run.ib.wrapper
    lanes = defaultdict(deque)

    def lane(ask, key):
        def queued(*args):
            if key not in w._futures:
                return ask(*args)
            waiter = asyncio.get_running_loop().create_future()
            lanes[key].append((functools.partial(ask, *args), waiter))
            return waiter

        return queued

    for name, key in QUESTIONS.items():
        setattr(ib, name, lane(getattr(ib, name), key))
    end = w._endReq

    def patched(key, result=None, success=True):
        end(key, result, success)
        if lanes.get(key) and key not in w._futures:
            start, waiter = lanes[key].popleft()
            chain(start(), waiter)

    w._endReq = patched


def x20_1(run):
    """X20 (1): vega and theta of -2 read as None, as the other greeks do
    (wrapper.py:1388-1389)."""
    w = run.ib.wrapper
    tick = w.tickOptionComputation

    def patched(
        reqId,
        tickType,
        tickAttrib,
        impliedVol,
        delta,
        optPrice,
        pvDividend,
        gamma,
        vega,
        theta,
        undPrice,
    ):
        vega = None if vega == -2 else vega
        theta = None if theta == -2 else theta
        tick(
            reqId,
            tickType,
            tickAttrib,
            impliedVol,
            delta,
            optPrice,
            pvDividend,
            gamma,
            vega,
            theta,
            undPrice,
        )

    w.tickOptionComputation = patched


def x20_2(run):
    """X20 (2): domBids and domAsks follow position order; ib_async lists them in insertion order
    (wrapper.py:1348-1357)."""
    w = run.ib.wrapper
    depth = w.updateMktDepthL2

    def patched(reqId, position, marketMaker, operation, side, price, size, isSmartDepth=False):
        ticker = w.reqId2Ticker.get(reqId)
        for name in ("domBidsDict", "domAsksDict"):
            if ticker is not None and type(getattr(ticker, name)) is dict:
                setattr(ticker, name, PositionOrder(getattr(ticker, name)))
        depth(reqId, position, marketMaker, operation, side, price, size, isSmartDepth)

    w.updateMktDepthL2 = patched


def x20_3(run):
    """X20 (3): reqHistoricalData's timeout and reqScannerData's cancel also end the subscription
    and detach the request, so a late answer is dropped (ib.py:2366-2372, 2489-2492)."""
    ib, w = run.ib, run.ib.wrapper
    historical, scanner = ib.reqHistoricalDataAsync, ib.reqScannerDataAsync

    async def history(*args, **kwargs):
        bars = await historical(*args, **kwargs)
        future = w._futures.get(bars.reqId)
        if future is not None and future.cancelled():  # its own timeout ran
            w.endSubscription(bars)
            w._futures.pop(bars.reqId, None)
            w._results.pop(bars.reqId, None)
        return bars

    async def scan(*args, **kwargs):
        data = await scanner(*args, **kwargs)
        w.endSubscription(data)
        return data

    ib.reqHistoricalDataAsync, ib.reqScannerDataAsync = history, scan


def x20_4(run):
    """X20 (4): reqTickers ends every snapshot ticker on every path; ib_async skips it when a
    request fails (ib.py:2197-2200)."""
    ib, w = run.ib, run.ib.wrapper
    tickers = ib.reqTickersAsync

    async def patched(*contracts, **kwargs):
        try:
            return await tickers(*contracts, **kwargs)
        except BaseException:
            for contract in contracts:
                ticker = w.tickers.get(hash(contract))
                if ticker is not None:
                    w.endTicker(ticker, "snapshot")
            raise

    ib.reqTickersAsync = patched


def x20_6(run):
    """X20 (6): a user disconnect() sets every object event done before the reset; ib_async
    resets first, so its socket callback ends none (ib.py:402-406; client.py:431)."""
    ib, w = run.ib, run.ib.wrapper
    disconnect = ib.disconnect

    def patched():
        reset = w.reset

        def done_then_reset():
            w.setEventsDone()
            reset()

        with attr(w, "reset", done_then_reset):
            return disconnect()

    ib.disconnect = patched


def x20_8(run):
    """X20 (8): run() returns after a disconnect() call; ib_async's loop runs on
    (util.py:327-332)."""
    ib = run.ib
    disconnect = ib.disconnect

    def patched():
        out = disconnect()
        for future in run.run_futures:
            if not future.done():
                future.set_result(None)
        return out

    ib.disconnect = patched


def publication(run):
    """X20 (9): a session's state starts afresh when it is published; ib_async carries the old
    Wrapper over (client.py:238-243). X20 (10): it holds the connect's client id; ib_async's old
    socket's close resets it to -1 (ib.py:2039; wrapper.py:335, 361-368)."""
    w, client = run.ib.wrapper, run.ib.client
    connect = client.connectAsync

    async def patched(host, port, clientId, timeout=2.0):
        await connect(host, port, clientId, timeout)
        if "X20 (9)" in run.deltas:
            accounts = w.accounts
            w.reset()
            w.accounts = accounts
        w.clientId = int(clientId)

    client.connectAsync = patched


def x20_11(run):
    """X20 (11): 10225's re-request writes the list's stored end with formatIBDatetime; ib_async
    sends it as given (wrapper.py:1708-1721)."""
    w = run.ib.wrapper
    error = w.error

    def patched(reqId, errorCode, errorString, advancedOrderRejectJson):
        bars = w.reqId2Subscriber.get(reqId)
        if errorCode == 10225 and isinstance(bars, BarDataList):
            with attr(bars, "endDateTime", util.formatIBDatetime(bars.endDateTime)):
                return error(reqId, errorCode, errorString, advancedOrderRejectJson)
        return error(reqId, errorCode, errorString, advancedOrderRejectJson)

    w.error = patched


def user_info(run):
    """req_user_info: the white-branding id is the result; ib_async returns []
    (wrapper.py:1571-1572)."""
    w = run.ib.wrapper
    w.userInfo = lambda reqId, whiteBrandingId: w._endReq(reqId, whiteBrandingId)


def applied_by_the_steps(run):
    """X22 and §3.3 are applied to each error by its origin, X20 (12) by the drop step, and §3.4a
    by the steps it marks."""


PATCHES = {
    "X11": x11,
    "X12": x12,
    "X13": x13,
    "X20 (1)": x20_1,
    "X20 (2)": x20_2,
    "X20 (3)": x20_3,
    "X20 (4)": x20_4,
    "X20 (6)": x20_6,
    "X20 (8)": x20_8,
    "X20 (9)": publication,
    "X20 (10)": publication,
    "X20 (11)": x20_11,
    "X20 (12)": applied_by_the_steps,
    "X22": applied_by_the_steps,
    "§3.3 single-value Err": applied_by_the_steps,
    "§3.4a manual cancel time": applied_by_the_steps,
    "req_user_info": user_info,
}
SINGLE_VALUE_SENDS = frozenset(
    {
        "reqHeadTimeStamp",
        "reqFundamentalData",
        "reqNewsArticle",
        "reqUserInfo",
        "reqWshMetaData",
        "reqWshEventData",
    }
)


# ---------------------------------------------------------------- the replay


class Run:
    """One replay of a scenario through a fresh IB, with the given departures applied."""

    def __init__(self, scenario, deltas):
        global RUN
        self.deltas, self.declared = frozenset(deltas), frozenset(scenario.get("delta", []))
        Clock.t = START
        self.loop = VirtualLoop()
        asyncio.set_event_loop(self.loop)
        util.globalErrorEvent._slots.clear()
        util.globalErrorEvent._value = NO_VALUE
        self.ib = ib = IB()
        self.refs, self.ids, self.labels, self.pending = {}, defaultdict(list), {}, {}
        self.log, self.results, self.run_futures, self.single, self.fatal = [], {}, [], set(), None
        for name in IB.events:
            self.labels[id(getattr(ib, name))] = (snake(name), "IB")
        for name in Client.events:
            self.labels[id(getattr(ib.client, name))] = (snake(name), "Client")
        self.labels[id(util.globalErrorEvent)] = ("global_error_event", "util")
        first = Client.reqMktData.__code__.co_firstlineno
        for name, fn in vars(Client).items():
            if (
                inspect.isfunction(fn)
                and fn.__code__.co_firstlineno >= first
                and name != "startApi"
            ):
                setattr(
                    ib.client,
                    name,
                    self.sender(name, getattr(ib.client, name), inspect.signature(fn)),
                )
        for patch in dict.fromkeys(PATCHES[d] for d in DEPARTURES if d in self.deltas):
            patch(self)
        RUN = self
        if "connected" in scenario:
            self.step_connected(scenario)

    # what is observed

    def sender(self, name, send, signature):
        def sent(*args, **kwargs):
            out = send(*args, **kwargs)
            bound = signature.bind(None, *args, **kwargs)
            bound.apply_defaults()
            arguments = {k: v for k, v in bound.arguments.items() if k != "self"}
            self.log.append(
                {"send": snake(name), "args": {snake(k): norm(v) for k, v in arguments.items()}}
            )
            number = next(
                (arguments[p] for p in ("reqId", "orderId", "tickerId") if p in arguments), None
            )
            if CALL.get() and number is not None and not name.startswith("cancel"):
                self.ids[CALL.get()].append(number)
            if (
                name in SINGLE_VALUE_SENDS
                or (name == "reqHistoricalData" and arguments["whatToShow"] == "SCHEDULE")
                or (name == "placeOrder" and arguments["order"].whatIf)
            ):
                self.single.add(number)
            return out

        return sent

    def owned(self, event, owner):
        for name in OBJECT_EVENTS:
            if getattr(owner, name, None) is event:
                return snake(name), tag_of(type(owner))
        return None

    def emitted(self, event, args):
        label = self.labels.get(id(event)) or (self.owned(event, args[0]) if args else None)
        if label is not None:
            self.log.append({"emit": label[0], "on": label[1], "args": [norm(a) for a in args]})

    def ended(self, event):
        if id(event) in self.labels:
            name, on = self.labels[id(event)]
            self.log.append({"done": name, "on": on})
            return
        w = self.ib.wrapper
        for owner in (
            *w.tickers.values(),
            *w.reqId2Subscriber.values(),
            *w.trades.values(),
            *self.refs.values(),
        ):
            label = self.owned(event, owner)
            if label is not None:
                self.log.append({"done": label[0], "on": label[1], "key": key_of(owner)})
                return

    def state(self):
        ib = self.ib
        views = {
            "trades": ib.trades(),
            "tickers": ib.tickers(),
            "positions": ib.positions(),
            "portfolio": ib.portfolio(),
            "account_values": ib.accountValues(),
            "fills": ib.fills(),
            "pnl": ib.pnl(),
            "pnl_single": ib.pnlSingle(),
            "realtime_bars": ib.realtimeBars(),
        }
        return {k: norm(v) for k, v in views.items() if v}

    def decode(self, v):
        if isinstance(v, list):
            return [self.decode(x) for x in v]
        if not isinstance(v, dict):
            return v
        if "@" in v:
            return find_class(v["@"])(**{k: self.decode(x) for k, x in v.items() if k != "@"})
        if "ref" in v:
            return self.refs[v["ref"]]
        if "id" in v:
            return self.ids[v["id"]][v.get("n", 0)]
        if "f" in v:
            return float(v["f"])
        if "dt" in v:
            return dt.datetime.fromisoformat(v["dt"]).astimezone(zone(v["tz"]))
        if "naive" in v:
            return dt.datetime.fromisoformat(v["naive"])
        if "date" in v:
            return dt.date.fromisoformat(v["date"])
        return {k: self.decode(x) for k, x in v.items()}

    # running

    def settle(self):
        loop = self.loop
        for _ in range(10_000):
            loop.call_soon(loop.stop)
            loop.run_forever()
            due = any(not h.cancelled() and h.when() <= loop.time() for h in loop._scheduled)
            if not loop._ready and not due:
                return
        raise RuntimeError("the loop does not settle")

    def play(self, steps):
        out = []
        for step in steps:
            self.log, self.results = [], {}
            if "delta" in step and step["delta"] not in self.deltas:
                out.append({"skipped": True})
                continue
            self.loop.call_soon(self.guarded, step)
            self.settle()
            if isinstance(self.fatal, SystemExit):
                raise self.fatal
            if self.fatal is not None:
                raise RuntimeError(f"step {step}") from self.fatal
            record = {"log": self.log}
            if self.results:
                record["results"] = self.results
            record["state"] = self.state()
            out.append(record)
        return out

    def finish(self):
        global RUN
        RUN = None
        pending = sorted(self.pending)
        for task in asyncio.all_tasks(self.loop):
            task.cancel()
        self.settle()
        self.ib.disconnect = lambda: None  # the collector's IB.__del__ runs nothing more
        self.loop.close()
        asyncio.set_event_loop(None)
        return pending

    def guarded(self, step):
        try:
            self.exec(step)
        except BaseException as e:
            self.fatal = e

    def exec(self, step):
        for kind in STEP_KINDS:
            if kind in step:
                return getattr(self, "step_" + kind)(step)
        raise ValueError(f"unknown step {step}")

    def track(self, label, future):
        def finished(f):
            if label is None:
                return f.cancelled() or f.exception()
            self.pending.pop(label, None)
            if f.cancelled():
                self.results[label] = {"raise": {"@": "CancelledError", "message": ""}}
            elif f.exception() is not None:
                self.results[label] = {"raise": norm(f.exception())}
            else:
                self.refs[label] = f.result()
                self.results[label] = norm(f.result())

        if label is not None:
            self.pending[label] = future
        future.add_done_callback(finished)

    async def labelled(self, awaitable, label):
        CALL.set(label)
        return await awaitable

    async def blocked(self, awaitable, label):
        """util.run(awaitable, timeout=RequestTimeout) as the caller it blocks sees it
        (util.py:344-364)."""
        CALL.set(label)
        timeout = self.ib.RequestTimeout
        task = asyncio.ensure_future(
            asyncio.wait_for(awaitable, timeout) if timeout else awaitable
        )

        def onError(_):
            task.cancel()

        util.globalErrorEvent.connect(onError)
        try:
            return await task
        except asyncio.CancelledError as e:
            raise util.globalErrorEvent.value() or e
        finally:
            util.globalErrorEvent.disconnect(onError)

    # steps

    def step_let(self, step):
        for name, value in step["let"].items():
            self.refs[name] = self.decode(value)

    def step_call(self, step):
        name, label = step["call"], step.get("as")
        target = self.refs[step["on"]] if "on" in step else self.ib
        args, kwargs = self.decode(step.get("args", [])), self.decode(step.get("kwargs", {}))
        if target is self.ib and name == "run" and not args:
            future = self.loop.create_future()  # ib_async's loop runs until something stops it
            self.run_futures.append(future)
            self.track(label, future)
            return
        token = CALL.set(label)
        try:
            if target is self.ib and blocking(name):
                twin = getattr(self.ib, name + "Async")(*args, **kwargs)
                self.track(label, asyncio.ensure_future(self.blocked(twin, label)))
                return
            out = resolve(target, name)(*args, **kwargs)
        except Exception as e:
            if label is not None:
                self.results[label] = {"raise": norm(e)}
            return
        finally:
            CALL.reset(token)
        if inspect.isawaitable(out):
            self.track(label, asyncio.ensure_future(self.labelled(out, label)))
        elif label is not None:
            self.refs[label] = out
            self.results[label] = norm(out)

    def step_get(self, step):
        target = self.refs[step["on"]] if "on" in step else self.ib
        self.results[step["as"]] = norm(resolve(target, step["get"]))

    def step_set(self, step):
        *path, name = step["set"].split(".")
        target = self.refs[step["on"]] if "on" in step else self.ib
        setattr(
            resolve(target, ".".join(path)) if path else target, name, self.decode(step["value"])
        )

    def step_show(self, step):
        self.results[step["as"]] = norm(self.refs[step["show"]])

    def step_eq(self, step):
        a, b = self.decode(step["eq"])
        self.results[step["as"]] = a == b

    def step_advance(self, step):
        Clock.t += step["advance"]

    def step_read(self, step):
        w, client = self.ib.wrapper, self.ib.client
        w.tcpDataArrived()
        for callback in step["read"]:
            if client.connState != Client.CONNECTED:
                # a close empties the client's buffer, which ends the read (client.py:123, 365-367)
                break
            self.callback(callback)
        w.tcpDataProcessed()

    def callback(self, callback):
        if isinstance(callback, dict):
            name, args, origin = callback["cb"], callback["args"], callback.get("origin")
        else:
            name, args, origin = callback[0], callback[1:], None
        args = self.decode(args)
        with contextlib.ExitStack() as rules:
            if name == "error":
                self.error_rules(rules, args, origin)
            try:
                getattr(self.ib.wrapper, name)(*args)
            except Exception as e:  # ib_async's decoder logs it and goes on (decoder.py:171-183)
                print(f"  {name}{tuple(args)} raised {e!r} in ib_async", file=sys.stderr)

    def error_rules(self, rules, args, origin):
        """Apply to an error what the engine says it is about, where ib_async-dx departs by it."""
        w, (req_id, code) = self.ib.wrapper, args[:2]
        trade = w.trades.get((w.clientId, req_id)) if req_id != -1 else None
        pending = trade is not None and trade.orderStatus.status == "PendingSubmit"
        warns = (code in WARNING_CODES or 2100 <= code < 2200) and not (
            code == 110 and (req_id in w._futures or pending)
        )  # ib_async's decision (wrapper.py:1609-1622)
        single = req_id in self.single and req_id in w._futures and not warns
        if single and not self.ib.RaiseRequestErrors and self.departs("§3.3 single-value Err"):
            # §3.3: a request whose result is one value ends with its error, whatever
            # RaiseRequestErrors says
            rules.enter_context(attr(self.ib, "RaiseRequestErrors", True))
        op = origin["Order"]["op"] if isinstance(origin, dict) and "Order" in origin else None
        want = True if op == "Modify" else False if op in ("Place", "Exercise") else warns
        if want != warns and self.departs("X22"):
            # X22: a refused modify takes the warning path, a refused placement or exercise the
            # error path. ib_async's own error() runs, with its warning set (wrapper.py:1609),
            # built by the frozenset the wrapper module names, holding or lacking the code.
            assert not 2100 <= code < 2200 and code != 110, "no such refusal"

            def codes(literal):
                return builtins.frozenset(literal) | {code} if want else builtins.frozenset()

            rules.enter_context(attr(ib_wrapper, "frozenset", codes))

    def departs(self, name):
        """Whether this replay applies a departure the step meets; a scenario must name each one
        its steps meet."""
        if name not in self.declared:
            raise SystemExit(f"a step departs by {name!r}, which the scenario does not name")
        return name in self.deltas

    def step_logon(self, step):
        """The gateway's handshake answer, next valid id and accounts, in one packet
        (client.py:384-410)."""
        s = step["logon"]
        messages = [
            [str(SERVER_VERSION), "20240102 15:00:00 UTC"],
            ["9", "1", str(s["next_id"])],
            ["15", "1", ",".join(s["accounts"])],
        ]
        data = b""
        for message in messages:
            body = ("\0".join(message) + "\0").encode()
            data += struct.pack(">I", len(body)) + body
        self.ib.client.conn.data_received(data)

    def step_connected(self, step):
        s, client, w = step["connected"], self.ib.client, self.ib.wrapper
        client.conn.open()
        client.host, client.port, client.clientId = "127.0.0.1", 7497, s["client_id"]
        client.connState, client._apiReady, client._hasReqId = Client.CONNECTED, True, True
        client._serverVersion = client.decoder.serverVersion = SERVER_VERSION
        client._reqIdSeq, client._accounts = s["next_id"], list(s["accounts"])
        w.clientId, w.accounts = s["client_id"], list(s["accounts"])

    def step_peer_close(self, step):
        self.ib.client.conn.connection_lost(None)

    def step_handler(self, step):
        name, steps = step["handler"], step.get("do", [])

        def handler(*args):
            self.log.append({"handler": name})
            for s in steps:
                self.exec(s)

        self.refs[name] = handler
        resolve(self.ib, step["on"]).connect(handler)

    def step_clear(self, step):
        resolve(self.ib, step["clear"]).clear()

    def step_helper(self, step):
        source = self.refs[step["on"]]
        if isinstance(source, Ticker):
            source = source.updateEvent
        stage = getattr(source, step["helper"])(*self.decode(step.get("args", [])))
        self.refs[step["as"]] = stage
        self.labels[id(stage)] = (step["as"], "helper")

    def step_drop(self, step):
        self.ib.__del__()  # what collecting an IB runs (ib.py:284-285)
        if "X20 (12)" in self.deltas:
            # X20 (12): every IB-level event is set done, tick_event included (ib.py:311-318)
            for name in IB.events:
                getattr(self.ib, name).set_done()
            self.log.append({"done": "tick_event", "on": "IB"})


STEP_KINDS = (
    "let", "call", "get", "set", "show", "eq", "advance", "read", "logon", "connected",
    "peer_close", "handler", "clear", "helper", "drop",
)  # fmt: skip


def replay(scenario, deltas):
    run = Run(scenario, deltas)
    try:
        steps = run.play(scenario["steps"])
    finally:
        pending = run.finish()
    return {"steps": steps, "pending": pending}


# ---------------------------------------------------------------- files


def dump(v, depth=0):
    """JSON with one log entry, result or state item per line."""
    pad = " " * (depth + 1)
    if isinstance(v, dict) and v and depth < 4:
        items = [
            f"{pad}{json.dumps(k, ensure_ascii=False)}: {dump(x, depth + 1)}" for k, x in v.items()
        ]
        return "{\n" + ",\n".join(items) + "\n" + " " * depth + "}"
    if isinstance(v, list) and v and depth <= 4:
        return "[\n" + ",\n".join(pad + dump(x, depth + 1) for x in v) + "\n" + " " * depth + "]"
    return json.dumps(v, ensure_ascii=False, allow_nan=False, separators=(", ", ": "))


def expectation(path):
    scenario = json.loads(path.read_text())
    deltas = scenario.get("delta", [])
    unknown = [d for d in deltas if d not in DEPARTURES]
    if unknown:
        raise SystemExit(f"{path.name}: no departure named {unknown}")
    out = {"scenario": path.stem, "delta": deltas, **replay(scenario, deltas)}
    if deltas:
        own = replay(scenario, [])
        steps = {str(i): s for i, (s, e) in enumerate(zip(own["steps"], out["steps"])) if s != e}
        if not steps and own["pending"] == out["pending"]:
            raise SystemExit(f"{path.name}: its departures change nothing")
        out["ib_async"] = {"steps": steps, "pending": own["pending"]}
    return scenario, out


def main(names):
    assert ib_async.__version__ == "2.1.0", (
        f"ib_async {ib_async.__version__}; the oracle is 2.1.0's"
    )
    for name, rust in (
        ("rule80A", "rule_80_a"),
        ("low13week", "low_13_week"),
        ("yield_", "yield_"),
    ):
        assert snake(name) == rust, (name, snake(name))  # the parity examples (design §2)
    instrument()
    logging.getLogger("ib_async").setLevel(logging.CRITICAL + 1)  # its log lines are not observed
    paths = sorted(p for p in SCENARIOS.glob("*.json") if not p.name.endswith(".expected.json"))
    if names:
        paths = [p for p in paths if p.stem in names]
    tested = set()
    for path in paths:
        scenario, out = expectation(path)
        tested.update(scenario.get("delta", []))
        path.with_suffix(".expected.json").write_text(dump(out) + "\n")
        print(f"{path.stem}: {len(out['steps'])} steps", file=sys.stderr)
    if not names:
        (SCENARIOS / "defaults.expected.json").write_text(dump(defaults_table(), 3) + "\n")
        untested = [d for d in DEPARTURES if d not in tested]
        if untested:
            raise SystemExit(f"stated departures with no scenario: {untested}")


if __name__ == "__main__":
    main(sys.argv[1:])

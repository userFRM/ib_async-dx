"""Run an unmodified `ib_async` program without a gateway.

`ib_async` is layered: `IB`, `Wrapper`, `Ticker`, `Trade` and everything above
them are transport-agnostic, and only its `Client`/`Connection` know there is a
socket to a gateway on localhost. This replaces that one layer with this
engine. Everything above it — the events, the async variants, the notebooks —
runs unchanged, from the copy of `ib_async` already installed.

No part of `ib_async` is copied or modified. `ib_async_dx.IB` attaches itself
when it connects; `attach` is for an `ib_async.IB` a program already holds.

    from ib_async import IB, Stock
    import ib_async_dx

    ib = ib_async_dx.attach(IB(), username="…", password="…")
    ib.connect()                      # names no host: there is no gateway

    spy = Stock("SPY", "SMART", "USD")
    ib.qualifyContracts(spy)
    print(ib.reqHistoricalData(spy, "", "2 D", "1 hour", "TRADES", useRTH=True))

`IB.connect` takes a host, a port and a client id because it was written for a
gateway. The host and port are accepted and ignored; the client id keys this
session's orders. The credentials are given to `attach`, or left to
`IB_USERNAME` and `IB_PASSWORD`.
"""

import asyncio
import collections
import logging
import os
import pathlib
import threading
import time

from eventkit import Event
from ib_async.client import Client as _TheirClient
from ib_async.order import Order as _TheirOrder

import ibkr_dx as _ibkr_dx

from . import _messages

_logger = logging.getLogger(__name__)


def _refuse_options(named: str, given) -> None:
    """A free-form option list this request cannot carry.

    Accepted and dropped, a caller who tuned a request with one would be
    answered by an untuned request and have no way to tell. Empty or absent is
    what every ordinary call passes, and that is taken; anything in it is said
    out loud.
    """
    if given:
        raise NotImplementedError(
            f"{named}={given!r} is not carried here: this request has no "
            "free-form option list to send it under, so the request would go "
            "out without it and answer something other than what was asked"
        )


#: The widest number this protocol carries on a request.
#:
#: A request id is four billion wide and the top quarter of that range is this
#: client's own, for the calls it numbers on a caller's behalf. An order id is
#: not held to it: the venue numbers orders as wide as it likes, and an account
#: whose orders have outgrown a request id is ordinary rather than broken.
WIDEST_REQUEST_ID = 0xC000_0000 - 1


#: How often the pump makes a pass: the finest step at which anything the
#: engine holds reaches ib_async.
PASS_INTERVAL = 0.01


class IbkrDxClient:
    """What `ib_async.IB` talks to, answered by this engine.

    Holds the same attributes and events `ib_async.Client` does, because `IB`
    reads them directly.
    """

    DISCONNECTED, CONNECTING, CONNECTED = range(3)
    MinClientVersion = 157
    MaxClientVersion = 178

    def __init__(self, wrapper, username="", password="", paper=True,
                 session_file=None, client_id=None, readonly=False):
        self.wrapper = wrapper
        self._username = username or os.environ.get("IB_USERNAME", "")
        self._password = password or os.environ.get("IB_PASSWORD", "")
        self._paper = paper
        # Where a caller said so before connecting. `connectAsync` takes one of
        # its own; unstated there, this is what stands.
        self._readonly = bool(readonly)
        self.apiStart = Event("apiStart")
        self.apiEnd = Event("apiEnd")
        self.apiError = Event("apiError")
        self.throttleStart = Event("throttleStart")
        self.throttleEnd = Event("throttleEnd")

        self.host = ""
        self.port = -1
        self.clientId = -1
        self.optCapab = ""
        self.connectOptions = b""
        self.connState = IbkrDxClient.DISCONNECTED
        self._reqIdSeq = 1
        self._accounts: list[str] = []
        self._loop = None
        self._pump = None
        self._stop = threading.Event()
        #: When the session started, and the requests sent on it.
        self._since = time.time()
        self._sent = 0

        # The engine, with ib_async's own wrapper as the callback target: this
        # client already resolves a callback under the reference client's
        # spelling, which is the spelling ib_async's wrapper uses.
        self._callbacks = _LoopBound(wrapper)
        self._callbacks.connectionClosed = self._session_ended
        self._client = _ibkr_dx.EClient(self._callbacks)

        # Where this session is kept between runs. The venue answers a request
        # that names a session it still holds with a challenge rather than a
        # whole handshake. It lets a session go soon after the process holding
        # it ends, so this covers a quick restart, not a later one. Owner only,
        # sealed with the password, and refused if it names another account.
        # Pass session_file=False to attach() to keep nothing.
        self._session_file = session_file
        # Which counter this session counts on. Their own connect names one
        # too; stated here it is what the counter is keyed by before that call
        # is ever made.
        if client_id is not None:
            self.clientId = int(client_id)

    # ── connection ──

    async def connectAsync(self, host, port, clientId, timeout=2.0, readonly=None,
                           account=""):
        """Open the session. Host and port name a gateway; there is none.

        ``readonly`` is carried to the session, which refuses to send anything
        that places, changes or withdraws an order. Accepted and dropped, a
        program that asked for a read-only connection got one that could trade.
        """
        readonly = self._readonly if readonly is None else bool(readonly)
        self.host, self.port, self.clientId = host, int(port), int(clientId)
        self.connState = IbkrDxClient.CONNECTING
        self._since, self._sent = time.time(), 0
        self._callbacks.reset()
        self._loop = asyncio.get_running_loop()
        self.wrapper.__dict__.setdefault("clientId", self.clientId)

        # Blocking, so it runs off the loop: everything else here has to stay
        # able to answer while the session opens.
        await self._loop.run_in_executor(
            None,
            lambda: self._client.connect(
                username=self._username,
                password=self._password,
                paper=self._paper,
                client_id=self.clientId,
                readonly=readonly,
                session_file=self._session_file,
            ),
        )
        self.connState = IbkrDxClient.CONNECTED

        # What the handshake tells ib_async before it considers the API
        # ready. Asked for rather than composed: the client answers this with
        # every account the login holds, and the default account read off it
        # is the first one — so an advisor with several saw one, standing for
        # all of them.
        self._client.req_managed_accts()
        # Answered on the next pass of dispatch, as every request is, and the
        # pump that makes those passes is not running yet: one pass here, so
        # the answer is in hand before it is read.
        self._pass_once()
        self._accounts = list(getattr(self.wrapper, "accounts", []))
        # The number their client counts on, which numbers its requests and
        # the orders it places alike, as a gateway announces it when the API
        # starts: their client takes it as the next id to hand out, and their
        # wrapper hears it. The engine's is the first number past every order
        # id the account has used that a request can also carry, so no id this
        # counter hands out names an order the venue already holds, and one
        # counter means an order never takes the number of a request that is
        # still waiting.
        next_valid = self._client.next_shared_id()
        self.updateReqId(next_valid)
        self.wrapper.nextValidId(next_valid)

        self._start_pump()
        self.apiStart.emit()

    def disconnect(self):
        """End the session, as ib_async's own client ends one.

        `connectionClosed` is not called here. Their wrapper treats it as a
        session that went away underneath them: it fails every request still
        waiting and raises on their global error event, which is right for a
        socket that dropped and wrong for a caller who asked to stop.
        """
        self.connState = IbkrDxClient.DISCONNECTED
        self._stop.set()
        self._client.disconnect()

    def _session_ended(self):
        """The engine ended the session: what ib_async's client does when its
        socket closes. Waiting requests fail, and ``apiEnd`` fires, which
        their ``IB`` hears as ``disconnectedEvent``.

        The prices this pass stated before the end reach their tickers first,
        as what a socket carried before it closed is read before the close.
        Held to the end of the pass, they reached a wrapper the close had
        already cleared, which logged each as a request it did not know.
        """
        self._callbacks.end_pass()
        self.connState = IbkrDxClient.DISCONNECTED
        self._stop.set()
        self.wrapper.setEventsDone()
        self.wrapper.connectionClosed()
        self.apiEnd.emit()

    def _pass_once(self):
        """One dispatch, then the boundary ib_async flushes on.

        Their wrapper holds ticker updates until the batch of messages ends,
        and emits their events there. Their own transport marks a batch only
        when data arrives; here a batch is what was delivered since the last
        pass, including an answer given inside a request call. A pass with
        nothing delivered leaves their clock alone, so ``timeoutEvent`` fires,
        and emits no ``updateEvent``.

        A pass the pump queued before the caller disconnected does nothing:
        run later, on the loop the next connect drives, it would tell their
        wrapper the session had dropped, and their global error event would
        cancel that connect.
        """
        if self.connState == IbkrDxClient.DISCONNECTED:
            return
        self._callbacks.begin_pass()
        self._client.poll()
        self._callbacks.end_pass()
        if self._callbacks.arrived:
            self._callbacks.arrived = False
            processed = getattr(self.wrapper, "tcpDataProcessed", None)
            if processed:
                processed()

    def _start_pump(self):
        """Drive dispatch, and land every callback on ib_async's own loop.

        ib_async is asyncio end to end: its futures are resolved by wrapper
        callbacks and must be touched from the loop thread.
        """
        self._stop.clear()

        def run():
            while not self._stop.is_set():
                self._loop.call_soon_threadsafe(self._pass_once)
                self._stop.wait(PASS_INTERVAL)

        self._pump = threading.Thread(target=run, daemon=True)
        self._pump.start()

    # ── what IB reads directly ──

    def isConnected(self):
        """Whether a session is open, as ib_async's client answers it.

        An outage the engine is still mending (1100 until 1102) leaves the
        session open, as a gateway's does. A session the engine ends is
        closed by ``_session_ended``.
        """
        return self.connState == IbkrDxClient.CONNECTED

    def isReady(self):
        return self.isConnected()

    def serverVersion(self):
        return self.MaxClientVersion

    def getAccounts(self):
        return list(self._accounts)

    def getReqId(self):
        if not self.isConnected():
            raise ConnectionError("Not connected")
        # Past every order id the venue has named, those it names after the
        # connect among them: the history of an order that filled can come
        # later than the wait at connect, and the venue refuses an id a fill
        # has spent. Their wrapper raises the counter only on an open order.
        # After the venue reconnects, the first call waits, three seconds at
        # most, for the venue to name the working orders again, as the
        # connect does.
        self.updateReqId(self._client.next_shared_id())
        # Hands out the current value and then advances, as their own client
        # does, so an id seeded by `updateReqId` is the next one issued rather
        # than the one after it.
        new_id = self._reqIdSeq
        self._reqIdSeq += 1
        return new_id

    def updateReqId(self, minReqId):
        # Their wrapper raises this counter past every order id it sees, so
        # that the next order their client numbers is not one the account is
        # already working. Their client numbers orders and requests out of it
        # alike, and so does this one.
        #
        # An order id placed elsewhere can go wider than a request id, which
        # is four billion wide with the top of that reserved. A raise past what
        # a request can carry buys nothing and costs everything: on an account
        # with such an order, every request afterwards was refused as a number
        # this protocol cannot carry, and an unmodified program could not so
        # much as name a contract. The counter starts past every id the
        # account has used that a request can carry, so an id it hands out is
        # clear of that order as well. Such a raise is let go of rather than
        # taken to the top of the range, which saturates and steps over the
        # edge on the next request.
        if minReqId > WIDEST_REQUEST_ID:
            return
        self._reqIdSeq = max(self._reqIdSeq, minReqId)

    def connectionStats(self):
        """When the session started, how long it has run, and the messages
        each way, as their client counts them: a request is one sent, and what
        reaches their wrapper one received. The byte counts are nought: the
        engine does not count the bytes of its connections."""
        from ib_async.objects import ConnectionStats

        if not self.isReady():
            raise ConnectionError("Not connected")
        return ConnectionStats(
            self._since, time.time() - self._since, 0, 0,
            self._callbacks.received, self._sent,
        )

    def setConnectOptions(self, options):
        self.connectOptions = options.encode()

    def _connected(self):
        """The engine, to send on. Like ib_async's client, nothing is sent
        while not connected."""
        if not self.isConnected():
            raise ConnectionError("Not connected")
        return self._client

    def _send(self, request, *args):
        """One request to the engine, as their client writes one message.

        The engine answers a request it refuses inside the call itself. Their
        client's answers come back on the socket once the call has returned,
        so a refusal is held for the next pass (see `_LoopBound.error`).
        """
        engine = self._connected()
        self._sent += 1
        self._callbacks.asking += 1
        try:
            return getattr(engine, request)(*args)
        finally:
            self._callbacks.asking -= 1

    def _send_theirs(self, request, *args):
        """A request as their client makes it, each argument rebuilt as the
        engine's.

        An object holding a value its field cannot take — text where a number
        goes — is one a gateway cannot read off their client's message, and it
        is refused as a gateway refuses a message it cannot read: 320, under
        the request's own number, once the call has returned. Every request of
        their client that carries an object is numbered by its first argument.
        """
        try:
            ours = [_as_ours(a) for a in args]
        except ValueError as why:
            self._connected()
            self._sent += 1
            self._callbacks.refused.append((args[0], *_unreadable(why)))
            return None
        return self._send(request, *ours)

    # ── their raw messages ──

    #: Their client's own: it writes the fields as one message, as theirs
    #: does, and hands the message to `sendMsg`.
    send = _TheirClient.send

    def sendMsg(self, msg):
        """A message as their client writes one, sent as the request that
        writes it.

        There is no socket to write it to, so it is read back, as a gateway
        reads one, into the request their client names for it, and answered
        as a gateway answers it:

        * a message naming no request their client writes is logged, and
          nothing answers it;
        * one that does not read as the request it names is refused with 320,
          once the call has returned, under that request's number where it was
          read before the field that failed, and under -1 before it.

        While not connected nothing is sent, as their client's socket sends
        nothing, and an empty message is nothing to send.
        """
        if not self.isConnected() or not msg:
            return
        try:
            read = _messages.read(msg)
        except _messages.Unreadable as why:
            self._sent += 1
            self._callbacks.refused.append((why.reqId, *_unreadable(why)))
            return
        if read is None:
            self._sent += 1
            named = msg.split("\0", 1)[0]
            _logger.error(f"Invalid incoming request type - {named}")
            return
        request, args = read
        if request == "placeOrder":
            self._place_order(*args)
        else:
            getattr(self, request)(*args)

    # ── requests: the same shape, all the way down ──

    def placeOrder(self, orderId, contract, order):
        """An order, written by their client and read back as a gateway reads
        it, so what reaches the engine is what their client sends: a field
        their client does not write is not carried, and one it clears is
        cleared, `volatility` on any order but a volatility order among them.
        """
        _TheirClient.placeOrder(self, orderId, contract, order)

    def _place_order(self, orderId, contract, order):
        """An order as a gateway takes it off their client's message.

        Three attributes their client writes on every order are ones the venue
        no longer takes. A gateway refuses an order stating one where the
        venue has retired them for the account, and otherwise says so and
        places the order without it.
        """
        for name, spelled, refusal, warning in _RETIRED:
            unset = getattr(_TheirOrder, name)
            if getattr(order, name) == unset:
                continue
            text = f"The '{spelled}' order attribute is not supported."
            if "DEPRETFQNC" in self._client.enabled_features():
                self._sent += 1
                self._callbacks.refused.append((orderId, refusal, text, ""))
                return
            self._callbacks.refused.append((orderId, warning, f"Warning: {text}", ""))
            setattr(order, name, unset)
        self._send_theirs("place_order", orderId, contract, order)

    def reqMktData(self, reqId, contract, genericTickList, snapshot,
                   regulatorySnapshot, mktDataOptions):
        _refuse_options("mktDataOptions", mktDataOptions)
        self._send_theirs(
            "req_mkt_data", reqId, contract, genericTickList, snapshot,
            regulatorySnapshot,
        )

    def reqHistoricalData(self, reqId, contract, endDateTime, durationStr,
                          barSizeSetting, whatToShow, useRTH, formatDate,
                          keepUpToDate, chartOptions):
        _refuse_options("chartOptions", chartOptions)
        self._send_theirs(
            "req_historical_data", reqId, contract, endDateTime, durationStr,
            barSizeSetting, whatToShow, 1 if useRTH else 0, formatDate,
            keepUpToDate, [],
        )

    # These two stay written out. `__getattr__` forwards every argument
    # through `_as_ours`, which turns None into an empty list — right for an
    # options list and wrong for a string, which is what these carry.
    def reqAccountUpdates(self, subscribe, acctCode):
        self._send("req_account_updates", subscribe, acctCode)

    def reqAccountSummary(self, reqId, groupName, tags):
        self._send("req_account_summary", reqId, groupName, tags)

    def __getattr__(self, name):
        """Every other request, under the name this engine carries it by.

        Both sides follow the reference client's own signatures, so a request
        with no special handling above is forwarded as it stands rather than
        written out again here.
        """
        if name.startswith("_"):
            raise AttributeError(name)

        request = _our_name_for(name, self._client)
        if hasattr(self._client, request):
            return lambda *args: self._send_theirs(request, *args)
        if not callable(getattr(_TheirClient, name, None)):
            raise AttributeError(
                f"{type(self).__name__!r} object has no attribute {name!r}"
            )

        def unanswered(*args):
            # A request their client makes and the engine does not carry: the
            # handshake a program makes with the gateway it connects to. A
            # gateway never answers it, and nothing answers it here.
            self._connected()
            self._sent += 1

        return unanswered


#: Where the two name the same figure differently. Their bar calls its average
#: price what the reference client calls `wap`, and their order state was
#: written before the venue renamed a commission to include its fees.
_OUR_NAME = {
    "average": "wap",
    "commission": "commissionAndFees",
    "minCommission": "minCommissionAndFees",
    "maxCommission": "maxCommissionAndFees",
    "commissionCurrency": "commissionAndFeesCurrency",
}


def _our_name_for(their_name, carrier):
    """What this engine calls the request they call `their_name`.

    A capital starts a word, which is enough for every name either side spells
    out — but not for the ones that run words together. `reqPnL` split that way
    is `req_pn_l` and matches nothing, so a program asking this engine for a
    running profit was told it carries none. So the split is a first guess, and
    what the engine actually carries decides: one name, ignoring where the
    underscores fell.
    """
    split = "".join("_" + c.lower() if c.isupper() else c for c in their_name)
    if hasattr(carrier, split):
        return split
    flattened = their_name.lower()
    for carried in dir(carrier):
        if carried.replace("_", "") == flattened:
            return carried
    return split


#: Where the two name the same record differently. Their commission report was
#: named before the venue started charging fees through it, and this client
#: names it for what it carries now — so the record that says what a trade cost
#: reached their wrapper as a type nothing there could read, and every fill
#: raised.
_THEIR_TYPE_NAME = {
    "CommissionAndFeesReport": "CommissionReport",
}


def _is_named_tuple(t):
    """Whether a type is one of their record tuples.

    Their historical ticks are `NamedTuple`s rather than dataclasses, so a
    conversion that only knows dataclasses hands those straight through — and
    a caller reading `tick.priceBid` off an ibkr_dx record finds a field spelled
    the other way.
    """
    return isinstance(t, type) and issubclass(t, tuple) and hasattr(t, "_fields")


def _their_type(name):
    """The type of theirs that goes by this name, if there is one."""
    import dataclasses

    import ib_async.contract as contract_types
    import ib_async.objects as objects
    import ib_async.order as order_types

    name = _THEIR_TYPE_NAME.get(name, name)
    for module in (contract_types, objects, order_types):
        found = getattr(module, name, None)
        if found is not None and (dataclasses.is_dataclass(found) or _is_named_tuple(found)):
            return found
    return None


def _field_of(value, name):
    """One field of an ibkr_dx record, under whichever name it goes by.

    A moment is handed over as their own, because their records declare it as
    a datetime and a number read as one is an instant in 1970.
    """
    if name == "conjunction":
        # The engine says whether the join is an "and"; theirs says "a" or "o".
        isAnd = getattr(value, "isConjunctionConnection", None)
        return None if isAnd is None else "a" if isAnd else "o"
    got = getattr(value, name, None)
    if got is None:
        got = getattr(value, _OUR_NAME.get(name, name), None)
    if got is None or name not in ("time", "date"):
        return got
    if isinstance(got, int) and not isinstance(got, bool):
        # Seconds since the epoch, which ib_async's records declare as a
        # datetime. Parsed by ib_async's own parser, so the instant matches
        # what the rest of ib_async reads.
        from ib_async.util import parseIBDatetime

        return parseIBDatetime(str(got))
    # A string is handed over as it stands. The engine writes a bar the way
    # their parser reads one — the instant on the exchange's clock with the
    # zone after it — and composing one here from the venue's own stamp, which
    # is UTC, put every bar out by whatever that zone is from UTC.
    return got


def _as_theirs(value):
    """An ibkr_dx object, rebuilt as the same-named `ib_async` type.

    Both sides carry the reference client's own field names, so the conversion
    is driven by the `ib_async` dataclass rather than written out per type or
    per callback: a field only `ib_async` declares keeps its default, and
    neither side needs editing when the other gains one. Anything with no type
    of that name in ib_async — a number, a string, an ibkr-dx-only type — is
    handed over as it is.
    """
    import dataclasses

    if isinstance(value, (str, bytes, int, float, bool, type(None))):
        return value
    if isinstance(value, (list, tuple)):
        return type(value)(_as_theirs(v) for v in value)

    theirs = _their_type(type(value).__name__)
    if theirs is None or dataclasses.is_dataclass(value):
        return value

    # Built in one go, as their decoder builds one: a record tuple takes its
    # fields in order, and some of their dataclasses take every field when
    # they are made and cannot be changed after. Made empty and filled one
    # field at a time, those could not be made at all.
    if _is_named_tuple(theirs):
        return theirs(*[_as_theirs(_field_of(value, name)) for name in theirs._fields])
    # A field the engine does not state keeps their default.
    return theirs(**{
        field.name: _as_theirs(got)
        for field in dataclasses.fields(theirs)
        if (got := _field_of(value, field.name)) is not None
    })


class _LoopBound:
    """ib_async's wrapper, reached under the names this engine calls.

    A callback carries an ibkr_dx object; the `ib_async` wrapper expects its own.
    Every argument is rebuilt on the way through, by its own type name, so a
    callback nobody thought to list is carried too.
    """

    #: The size that goes with each price, by tick type: bid, ask, last, and
    #: the same three delayed.
    _SIZE_OF = {1: 0, 2: 3, 4: 5, 66: 69, 67: 70, 68: 71}
    _PRICE_OF = {size: price for price, size in _SIZE_OF.items()}

    def __init__(self, wrapper):
        self._wrapper = wrapper
        #: The size last stated for each side of each request, by tick type.
        self._sizes: dict[tuple[int, int], float] = {}
        #: Prices this pass stated whose size it has not stated yet.
        self._priced: dict[tuple[int, int], float] = {}
        #: Whether anything was delivered since the last pass ended a batch.
        self.arrived = False
        #: The messages delivered to their wrapper.
        self.received = 0
        #: How many requests are being made right now.
        self.asking = 0
        #: Refusals stated inside a request call, for the next pass.
        self.refused = collections.deque()

    def reset(self):
        """A new session: nothing held for the last one reaches it, as their
        client clears its queues when it connects."""
        self._sizes.clear()
        self._priced.clear()
        self.refused.clear()
        self.arrived = False
        self.received = 0

    def _deliver(self, method, *args):
        """One message to their wrapper.

        Their batch starts on the first message since the last one ended,
        where their own transport starts one when data arrives. A message
        their wrapper raises on is logged and passed over, as their decoder
        treats one: raised into the engine, it closed the session.
        """
        if not self.arrived:
            self.arrived = True
            arrived = getattr(self._wrapper, "tcpDataArrived", None)
            if arrived:
                arrived()
        self.received += 1
        try:
            method(*args)
        except Exception:
            _logger.exception(f"Error handling {getattr(method, '__name__', method)}{args!r}")

    def histogram_data(self, req_id, items):
        """The spread of trades across prices, in their own type.

        This engine hands over each entry as a price and a count; their
        wrapper reads two named fields off it.
        """
        from ib_async.objects import HistogramData

        # Built as theirs either way. This engine states a bucket the way the
        # reference client does — `price` and `size` — and their wrapper reads
        # `price` and `count`, so an entry passed straight through carries a
        # name they do not read. Handing back whatever arrived was right only
        # while this engine handed back a pair.
        self._deliver(
            self._wrapper.histogramData,
            req_id,
            [
                HistogramData(
                    price=item.price if hasattr(item, "price") else item[0],
                    count=int(item.size if hasattr(item, "size") else item[1]),
                )
                for item in items
            ],
        )

    def tick_price(self, req_id, tick_type, price, attrib=None):
        """A price, delivered with the size that goes with it.

        This engine states a price and its size as the reference client does,
        as two ticks, and a pass states every price before any size. A
        gateway sends the two as one message, which their decoder hands over
        as one `priceSizeTick`. So a price that has a size waits for the size
        this pass states with it, or for the end of the pass, where it goes
        with the size standing.
        """
        if tick_type in self._SIZE_OF:
            self._priced[req_id, tick_type] = price
        else:
            self._deliver(self._wrapper.priceSizeTick, req_id, tick_type, price, 0.0)

    def tick_size(self, req_id, tick_type, size):
        """A size: with the price this pass stated beside it, or on its own.

        A size that changed while its price did not is what a gateway sends
        on its own, and their decoder hands that over as `tickSize`.
        """
        price_type = self._PRICE_OF.get(tick_type)
        if price_type is not None:
            self._sizes[req_id, tick_type] = size
            price = self._priced.pop((req_id, price_type), None)
            if price is not None:
                self._deliver(self._wrapper.priceSizeTick, req_id, price_type, price, size)
                return
        self._deliver(self._wrapper.tickSize, req_id, tick_type, size)

    def end_pass(self):
        """The prices this pass stated with no size beside them, each with the
        size standing: the size did not change, so the pass did not state it."""
        priced, self._priced = self._priced, {}
        for (req_id, price_type), price in priced.items():
            size = self._sizes.get((req_id, self._SIZE_OF[price_type]), 0.0)
            self._deliver(self._wrapper.priceSizeTick, req_id, price_type, price, size)

    #: Where their wrapper names a callback something other than the
    #: reference client does, and the two it does not carry at all: display
    #: groups belong to a window, and there is none here or there.
    _THEIR_NAME = {
        "real_time_bar": "realtimeBar",
        "commission_and_fees_report": "commissionReport",
        "display_group_list": None,
        "display_group_updated": None,
    }

    # Under both spellings. This engine looks for the reference client's name
    # first, so a callback answered here under one spelling only would be
    # reached past — straight to their wrapper, and the translation skipped.
    tickPrice = tick_price
    tickSize = tick_size
    histogramData = histogram_data

    def error(self, req_id, when, code, text, advanced=""):
        """A refusal, in the shape their wrapper declares, when a gateway's
        would arrive.

        This engine states when the venue said it, as the current reference
        client does. `ib_async` predates that argument and declares four, so
        their wrapper is handed four: passed five it raises, and every error
        and notice of the session would be lost.

        A refusal stated inside a request call waits for the next pass. A
        gateway's comes back on the socket after the call has returned, and
        their `placeOrder` makes its `Trade` after the call: delivered inside
        it, a refused new order had no trade to mark and stayed PendingSubmit.
        """
        refusal = (req_id, code, text, advanced)
        if self.asking:
            self.refused.append(refusal)
        else:
            self._deliver(self._wrapper.error, *refusal)

    def begin_pass(self):
        """What was refused inside a request call before this pass.

        Only that: a refusal of a request made while these are delivered — a
        handler asking again — waits for the next pass, as a gateway's answer
        to it would come back on the socket after the call. Delivered in the
        same pass, a handler that asks again on every refusal held the loop
        for as long as it kept asking.
        """
        for _ in range(len(self.refused)):
            self._deliver(self._wrapper.error, *self.refused.popleft())

    def __getattr__(self, name):
        # Under either spelling: this engine calls a callback by the name it
        # holds it under, and their wrapper declares the reference client's.
        if name in self._THEIR_NAME:
            named = self._THEIR_NAME[name]
            if named is None:
                return lambda *args: None
            # Rebuilt on the way through, like every other callback. Handed
            # over as it stands, a fill carries this engine's own cost record
            # and ib_async's wrapper reads a field its own record spells
            # differently, so the cost is dropped.
            method = getattr(self._wrapper, named)
        else:
            method = getattr(self._wrapper, name, None)
            if method is None:
                words = name.split("_")
                camel = words[0] + "".join(w.title() for w in words[1:])
                method = getattr(self._wrapper, camel, None)
        if method is None:
            raise AttributeError(name)

        def carrying(*args):
            try:
                theirs = [_as_theirs(a) for a in args]
            except Exception:
                # As their decoder treats a message it cannot handle: said, and
                # the session carries on without it.
                _logger.exception(f"Error handling {name}{args!r}")
                return
            self._deliver(method, *theirs)

        return carrying


#: Their unset numbers, which their client sends as an empty field.
_UNSET_DOUBLE = 1.7976931348623157e308
_UNSET_INTEGER = 2147483647


def _unreadable(why):
    """A request a gateway could not read off their client's message, as it
    refuses one: code, text and no advanced reject."""
    return 320, f"Error reading request:{why}", ""


#: The order attributes their client writes and the venue no longer takes:
#: each by name, as a gateway spells it, and the code it refuses an order
#: stating one under where the venue has retired them for the account, then
#: the code it warns under otherwise, placing the order without it.
_RETIRED = (
    ("eTradeOnly", "EtradeOnly", 10268, 2168),
    ("firmQuoteOnly", "FirmQuoteOnly", 10269, 2169),
    ("nbboPriceCap", "NbboPriceCap", 10270, 2170),
)

#: How their order conditions say one joins the next, as the engine says it:
#: whether the join is an "and".
_CONJUNCTION = {"a": True, "o": False}


def _states_nothing(held):
    """A field their client sends empty: None, an empty string or list, or
    one of their unset numbers."""
    return (
        held is None or held == "" or held == [] or held == _UNSET_DOUBLE
        or (isinstance(held, int) and not isinstance(held, bool)
            and held == _UNSET_INTEGER)
    )


def _as_ours(value):
    """An `ib_async` object, rebuilt as this engine's type of the same name."""
    import dataclasses

    if value is None:
        # Their optional lists arrive as None; every request here takes a
        # list, and an absent one is an empty one.
        return []

    named = isinstance(value, tuple) and hasattr(value, "_fields")
    if isinstance(value, (list, tuple)) and not named:
        # A list of ib_async objects is a list of objects to rebuild, not a
        # value to hand across whole. Handed across, an algo's parameters and a
        # combination's routing arrive as ib_async types and are refused, so
        # the order carries the strategy without what tunes it.
        return [_as_ours(item) for item in value]

    if named:
        # A tag and its value is a record they spell as a tuple. Read as a
        # sequence it becomes two loose strings, which is not what either side
        # means by one; read as a record it is the pair this engine holds.
        rebuilt = getattr(_ibkr_dx, type(value).__name__, None)
        return value if rebuilt is None else rebuilt(*value)

    if not dataclasses.is_dataclass(value):
        return value
    # Under its own name, or the name of what it is a kind of: their `Stock`
    # and `Forex` are contracts, and this engine holds one type for all of
    # them.
    ours = next(
        (
            found
            for kind in type(value).__mro__
            if (found := getattr(_ibkr_dx, kind.__name__, None)) is not None
        ),
        None,
    )
    if ours is None:
        return value
    made = ours()
    for field in dataclasses.fields(value):
        held = getattr(value, field.name, None)
        # Carried as their client sends it: every field, one at their own
        # default among them, except what goes out as an empty field. That is
        # left to this engine's default, which is its own "not stated". Left
        # out at their default, `openClose` went as nothing where their
        # client sends "O".
        if _states_nothing(held):
            continue
        name = field.name
        if name == "conjunction":
            name, held = "isConjunctionConnection", _CONJUNCTION.get(held, held)
        # Already what the engine holds — a condition's type is its class's
        # own on both sides, and fixed on this one — or a field the engine has
        # no place for, at the value their client sends on every order:
        # `eTradeOnly` and `firmQuoteOnly`, always False, state nothing.
        if getattr(made, name, field.default) == held:
            continue
        try:
            setattr(made, name, _as_ours(held))
        except (AttributeError, TypeError, ValueError) as why:
            # A field that cannot be carried is one the order goes out
            # without, so it is raised rather than swallowed: an algo without
            # its parameters or a commission directed nowhere is an order on
            # terms nobody stated.
            raise ValueError(
                f"{type(value).__name__}.{field.name} was set to "
                f"{getattr(value, field.name)!r}, which this client cannot "
                f"carry: {why}"
            ) from why

    return made


def attach(ib, username="", password="", paper=True, session_file=None,
           client_id=None, readonly=False):
    """Point an `ib_async.IB` at this engine, and hand it back.

    The credentials are this session's; left out, `IB_USERNAME` and
    `IB_PASSWORD` are used.

    The session is kept between runs, under this account's own file in
    ``~/.ibkr_dx``. A venue answers a request that names a session it still holds
    with a challenge rather than a whole handshake. It lets a session go soon
    after the process holding it ends, so this covers a quick restart; a program
    started again later logs in afresh. Name another path to move it, or pass
    ``False`` to keep nothing and log in fully every time.

    Orders and requests are numbered from one counter, as ib_async's own
    client numbers them, kept past every order id the account has used, which
    the venue names at every connect and whenever it names another: nothing
    about them is kept between runs.
    """
    if session_file is None:
        who = username or os.environ.get("IB_USERNAME", "")
        kind = "paper" if paper else "live"
        session_file = str(pathlib.Path.home() / ".ibkr_dx" / f"session-{who}-{kind}")
    elif session_file is False:
        session_file = None
    ib.client = IbkrDxClient(ib.wrapper, username, password, paper, session_file,
                          client_id, readonly)
    ib.wrapper.client = ib.client
    # ib_async's IB ties this to the client it builds for itself.
    ib.client.apiEnd += ib.disconnectedEvent
    return ib

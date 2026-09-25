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
session's orders. The credentials are given to `attach`, or left to an `IBC`
started in the same context, or to `IB_USERNAME` and `IB_PASSWORD`.
"""

import asyncio
import collections
import contextlib
import contextvars
import dataclasses
import inspect
import logging
import os
import pathlib
import sys
import threading
import time
import weakref

from eventkit import Event
from ib_async.client import Client as _TheirClient
from ib_async.ib import IB as _TheirIB

import ibkr_dx as _ibkr_dx

from . import _messages

_logger = logging.getLogger(__name__)


@dataclasses.dataclass
class IbcLogin:
    """The login an ``IBC`` was started with, as a gateway it launched would
    hold it, the clients whose sessions it opened, and whether the ``IBC``
    has been terminated."""

    userid: str
    password: str
    paper: bool
    clients: "weakref.WeakSet[IbkrDxClient]" = dataclasses.field(default_factory=weakref.WeakSet)
    ended: bool = False


#: The login the last ``IBC`` started in this context holds, for a connect
#: in the same context that names none. A ``Watchdog`` runs in a task of its
#: own, so each one's is its own.
IBC_LOGIN: contextvars.ContextVar[IbcLogin | None] = contextvars.ContextVar(
    "ib_async_dx_ibc_login", default=None
)


#: The widest number this protocol carries on a request.
#:
#: A request id is four billion wide and the top quarter of that range is this
#: client's own, for the calls it numbers on a caller's behalf. An order id is
#: not held to it: the venue numbers orders as wide as it likes, and an account
#: whose orders have outgrown a request id is ordinary rather than broken.
WIDEST_REQUEST_ID = 0xC000_0000 - 1


#: Where their `IB` numbers an order with `getReqId`: a new order, a
#: bracket's three, and a what-if, which the engine numbers as an order.
_NUMBERS_AN_ORDER = frozenset(
    f.__code__ for f in (_TheirIB.placeOrder, _TheirIB.bracketOrder, _TheirIB.whatIfOrderAsync)
)


#: How long after one pass the next is made: the finest step at which
#: anything the engine holds reaches ib_async.
PASS_INTERVAL = 0.01


class IbkrDxClient:
    """What `ib_async.IB` talks to, answered by this engine.

    Holds the same attributes and events `ib_async.Client` does, because `IB`
    reads them directly.
    """

    DISCONNECTED, CONNECTING, CONNECTED = range(3)
    MinClientVersion = 157
    MaxClientVersion = 178
    #: Their client's, and not applied: nothing between the program and the
    #: venue paces requests, so there is nothing for these to set.
    MaxRequests = _TheirClient.MaxRequests
    RequestsInterval = _TheirClient.RequestsInterval
    events = _TheirClient.events

    def __init__(self, wrapper, username="", password="", paper=True,
                 session_file=None, readonly=False):
        self.wrapper = wrapper
        # As given: what is left empty is settled when the session opens (see
        # `_login`), from an IBC started in the same context or the
        # environment.
        self._username = username
        self._password = password
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
        #: Which session, or attempt at one, is the current one. Every
        #: connect and every end counts one, so a pass, a login or a step of a
        #: connect that belongs to an earlier one finds it is not current.
        self._generation = 0
        #: The login a connect is waiting on, the thread it runs on, and the
        #: next pass.
        self._logging_in = None
        self._login_thread = None
        self._pass = None
        #: When the session started, and the requests sent on it.
        self._since = time.time()
        self._sent = 0

        # The engine, with ib_async's own wrapper as the callback target: this
        # client already resolves a callback under the reference client's
        # spelling, which is the spelling ib_async's wrapper uses.
        self._callbacks = _LoopBound(wrapper)
        self._callbacks.connectionClosed = self._engine_closed
        self._client = _ibkr_dx.EClient(self._callbacks)

        # Where this session is kept between runs. The venue answers a request
        # that names a session it still holds with a challenge rather than a
        # whole handshake. It lets a session go soon after the process holding
        # it ends, so this covers a quick restart, not a later one. Owner only,
        # sealed with the password, and refused if it names another account.
        # Pass session_file=False to attach() to keep nothing.
        self._session_file = session_file

    # ── connection ──

    #: Their client's own: `connect` runs `connectAsync` to the end, and `run`
    #: runs the loop. The engine has methods of both names that do otherwise.
    connect = _TheirClient.connect
    run = _TheirClient.run

    async def connectAsync(self, host, port, clientId, timeout=2.0, readonly=None,
                           account=""):
        """Open the session. Host and port name a gateway; there is none.

        The login is the engine's, and runs off the loop, which keeps turning
        while it waits. Neither ``timeout`` nor a cancel stops it in the
        engine: a paper login presents no second factor, and a live one waits
        on it for as long as the engine lets it, as a gateway's login is made
        before a program connects. The engine's wait for the venue to name the
        orders already working, three seconds at most, is part of the login.
        ``timeout`` (0 or None: no limit) is ib_async's, which bounds each
        request of its startup sync with it.

        A connect that is cancelled, or overtaken by a ``disconnect()`` or by
        another connect, ends at once, and the engine drops the session its
        login opens rather than keep it. The login itself runs on until the
        engine returns, on a thread that does not hold the program open. A
        failed login raises ``ConnectionError``, and ``apiError`` says why, as
        their client says.

        ``readonly`` is carried to the session, which refuses to send anything
        that places, changes or withdraws an order. Accepted and dropped, a
        program that asked for a read-only connection got one that could trade.
        """
        # Their own client closes the session it holds before it opens
        # another. One still logging in is let go of the same way.
        if self.isConnected():
            self.wrapper.ib.disconnect()
        elif self.connState == IbkrDxClient.CONNECTING:
            self.disconnect()
        self._retire()
        attempt = self._generation
        readonly = self._readonly if readonly is None else bool(readonly)
        self.host, self.port, self.clientId = host, int(port), int(clientId)
        self.connState = IbkrDxClient.CONNECTING
        self._since, self._sent = time.time(), 0
        self._callbacks.reset()
        self._loop = asyncio.get_running_loop()
        self._callbacks.thread = threading.get_ident()
        self.wrapper.clientId = self.clientId
        username, password, paper, session_file = self._login()
        client_id = self.clientId
        if self._login_thread is not None and self._login_thread.is_alive():
            # The last login is still inside its engine, which drops what it
            # opens. This one gets an engine of its own, so neither can end
            # the other's session.
            self._client = _ibkr_dx.EClient(self._callbacks)
        engine = self._client

        def login():
            engine.connect(
                username=username,
                password=password,
                paper=paper,
                client_id=client_id,
                readonly=readonly,
                session_file=session_file,
            )
            # A disconnect that reached the engine while it was installing the
            # session can leave that session open. One no longer wanted is
            # closed here, by the login that opened it.
            if attempt != self._generation:
                engine.disconnect()

        try:
            # Blocking, so it runs off the loop. What the engine announces
            # from inside it reaches nothing (see `_LoopBound._deliver`): that
            # is the login's thread, not the loop's. The same three are said
            # below, on the loop.
            self._logging_in = self._off_loop(login)
            await self._logging_in
            self._still(attempt)
            self._logging_in = None
            self._callbacks.connectAck()
            self._still(attempt)

            # What the handshake tells ib_async before it considers the API
            # ready. Asked for rather than composed: the client answers this
            # with every account the login holds, and the default account read
            # off it is the first one — so an advisor with several saw one,
            # standing for all of them. Answered on the next pass, as every
            # request is: one pass here, so the answer is in hand.
            self._client.req_managed_accts()
            self._pass_once()
            self._still(attempt)
            self._accounts = list(getattr(self.wrapper, "accounts", []))
            # The number their client counts on, which numbers its requests
            # and the orders it places alike, as a gateway announces it when
            # the API starts: their client takes it as the next id to hand
            # out, and their wrapper hears it. The engine's is the first number
            # past every order id the account has used that a request can also
            # carry, so no id this counter hands out names an order the venue
            # already holds. The engine's login has already waited for the
            # venue to name the orders working, so this answers at once; the
            # bound and the cancel are a guard.
            self._logging_in = self._loop.run_in_executor(None, self._client.next_shared_id)
            next_valid = await asyncio.wait_for(self._logging_in, timeout or None)
            self._logging_in = None
            self._still(attempt)
            self.updateReqId(next_valid)
            self.wrapper.nextValidId(next_valid)
            self._still(attempt)
        except BaseException as e:
            overtaken = attempt != self._generation
            if not overtaken:
                self.disconnect()
            msg = f"API connection failed: {e!r}"
            _logger.error(msg)
            self.apiError.emit(msg)
            if isinstance(e, RuntimeError):
                raise ConnectionError(str(e)) from e
            if overtaken and isinstance(e, asyncio.CancelledError) and not (
                asyncio.current_task().cancelling()
            ):
                raise ConnectionError(
                    "Connection abandoned: disconnect() or another connect was "
                    "called while it was still logging in"
                ) from None
            raise
        self.connState = IbkrDxClient.CONNECTED
        self._pass = self._loop.call_later(PASS_INTERVAL, self._next_pass, attempt)
        self.apiStart.emit()

    def _off_loop(self, call):
        """``call`` on a thread of its own, awaited on the loop.

        A daemon thread, where the loop's executor was used: the executor's
        workers are waited for as the program exits, so a live login given up
        on while it waited on its second factor held the program open until
        the engine gave up too.
        """
        loop, done = self._loop, self._loop.create_future()

        def settle(error):
            if done.done():
                return
            if error is None:
                done.set_result(None)
            else:
                done.set_exception(error)

        def run():
            error = None
            try:
                call()
            except BaseException as e:
                error = e
            try:
                loop.call_soon_threadsafe(settle, error)
            except RuntimeError:
                pass  # the loop has closed, and nothing waits on this

        self._login_thread = threading.Thread(target=run, name="ib_async_dx login", daemon=True)
        self._login_thread.start()
        return done

    def _login(self):
        """The login this session opens with, and where it is kept.

        The one given to `attach` or `connect`; where they name none, the one
        an ``IBC`` was started with in this context; and what that leaves
        empty, ``IB_USERNAME`` and ``IB_PASSWORD``.
        """
        username, password, paper = self._username, self._password, self._paper
        started = IBC_LOGIN.get()
        if started is not None and not started.ended and not (username or password):
            username, password, paper = started.userid, started.password, started.paper
            started.clients.add(self)
        username = username or os.environ.get("IB_USERNAME", "")
        password = password or os.environ.get("IB_PASSWORD", "")
        session_file = self._session_file
        if session_file is None:
            kind = "paper" if paper else "live"
            session_file = str(pathlib.Path.home() / ".ibkr_dx" / f"session-{username}-{kind}")
        elif session_file is False:
            session_file = None
        return username, password, paper, session_file

    def _still(self, attempt):
        """Raise unless this connect is still the current one: a disconnect,
        another connect, or the engine ending the session has overtaken it."""
        if attempt != self._generation or self.connState == IbkrDxClient.DISCONNECTED:
            raise ConnectionError("The session was closed while it was opening")

    def disconnect(self):
        """End the session, or the login still running for one, as ib_async's
        own client ends one.

        `connectionClosed` is not called here. Their wrapper treats it as a
        session that went away underneath them: it fails every request still
        waiting and raises on their global error event, which is right for a
        socket that dropped and wrong for a caller who asked to stop.

        A login still running holds the engine's turn while it announces the
        session and waits for the venue to name the working orders, three
        seconds at most, and the engine's disconnect waits for that turn. So
        the engine is told on a thread of its own, and the loop goes on.
        """
        logging_in = self.connState == IbkrDxClient.CONNECTING and (
            self._login_thread is not None and self._login_thread.is_alive()
        )
        self._retire()
        if logging_in:
            threading.Thread(
                target=self._client.disconnect, name="ib_async_dx disconnect", daemon=True
            ).start()
        else:
            self._client.disconnect()

    #: Their client's `reset` forgets the session. A session here is the
    #: engine's, logged in until it is ended, so forgetting it is ending it.
    reset = disconnect

    def _retire(self):
        """Nothing held for the current session or attempt goes on: its next
        pass is not made, and a connect waiting on its login stops waiting."""
        self._generation += 1
        self.connState = IbkrDxClient.DISCONNECTED
        # Held for the end of a pass a handler has just ended the session in:
        # their wrapper has been cleared, as their client's buffer is.
        self._callbacks._priced.clear()
        self._callbacks.refused.clear()
        self._callbacks.held.clear()
        if self._pass is not None:
            self._pass.cancel()
            self._pass = None
        if self._logging_in is not None:
            self._logging_in.cancel()
            self._logging_in = None

    def _session_ended(self):
        """The engine ended the session: what ib_async's client does when its
        socket closes. Waiting requests fail, and ``apiEnd`` fires, which
        their ``IB`` hears as ``disconnectedEvent``. Said once; and of a
        session still opening, not at all: the connect fails instead, as
        their client says nothing of a socket that closed before the API was
        ready.

        The prices this pass stated before the end reach their tickers first,
        as what a socket carried before it closed is read before the close.
        Held to the end of the pass, they reached a wrapper the close had
        already cleared, which logged each as a request it did not know.
        """
        if self.connState == IbkrDxClient.DISCONNECTED:
            return
        if not self.isConnected():
            # Still opening: the connect sees it, fails, and lets it go.
            self.connState = IbkrDxClient.DISCONNECTED
            return
        self._callbacks.end_pass()
        self._retire()
        self.wrapper.setEventsDone()
        self.wrapper.connectionClosed()
        self.apiEnd.emit()

    def _engine_closed(self):
        """The engine's `connectionClosed`: the session ended, heard on the
        loop. A login given up on closes the session it opened on an engine
        of its own, which says so on the login's thread: that is not the
        session this client holds."""
        self._callbacks.hear(self._session_ended)

    def _ended_underneath(self):
        """The session ends as one does when the gateway a program is
        connected to is stopped: as `_session_ended` says it. A login still
        running is let go of.

        The engine says the close inside its disconnect, heard once the call
        has returned; ended from another thread, it is said here. A handler
        that connected again on hearing it has a session of its own, which
        this leaves alone."""
        if not self.isConnected():
            self.disconnect()
            return
        attempt = self._generation
        with self._callbacks.after_the_call():
            self._client.disconnect()
        if attempt == self._generation:
            self._session_ended()

    def _next_pass(self, attempt):
        """A pass, on the loop, and the next one after it, for as long as the
        session it was made for is the current one.

        A pass that raises ends the session, as their transport closes a
        socket whose data it could not handle: once, with every waiting
        request failed.

        The next is due before this one is made. A handler that waits on the
        session, as a blocking call does under ``util.startLoop()``, turns
        the loop inside this pass, and what it waits for comes on the next.
        """
        if attempt != self._generation:
            return
        self._pass = self._loop.call_later(PASS_INTERVAL, self._next_pass, attempt)
        try:
            self._pass_once()
        except Exception:
            _logger.exception("The session's delivery failed, and the session is ended")
            self._ended_underneath()

    def _pass_once(self):
        """One dispatch, then the boundary ib_async flushes on.

        Their wrapper holds ticker updates until the batch of messages ends,
        and emits their events there. Their own transport marks a batch only
        when data arrives; here a batch is what was delivered since the last
        pass, including an answer given inside a request call. A pass with
        nothing delivered leaves their clock alone, so ``timeoutEvent`` fires,
        and emits no ``updateEvent``.

        A client that has been disconnected makes none: a pass made on a
        session the caller has ended would tell their wrapper the session had
        dropped, and their global error event would cancel the next connect.
        """
        if self.connState == IbkrDxClient.DISCONNECTED:
            return
        with self._callbacks.after_the_call():
            self._callbacks.begin_pass()
            self._client.poll()
        self._callbacks.end_pass()
        if self._callbacks.arrived:
            self._callbacks.arrived = False
            processed = getattr(self.wrapper, "tcpDataProcessed", None)
            if processed:
                processed()

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
        """178 once connected, and 0 until then, as their client answers it."""
        return self.MaxClientVersion if self.isConnected() else 0

    def getAccounts(self):
        return list(self._accounts)

    def getReqId(self):
        if not self.isConnected():
            raise ConnectionError("Not connected")
        if sys._getframe(1).f_code in _NUMBERS_AN_ORDER:
            # The engine refuses a new order at or below an id the account
            # has used or saved for this client id (103). Where those fit a
            # request, the counter below is past them and numbers the order,
            # as their client numbers one. Where they do not, no request can
            # carry an id past them: the order takes the engine's next order
            # id, and the counter goes on for requests. An id the engine
            # reserved and the order did not take only raises its counter.
            order_id = self._client.next_order_id()
            if order_id > WIDEST_REQUEST_ID:
                return order_id
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
        if new_id > WIDEST_REQUEST_ID:
            # The rest of the range is the engine's own, and it refuses a
            # request numbered there: raised here rather than number one.
            raise OverflowError(
                f"request id {new_id} is past the widest this protocol carries, "
                f"{WIDEST_REQUEST_ID}: the ids this session can number are spent"
            )
        self._reqIdSeq += 1
        return new_id

    def updateReqId(self, minReqId):
        # Their wrapper raises this counter past every order id it sees, so
        # that the next order their client numbers is not one the account is
        # already working. Their client numbers orders and requests out of it
        # alike, and so does this one while the account's order ids fit a
        # request (see `getReqId`).
        #
        # An order id placed elsewhere can go wider than a request id, which
        # is four billion wide with the top of that reserved. A raise past what
        # a request can carry buys nothing and costs everything: on an account
        # with such an order, every request afterwards was refused as a number
        # this protocol cannot carry, and an unmodified program could not so
        # much as name a contract. The counter starts past every id the
        # account has used that a request can carry, so a request it numbers
        # is clear of that order as well, and an order takes the engine's own
        # id past it. Such a raise is let go of rather than taken to the top
        # of the range, which saturates and steps over the edge on the next
        # request.
        if minReqId > WIDEST_REQUEST_ID:
            return
        self._reqIdSeq = max(self._reqIdSeq, minReqId)

    def connectionStats(self):
        """When the session started, how long it has run, the bytes each way
        and the messages each way. The bytes are the engine's count of the
        session's protocol bytes with the venue. The messages are counted as
        their client counts them: a request is one sent, and what reaches
        their wrapper one received."""
        from ib_async.objects import ConnectionStats

        if not self.isReady():
            raise ConnectionError("Not connected")
        traffic = self._client.traffic()
        return ConnectionStats(
            self._since, time.time() - self._since,
            traffic["bytes_received"], traffic["bytes_sent"],
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

        The engine states "not connected" inside the call where it has given
        the session up and this client has not yet heard the close. Their
        client's answers come back on the socket once the call has returned,
        so that is held for the next pass (see `_LoopBound.error`).
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
            # Their `placeOrder` writes a message, so the order read back from
            # one goes to the engine rather than round again.
            self._send_theirs("place_order", *args)
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

    def reqMktData(self, reqId, contract, genericTickList, snapshot,
                   regulatorySnapshot, mktDataOptions):
        self._send_theirs(
            "req_mkt_data", reqId, contract, genericTickList, snapshot,
            regulatorySnapshot, mktDataOptions,
        )

    def cancelMktData(self, reqId):
        """A subscription ended, and what was kept for its quotes with it."""
        self._callbacks.forget(reqId)
        self._send_theirs("cancel_mkt_data", reqId)

    def reqHistoricalData(self, reqId, contract, endDateTime, durationStr,
                          barSizeSetting, whatToShow, useRTH, formatDate,
                          keepUpToDate, chartOptions):
        self._send_theirs(
            "req_historical_data", reqId, contract, endDateTime, durationStr,
            barSizeSetting, whatToShow, 1 if useRTH else 0, formatDate,
            keepUpToDate, chartOptions,
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
        written out again here. Its arguments are taken as their client's
        method takes them, by position or by keyword.
        """
        if name.startswith("_"):
            raise AttributeError(name)

        theirs = getattr(_TheirClient, name, None)
        request = _our_name_for(name, self._client)
        if name not in _HANDSHAKE and hasattr(self._client, request):
            def forward(*args):
                return self._send_theirs(request, *args)
        elif callable(theirs):
            def forward(*args):
                # A request their client makes that no gateway answers: the
                # handshake a program makes with the gateway it connects to.
                # Their client sends it and hears nothing, so it is counted
                # as sent and nothing answers it here. The engine's own
                # verify calls answer as ibapi's client does, which refuses
                # them locally; ib_async's does not, so they are not called.
                self._connected()
                self._sent += 1
        else:
            raise AttributeError(
                f"{type(self).__name__!r} object has no attribute {name!r}"
            )
        if not callable(theirs):
            # One of the engine's own, with no method of theirs to say how it
            # is called: its arguments in the engine's order.
            return forward

        signature = inspect.signature(theirs)

        def carried(*args, **kwargs):
            call = signature.bind(self, *args, **kwargs)
            call.apply_defaults()
            return forward(*call.args[1:])

        return carried


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


# The handshake requests their client can send a gateway, which never answers
# them (verifyRequest and the three after it).
_HANDSHAKE = frozenset({
    "verifyRequest", "verifyMessage", "verifyAndAuthRequest", "verifyAndAuthMessage",
})


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
    if isinstance(value, (str, bytes, int, float, bool, type(None))):
        return value
    if _is_named_tuple(type(value)):
        # A record, field by field: as a sequence it is one argument to a
        # type that takes one per field.
        return type(value)(*[_as_theirs(v) for v in value])
    if isinstance(value, (list, tuple)):
        return type(value)(_as_theirs(v) for v in value)

    theirs = _their_type(type(value).__name__)
    if theirs is None or isinstance(value, theirs):
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
        #: The size last stated for each side, by request and then tick type.
        self._sizes: dict[int, dict[int, float]] = {}
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
        #: Whether the engine is inside a call that says things, and what it
        #: said there, to be heard once it has returned.
        self.holding = False
        self.held = collections.deque()
        #: The thread running the loop, set as a session opens. Nothing is
        #: delivered from any other.
        self.thread = threading.get_ident()

    def reset(self):
        """A new session: nothing held for the last one reaches it, as their
        client clears its queues when it connects."""
        self._sizes.clear()
        self._priced.clear()
        self.refused.clear()
        self.held.clear()
        self.arrived = False
        self.received = 0

    def forget(self, req_id):
        """A subscription over: the sizes kept for its quotes go with it."""
        self._sizes.pop(req_id, None)

    def _deliver(self, method, *args):
        """One message to their wrapper.

        Their batch starts on the first message since the last one ended,
        where their own transport starts one when data arrives. A message
        their wrapper raises on is logged and passed over, as their decoder
        treats one: raised into the engine, it closed the session.

        Delivered on the loop's thread only. The engine announces a session
        from inside its login, on the login's thread, and those announcements
        are made again on the loop. A login given up on can still announce its
        session there, after another session has opened.
        """
        self.hear(lambda: self._hand(method, args))

    def hear(self, call):
        """What the engine said, on the loop's thread, once the engine call
        it was said in has returned (see `after_the_call`)."""
        if threading.get_ident() != self.thread:
            return
        if self.holding:
            self.held.append(call)
        else:
            call()

    @contextlib.contextmanager
    def after_the_call(self):
        """What the engine says inside a pass or a close, heard in its order
        once the call has returned.

        The engine holds the session's turn while it reads and while it
        closes, and calls back inside that. A handler run there that waited on
        the session waited on a turn its own caller held: a connect made on
        hearing the session end never logged in, and a blocking request under
        ``util.startLoop()`` was never answered, since a read inside a read
        delivers nothing.
        """
        if self.holding or threading.get_ident() != self.thread:
            yield
            return
        self.holding = True
        try:
            yield
        finally:
            self.holding = False
            while self.held:
                self.held.popleft()()

    def _hand(self, method, args):
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
            self._sizes.setdefault(req_id, {})[tick_type] = size
            price = self._priced.pop((req_id, price_type), None)
            if price is not None:
                self._deliver(self._wrapper.priceSizeTick, req_id, price_type, price, size)
                return
        self._deliver(self._wrapper.tickSize, req_id, tick_type, size)

    def end_pass(self, req_id=None):
        """The prices this pass stated with no size beside them, of every
        request or of one, each with the size standing: the size did not
        change, so the pass did not state it."""
        for key in [key for key in self._priced if req_id in (None, key[0])]:
            price = self._priced.pop(key)
            size = self._sizes.get(key[0], {}).get(self._SIZE_OF[key[1]], 0.0)
            self._deliver(self._wrapper.priceSizeTick, *key, price, size)

    #: Where their wrapper names a callback something other than the
    #: reference client does, and the two it does not carry at all: display
    #: groups belong to a window, and there is none here or there.
    _THEIR_NAME = {
        "real_time_bar": "realtimeBar",
        "commission_and_fees_report": "commissionReport",
        "display_group_list": None,
        "display_group_updated": None,
    }

    def tick_snapshot_end(self, req_id):
        """A snapshot answered: its prices reach the ticker first, as a
        gateway sends them before the end, and then the sizes kept for them
        go, since the subscription is over."""
        self.end_pass(req_id)
        self.forget(req_id)
        self._deliver(self._wrapper.tickSnapshotEnd, req_id)

    # Under both spellings. This engine looks for the reference client's name
    # first, so a callback answered here under one spelling only would be
    # reached past — straight to their wrapper, and the translation skipped.
    tickPrice = tick_price
    tickSize = tick_size
    histogramData = histogram_data
    tickSnapshotEnd = tick_snapshot_end

    def error(self, req_id, when, code, text, advanced=""):
        """A refusal, in the shape their wrapper declares, when a gateway's
        would arrive.

        This engine states when the venue said it, as the current reference
        client does. `ib_async` predates that argument and declares four, so
        their wrapper is handed four: passed five it raises, and every error
        and notice of the session would be lost.

        The engine delivers a refusal on a pass, except "not connected" on a
        session it has given up, which it states inside the request call.
        That waits for the next pass too. A gateway's comes back on the socket
        after the call has returned, and their `placeOrder` makes its `Trade`
        after the call: delivered inside it, a refused new order had no trade
        to mark and stayed PendingSubmit.
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
            if not self.refused:
                return  # a handler ended the session
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
        # no place for, at the value their client sends on every order.
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
           readonly=False):
    """Point an `ib_async.IB` at this engine, and hand it back.

    The credentials are this session's; left out, those of an ``IBC`` started
    in the same context, and failing that `IB_USERNAME` and `IB_PASSWORD`.
    The client id is the one `connect` names.

    A session the instance holds is ended first, as its ``disconnect()`` ends
    one, and a login still running on it is let go of: the client it replaces
    would otherwise hold a session nobody could reach, beside a wrapper the
    new one shares.

    The session is kept between runs, under this account's own file in
    ``~/.ibkr_dx``. A venue answers a request that names a session it still holds
    with a challenge rather than a whole handshake. It lets a session go soon
    after the process holding it ends, so this covers a quick restart; a program
    started again later logs in afresh. Name another path to move it, or pass
    ``False`` to keep nothing and log in fully every time.

    Orders and requests are numbered from one counter, as ib_async's own
    client numbers them, kept past every order id the account has used, which
    the venue names at every connect and whenever it names another, and past
    the next order id the engine saved for this account and client id. Where
    those ids have outgrown what a request can carry, a new order, a bracket
    and a what-if take the engine's next order id instead, and requests go on
    from the counter.
    """
    ib.disconnect()
    if ib.client.connState != ib.client.DISCONNECTED:
        ib.client.disconnect()
    ib.client.apiEnd -= ib.disconnectedEvent
    ib.client = IbkrDxClient(ib.wrapper, username, password, paper, session_file,
                             readonly)
    ib.wrapper.client = ib.client
    # ib_async's IB ties this to the client it builds for itself.
    ib.client.apiEnd += ib.disconnectedEvent
    return ib

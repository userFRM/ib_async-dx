"""ib_async's ``ibcontroller`` module, with an :class:`IBC` that launches nothing.

Every other name ib_async's module has is ib_async's own object here, its
``Watchdog`` among them, except ``IB``, which is this package's, as it is on
``ib_async_dx``.
"""

import ib_async.ibcontroller
from ib_async.ibcontroller import *  # noqa: F403

from .bridge import IBC_LOGIN, IbcLogin
from .ib import IB  # noqa: F401  in place of ib_async's, which connects to a gateway

#: What ``from ib_async_dx.ibcontroller import *`` binds: ib_async's module's
#: names.
__all__ = [name for name in dir(ib_async.ibcontroller) if not name.startswith("_")]


class IBC(ib_async.ibcontroller.IBC):
    """ib_async's IBC, with no gateway to launch: the engine logs in itself.

    Starting holds the login it names, as the gateway it would launch holds
    one: ``userid`` and ``password``, on a live session where ``tradingMode``
    is ``'live'`` and on paper otherwise. A connect in the same context that
    names no login of its own logs in with it; ib_async's ``Watchdog`` starts
    its IBC and connects its IB in one task, so every connect it makes does.
    An empty ``userid`` or ``password`` is read from ``IB_USERNAME`` or
    ``IB_PASSWORD``. Terminating ends every session that login opened, as
    stopping a gateway ends the sessions of the programs connected to it, and
    no connect logs in with it afterwards.

    ib_async's own ``Watchdog``, handed one, is then a reconnect loop: it
    connects its ``IB``, and when the session ends it waits ``retryDelay`` and
    ``appStartupTime`` and connects again. The paths, the Java settings and
    the FIX login are a gateway's, and nothing reads them: a login or a
    ``TradingMode`` kept in IBC's own ``config.ini`` is not read either.
    """

    def __post_init__(self):
        super().__post_init__()
        self._started = None

    def start(self):
        """Hold the login, for the connects made in the caller's context."""
        self._started = IbcLogin(self.userid, self.password, self.tradingMode != "live")
        IBC_LOGIN.set(self._started)

    def terminate(self):
        """End every session the login opened, and hold it no longer, in any
        context it was started in."""
        started, self._started = self._started, None
        if started is None:
            return
        started.ended = True
        if IBC_LOGIN.get() is started:
            IBC_LOGIN.set(None)
        for client in list(started.clients):
            client._ended_underneath()

    async def startAsync(self):
        self.start()

    async def terminateAsync(self):
        self.terminate()

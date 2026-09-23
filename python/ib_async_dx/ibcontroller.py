"""ib_async's ``ibcontroller`` module, with an :class:`IBC` that starts nothing.

Every other name ib_async's module has is ib_async's own object here, its
``Watchdog`` among them, except ``IB``, which is this package's, as it is on
``ib_async_dx``.
"""

import ib_async.ibcontroller
from ib_async.ibcontroller import *  # noqa: F403

from .ib import IB  # noqa: F401  in place of ib_async's, which connects to a gateway

#: What ``from ib_async_dx.ibcontroller import *`` binds: ib_async's module's
#: names.
__all__ = [name for name in dir(ib_async.ibcontroller) if not name.startswith("_")]


class IBC(ib_async.ibcontroller.IBC):
    """ib_async's IBC, with no gateway to start or stop.

    The engine logs in itself, on ``connect``, so starting and terminating do
    nothing. ib_async's own ``Watchdog``, handed one, is then a reconnect loop:
    it connects its ``IB``, and when the session ends it waits ``retryDelay``
    and ``appStartupTime`` and connects again.

    The login is the one ``connect`` takes with no login arguments,
    ``IB_USERNAME`` and ``IB_PASSWORD``, on a paper session. ``userid``,
    ``password`` or a live ``tradingMode`` would log a gateway in, and there is
    none: given one, this raises rather than log in with another login. A
    ``Watchdog`` keeps another login, or a live session, on an ``ib_async.IB``
    given it by ``attach``.
    """

    def __post_init__(self):
        super().__post_init__()
        stated = [name for name in ("userid", "password") if getattr(self, name)]
        if self.tradingMode == "live":
            stated.append("tradingMode='live'")
        if stated:
            raise ValueError(
                f"IBC was given {', '.join(stated)}, which only a gateway's "
                "login reads, and there is no gateway to log in: the engine "
                "logs in when its IB connects, with IB_USERNAME and IB_PASSWORD "
                "on a paper session. For another login or a live session, hand "
                "the Watchdog ib_async_dx.attach(ib_async.IB(), username=..., "
                "password=..., paper=False), which keeps them across every "
                "reconnect"
            )

    async def startAsync(self):
        pass

    async def terminateAsync(self):
        pass

"""ib_async on the ibkr-dx engine, without a gateway.

This is ib_async's public API unchanged: the same names, the same objects and
the same submodules, run by ib_async's own code as a dependency. The
differences are :class:`IB`, a subclass of ``ib_async.IB`` whose transport is
the engine, so its ``connect`` takes a login instead of a gateway's address,
and :class:`IBC`, which has no gateway to start.

:func:`attach` does the same for an ``ib_async.IB`` a program already holds.

ib_async-dx is not affiliated with ib_async or with Interactive Brokers.
"""

import importlib
import importlib.metadata
import pkgutil
import sys

import ib_async
from ib_async import *  # noqa: F403

from .bridge import attach
from .ib import (
    IB,
    CompetingSession,
    CorporateAction,
    OptionModel,
    OrderPreset,
    PositionElsewhere,
    ScannedStrategy,
    SpreadScan,
    TickerExtras,
)
from .ibcontroller import IBC

__all__ = list(ib_async.__all__)

# ``__version__`` and ``__version_info__`` are ib_async's, from the star import
# above, so a program that checks the API level it runs on reads what it always
# did. This package's own version:
__ib_async_dx_version__ = importlib.metadata.version("ib_async-dx")

# ib_async's submodules, as themselves, so ``import ib_async_dx.contract`` and
# ``from ib_async_dx.util import df`` work. ``ib`` and ``ibcontroller`` are
# this package's own.
for _module in pkgutil.iter_modules(ib_async.__path__):
    if _module.name not in ("ib", "ibcontroller"):
        globals()[_module.name] = sys.modules[f"{__name__}.{_module.name}"] = (
            importlib.import_module(f"ib_async.{_module.name}")
        )
del _module, ib_async, importlib, pkgutil, sys

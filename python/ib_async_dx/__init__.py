"""ib_async on the ibkr-dx engine, without a gateway.

This is ib_async's public API unchanged: the same names, the same objects and
the same submodules, run by ib_async's own code as a dependency. The one
difference is :class:`IB`, a subclass of ``ib_async.IB`` whose transport is
the engine, so its ``connect`` takes a login instead of a gateway's address.

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
from .ib import IB, CompetingSession, OptionModel, OrderPreset, TickerExtras

__all__ = list(ib_async.__all__)

#: This package's version. ``__version_info__`` stays ib_async's.
__version__ = importlib.metadata.version("ib_async-dx")
#: The ib_async whose API this runs.
__ib_async_version__ = ib_async.__version__

# ib_async's submodules, as themselves, so ``import ib_async_dx.contract`` and
# ``from ib_async_dx.util import df`` work. ``ib`` is this package's own.
for _module in pkgutil.iter_modules(ib_async.__path__):
    if _module.name != "ib":
        globals()[_module.name] = sys.modules[f"{__name__}.{_module.name}"] = (
            importlib.import_module(f"ib_async.{_module.name}")
        )
del _module, ib_async, importlib, pkgutil, sys

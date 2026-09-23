"""`import ib_async_dx` is `import ib_async`, with two classes that differ.

A program changes its import line and nothing else it reads: the same names,
the same objects, the same submodules, the same version. Only `IB` and `IBC`
are this package's: `IB` is a subclass of theirs whose `connect` takes a login,
and `IBC` a subclass of theirs with no gateway to start.
"""

import importlib
import importlib.metadata
import inspect
import pkgutil

import ib_async

import ib_async_dx

SUBMODULES = [
    "client", "connection", "contract", "decoder", "flexreport", "ib",
    "ibcontroller", "objects", "order", "ticker", "util", "version", "wrapper",
]


def test_the_names_are_theirs_and_so_are_the_objects():
    assert ib_async_dx.__all__ == ib_async.__all__
    assert len(ib_async_dx.__all__) == 103
    differ = [n for n in ib_async.__all__ if getattr(ib_async_dx, n) is not getattr(ib_async, n)]
    assert differ == ["IB", "IBC"], "only IB and IBC"


def test_the_version_is_ib_asyncs_and_this_packages_is_apart():
    """A program that checks the API level it runs on reads ib_async's."""
    assert ib_async_dx.__version__ is ib_async.__version__
    assert ib_async_dx.__version_info__ is ib_async.__version_info__
    assert ib_async_dx.__ib_async_dx_version__ == importlib.metadata.version("ib_async-dx")
    assert "__ib_async_dx_version__" not in ib_async_dx.__all__


def test_every_submodule_of_theirs_resolves_here():
    assert [m.name for m in pkgutil.iter_modules(ib_async.__path__)] == SUBMODULES
    for name in SUBMODULES:
        ours = importlib.import_module(f"ib_async_dx.{name}")
        assert getattr(ib_async_dx, name) is ours, f"ib_async_dx.{name} as an attribute"
        if name not in ("ib", "ibcontroller"):
            assert ours is importlib.import_module(f"ib_async.{name}"), name


def test_from_a_submodule_import_works():
    from ib_async_dx.contract import Stock
    from ib_async_dx.util import df

    import ib_async_dx.order

    assert df is ib_async.util.df
    assert Stock is ib_async.Stock
    assert ib_async_dx.order.LimitOrder is ib_async.LimitOrder


def test_the_ib_module_is_theirs_with_ib_replaced():
    from ib_async_dx.ib import IB, StartupFetch

    assert IB is ib_async_dx.IB
    assert StartupFetch is ib_async.StartupFetch
    public = [n for n in dir(ib_async.ib) if not n.startswith("_") and n != "IB"]
    assert all(getattr(ib_async_dx.ib, n) is getattr(ib_async.ib, n) for n in public)
    # A star import binds the names theirs does, and no others.
    theirs, ours = {}, {}
    exec("from ib_async.ib import *", theirs)
    exec("from ib_async_dx.ib import *", ours)
    assert ours.keys() == theirs.keys()


def test_the_ibcontroller_module_is_theirs_with_IB_and_IBC_replaced():
    from ib_async_dx.ibcontroller import IB, IBC, Watchdog

    assert IB is ib_async_dx.IB, "their module names their gateway's IB"
    assert IBC is ib_async_dx.IBC and IBC is not ib_async.IBC
    assert issubclass(IBC, ib_async.IBC)
    assert Watchdog is ib_async.Watchdog, "their Watchdog, unmodified"
    public = [
        n for n in dir(ib_async.ibcontroller)
        if not n.startswith("_") and n not in ("IB", "IBC")
    ]
    assert all(getattr(ib_async_dx.ibcontroller, n) is getattr(ib_async.ibcontroller, n)
               for n in public)
    theirs, ours = {}, {}
    exec("from ib_async.ibcontroller import *", theirs)
    exec("from ib_async_dx.ibcontroller import *", ours)
    assert ours.keys() == theirs.keys()


def test_ib_is_their_ib():
    assert issubclass(ib_async_dx.IB, ib_async.IB)
    assert ib_async_dx.IB.__init__ is ib_async.IB.__init__
    assert "attach" not in ib_async_dx.__all__
    assert callable(ib_async_dx.attach)


def test_connect_is_theirs_plus_the_login():
    """Their parameters, defaults and kinds, then four keyword-only ones."""
    added = [
        ("username", ""),
        ("password", ""),
        ("paper", True),
        ("sessionFile", None),
    ]
    for name in ("connect", "connectAsync"):
        ours = list(inspect.signature(getattr(ib_async_dx.IB, name)).parameters.values())
        theirs = list(inspect.signature(getattr(ib_async.IB, name)).parameters.values())
        assert ours[: len(theirs)] == theirs, name
        extra = ours[len(theirs):]
        assert [(p.name, p.default) for p in extra] == added, name
        assert all(p.kind is inspect.Parameter.KEYWORD_ONLY for p in extra), name
    assert inspect.iscoroutinefunction(ib_async_dx.IB.connectAsync)

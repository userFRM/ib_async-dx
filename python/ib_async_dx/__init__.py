"""The ib_async shape, on the IBKR-DX engine.

``Client`` (also exported as ``IB``) is a facade over ``ibkr_dx.EClient``:
methods that send a question and hand the answer back, not a second client, so
the two see the same session.

``attach`` and ``IbkrDxClient`` run an unmodified ``ib_async`` program on the
engine. They need the ``bridge`` extra (``ib_async`` and ``eventkit``), so they
are loaded on first use rather than with the package.
"""

from ._ib import Client

#: The session under the name the widely used asynchronous wrapper gives it, so
#: a program written against that one finds what it is looking for. The same
#: class either way.
IB = Client

__all__ = ["IB", "Client", "attach", "IbkrDxClient"]


# ponytail: lazy so the facade imports without the bridge extra installed.
def __getattr__(name):
    if name in ("attach", "IbkrDxClient"):
        from . import bridge

        return getattr(bridge, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

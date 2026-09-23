"""ib_async's ``ib`` module, with :class:`IB` running on the ibkr-dx engine.

Every other name ib_async's module has is ib_async's own object here.
:class:`TickerExtras`, :class:`OptionModel`, :class:`OrderPreset` and
:class:`CompetingSession` are this package's, and outside ``__all__``.
"""

import asyncio
import dataclasses
import datetime
import math
from collections.abc import Awaitable
from typing import Literal, NamedTuple

import ib_async.ib
from ib_async import util
from ib_async.contract import Contract, TagValue
from ib_async.ib import *  # noqa: F403
from ib_async.ib import StartupFetch, StartupFetchALL
from ib_async.objects import Position
from ib_async.ticker import Ticker

from .bridge import IbkrDxClient, _as_ours, _refuse_options, attach

#: What ``from ib_async_dx.ib import *`` binds: ib_async's module's names.
__all__ = [name for name in dir(ib_async.ib) if not name.startswith("_")]


@dataclasses.dataclass
class TickerExtras:
    """What the venue states for a ticker's contract beyond ib_async's Ticker.

    Series keep the venue's own numbers. A figure the venue has not stated is
    nan.
    """

    sharesOutstanding: float = math.nan
    openAYearAgo: float = math.nan
    #: A short-sale circuit breaker is on (Rule 201 style), not shortability.
    shortSaleRestricted: bool = False
    statedFigures: dict[int, list[float]] = dataclasses.field(default_factory=dict)
    #: Per series, the whole and the fractional table, each by the venue's own
    #: numbering.
    numberedFigures: dict[int, tuple[dict[int, float], dict[int, float]]] = (
        dataclasses.field(default_factory=dict)
    )
    pairedFigures: dict[int, list[tuple[float, float]]] = dataclasses.field(
        default_factory=dict
    )


@dataclasses.dataclass(frozen=True)
class OptionModel:
    """The venue's option model, all its figures: the eight of
    OptionComputation and the ten it has no field for. A figure the venue has
    not stated is None; ``priceBasedVol`` is always a bool, False when the
    venue did not state it."""

    impliedVol: float | None = None
    delta: float | None = None
    optPrice: float | None = None
    pvDividend: float | None = None
    gamma: float | None = None
    vega: float | None = None
    theta: float | None = None
    undPrice: float | None = None
    calDays: float | None = None
    rate: float | None = None
    rho: float | None = None
    fugit: float | None = None
    exerciseBoundary: float | None = None
    forwardCoeff: float | None = None
    modelYield: float | None = None
    bridgeYield: float | None = None
    timeValue: float | None = None
    priceBasedVol: bool | None = None


class OrderPreset(NamedTuple):
    """One set of order defaults the account holds. Its values are not carried."""

    key: str
    version: str
    lastChanged: str


class CompetingSession(NamedTuple):
    """Another session that held the account when this one connected."""

    origin: str
    loggedInAt: datetime.datetime
    #: This session may read but not trade, because the other holds the account.
    readOnly: bool


def _stated(value, unset=math.nan):
    """The engine's figure, with its unset double replaced by ``unset``."""
    return unset if value == util.UNSET_DOUBLE else value


def _optionModel(stated: dict | None) -> OptionModel | None:
    if stated is None:
        return None
    return OptionModel(**{name: _stated(v, None) for name, v in stated.items()})


class IB(ib_async.ib.IB):
    """ib_async's IB, with the ibkr-dx engine as its transport.

    ib_async's own code does everything: :meth:`connect` puts the engine in
    place of ib_async's Client (see :func:`ib_async_dx.bridge.attach`), then
    runs ib_async's connect. Beyond ib_async:

    * ``connect`` and ``connectAsync`` take the account's login;
    * two ib_async 2.1 bugs are fixed: ``reqUserInfo`` returned ``[]``, and
      the startup sync asked for positions whatever ``fetchFields`` said;
    * the engine's calls beyond the documented API are methods here, from
      :meth:`reqMktDataEx` on.
    """

    #: False only while connectAsync runs without StartupFetch.POSITIONS.
    _fetchPositions = True

    def connect(
        self,
        host: str = "127.0.0.1",
        port: int = 7497,
        clientId: int = 1,
        timeout: float = 4,
        readonly: bool = False,
        account: str = "",
        raiseSyncErrors: bool = False,
        fetchFields: StartupFetch = StartupFetchALL,
        *,
        username: str = "",
        password: str = "",
        paper: bool = True,
        sessionFile: str | Literal[False] | None = None,
    ):
        """Log in to IBKR and synchronize, as ib_async's connect does.

        This method is blocking. There is no gateway: ``host`` and ``port``
        are not used, and ``clientId`` keys this session's orders.
        ``readonly`` also makes the engine refuse order requests.

        Args:
            username: The account's login name. Left empty, ``IB_USERNAME``.
            password: Its password. Left empty, ``IB_PASSWORD``.
            paper: A paper session unless ``False``.
            sessionFile: Where the session is kept between runs. ``None`` is
                ``~/.ibkr_dx/session-{username}-{paper|live}``; ``False``
                keeps nothing.

        The other arguments are ib_async's.
        """
        return self._run(
            self.connectAsync(
                host,
                port,
                clientId,
                timeout,
                readonly,
                account,
                raiseSyncErrors,
                fetchFields,
                username=username,
                password=password,
                paper=paper,
                sessionFile=sessionFile,
            )
        )

    async def connectAsync(
        self,
        host: str = "127.0.0.1",
        port: int = 7497,
        clientId: int = 1,
        timeout: float | None = 4,
        readonly: bool = False,
        account: str = "",
        raiseSyncErrors: bool = False,
        fetchFields: StartupFetch = StartupFetchALL,
        *,
        username: str = "",
        password: str = "",
        paper: bool = True,
        sessionFile: str | Literal[False] | None = None,
    ):
        # ib_async's own client closes the session it holds before opening
        # another.
        self.disconnect()
        attach(self, username, password, paper, sessionFile, clientId, readonly)
        wrapper = self.wrapper
        # ib_async 2.1's Wrapper.userInfo ends the request without the White
        # Branding ID it was answered with, so reqUserInfo() returned [].
        wrapper.userInfo = lambda reqId, whiteBrandingId: wrapper._endReq(
            reqId, whiteBrandingId
        )
        # The answer to reqCurrentTimeInMillis, which ib_async's Wrapper lacks.
        wrapper.currentTimeInMillis = lambda timeInMillis: wrapper._endReq(
            "currentTimeInMillis", timeInMillis
        )
        self._fetchPositions = bool(fetchFields & StartupFetch.POSITIONS)
        try:
            await super().connectAsync(
                host, port, clientId, timeout, readonly, account, raiseSyncErrors,
                fetchFields,
            )
        finally:
            self._fetchPositions = True
        if session := self.competingSession():
            self._logger.warning(
                f"Another session was logged in on this account when this one "
                f"connected: {session}"
            )
        return self

    def reqPositionsAsync(self) -> Awaitable[list[Position]]:
        if self._fetchPositions:
            return super().reqPositionsAsync()
        # ib_async 2.1's startup sync asks for positions even when fetchFields
        # leaves StartupFetch.POSITIONS out. connectAsync lowers the flag for
        # that one request, answered here from the cache without asking; it is
        # raised again at once, so any later request asks as usual.
        self._fetchPositions = True
        future = asyncio.get_running_loop().create_future()
        future.set_result(self.positions())
        return future

    # ── Beyond ib_async: the engine's calls past the documented API ──

    @property
    def _eclient(self):
        """The engine's EClient. While not connected, the error ib_async
        raises."""
        if not isinstance(self.client, IbkrDxClient) or not self.client.isConnected():
            raise ConnectionError("Not connected")
        return self.client._client

    def _reqIdOf(self, ticker: Ticker) -> int:
        return self.wrapper.ticker2ReqId["mktData"].get(ticker, 0)

    def reqMktDataEx(
        self,
        contract: Contract,
        genericTickList: str = "",
        snapshot: bool = False,
        regulatorySnapshot: bool = False,
        mktDataOptions: list[TagValue] = [],
        marketDataType: int | None = None,
    ) -> Ticker:
        """:meth:`reqMktData`, with a market data type for this request only.

        ``marketDataType`` is numbered as in :meth:`reqMarketDataType`: 1 live,
        2 frozen, 3 delayed, 4 delayed frozen. ``None`` keeps the session's.
        A contract holds one subscription: asked again while subscribed, it
        follows the one that is up, so cancel between two types.
        """
        if marketDataType is None:
            return self.reqMktData(
                contract, genericTickList, snapshot, regulatorySnapshot, mktDataOptions
            )
        # The engine numbers them 0 live, 1 delayed, 2 frozen, 3 delayed frozen.
        mode = {1: 0, 2: 2, 3: 1, 4: 3}.get(marketDataType)
        if mode is None:
            raise ValueError(
                f"marketDataType={marketDataType!r}: 1 live, 2 frozen, 3 delayed "
                "or 4 delayed frozen"
            )
        _refuse_options("mktDataOptions", mktDataOptions)
        eclient = self._eclient
        reqId = self.client.getReqId()
        ticker = self.wrapper.startTicker(reqId, contract, "mktData")
        eclient.req_mkt_data_ex(
            reqId, _as_ours(contract), genericTickList, snapshot, regulatorySnapshot,
            mode,
        )
        return ticker

    def reqCurrentTimeInMillis(self) -> int:
        """The venue's clock in milliseconds since the epoch.

        This method is blocking. It is the local clock corrected by the
        venue's, so it is given to the millisecond and accurate to about a
        second.
        """
        return self._run(self.reqCurrentTimeInMillisAsync())

    def reqCurrentTimeInMillisAsync(self) -> Awaitable[int]:
        eclient = self._eclient
        future = self.wrapper.startReq("currentTimeInMillis")
        eclient.req_current_time_in_millis()
        return future

    def tickerExtras(self, ticker: Ticker) -> TickerExtras:
        """What the venue has stated for ``ticker``'s market data request
        beyond ib_async's Ticker, read now.

        A series is asked for by its number in ``genericTickList``.
        """
        eclient, reqId = self._eclient, self._reqIdOf(ticker)
        figures = eclient.contract_figures(reqId) or {}
        return TickerExtras(
            sharesOutstanding=_stated(figures.get("sharesOutstanding", math.nan)),
            openAYearAgo=_stated(figures.get("openAYearAgo", math.nan)),
            shortSaleRestricted=eclient.short_sale_restricted(reqId),
            statedFigures={
                series: [_stated(v) for v in eclient.stated_figures(reqId, series)]
                for series in eclient.stated_figures_series(reqId)
            },
            numberedFigures={
                series: tuple(
                    {n: _stated(v) for n, v in eclient.numbered_figures(reqId, series, fractional)}
                    for fractional in (False, True)
                )
                for series in eclient.numbered_figures_series(reqId)
            },
            pairedFigures={
                series: [
                    (_stated(a), _stated(b)) for a, b in eclient.paired_figures(reqId, series)
                ]
                for series in eclient.paired_figures_series(reqId)
            },
        )

    def optionModel(self, ticker: Ticker) -> OptionModel | None:
        """The venue's model of ``ticker``'s option, or None until it states one."""
        return _optionModel(self._eclient.option_model(self._reqIdOf(ticker)))

    def closingOptionModel(self, ticker: Ticker) -> OptionModel | None:
        """The same model as it stood at the close, or None until it is stated."""
        return _optionModel(self._eclient.closing_option_model(self._reqIdOf(ticker)))

    def companyData(self, contract: Contract) -> dict[int, list[tuple[str, str]]]:
        """What the venue states about ``contract``'s company or terms, by series.

        Each series is the venue's own key and value pairs, asked for by its
        number in ``genericTickList`` and kept after the cancel. Empty means
        not entitled or nothing stated; the two cannot be told apart.
        """
        eclient, conId = self._eclient, contract.conId
        return {
            series: eclient.company_data(conId, series)
            for series in eclient.company_data_series(conId)
        }

    def enabledFeatures(self) -> list[str]:
        """The capability tokens the venue granted this account at logon."""
        return self._eclient.enabled_features()

    def orderPermissions(self) -> dict[str, list[str]]:
        """The order types the venue permits, by security type."""
        return self._eclient.order_permissions()

    def permittedOrderTypes(self, secType: str) -> list[str] | None:
        """The order types permitted for ``secType``, or None if it is not
        permitted. An order the account may not place comes back Inactive with
        no text."""
        return self._eclient.permitted_order_types(secType)

    def algorithms(self) -> dict[str, list[str]]:
        """The algorithms the venue offers, keyed ``PROVIDER/SECTYPE``."""
        return self._eclient.algorithms()

    def algorithmsFor(self, secType: str) -> list[str]:
        """The algorithms offered for ``secType``, across every provider."""
        return self._eclient.algorithms_for(secType)

    def orderPresets(self) -> list[OrderPreset]:
        """The sets of order defaults this account holds."""
        return [OrderPreset(*preset) for preset in self._eclient.order_presets()]

    def competingSession(self) -> CompetingSession | None:
        """Another session that held this account when this one connected."""
        stated = self._eclient.competing_session()
        if stated is None:
            return None
        origin, since, readOnly = stated
        loggedInAt = datetime.datetime.strptime(since, "%Y%m%d-%H:%M:%S")
        return CompetingSession(
            origin, loggedInAt.replace(tzinfo=datetime.timezone.utc), readOnly
        )

    def reqPing(self) -> None:
        """Measure the round trip to the venue; :meth:`lastRtt` reads it."""
        self._eclient.req_ping()

    def lastRtt(self) -> float | None:
        """The latest round trip to the venue in milliseconds, or None."""
        return self._eclient.last_rtt_ms()

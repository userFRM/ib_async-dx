"""ib_async's client's messages, read back into the requests that wrote them.

ib_async's ``Client`` writes each request as one message of fields and sends
it with ``send`` and ``sendMsg``, which a program can also call itself. There
is no socket here, so a message is read back as a gateway reads one, field by
field in the order ib_async's client writes them (at server version 178, the
one this client states), into the request ib_async's client names for it.

Only what ib_async's client writes is read. A field is read as ib_async reads
it: as the type of the field's default, empty for the default itself.
"""

import dataclasses
import math

from ib_async.contract import ComboLeg, Contract, DeltaNeutralContract, TagValue
from ib_async.objects import ExecutionFilter, ScannerSubscription, WshEventData
from ib_async.order import Order, OrderComboLeg, OrderCondition
from ib_async.util import UNSET_DOUBLE

#: A contract as ib_async's client writes it: its twelve fields, in order.
_CONTRACT = (
    "conId symbol secType lastTradeDateOrContractMonth strike right multiplier "
    "exchange primaryExchange currency localSymbol tradingClass"
)


class Unreadable(ValueError):
    """A message that does not read as the request it names.

    ``reqId`` is that request's own number where it was read before the field
    that failed, and -1 before it: a gateway answers such a message under
    whichever it has.
    """

    def __init__(self, why, reqId=-1):
        super().__init__(why)
        self.reqId = reqId


class _Fields:
    """One message's fields, read in order."""

    def __init__(self, fields):
        self._fields = iter(fields)
        #: The request's own number, once it has been read.
        self.reqId = -1

    def __next__(self):
        return next(self._fields)

    def done(self):
        """Whether every field has been read."""
        return next(self._fields, None) is None

    def int(self):
        field = next(self)
        return int(field) if field else 0

    def id(self):
        """The request's own number."""
        self.reqId = self.int()
        return self.reqId

    def float(self):
        field = next(self)
        return math.inf if field == "Infinite" else float(field) if field else 0.0

    def bool(self):
        field = next(self)
        return bool(int(field)) if field else False

    def tags(self):
        """A list of tags and values, which ib_async writes as one field."""
        return [TagValue(*pair.split("=", 1)) for pair in next(self).split(";") if pair]

    def into(self, obj, names):
        """The next fields into ``obj``'s attributes of these names, each read
        as the type of its default, as ib_async's decoder reads one; empty is
        the default."""
        fields = {f.name: f for f in dataclasses.fields(obj)}
        for name in names.split():
            field, default = next(self), fields[name].default
            if default is dataclasses.MISSING or type(default) is str:
                value = field
            elif not field:
                value = default
            elif type(default) is bool:
                value = bool(int(field))
            elif type(default) is int:
                value = int(field)
            else:
                value = math.inf if field == "Infinite" else float(field)
            setattr(obj, name, value)
        return obj

    def contract(self, names=_CONTRACT):
        return self.into(Contract(), names)

    def legs(self, contract):
        """A combination's legs, as ib_async writes them for a quote or bars."""
        if contract.secType == "BAG":
            contract.comboLegs = [
                self.into(ComboLeg(), "conId ratio action exchange")
                for _ in range(self.int())
            ]

    def hedge(self, contract):
        """The contract's delta-neutral hedge, if it states one."""
        if self.bool():
            contract.deltaNeutralContract = self.into(
                DeltaNeutralContract(), "conId delta price"
            )


def _mkt_data(f):
    reqId, contract = f.id(), f.contract()
    f.legs(contract)
    f.hedge(contract)
    return reqId, contract, next(f), f.bool(), f.bool(), f.tags()


def _place_order(f):
    orderId, contract, order = f.id(), f.contract(), Order()
    f.into(contract, "secIdType secId")
    f.into(order, (
        "action totalQuantity orderType lmtPrice auxPrice tif ocaGroup account "
        "openClose origin orderRef transmit parentId blockOrder sweepToFill "
        "displaySize triggerMethod outsideRth hidden"
    ))
    if contract.secType == "BAG":
        contract.comboLegs = [
            f.into(ComboLeg(), (
                "conId ratio action exchange openClose shortSaleSlot "
                "designatedLocation exemptCode"
            ))
            for _ in range(f.int())
        ]
        order.orderComboLegs = [
            f.into(OrderComboLeg(), "price") for _ in range(f.int())
        ]
        order.smartComboRoutingParams = [
            TagValue(next(f), next(f)) for _ in range(f.int())
        ]
    next(f)  # shares allocation, which ib_async always writes empty
    f.into(order, (
        "discretionaryAmt goodAfterTime goodTillDate faGroup faMethod "
        "faPercentage modelCode shortSaleSlot designatedLocation exemptCode "
        "ocaType rule80A settlingFirm allOrNone minQty percentOffset eTradeOnly "
        "firmQuoteOnly nbboPriceCap auctionStrategy startingPrice stockRefPrice "
        "delta stockRangeLower stockRangeUpper overridePercentageConstraints "
        "volatility volatilityType deltaNeutralOrderType deltaNeutralAuxPrice"
    ))
    if order.deltaNeutralOrderType:
        f.into(order, (
            "deltaNeutralConId deltaNeutralSettlingFirm "
            "deltaNeutralClearingAccount deltaNeutralClearingIntent "
            "deltaNeutralOpenClose deltaNeutralShortSale "
            "deltaNeutralShortSaleSlot deltaNeutralDesignatedLocation"
        ))
    f.into(order, (
        "continuousUpdate referencePriceType trailStopPrice trailingPercent "
        "scaleInitLevelSize scaleSubsLevelSize scalePriceIncrement"
    ))
    if 0 < order.scalePriceIncrement < UNSET_DOUBLE:
        f.into(order, (
            "scalePriceAdjustValue scalePriceAdjustInterval scaleProfitOffset "
            "scaleAutoReset scaleInitPosition scaleInitFillQty scaleRandomPercent"
        ))
    f.into(order, "scaleTable activeStartTime activeStopTime hedgeType")
    if order.hedgeType:
        f.into(order, "hedgeParam")
    f.into(order, "optOutSmartRouting clearingAccount clearingIntent notHeld")
    f.hedge(contract)
    f.into(order, "algoStrategy")
    if order.algoStrategy:
        order.algoParams = [TagValue(next(f), next(f)) for _ in range(f.int())]
    f.into(order, "algoId whatIf")
    order.orderMiscOptions = f.tags()
    f.into(order, "solicited randomizeSize randomizePrice")
    if order.orderType in {"PEG BENCH", "PEGBENCH"}:
        f.into(order, (
            "referenceContractId isPeggedChangeAmountDecrease "
            "peggedChangeAmount referenceChangeAmount referenceExchangeId"
        ))
    for _ in range(f.int()):
        condition = OrderCondition.createClass(f.int())()
        order.conditions.append(f.into(condition, " ".join(
            field.name for field in dataclasses.fields(condition)[1:]
        )))
    if order.conditions:
        f.into(order, "conditionsIgnoreRth conditionsCancelOrder")
    f.into(order, (
        "adjustedOrderType triggerPrice lmtPriceOffset adjustedStopPrice "
        "adjustedStopLimitPrice adjustedTrailingAmount adjustableTrailingUnit "
        "extOperator"
    ))
    f.into(order.softDollarTier, "name val")
    f.into(order, (
        "cashQty mifid2DecisionMaker mifid2DecisionAlgo mifid2ExecutionTrader "
        "mifid2ExecutionAlgo dontUseAutoPriceForHedge isOmsContainer "
        "discretionaryUpToLimitPrice usePriceMgmtAlgo duration postToAts "
        "autoCancelParent advancedErrorOverride manualOrderTime"
    ))
    if contract.exchange == "IBKRATS":
        f.into(order, "minTradeQty")
    if order.orderType in {"PEG BEST", "PEGBEST"}:
        f.into(order, "minCompeteSize competeAgainstBestOffset")
        if order.competeAgainstBestOffset == math.inf:
            f.into(order, "midOffsetAtWhole midOffsetAtHalf")
    elif order.orderType in {"PEG MID", "PEGMID"}:
        f.into(order, "midOffsetAtWhole midOffsetAtHalf")
    return orderId, contract, order


def _contract_details(f):
    reqId, contract = f.id(), f.contract()
    return reqId, f.into(contract, "includeExpired secIdType secId issuerId")


def _historical_data(f):
    reqId, contract = f.id(), f.contract()
    f.into(contract, "includeExpired")
    end, barSize, duration, useRTH, what, formatDate = (
        next(f), next(f), next(f), f.bool(), next(f), f.int(),
    )
    f.legs(contract)
    return reqId, contract, end, duration, barSize, what, useRTH, formatDate, f.bool(), f.tags()


def _exercise_options(f):
    reqId = f.id()
    contract = f.contract(
        "conId symbol secType lastTradeDateOrContractMonth strike right "
        "multiplier exchange currency localSymbol tradingClass"
    )
    return reqId, contract, f.int(), f.int(), next(f), f.int()


def _scanner_subscription(f):
    reqId = f.id()
    subscription = f.into(ScannerSubscription(), " ".join(
        field.name for field in dataclasses.fields(ScannerSubscription)
    ))
    filterOptions = f.tags()
    return reqId, subscription, f.tags(), filterOptions


def _fundamental_data(f):
    reqId = f.id()
    contract = f.contract(
        "conId symbol secType exchange primaryExchange currency localSymbol"
    )
    reportType = next(f)
    f.int()  # how many options follow, which the next field states itself
    return reqId, contract, reportType, f.tags()


def _calculation(f):
    reqId, contract, price, underPrice = f.id(), f.contract(), f.float(), f.float()
    f.int()  # how many options follow, which the next field states itself
    return reqId, contract, price, underPrice, f.tags()


def _head_time_stamp(f):
    reqId, contract = f.id(), f.contract()
    f.into(contract, "includeExpired")
    useRTH, what = f.bool(), next(f)
    return reqId, contract, what, useRTH, f.int()


def _histogram_data(f):
    reqId, contract = f.id(), f.contract()
    f.into(contract, "includeExpired")
    return reqId, contract, f.bool(), next(f)


def _historical_ticks(f):
    reqId, contract = f.id(), f.contract()
    f.into(contract, "includeExpired")
    return (
        reqId, contract, next(f), next(f), f.int(), next(f), f.bool(), f.bool(),
        f.tags(),
    )


def _replace_fa(f):
    faData, cxml = f.int(), next(f)
    return f.id(), faData, cxml


def _start_api(f):
    """The client's id and capabilities, which their `startApi` states from
    the client itself: it takes no arguments, and this client holds both
    from its connect. A gateway reads the client's id as the request's."""
    f.id(), next(f)
    return ()


def _versioned(read):
    """A message whose second field is its version, which states nothing
    ib_async's client does not already know."""

    def reading(f):
        next(f)
        return read(f)

    return reading


def _scalars(kinds, versioned=True):
    """A message of plain fields, read in order: r the request's own number,
    i an int, f a float, s a string, b a bool, t a list of tags."""

    def read(f):
        read_one = {"r": f.id, "i": f.int, "f": f.float, "s": lambda: next(f),
                    "b": f.bool, "t": f.tags}
        return tuple([read_one[kind]() for kind in kinds])

    return _versioned(read) if versioned else read


#: Each message ib_async's client writes, by its first field: the request that
#: wrote it, and how to read the rest back into that request's arguments.
REQUESTS = {
    1: ("reqMktData", _versioned(_mkt_data)),
    2: ("cancelMktData", _scalars("r")),
    3: ("placeOrder", _place_order),
    4: ("cancelOrder", _scalars("rs")),
    5: ("reqOpenOrders", _scalars("")),
    6: ("reqAccountUpdates", _scalars("bs")),
    7: ("reqExecutions", _versioned(lambda f: (f.id(), f.into(ExecutionFilter(), (
        "clientId acctCode time symbol secType exchange side"
    ))))),
    8: ("reqIds", _scalars("i")),
    9: ("reqContractDetails", _versioned(_contract_details)),
    10: ("reqMktDepth", _versioned(lambda f: (f.id(), f.contract(), f.int(), f.bool(), f.tags()))),
    11: ("cancelMktDepth", _scalars("rb")),
    12: ("reqNewsBulletins", _scalars("b")),
    13: ("cancelNewsBulletins", _scalars("")),
    14: ("setServerLogLevel", _scalars("i")),
    15: ("reqAutoOpenOrders", _scalars("b")),
    16: ("reqAllOpenOrders", _scalars("")),
    17: ("reqManagedAccts", _scalars("")),
    18: ("requestFA", _scalars("i")),
    19: ("replaceFA", _versioned(_replace_fa)),
    20: ("reqHistoricalData", _historical_data),
    21: ("exerciseOptions", _versioned(_exercise_options)),
    22: ("reqScannerSubscription", _scanner_subscription),
    23: ("cancelScannerSubscription", _scalars("r")),
    24: ("reqScannerParameters", _scalars("")),
    25: ("cancelHistoricalData", _scalars("r")),
    49: ("reqCurrentTime", _scalars("")),
    50: ("reqRealTimeBars", _versioned(lambda f: (
        f.id(), f.contract(), f.int(), next(f), f.bool(), f.tags(),
    ))),
    51: ("cancelRealTimeBars", _scalars("r")),
    52: ("reqFundamentalData", _versioned(_fundamental_data)),
    53: ("cancelFundamentalData", _scalars("r")),
    54: ("calculateImpliedVolatility", _versioned(_calculation)),
    55: ("calculateOptionPrice", _versioned(_calculation)),
    56: ("cancelCalculateImpliedVolatility", _scalars("r")),
    57: ("cancelCalculateOptionPrice", _scalars("r")),
    58: ("reqGlobalCancel", _scalars("")),
    59: ("reqMarketDataType", _scalars("i")),
    61: ("reqPositions", _scalars("")),
    62: ("reqAccountSummary", _scalars("rss")),
    63: ("cancelAccountSummary", _scalars("r")),
    64: ("cancelPositions", _scalars("")),
    65: ("verifyRequest", _scalars("ss")),
    66: ("verifyMessage", _scalars("s")),
    67: ("queryDisplayGroups", _scalars("r")),
    68: ("subscribeToGroupEvents", _scalars("ri")),
    69: ("updateDisplayGroup", _scalars("rs")),
    70: ("unsubscribeFromGroupEvents", _scalars("r")),
    71: ("startApi", _versioned(_start_api)),
    72: ("verifyAndAuthRequest", _scalars("sss")),
    73: ("verifyAndAuthMessage", _scalars("ss")),
    74: ("reqPositionsMulti", _scalars("rss")),
    75: ("cancelPositionsMulti", _scalars("r")),
    76: ("reqAccountUpdatesMulti", _scalars("rssb")),
    77: ("cancelAccountUpdatesMulti", _scalars("r")),
    78: ("reqSecDefOptParams", _scalars("rsssi", versioned=False)),
    79: ("reqSoftDollarTiers", _scalars("r", versioned=False)),
    80: ("reqFamilyCodes", _scalars("", versioned=False)),
    81: ("reqMatchingSymbols", _scalars("rs", versioned=False)),
    82: ("reqMktDepthExchanges", _scalars("", versioned=False)),
    83: ("reqSmartComponents", _scalars("rs", versioned=False)),
    84: ("reqNewsArticle", _scalars("rsst", versioned=False)),
    85: ("reqNewsProviders", _scalars("", versioned=False)),
    86: ("reqHistoricalNews", _scalars("risssit", versioned=False)),
    87: ("reqHeadTimeStamp", _head_time_stamp),
    88: ("reqHistogramData", _histogram_data),
    89: ("cancelHistogramData", _scalars("r", versioned=False)),
    90: ("cancelHeadTimeStamp", _scalars("r", versioned=False)),
    91: ("reqMarketRule", _scalars("i", versioned=False)),
    92: ("reqPnL", _scalars("rss", versioned=False)),
    93: ("cancelPnL", _scalars("r", versioned=False)),
    94: ("reqPnLSingle", _scalars("rssi", versioned=False)),
    95: ("cancelPnLSingle", _scalars("r", versioned=False)),
    96: ("reqHistoricalTicks", _historical_ticks),
    97: ("reqTickByTickData", lambda f: (f.id(), f.contract(), next(f), f.int(), f.bool())),
    98: ("cancelTickByTickData", _scalars("r", versioned=False)),
    99: ("reqCompletedOrders", _scalars("b", versioned=False)),
    100: ("reqWshMetaData", _scalars("r", versioned=False)),
    101: ("cancelWshMetaData", _scalars("r", versioned=False)),
    102: ("reqWshEventData", lambda f: (f.id(), f.into(WshEventData(), (
        "conId filter fillWatchlist fillPortfolio fillCompetitors startDate "
        "endDate totalLimit"
    )))),
    103: ("cancelWshEventData", _scalars("r", versioned=False)),
    104: ("reqUserInfo", _scalars("r", versioned=False)),
}


def read(msg):
    """The request ib_async's client wrote as ``msg``, and its arguments; None
    for a message whose first field names no request their client writes.

    Raises Unreadable for a message that does not read as the request it
    names, to the last field.
    """
    fields = msg.split("\0")
    if fields[-1] == "":
        # The separator ib_async's client writes after every field.
        fields.pop()
    try:
        request, reading = REQUESTS[int(fields[0])]
    except (IndexError, KeyError, ValueError):
        return None
    f = _Fields(fields[1:])
    try:
        args = reading(f)
    except StopIteration:
        raise Unreadable(f"the message {msg!r} ends before {request} does", f.reqId) from None
    except (KeyError, ValueError, TypeError) as why:
        raise Unreadable(
            f"the message {msg!r} does not read as {request}: {why!r}", f.reqId,
        ) from why
    if not f.done():
        raise Unreadable(f"the message {msg!r} has fields past the end of {request}", f.reqId)
    return request, args

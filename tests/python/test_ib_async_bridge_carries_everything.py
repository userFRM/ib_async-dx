"""What crosses the bridge to `ib_async` crosses whole.

A request names a contract and a callback carries a record. Both are rebuilt on
the way over, and both were being rebuilt short: the request paths copied a
fixed list of fields, so a combination arrived with no legs, and the one
callback renamed on the way through skipped the rebuild entirely, so a fill's
cost arrived as a type their wrapper cannot read.
"""

import ib_async

import ibkr_dx
from ib_async_dx.bridge import IbkrDxClient, _LoopBound


def _combo():
    c = ib_async.Contract(secType="BAG", symbol="SPY", exchange="SMART", currency="USD")
    c.comboLegs = [
        ib_async.ComboLeg(conId=1, ratio=1, action="BUY", exchange="SMART"),
        ib_async.ComboLeg(conId=2, ratio=2, action="SELL", exchange="SMART"),
    ]
    return c


class Sent:
    """Stands in for the client, keeping the contract each request was given."""

    def __init__(self):
        self.contracts = []

    def _keep(self, contract):
        self.contracts.append(contract)

    def req_contract_details(self, req_id, contract):
        self._keep(contract)

    def req_mkt_data(self, req_id, contract, *a):
        self._keep(contract)

    def req_historical_data(self, req_id, contract, *a):
        self._keep(contract)


def _client():
    ib = ib_async.IB()
    c = IbkrDxClient(ib.wrapper)
    c.connState = c.CONNECTED
    c._client = Sent()
    return c


def test_a_combination_keeps_its_legs_on_every_request_path():
    c = _client()
    combo = _combo()
    c.reqContractDetails(1, combo)
    c.reqMktData(2, combo, "", False, False, None)
    c.reqHistoricalData(3, combo, "", "1 D", "1 min", "TRADES", True, 1, False, None)

    assert len(c._client.contracts) == 3
    for sent in c._client.contracts:
        legs = sent.comboLegs
        assert [leg.conId for leg in legs] == [1, 2], f"the legs were dropped: {legs}"
        assert [leg.ratio for leg in legs] == [1, 2]


def test_a_contract_named_by_more_than_its_symbol_keeps_the_rest():
    c = _client()
    contract = ib_async.Contract(secType="STK", symbol="SPY", exchange="SMART", currency="USD")
    contract.secIdType = "ISIN"
    contract.secId = "US78462F1030"
    contract.includeExpired = True
    c.reqContractDetails(1, contract)

    sent = c._client.contracts[0]
    assert sent.secIdType == "ISIN" and sent.secId == "US78462F1030"
    assert sent.includeExpired is True


def test_a_fills_cost_arrives_as_the_record_their_wrapper_reads():
    """The callback is renamed on the way over, and was handed the argument
    unrebuilt. Their wrapper reads a field their own record spells its own
    way, so every fill lost what it cost."""
    seen = []

    class Wrapper:
        def commissionReport(self, report):
            seen.append(report)

    bound = _LoopBound(Wrapper())
    ours = ibkr_dx.CommissionAndFeesReport()
    ours.execId = "0001.1"
    ours.commissionAndFees = 1.25
    ours.currency = "USD"
    bound.commission_and_fees_report(ours)

    assert seen, "the cost reached nothing"
    assert isinstance(seen[0], ib_async.CommissionReport), type(seen[0])
    assert seen[0].commission == 1.25
    assert seen[0].execId == "0001.1"


def test_every_account_the_login_holds_crosses_over():
    """The default account read off the client is the first one. Used as the
    whole list, an advisor with several saw one standing for all of them."""
    ib = ib_async.IB()
    c = IbkrDxClient(ib.wrapper)

    class Several:
        def connect(self, **kwargs):
            pass

        def get_account_id(self):
            return "DU1"

        def req_managed_accts(self):
            c._callbacks.managed_accounts("DU1,DU2,DU3")

        def poll(self):
            pass

        def next_order_id(self):
            return 1

        def next_shared_id(self):
            # What a caller numbering its orders and its requests out of one
            # counter starts from. The real client answers both.
            return 1

    c._client = Several()

    import asyncio
    asyncio.run(c.connectAsync("", 0, 1))

    assert ib.managedAccounts() == ["DU1", "DU2", "DU3"], ib.managedAccounts()
    assert c.getAccounts() == ["DU1", "DU2", "DU3"], c.getAccounts()


def test_a_size_with_no_price_beside_it_is_a_size_tick():
    """A size that changed while its price did not is what a gateway sends
    on its own, and their decoder hands that over as `tickSize`. Sent as a
    `priceSizeTick`, it stated a price nobody had quoted."""
    seen = []

    class Wrapper:
        def tickSize(self, reqId, tickType, size):
            seen.append(("tickSize", tickType, size))

        def priceSizeTick(self, reqId, tickType, price, size):
            seen.append(("priceSizeTick", tickType, price, size))

    bound = _LoopBound(Wrapper())
    bound.tick_size(1, 0, 400.0)
    bound.end_pass()
    assert seen == [("tickSize", 0, 400.0)], seen


def test_a_bar_carries_its_average_price():
    """The engine names it as the reference client does, `wap`; their bar
    reads `average`. Unmapped, every bar arrived with an average of nought."""
    from ib_async_dx.bridge import _as_theirs

    bar = ibkr_dx.BarData()
    bar.date, bar.close, bar.wap, bar.barCount = "20260918", 101.0, 100.75, 12
    theirs = _as_theirs(bar)
    assert isinstance(theirs, ib_async.BarData)
    assert (theirs.close, theirs.average, theirs.barCount) == (101.0, 100.75, 12)


def test_a_condition_arrives_saying_how_it_joins_the_next():
    """The engine says whether the join is an and; their condition says "a"
    or "o". Unmapped, an or arrived as their default, an and."""
    from ib_async_dx.bridge import _as_theirs

    joined = ibkr_dx.PriceCondition()
    joined.isConjunctionConnection = False
    assert _as_theirs(joined).conjunction == "o"
    joined.isConjunctionConnection = True
    assert _as_theirs(joined).conjunction == "a"


def test_a_record_theirs_builds_whole_arrives_whole():
    """Their family code and routing component take every field when they are
    made, and cannot be changed after. Made empty and filled one field at a
    time, neither could be made at all."""
    from ib_async_dx.bridge import _as_theirs

    code = ibkr_dx.FamilyCode()
    code.accountID, code.familyCodeStr = "DU000000", "F1"
    assert _as_theirs(code) == ib_async.FamilyCode("DU000000", "F1")

    component = ibkr_dx.SmartComponent()
    component.bitNumber, component.exchange, component.exchangeLetter = 4, "ARCA", "P"
    assert _as_theirs(component) == ib_async.SmartComponent(4, "ARCA", "P")


def test_a_callback_that_cannot_be_rebuilt_is_logged_and_passed_over(caplog):
    """As their decoder treats a message it cannot handle: logged, and the
    session carries on. Swallowed, a field arrived at its default and nothing
    said so; raised, it closed the session."""
    import logging

    class FamilyCode:
        """Named as the engine's, and short of a field theirs requires."""

        accountID = "DU000000"

    seen = []

    class Wrapper:
        def familyCodes(self, codes):
            seen.append(codes)

    with caplog.at_level(logging.ERROR, logger="ib_async_dx"):
        _LoopBound(Wrapper()).family_codes([FamilyCode()])
    assert seen == []
    assert caplog.records[-1].name == "ib_async_dx.bridge"
    assert "family_codes" in caplog.records[-1].getMessage()


def test_a_record_tuple_is_rebuilt_field_by_field():
    """Their ticks are named tuples. Rebuilt as a sequence, one of several
    fields was handed the whole record and the callback was lost."""
    import datetime

    from ib_async_dx.bridge import _as_theirs

    tick = ib_async.TickData(datetime.datetime(2026, 9, 24), 1, 100.25, 300.0)
    assert _as_theirs(tick) == tick


def test_a_record_named_as_theirs_and_not_theirs_is_rebuilt_as_theirs():
    """Handed over because it was a dataclass, a record of another type
    reached their wrapper under their record's name."""
    import dataclasses

    from ib_async_dx.bridge import _as_theirs

    @dataclasses.dataclass
    class FamilyCode:
        accountID: str = "DU000000"
        familyCodeStr: str = "F1"

    assert _as_theirs(FamilyCode()) == ib_async.FamilyCode("DU000000", "F1")
    theirs = ib_async.FamilyCode("DU000000", "F1")
    assert _as_theirs(theirs) is theirs, "their own record is handed over as it is"

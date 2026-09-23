"""Tests for ibapi-compatible class construction, fields, and subclassing."""


def test_an_order_the_transport_refused_is_not_left_working():
    """A refused order does not stay in `openTrades()`.

    A transport that will not carry the order says so on the error callback
    and returns, which is how the reference client answers a request it
    refuses. The record was written before the call and never taken back, so
    an order the venue never received read as one working at it.
    """
    import ibkr_dx
    import ib_async_dx

    class Refusing:
        """Answers a place the way a transport that cannot send does."""

        def __init__(self, wrapper):
            self.wrapper = wrapper

        def next_order_id(self):
            return 11

        def place_order(self, order_id, contract, order):
            self.wrapper.error(order_id, 504, "not connected", "")

    ib = ib_async_dx.IB()
    ib.client = Refusing(ib.wrapper)

    contract = ibkr_dx.Contract(symbol="SPY", secType="STK", exchange="SMART", currency="USD")
    order = ibkr_dx.Order(action="BUY", totalQuantity=1, orderType="MKT")
    trade = ib.placeOrder(contract, order)

    assert trade is not None, "the caller still gets the object to read"
    assert ib.wrapper.trade_for(11) is None, "and it is not a working order"
    assert all(t.order.orderId != 11 for t in ib.openTrades())


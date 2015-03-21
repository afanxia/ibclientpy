"""Offline testing client providing a subset of the functionality available in
the ibclientpy.client.Client class. Calls to the broker are replaced with
locally handled simulated results.

"""
from asyncio import Future
import asyncio
import logging
import ibapipy.data.execution as ibe
import ibclientpy.client
import ibclientpy.commissions as comms
import ibclientpy.config as config


LOG = logging.getLogger(__name__)


class OfflineClient(ibclientpy.client.Client):
    """Offline testing client providing a subset of the functionality available
    in the ibclientpy.client.Client class.

    Attributes not specified in the constructor:
    contract      -- contract associated with the offline ticks
    offline_ticks -- queue of ticks used to provide pricing data
    orders        -- dictionary of orders by request ID
    is_updating   -- True if ticks are being sent; False, otherwise

    """

    def __init__(self, contract):
        """Initialize a new instance of an OfflineClient."""
        ibclientpy.client.Client.__init__(self)
        self.next_id = 1
        self.contract = contract
        self.offline_ticks = asyncio.Queue()
        self.orders = {}
        self.is_updating = False

    # *************************************************************************
    # Internal Methods
    # *************************************************************************

    def __handle_orders__(self, tick):
        """Process orders based on the specified incoming tick.

        Keyword arguments:
        tick -- ibapipy.data.tick.Tick object

        """
        need_filled, need_cancelled, need_updated = [], [], []
        # Check for orders to fill
        for order_id in self.orders:
            order = self.orders[order_id]
            can_fill, price = check_order(order, tick)
            if can_fill:
                need_filled.append((order, price))
        # Fill orders
        for order, price in need_filled:
            cancel_id = fill_order(order, self.contract, price,
                                   tick.milliseconds, self.oca_relations)
            if cancel_id >= 0 and cancel_id in self.orders:
                self.orders[cancel_id].status = 'cancelled'

    # *************************************************************************
    # Connection
    # *************************************************************************

    @asyncio.coroutine
    def connect(self, host=config.HOST, port=config.PORT,
                client_id=config.CLIENT_ID):
        """Connect to the remote TWS.

        Keyword arguments:
        host      -- host name or IP address of the TWS machine
        port      -- port number on the TWS machine
        client_id -- number used to identify this client connection

        """
        pass

    @asyncio.coroutine
    def disconnect(self):
        """Disconnect from the remote TWS."""
        pass

    @asyncio.coroutine
    def is_connected(self):
        """Return True if the Client is connected; False, otherwise."""
        return False

    # *************************************************************************
    # Accounts
    # *************************************************************************

    @asyncio.coroutine
    def get_account_name(self):
        """Return the account name associated with this session as a string.

        """
        pass

    @asyncio.coroutine
    def get_account(self, account_name):
        """Return the ibapipy.data.account.Account instance associated with the
        specified account name.

        Keyword arguments:
        account_name -- account name as a string

        """
        pass

    # *************************************************************************
    # Contracts
    # *************************************************************************

    @asyncio.coroutine
    def get_contract(self, contract):
        """Return a fully populated ibapipy.data.contract.Contract instance
        from the specified "basic" Contract. The specified contract should
        be populated with the 'sec_type', 'symbol', 'currency', and 'exchange'
        attributes.

        Keyword arguments:
        contract -- basic ibapipy.data.contract.Contract

        """
        pass

    # *************************************************************************
    # Orders
    # *************************************************************************

    @asyncio.coroutine
    def get_orders(self):
        """Return a tuple of ibapipy.data.order.Order instances with additional
        attributes on each order. Each order has a 'contract' field and
        'executions' field added to it where the contract is the underlying
        contract being traded and 'executions' is a list of executions
        associated with the order.

        """
        return tuple(self.orders.values())

    @asyncio.coroutine
    def place_order(self, contract, order, profit_offset=0, loss_offset=0):
        """Place an order for the specified contract. If profit offset or loss
        offset is non-zero, a corresponding order will be placed after the
        parent order has been filled.

        The sign of the profit/loss offsets does not matter. Profit targets
        will always be placed above the entry price for long positions and
        below the entry price for short positions. Loss targets will always be
        placed below the entry price for long positions and above the entry
        price for short positions.

        Keyword arguments:
        contract      -- ibapipy.data.contract.Contract object
        parent        -- ibapipy.data.order.Order object
        profit_offset -- profit target offset from parent's fill price
                         (default: 0)
        loss_offset   -- loss target offset from parent's fill price
                         (default: 0)

        """
        if order.order_id in self.orders:
            req_id = order.order_id
        else:
            req_id = self.next_id
            self.next_id += 1
            self.id_contracts[req_id] = contract
            order.order_id = req_id
            order.perm_id = req_id
        self.bracket_parms[req_id] = (profit_offset, loss_offset)
        order.contract = contract
        order.executions = []
        self.orders[req_id] = order
        return req_id

    @asyncio.coroutine
    def cancel_order(self, req_id):
        """Cancel the order associated with the specified request ID.

        Keyword arguments:
        req_id -- request ID

        """
        if req_id in self.orders:
            order = self.orders[req_id]
            order.status = 'cancelled'

    # *************************************************************************
    # Pricing
    # *************************************************************************

    @asyncio.coroutine
    def get_history(self, contract, start, end, timezone):
        """Return a generator containing tuples of the form (int, list) where
        'int' is the number of historical ticks remaining the the request and
        'list' is a list of historical ticks being returned as part of the
        intermediate result.

        There will be intermittent delays in the generated data as needed to
        prevent IB pacing violations.

        Keyword arguments:
        contract   -- ibapipy.data.contract.Contract object
        start_date -- start date in 'yyyy-mm-dd hh:mm' format
        end_date   -- end date in 'yyyy-mm-dd hh:mm' format
        timezone   -- timezone in 'Country/Region' format

        """
        pass

    @asyncio.coroutine
    def get_ticks(self, results, contract):
        """Populate the specified results queue with realtime ticks. Ticks will
        be put into the queue until cancel_ticks() is called at which point
        None will be put in the queue to signify the end of data.

        Keyword arguments:
        contract -- ibapipy.data.contract.Contract object

        """
        req_id = self.next_id
        self.next_id += 1
        self.id_contracts[req_id] = contract
        self.is_updating = True
        while self.is_updating and not self.offline_ticks.empty():
            tick = yield from self.offline_ticks.get()
            yield from results.put(tick)
        yield from results.put(None)
        self.is_updating = False

    @asyncio.coroutine
    def cancel_ticks(self):
        """Stop receiving ticks from the get_ticks() method."""
        self.is_updating = False


def create_execution(order, milliseconds):
    """Create an execution to fill the specified order. To avoid dealing with
    time formatting, the execution.time field will be blank, although
    execution.milliseconds will contain the time in milliseconds.

    Keyword arguments:
    order        -- Order object
    milliseconds -- "fill" time in milliseconds

    """
    execution = ibe.Execution()
    execution.order_id = order.order_id
    execution.exec_id = order.order_id
    execution.milliseconds = milliseconds
    execution.side = 'bot' if order.action == 'buy' else 'sld'
    execution.shares = order.total_quantity
    execution.price = order.avg_fill_price
    execution.perm_id = order.order_id
    execution.cum_qty = order.total_quantity
    execution.avg_price = order.avg_fill_price
    return execution


def check_order(order, tick):
    """Check the specified order to see if it should be filled and return True
    if it can be filled (or False, otherwise) as well as the price at which the
    order should be filled.

    Keyword arguments:
    order -- ibapipy.data.order.Order object
    tick  -- ibapipy.data.tick.Tick representing the current price

    """
    if order.status == 'filled' or order.status == 'cancelled':
        return False, 0
    result = False
    price = tick.ask if order.action == 'buy' else tick.bid
    order.status = 'submitted'
    # Market order
    if order.order_type == 'mkt':
        result = True
    # Limit order
    elif order.order_type == 'lmt':
        lmt_price = order.lmt_price
        buy_ok = order.action == 'buy' and price <= lmt_price
        sell_ok = order.action == 'sell' and price >= lmt_price
        if buy_ok or sell_ok:
            result = True
    # Stop order
    elif order.order_type == 'stp':
        aux_price = order.aux_price
        buy_ok = order.action == 'buy' and price >= aux_price
        sell_ok = order.action == 'sell' and price <= aux_price
        if buy_ok or sell_ok:
            result = True
    return result, price


def fill_order(order, contract, price, milliseconds, oca_relations):
    """Fill the specified order and return an order ID >= 0 if there is another
    OCA order that should be cancelled. Otherwise, return -1.

    Keyword arguments:
    order         -- ibapipy.data.order.Order object to fill
    local_symbol  -- ticker symbol
    price         -- fill price
    milliseconds  -- fill time in milliseconds since the Epoch
    oca_relations -- dictionary of OCA relationships

    """
    # Fill the order
    order.status = 'filled'
    order.avg_fill_price = price
    order.commission = comms.est_comm(contract, price, order.total_quantity)
    order.executions.append(create_execution(order, milliseconds))
    # Try to find the other order in the OCA group
    oca_id = -1
    if order.oca_group != '' and order.oca_group in oca_relations:
        parent_key = order.oca_group
        profit_id, loss_id = oca_relations[parent_key]
        if order.order_id == profit_id:
            oca_id = loss_id
        else:
            oca_id = profit_id
    return oca_id

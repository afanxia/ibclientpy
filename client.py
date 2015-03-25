"""Encapsulates the ibapipy.client_socket.ClientSocket class and provides an
[ideally] simpler API for interacting with TWS.

"""
from asyncio import Future
import asyncio
import logging
import time
import ibapipy.data.contract as ibc
import ibapipy.data.execution_filter as ibef
import ibclientpy.client_adapter as ibca
import ibclientpy.historical_data as ibhd
import ibclientpy.order_handler as iboh
import ibclientpy.config as config


LOG = logging.getLogger(__name__)


class Client:
    """Simplified interface to an ibapipy.client_socket.ClientSocket.

    Attributes not specified in the constructor:
    adapter       -- ibclientpy.client_adapter.ClientAdapter object that
                     provides access to the ibapipy ClientSocket
    next_id       -- next available request ID
    id_contracts  -- dictionary of contracts by request ID

    """

    def __init__(self, loop=None):
        """Initialize a new instance of a Client.

        Keyword arguments:
        loop -- asyncio event loop

        """
        self.loop = loop
        self.adapter = ibca.ClientAdapter(self, loop)
        self.next_id = -1
        self.id_contracts = {}
        self.order_handler = iboh.OrderHandler(self)
        self.history_pending = []

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
        if self.is_connected():
            return
        yield from self.adapter.connect(host, port, client_id)
        # Automatically associate newly opened TWS orders with this client
        if client_id == 0:
            yield from self.adapter.req_auto_open_orders(True)
        # Wait for the next ID to get updated
        self.next_id = yield from self.get_next_valid_id()

    @asyncio.coroutine
    def disconnect(self):
        """Disconnect from the remote TWS."""
        if self.is_connected():
            yield from self.adapter.disconnect()
            self.next_id = -1

    @asyncio.coroutine
    def get_next_valid_id(self):
        """Return the next valid request ID that can be used for orders."""
        future = yield from self.adapter.req_ids(1)
        yield from future
        return future.result()

    def is_connected(self):
        """Return True if the Client is connected; False, otherwise."""
        return self.adapter.is_connected

    # *************************************************************************
    # Accounts
    # *************************************************************************

    @asyncio.coroutine
    def get_account_name(self):
        """Return the account name associated with this session as a string.

        """
        future = yield from self.adapter.req_managed_accts()
        yield from future
        return future.result()

    @asyncio.coroutine
    def get_account(self):
        """Return the ibapipy.data.account.Account instance associated with
        this session.

        """
        account_name = yield from self.get_account_name()
        future = yield from self.adapter.req_account_updates(account_name)
        yield from future
        return future.result()

    # *************************************************************************
    # Contracts
    # *************************************************************************

    @asyncio.coroutine
    def get_contract(self, contract):
        """Return a fully populated ibapipy.data.contract.Contract instance
        from the specified Contract. The specified contract should have the
        'sec_type', 'symbol', 'currency', and 'exchange' attributes populated.

        Keyword arguments:
        contract -- ibapipy.data.contract.Contract instance

        """
        req_id = self.next_id
        self.next_id += 1
        basic_contract = get_basic_contract(contract)
        future = yield from self.adapter.req_contract_details(req_id,
                                                              basic_contract)
        yield from future
        return future.result()

    # *************************************************************************
    # Errors
    # *************************************************************************

    def on_error(self, req_id, code, message):
        """Called by the ClientAdapter when errors are reported. This
        encapsulates the following methods from the TWS API:
            - error

        Keyword arguments:
        req_id  -- request ID
        code    -- error code
        message -- error message

        """
        LOG.info('Client error/message: {0}; {1}; {2}'.format(req_id, code,
                                                              message))

    # *************************************************************************
    # Holdings
    # *************************************************************************

    def on_holding(self, contract, holding):
        """Called by the ClientAdapter when holding data is updated. This
        encapsulates the following methods from the TWS API:
            - update_portfolio

        Keyword arguments:
        contract -- ibapipy.data.contract.Contract object
        holding  -- ibapipy.data.holding.Holding object

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
        self.adapter.executions_fut = Future()
        self.adapter.orders_fut = Future()
        req_id = self.next_id
        self.next_id += 1
        yield from self.adapter.req_executions(req_id, ibef.ExecutionFilter())
        yield from self.adapter.req_all_open_orders()
        yield from self.adapter.executions_fut
        yield from self.adapter.orders_fut
        return self.adapter.orders_fut.result()

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
        req_id = self.next_id
        self.next_id += 1
        self.id_contracts[req_id] = contract
        order.order_id = req_id
        self.order_handler.add_order(order, profit_offset, loss_offset)
        yield from self.adapter.place_order(req_id, contract, order)
        return req_id

    @asyncio.coroutine
    def cancel_order(self, req_id):
        """Cancel the order associated with the specified request ID.

        Keyword arguments:
        req_id -- request ID

        """
        self.adapter.order_cancel_fut = Future()
        yield from self.adapter.cancel_order(req_id)
        yield from self.adapter.order_cancel_fut

    # *************************************************************************
    # Pricing
    # *************************************************************************

    @asyncio.coroutine
    def get_next_history_block(self, contract, start, end, timezone):
        """Return the next available block of historical ticks for the
        specified contract and time period. The result will be of the form
        (int, tuple) where "int" is the number of blocks remaining to be filled
        and "tuple" is a list of historical ticks in the current block of
        prices.

        If no more historical blocks are available, (0, None) will be
        returned.

        There will be intermittent delays in the generated data as needed to
        prevent IB pacing violations.

        Keyword arguments:
        contract   -- ibapipy.data.contract.Contract object
        start_date -- start date in 'yyyy-mm-dd hh:mm' format
        end_date   -- end date in 'yyyy-mm-dd hh:mm' format
        timezone   -- timezone in 'Country/Region' format

        """
        # Grab from the queue if we have data waiting
        if self.adapter.history_queue.qsize() > 0:
            ticks = yield from self.adapter.history_queue.get()
            return len(self.history_pending), ticks
        # Build the initial request if needed
        if len(self.history_pending) == 0:
            blocks = ibhd.get_parameters(contract, start, end, timezone)
            self.history_pending.extend(blocks)
        # Retrieve the history for the next set of parameters
        parms = self.history_pending.pop(0)
        yield from asyncio.sleep(parms['delay'])
        req_id = self.next_id
        self.next_id += 1
        self.id_contracts[req_id] = parms['contract']
        yield from self.adapter.req_historical_data(
            req_id, get_basic_contract(parms['contract']),
            parms['end_date_time'], parms['duration_str'],
            parms['bar_size_setting'], parms['what_to_show'],
            parms['use_rth'], parms['format_date'])
        ticks = yield from self.adapter.history_queue.get()
        # Check if we're done with the overall request
        if len(self.history_pending) == 0:
            yield from self.adapter.history_queue.put(None)
        return len(self.history_pending), ticks

    @asyncio.coroutine
    def cancel_history(self):
        """Stop receiving ticks from the get_next_history_block() method."""
        self.history_pending.clear()
        while self.adapter.history_queue.qsize() > 0:
            yield from self.adapter.history_queue.get()
        yield from self.adapter.history_queue.put(None)

    @asyncio.coroutine
    def get_next_tick(self, contract):
        """Return the next available realtime tick for the specified contract.
        If no more ticks are available (e.g. cancel_ticks() has been called),
        None will be returned.

        Keyword arguments:
        contract -- ibapipy.data.contract.Contract instance

        """
        key = '{0}.{1}'.format(contract.symbol, contract.currency)
        # Get the request ID for the active market data stream
        if key in self.adapter.market_data_ids:
            req_id = self.adapter.market_data_ids[key]
        # Create a new market data request
        else:
            req_id = self.next_id
            self.next_id += 1
            self.id_contracts[req_id] = contract
            yield from self.adapter.req_mkt_data(req_id, contract)
        # Pull from the queue
        tick = yield from self.adapter.tick_queue[req_id].get()
        # If the tick is None, we're done so remove the old request ID
        if tick is None:
            self.adapter.market_data_ids.pop(key)
        return tick

    @asyncio.coroutine
    def cancel_ticks(self, contract):
        """Stop receiving ticks from the get_next_tick() method."""
        key = '{0}.{1}'.format(contract.symbol, contract.currency)
        if key in self.adapter.market_data_ids:
            req_id = self.adapter.market_data_ids[key]
            yield from self.adapter.cancel_mkt_data(req_id)
            yield from self.adapter.tick_queue[req_id].put(None)


def get_basic_contract(contract):
    """TWS is complaining if we provide a fully populated contract. Here we
    we strip away everything but the core attributes.

    Keyword arguments:
    contract -- ibapipy.data.contract.Contract object

    """
    result = ibc.Contract()
    result.sec_type = contract.sec_type
    result.symbol = contract.symbol
    result.currency = contract.currency
    result.exchange = contract.exchange
    return result


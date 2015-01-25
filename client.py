"""Encapsulates the ibapipy.client_socket.ClientSocket class and provides an
[ideally] simpler API for interacting with TWS.

"""
from asyncio import Future
import asyncio
import logging
import time
import ibapipy.data.contract as ibc
import ibapipy.data.execution_filter as ibef
import ibapipy.data.order as ibo
import ibclientpy.client_adapter as ibca
import ibclientpy.historical_data as ibhd
import ibclientpy.config as config


LOG = logging.getLogger(__name__)


class Client:
    """Simplified interface to an ibapipy.client_socket.ClientSocket.

    Attributes not specified in the constructor:
    adapter       -- ibclientpy.client_adapter.ClientAdapter object that
                     provides access to the ibapipy ClientSocket
    next_id       -- next available request ID
    id_contracts  -- dictionary of contracts by request ID
    bracket_parms -- dictionary of (profit offset, loss offset) tuples for
                     bracket orders by parent order ID
    oca_relations -- dictionary of (profit order ID, loss order ID) tuples for
                     bracket orders by parent order ID

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
        self.bracket_parms = {}
        self.oca_relations = {}

    # *************************************************************************
    # Internal Methods
    # *************************************************************************

    def __update_brackets__(self, parent):
        """Place a take profit (limit) and stop loss (stop) around the
        specified parent order's fill price.

        Keyword arguments:
        parent -- ibapipy.data.order.Order object for the parent order

        """
        if parent.order_id not in self.bracket_parms:
            return
        if parent.order_id not in self.id_contracts:
            return
        if parent.status != 'filled' or parent.avg_fill_price == 0:
            return
        action = 'sell' if parent.action == 'buy' else 'buy'
        direction = 1 if parent.action == 'buy' else -1
        profit_offset, loss_offset = self.bracket_parms.pop(parent.order_id)
        contract = self.id_contracts[parent.order_id]
        parent_key = str(parent.perm_id)
        # Profit order
        if profit_offset != 0:
            limit_price = parent.avg_fill_price + \
                abs(profit_offset) * direction
            profit_order = ibo.Order(action, parent.total_quantity, 'lmt',
                                     lmt_price=limit_price)
            profit_order.oca_group = parent_key
            profit_order.oca_type = 2
            profit_order.tif = 'gtc'
            profit_id = self.place_order(contract, profit_order)
        else:
            profit_id = -1
        # Loss order
        if loss_offset != 0:
            stop_price = parent.avg_fill_price - abs(loss_offset) * direction
            loss_order = ibo.Order(action, parent.total_quantity, 'stp',
                                   aux_price=stop_price)
            loss_order.oca_group = parent_key
            loss_order.oca_type = 2
            loss_order.tif = 'gtc'
            loss_id = self.place_order(contract, loss_order)
        else:
            loss_id = -1
        self.oca_relations[parent_key] = (profit_id, loss_id)

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
        yield from self.adapter.connect(host, port, client_id)
        # Automatically associate newly opened TWS orders with this client
        if client_id == 0:
            yield from self.adapter.req_auto_open_orders(True)
        # Wait for the next ID to get updated
        self.next_id = yield from self.get_next_valid_id()

    @asyncio.coroutine
    def disconnect(self):
        """Disconnect from the remote TWS."""
        yield from self.adapter.disconnect()
        self.next_id = -1

    @asyncio.coroutine
    def get_next_valid_id(self):
        """Return the next valid request ID that can be used for orders."""
        self.adapter.next_valid_id_fut = Future()
        yield from self.adapter.req_ids(1)
        yield from self.adapter.next_valid_id_fut
        return self.adapter.next_valid_id_fut.result()

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
        self.adapter.managed_accounts_fut = Future()
        yield from self.adapter.req_managed_accts()
        yield from self.adapter.managed_accounts_fut
        return self.adapter.managed_accounts_fut.result()

    @asyncio.coroutine
    def get_account(self, account_name):
        """Return the ibapipy.data.account.Account instance associated with the
        specified account name.

        Keyword arguments:
        account_name -- account name as a string

        """
        self.adapter.update_account_fut = Future()
        yield from self.adapter.req_account_updates(True, account_name)
        yield from self.adapter.update_account_fut
        yield from self.adapter.req_account_updates(False, account_name)
        return self.adapter.update_account_fut.result()

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
        self.adapter.contract_fut = Future()
        req_id = self.next_id
        self.next_id += 1
        basic_contract = get_basic_contract(contract)
        yield from self.adapter.req_contract_details(req_id, basic_contract)
        yield from self.adapter.contract_fut
        return self.adapter.contract_fut.result()

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
        self.bracket_parms[req_id] = (profit_offset, loss_offset)
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
        blocks = ibhd.get_parameters(contract, start, end, timezone)
        self.adapter.historical_remaining = len(blocks) * config.MAX_BLOCK_SIZE
        for parms in blocks:
            time.sleep(parms['delay'])
            req_id = self.next_id
            self.next_id += 1
            self.id_contracts[req_id] = parms['contract']
            yield from self.adapter.req_historical_data(
                req_id, get_basic_contract(parms['contract']),
                parms['end_date_time'], parms['duration_str'],
                parms['bar_size_setting'], parms['what_to_show'],
                parms['use_rth'], parms['format_date'])

    @asyncio.coroutine
    def get_ticks(self, contract):
        """Return a generator containing realtime ticks. Ticks will be yielded
        until cancel_ticks() is called.

        Keyword arguments:
        contract -- ibapipy.data.contract.Contract object

        """
        req_id = self.next_id
        self.next_id += 1
        self.id_contracts[req_id] = contract
        ticks = self.adapter.req_mkt_data(req_id, contract)
        for tick in ticks:
            yield from tick

    @asyncio.coroutine
    def cancel_ticks(self):
        """Stop receiving ticks from the get_ticks() method."""
        self.adapter.is_receiving_ticks = False


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


def get_key(method_name, contract=None, account_name=None):
    """Return a key used in Client.callbacks and Client.callback_ids.

    Keyword arguments:
    method_name -- name of the originating method
    contract    -- ibapipy.data.contract.Contract object (default: None)
    account     -- account name (default: None)

    """
    if account_name is None:
        account_text = ''
    else:
        account_text = account_name.lower()
    if contract is None:
        contract_text = ''
    else:
        contract_text = '{0}{1}'.format(contract.symbol, contract.currency)
    return '{0}{1}{2}'.format(method_name, account_text, contract_text)

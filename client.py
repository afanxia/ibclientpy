"""Encapsulates the ibapipy.client_socket.ClientSocket class and provides an
[ideally] simpler API for interacting with TWS.

"""
import logging
import threading
import time
import ibapipy.data.contract as ibc
import ibapipy.data.execution_filter as ibef
import ibapipy.data.order as ibo
import ibclientpy.client_adapter as ibca
import ibclientpy.historical_downloader as ibhd
import ibclientpy.config as config


LOG = logging.getLogger(__name__)


class Client:
    """Simplified interface to an ibapipy.client_socket.ClientSocket.

    Attributes not specified in the constructor:
    adapter       -- ibclientpy.client_adapter.ClientAdapter object that
                     provides access to the ibapipy ClientSocket
    downloader    -- ibclientpy.historical_downloader.HistoricalDownloader
    next_id       -- next available request ID
    next_id_lock  -- lock used when updating next_id
    id_contracts  -- dictionary of contracts by request ID
    callbacks     -- dictionary of callback functions
    callback_ids  -- dictionary of request IDs for each callback function
    bracket_parms -- dictionary of (profit offset, loss offset) tuples for
                     bracket orders by parent order ID
    oca_relations -- dictionary of (profit order ID, loss order ID) tuples for
                     bracket orders by parent order ID

    """

    def __init__(self):
        """Initialize a new instance of a Client."""
        self.adapter = ibca.ClientAdapter(self)
        self.next_id = -1
        self.next_id_lock = threading.Lock()
        self.id_contracts = {}
        self.callbacks = {}
        self.callback_ids = {}
        self.downloader = None
        self.bracket_parms = {}
        self.oca_relations = {}

    # *************************************************************************
    # Internal Methods
    # *************************************************************************

    def __add_callback__(self, key, function, req_id=None):
        """Add 'function' as a callback for 'key'.

        Keyword arguments:
        key      -- key associated with the originating method
        function -- function to call
        req_id   -- request ID associated with the callback (default: None)

        """
        if function is None:
            return
        if key not in self.callbacks:
            self.callbacks[key] = []
        if function not in self.callbacks[key]:
            self.callbacks[key].append(function)
        if req_id is not None:
            self.callback_ids[key] = req_id

    def __del_all_callbacks__(self, key):
        """Remove all functions that are callbacks for 'key'.

        Keyword arguments:
        key      -- key associated with the originating method

        """
        if key in self.callbacks:
            self.callbacks.pop(key)
        if key in self.callback_ids:
            self.callback_ids.pop(key)

    def __del_callback__(self, key, function):
        """Remove 'function' as a callback for 'key'.

        Keyword arguments:
        key      -- key associated with the originating method
        function -- function being called

        """
        if function is None:
            return
        if key in self.callbacks and function in self.callbacks[key]:
            self.callbacks[key].remove(function)
            if len(self.callbacks[key]) == 0:
                self.callbacks.pop(key)
        if key in self.callback_ids:
            self.callback_ids.pop(key)

    def __get_req_id__(self):
        """Return the next available request ID and increment the ID counter
        by one.

        """
        self.next_id_lock.acquire()
        result = self.next_id
        self.next_id += 1
        self.next_id_lock.release()
        return result

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
        # Profit order
        if profit_offset != 0:
            limit_price = parent.avg_fill_price + \
                abs(profit_offset) * direction
            profit_order = ibo.Order(action, parent.total_quantity, 'lmt',
                                     lmt_price=limit_price)
            profit_order.oca_group = str(parent.perm_id)
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
            loss_order.oca_group = str(parent.perm_id)
            loss_order.oca_type = 2
            loss_order.tif = 'gtc'
            loss_id = self.place_order(contract, loss_order)
        else:
            loss_id = -1
        self.oca_relations[parent.order_id] = (profit_id, loss_id)

    # *************************************************************************
    # Connection
    # *************************************************************************

    def connect(self, host=config.HOST, port=config.PORT,
                client_id=config.CLIENT_ID):
        """Connect to the remote TWS.

        Keyword arguments:
        host      -- host name or IP address of the TWS machine
        port      -- port number on the TWS machine
        client_id -- number used to identify this client connection

        """
        self.adapter.connect(host, port, client_id)
        # Automatically associate newly opened TWS orders with this client
        self.adapter.req_auto_open_orders(True)
        # Wait for the next ID to get updated
        while self.next_id < 0:
            time.sleep(1)

    def disconnect(self):
        """Disconnect from the remote TWS."""
        self.adapter.disconnect()
        if self.downloader is not None:
            self.downloader.stop()
            self.downloader = None
        self.next_id = -1

    def is_connected(self):
        """Return True if the Client is connected; False, otherwise."""
        return self.adapter.is_connected

    def on_next_id(self, req_id):
        """Called by the ClientAdapter when the request ID is updated. This
        encapsulates the following methods from the TWS API:
            - next_valid_id

        Keyword arguments:
        req_id -- request ID

        """
        self.next_id = max(self.next_id, req_id)

    # *************************************************************************
    # Accounts
    # *************************************************************************

    def req_account_updates(self, account_name, callback):
        """Register the specified callback to receive account updates. The
        callback should have the same signature as 'on_account'.

        Keyword arguments:
        account_name -- account name
        callback     -- function to call on account updates

        """
        key = get_key('account', account_name=account_name)
        self.__add_callback__(key, callback)
        self.adapter.req_account_updates(True, account_name)

    def cancel_account_updates(self, account_name):
        """Unregister all callbacks from receiving account updates.

        Keyword arguments:
        account_name -- account name

        """
        key = get_key('account', account_name=account_name)
        self.__del_all_callbacks__(key)
        self.adapter.req_account_updates(False, account_name)

    def on_account(self, account):
        """Called by the ClientAdapter when account data is updated. This
        encapsulates the following methods from the TWS API:
            - account_download_end
            - update_account_time
            - update_account_value

        Keyword arguments:
        account -- ibapipy.data.account.Account object

        """
        key = get_key('account', account_name=account.account_name)
        if key in self.callbacks:
            for function in self.callbacks[key]:
                function(account)

    # *************************************************************************
    # Commissions
    # *************************************************************************

    def req_commission_updates(self, callback):
        """Register the specified callback to receive commission updates. The
        callback should have the same signature as 'on_commission'.

        Keyword arguments:
        callback -- function to call on commission updates

        """
        key = get_key('commission')
        self.__add_callback__(key, callback)

    def cancel_commission_updates(self, callback):
        """Unregister the specified callback from receiving commission updates.

        Keyword arguments:
        callback -- function being called on commission updates

        """
        key = get_key('commission')
        self.__del_callback__(key, callback)

    def on_commission(self, commission):
        """Called by the ClientAdapter when commission data is updated. This
        encapsulates the following methods from the TWS API:
            - commission_report

        Keyword arguments:
        commission -- ibapipy.data.commission_report.CommissionReport object

        """
        key = get_key('commission')
        if key in self.callbacks:
            for function in self.callbacks[key]:
                function(commission)

    # *************************************************************************
    # Contracts
    # *************************************************************************

    def req_contract(self, contract, callback):
        """Register the specified callback to receive a one-time update for the
        contract. The callback should have the same signature as 'on_contract'.

        Keyword arguments:
        callback -- function to call on contract updates

        """
        key = get_key('contract', contract)
        req_id = self.__get_req_id__()
        self.__add_callback__(key, callback, req_id)
        self.id_contracts[req_id] = contract
        self.adapter.req_contract_details(req_id, get_basic_contract(contract))

    def on_contract(self, contract):
        """Called by the ClientAdapter when contract data is updated. This
        encapsulates the following methods from the TWS API:
            - contract_details
            - contract_details_end

        Keyword arguments:
        contract -- ibapipy.data.contract.Contract object

        """
        key = get_key('contract', contract)
        if key in self.callbacks:
            for function in self.callbacks[key]:
                function(contract)
        self.__del_all_callbacks__(key)

    # *************************************************************************
    # Errors
    # *************************************************************************

    def req_error_messages(self, callback):
        """Register the specified callback to receive error messages. The
        callback should have the same signature as 'on_error'.

        Keyword arguments:
        callback -- function to call on error messages

        """
        key = get_key('errors')
        self.__add_callback__(key, callback)

    def cancel_error_messages(self, callback):
        """Unregister the specified callback from receiving error messages.

        Keyword arguments:
        callback -- function being called on error messages

        """
        key = get_key('errors')
        self.__del_callback__(key, callback)

    def on_error(self, req_id, code, message):
        """Called by the ClientAdapter when errors are reported. This
        encapsulates the following methods from the TWS API:
            - error

        Keyword arguments:
        req_id  -- request ID
        code    -- error code
        message -- error message

        """
        key = get_key('errors')
        if key in self.callbacks:
            for function in self.callbacks[key]:
                function(req_id, code, message)

    # *************************************************************************
    # Holdings
    # *************************************************************************

    def req_holding_updates(self, callback):
        """Register the specified callback to receive holding updates. The
        callback should have the same signature as 'on_holding'.

        Keyword arguments:
        callback -- function to call on holding updates

        """
        key = get_key('holding')
        self.__add_callback__(key, callback)

    def cancel_holding_updates(self, callback):
        """Unregister the specified callback from receiving holding updates.

        Keyword arguments:
        callback -- function being called on holding updates

        """
        key = get_key('holding')
        self.__del_callback__(key, callback)

    def on_holding(self, contract, holding):
        """Called by the ClientAdapter when holding data is updated. This
        encapsulates the following methods from the TWS API:
            - update_portfolio

        Keyword arguments:
        contract -- ibapipy.data.contract.Contract object
        holding  -- ibapipy.data.holding.Holding object

        """
        key = get_key('holding')
        if key in self.callbacks:
            for function in self.callbacks[key]:
                function(contract, holding)

    # *************************************************************************
    # Orders
    # *************************************************************************

    def req_order_updates(self, callback):
        """Register the specified callback to receive order and execution
        updates. The callback should have the same signature as 'on_order'.

        Keyword arguments:
        callback -- function to call on order and execution updates

        """
        key = get_key('order')
        req_id = self.__get_req_id__()
        self.__add_callback__(key, callback, req_id)
        self.adapter.req_executions(req_id, ibef.ExecutionFilter())
        self.adapter.req_all_open_orders()

    def cancel_order_updates(self, callback):
        """Unregister the specified callback from receiving order and execution
        updates.

        Keyword arguments:
        callback -- function being called on order and execution updates

        """
        key = get_key('order')
        self.__del_callback__(key, callback)

    def on_order(self, contract, order, executions):
        """Called by the ClientAdapter when holding data is updated. This
        encapsulates the following methods from the TWS API:
            - exec_details
            - exec_details_end
            - open_order
            - open_order_end
            - order_status

        Keyword arguments:
        contract   -- ibapipy.data.contract.Contract object
        order      -- ibapipy.data.order.Order object
        executions -- list of ibapipy.data.execution.Execution objects
                      associated with the order

        """
        key = get_key('order')
        if order is not None:
            self.__update_brackets__(order)
        if key in self.callbacks:
            for function in self.callbacks[key]:
                function(contract, order, executions)

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
        req_id = self.__get_req_id__()
        self.id_contracts[req_id] = contract
        self.bracket_parms[req_id] = (profit_offset, loss_offset)
        self.adapter.place_order(req_id, contract, order)
        return req_id

    def cancel_order(self, req_id):
        """Cancel the order associated with the specified request ID.

        Keyword arguments:
        req_id -- request ID

        """
        self.adapter.cancel_order(req_id)

    # *************************************************************************
    # Pricing
    # *************************************************************************

    def req_history(self, contract, start, end, timezone, callback):
        """Register the specified callback to receive a one-time update for
        historical data. The callback should have the same signature as
        'on_history'.

        Keyword arguments:
        contract   -- ibapipy.data.contract.Contract object
        start_date -- start date in 'yyyy-mm-dd hh:mm' format
        end_date   -- end date in 'yyyy-mm-dd hh:mm' format
        timezone   -- timezone in 'Country/Region' format
        callback   -- function to call on historical data updates

        """
        key = get_key('history', contract)
        self.__add_callback__(key, callback)
        if self.downloader is None:
            self.downloader = ibhd.HistoricalDownloader(self)
        self.downloader.request_history(contract, start, end, timezone)

    def history_outstanding(self):
        """Return the number of outstanding queued historical data requests. A
        single call to 'req_history' can result in a large number of smaller
        requests to prevent IB pacing violations.

        """
        if self.downloader is not None:
            return self.downloader.outstanding_count
        else:
            return 0

    def on_history(self, contract, tick, is_block_done, is_request_done):
        """Called by the ClientAdapter when historical data is updated. This
        encapsulates the following methods from the TWS API:
            - historical_data

        Keyword arguments:
        contract        -- ibapipy.data.contract.Contract object
        tick            -- ibapipy.data.tick.Tick object
        is_block_done   -- True if the current block of ticks is done
        is_request_done -- True if there is no more data available for the
                           original call to 'req_history'

        """
        key = get_key('history', contract)
        block_done = tick is None
        if block_done:
            self.downloader.block_received()
        request_done = self.downloader.outstanding_count == 0
        if key in self.callbacks:
            for function in self.callbacks[key]:
                function(contract, tick, block_done, request_done)
        if request_done:
            self.__del_all_callbacks__(key)

    def req_tick_updates(self, contract, callback):
        """Register the specified callback to receive tick updates. The
        callback should have the same signature as 'on_tick'.

        Keyword arguments:
        contract -- ibapipy.data.contract.Contract object
        callback -- function to call on holding updates

        """
        key = get_key('tick', contract)
        req_id = self.__get_req_id__()
        self.__add_callback__(key, callback, req_id)
        self.id_contracts[req_id] = contract
        self.adapter.req_mkt_data(req_id, contract)

    def cancel_tick_updates(self, contract):
        """Unregister all callbacks from receiving tick updates for the
        specified contract.

        Keyword arguments:
        contract -- ibapipy.data.contract.Contract object

        """
        key = get_key('tick', contract)
        req_id = self.callback_ids[key]
        self.__del_all_callbacks__(key)
        self.adapter.cancel_mkt_data(req_id)

    def on_tick(self, contract, tick):
        """Called by the ClientAdapter when tick data is updated. This
        encapsulates the following methods from the TWS API:
            - tick_size
            - tick_price

        """
        key = get_key('tick', contract)
        if key in self.callbacks:
            for function in self.callbacks[key]:
                function(contract, tick)


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

"""Encapsulates the ibapipy.client_socket.ClientSocket class and provides an
[ideally] simpler API for interacting with TWS. An attempt has been made to
clean up the API and replace multiple parameters/methods with objects where
possible.

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
    client_socket   -- underlying ibapipy.client_socket.ClientSocket object
    next_id         -- next available request ID
    id_contracts    -- dictionary of contracts by request ID
    history_queue   -- queue of pending historical data requests
    history_pending -- number of unfilled historical data requests
    bracket_parms   -- dictionary of (profit offset, loss offset) tuples for
                       bracket orders by parent order ID
    oca_relations   -- dictionary of (profit order ID, loss order ID) tuples
                       for bracket orders by parent order ID

    """

    def __init__(self):
        """Initialize a new instance of a Client."""
        self.adapter = ibca.ClientAdapter(self)
        self.next_id = -1
        self.next_id_lock = threading.Lock()
        self.id_contracts = {}
        self.id_tick_subscriptions = {}
        self.callbacks = {}
        self.downloader = None
        # Support for bracket orders
        self.bracket_parms = {}
        self.oca_relations = {}

    # *************************************************************************
    # Internal Methods
    # *************************************************************************

    def __add_callback__(self, name, function):
        """Add 'function' as a callback for 'name'.

        Keyword arguments:
        name     -- name of the originating member
        function -- function to call

        """
        if name not in self.callbacks:
            self.callbacks[name] = []
        if function not in self.callbacks[name]:
            self.callbacks[name].append(function)

    def __del_callback__(self, name, function):
        """Remove 'function' as a callback for 'name'.

        Keyword arguments:
        name     -- name of the originating member
        function -- function being called

        """
        if name in self.callbacks and function in self.callbacks[name]:
            self.callbacks[name].remove(function)
            if len(self.callbacks[name]) == 0:
                self.callbacks.pop(name)

    def __get_req_id__(self):
        """Return the next available request ID."""
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
        return self.adapter.is_connected

    def on_next_id(self, req_id):
        """Callback for updated request ID. Encapsulates the following methods
        from the TWS API:
        - next_valid_id

        """
        self.next_id = max(self.next_id, req_id)

    # *************************************************************************
    # Accounts
    # *************************************************************************

    def account_subscribe(self, account_name, callback):
        self.__add_callback__('account-{0}'.format(account_name.lower()),
                              callback)
        self.adapter.req_account_updates(True, account_name)

    def account_unsubscribe(self, account_name, callback):
        self.__del_callback__('account-{0}'.format(account_name.lower()),
                              callback)
        self.adapter.req_account_updates(False, account_name)

    def on_account(self, account):
        """Callback for updated account data. Encapsulates the following
        methods from the TWS API:
        - account_download_end
        - update_account_time
        - update_account_value

        Keyword arguments:
        account -- ibapipy.data.account.Account object

        """
        key = 'account-{0}'.format(account.account_name)
        if key in self.callbacks:
            for function in self.callbacks[key]:
                function(account)

    # *************************************************************************
    # Commissions
    # *************************************************************************

    def commission_subscribe(self, callback):
        self.__add_callback__('commission', callback)

    def commission_unsubscribe(self, callback):
        self.__del_callback__('commission', callback)

    def on_commission(self, commission):
        """Callback for updated commission data. Encapsulates the following
        methods from the TWS API:
        - commission_report

        Keyword arguments:
        commission -- ibapipy.data.commission_report.CommissionReport object

        """
        if 'commission' in self.callbacks:
            for function in self.callbacks['commission']:
                function(commission)

    # *************************************************************************
    # Contracts
    # *************************************************************************

    def contract_subscribe(self, contract, callback):
        # Add the callback
        key = 'contract-{0}-{1}'.format(contract.symbol, contract.currency)
        self.__add_callback__(key, callback)
        # Make the request
        req_id = self.__get_req_id__()
        self.id_contracts[req_id] = contract
        self.adapter.req_contract_details(req_id, get_basic_contract(contract))

    def contract_unsubscribe(self, contract, callback):
        key = 'contract-{0}-{1}'.format(contract.symbol, contract.currency)
        self.__del_callback__(key, callback)

    def on_contract(self, contract):
        """Callback for updated contract data. Encapsulates the following
        methods from the TWS API:
        - contract_details
        - contract_details_end

        Keyword arguments:
        contract -- ibapipy.data.contract.Contract object

        """
        key = 'contract-{0}-{1}'.format(contract.symbol, contract.currency)
        if key in self.callbacks:
            for function in self.callbacks[key]:
                function(contract)

    # *************************************************************************
    # Errors
    # *************************************************************************

    def errors_subscribe(self, callback):
        self.__add_callback__('error', callback)

    def errors_unsubscribe(self, callback):
        self.__del_callback__('error', callback)

    def on_error(self, req_id, code, message):
        """Callback for errors. Encapsulates the following methods from the TWS
        API:
        - error

        Keyword arguments:

        """
        if 'error' in self.callbacks:
            for function in self.callbacks['error']:
                function(req_id, code, message)

    # *************************************************************************
    # Holdings
    # *************************************************************************

    def holdings_subscribe(self, callback):
        self.__add_callback__('holdings', callback)

    def holdings_unsubscribe(self, callback):
        self.__del_callback__('holdings', callback)

    def on_holding(self, contract, holding):
        """Callback for updated holding data. Encapsulates the following
        methods from the TWS API:
        - update_portfolio

        """
        if 'holdings' in self.callbacks:
            for function in self.callbacks['holdings']:
                function(contract, holding)

    # *************************************************************************
    # Messages
    # *************************************************************************

    def messages_subscribe(self, callback):
        self.__add_callback__('messages', callback)

    def messages_unsubscribe(self, callback):
        self.__del_callback__('messages', callback)

    def on_message(self, req_id, code, message):
        """Callback for updated message data. Encapsulates the following
        methods from the TWS API:
        - error

        """
        if 'messages' in self.callbacks:
            for function in self.callbacks['messages']:
                function(req_id, code, message)

    # *************************************************************************
    # Orders
    # *************************************************************************

    def orders_subscribe(self, callback):
        self.__add_callback__('orders', callback)
        req_id = self.__get_req_id__()
        self.adapter.req_executions(req_id, ibef.ExecutionFilter())
        self.adapter.req_all_open_orders()

    def orders_unsubscribe(self, callback):
        self.__del_callback__('orders', callback)

    def on_order(self, contract, order, executions):
        """Callback for updated order data. Encapsulates the following
        methods from the TWS API:
        - exec_details
        - exec_details_end
        - open_order
        - open_order_end
        - order_status

        """
        if order is not None:
            self.__update_brackets__(order)
        if 'orders' in self.callbacks:
            for function in self.callbacks['orders']:
                function(contract, order, executions)

    def place_order(self, contract, order, profit_offset=0, loss_offset=0):
        """Place an order for the specified contract. If profit offset or loss
        offset is non-zero, a corresponding order will be placed after the
        parent order has been filled.

        Keyword arguments:
        contract      -- ibapipy.data.contract.Contract for the order
        parent        -- ibapipy.data.order.Order for the parent
        profit_offset -- profit target offset from parent's fill price
                         (default: 0)
        loss_offset   -- loss target offset from parent's fill price
                         (default: 0)

        """
        req_id = self.__get_req_id__()
        self.id_contracts[req_id] = contract
        self.bracket_parms[req_id] = (profit_offset, loss_offset)
        self.adpater.place_order(req_id, contract, order)

    def cancel_order(self, req_id):
        self.adapter.cancel_order(req_id)

    # *************************************************************************
    # Pricing
    # *************************************************************************

    def history_subscribe(self, contract, start, end, timezone, callback):
        """Subscribe to historical data.

        Keyword arguments:
        contract   -- ibapipy.data.contract.Contract object
        start_date -- start date in "yyyy-mm-dd hh:mm" format
        end_date   -- end date in "yyyy-mm-dd hh:mm" format
        timezone   -- timezone in "Country/Region" format

        """
        self.__add_callback__('history', callback)
        if self.downloader is None:
            self.downloader = ibhd.HistoricalDownloader(self)
        self.downloader.request_history(contract, start, end, timezone)

    def history_unsubscribe(self, callback):
        self.__del_callback__('history', callback)

    def history_outstanding(self):
        if self.downloader is not None:
            return self.downloader.outstanding_count
        else:
            return 0

    def on_history(self, contract, tick, is_block_done, is_request_done):
        """Callback for updated historical data. Encapsulates the following
        methods from the TWS API:
        - historical_data

        """
        block_done = tick is None
        if block_done:
            self.downloader.block_received()
        request_done = self.downloader.outstanding_count == 0
        if 'history' in self.callbacks:
            for function in self.callbacks['history']:
                function(contract, tick, block_done, request_done)

    def ticks_subscribe(self, contract, callback):
        key = 'ticks-{0}-{1}'.format(contract.symbol, contract.currency)
        self.__add_callback__(key, callback)
        req_id = self.__get_req_id__()
        self.id_tick_subscriptions[key] = req_id
        self.id_contracts[req_id] = contract
        self.adapter.req_mkt_data(req_id, contract)

    def ticks_unsubscribe(self, contract, callback):
        key = 'ticks-{0}-{1}'.format(contract.symbol, contract.currency)
        self.__del_callback__(key, callback)
        req_id = self.id_tick_subscriptions[key]
        self.id_tick_subscriptions.pop(key)
        self.adapter.cancel_mkt_data(req_id)

    def on_tick(self, contract, tick):
        """Callback for updated realtime data. Encapsulates the following
        methods from the TWS API:
        - tick_size
        - tick_price

        """
        key = 'ticks-{0}-{1}'.format(contract.symbol, contract.currency)
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

"""Offline testing client providing a subset of the functionality available in
the ibclientpy.client.Client class. Calls to the broker are replaced with
locally handled simulated results.

"""
import logging
import threading
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
    offline_ticks -- list of ticks used to provide pricing data
    orders        -- dictionary of orders by request ID
    tick_ready    -- semaphore used to control consumption of a tick
    is_updating   -- True if ticks are being sent; False, otherwise

    """

    def __init__(self):
        """Initialize a new instance of an OfflineClient."""
        ibclientpy.client.Client.__init__(self)
        self.contract = None
        self.offline_ticks = []
        self.orders = {}
        self.tick_ready = threading.Semphore()
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
            execution, cancel_id = fill_order(order, self.contract,
                                              price, tick.milliseconds,
                                              self.oca_relations)
            if cancel_id >= 0:
                need_cancelled.append(cancel_id)
            need_updated.append((order, execution))
            self.orders.pop(order.order_id)
        # Cancel orders
        for order_id in need_cancelled:
            self.cancel_order(order_id)
        # Send updates
        for order, execution in need_updated:
            self.on_order(self.contract, order, [execution])

    def __populate__(self):
        """Call the on_tick() method using the offline ticks list and acquire
        the tick_ready semaphore. When there are no more ticks to send, None is
        passed to on_tick() in lieu of a tick object.

        """
        for tick in self.offline_ticks:
            if not self.is_updating:
                return
            self.on_tick(self.contract, tick)
            self.tick_ready.acquire()
        # Send None for the tick to notify the downstream client we are done
        self.on_tick(self.contract, None)
        self.tick_ready.acquire()

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
        pass

    def disconnect(self):
        """Disconnect from the remote TWS."""
        pass

    def is_connected(self):
        """Return True if the Client is connected; False, otherwise."""
        return False

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
        pass

    def cancel_account_updates(self, account_name):
        """Unregister all callbacks from receiving account updates.

        Keyword arguments:
        account_name -- account name

        """
        pass

    # *************************************************************************
    # Contracts
    # *************************************************************************

    def req_contract(self, contract, callback):
        """Register the specified callback to receive a one-time update for the
        contract. The callback should have the same signature as 'on_contract'.

        Keyword arguments:
        callback -- function to call on contract updates

        """
        pass

    # *************************************************************************
    # Orders
    # *************************************************************************

    def req_order_updates(self, callback):
        """Register the specified callback to receive order and execution
        updates. The callback should have the same signature as 'on_order'.

        Keyword arguments:
        callback -- function to call on order and execution updates

        """
        key = ibclientpy.client.get_key('order')
        req_id = self.__get_req_id__()
        self.__add_callback__(key, callback, req_id)

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
            req_id = self.__get_req_id__()
            self.id_contracts[req_id] = contract
            order.order_id = req_id
            order.perm_id = req_id
        self.bracket_parms[req_id] = (profit_offset, loss_offset)
        self.orders[req_id] = order
        return req_id

    def cancel_order(self, req_id):
        """Cancel the order associated with the specified request ID.

        Keyword arguments:
        req_id -- request ID

        """
        if req_id in self.orders:
            order = self.orders.pop(req_id)
            order.status = 'cancelled'
            self.on_order(self.contract, order, [])

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
        pass

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
        pass

    def req_tick_updates(self, contract, callback):
        """Register the specified callback to receive tick updates. The
        callback should have the same signature as 'on_tick'.

        Keyword arguments:
        contract -- ibapipy.data.contract.Contract object
        callback -- function to call on holding updates

        """
        key = ibclientpy.client.get_key('tick', contract)
        req_id = self.__get_req_id__()
        self.__add_callback__(key, callback, req_id)
        self.id_contracts[req_id] = contract
        self.is_updating = True
        thread = threading.Thread(target=self.__populate__)
        thread.start()

    def cancel_tick_updates(self, contract):
        """Unregister all callbacks from receiving tick updates for the
        specified contract.

        Keyword arguments:
        contract -- ibapipy.data.contract.Contract object

        """
        key = ibclientpy.client.get_key('tick', contract)
        self.__del_all_callbacks__(key)
        self.is_updating = False
        self.tick_ready.release()

    def on_tick(self, contract, tick):
        """Called by the ClientAdapter when tick data is updated. This
        encapsulates the following methods from the TWS API:
            - tick_size
            - tick_price

        """
        if tick is not None:
            self.__handle_orders__(tick)
        key = ibclientpy.client.get_key('tick', contract)
        if key in self.callbacks:
            for function in self.callbacks[key]:
                function(contract, tick)
        self.tick_ready.release()


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
    """Fill the specified order and return an Execution and an order ID >= 0 if
    there is another OCA order that should be cancelled. Otherwise, return the
    execution and -1.

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
    execution = create_execution(order, milliseconds)
    # Try to find the other order in the OCA group
    oca_id = -1
    if order.oca_group != '' and order.oca_group in oca_relations:
        parent_id = order.oca_group
        profit_id, loss_id = oca_relations[parent_id]
        if order.order_id == profit_id:
            oca_id = loss_id
        else:
            oca_id = profit_id
    return execution, oca_id

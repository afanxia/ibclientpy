"""Provide a mechanism for keeping track of orders."""
import asyncio
import logging
import ibapipy.data.order as ibo


LOG = logging.getLogger(__name__)


class OrderHandler:
    """Provide a mechanism for keeping track of orders.

    Attributes not specified in the constructor:
    bracket_parms -- dictionary of {parent_id: (profit amt, loss amt), ...}
    child_orders  -- dictionary of {parent_id: [child1_id, ...], ...}
    executions    -- dictionary of {perm_id: {exec_id1: execution1, ...}, ...}
    orders        -- dictionary of {order_id: order, ...}
    parent_orders -- dictionary of {child_id: parent_id, ...}
    orders        -- dictionary of orders by order ID

    """

    def __init__(self, client):
        """Initialize a new instance of an OrderHandler.

        Keyword arguments:
        client -- ibclientpy.client.Client instance

        """
        self.client = client
        self.bracket_parms = {}
        self.child_orders = {}
        self.executions = {}
        self.orders = {}
        self.parent_orders = {}

    def __add_relation__(self, parent_id, child_id):
        """Update the child_orders and parent_orders dictionaries with the
        specified relationship.

        Keyword arguments:
        parent_id -- parent order's order_id
        child_id  -- child order's order_id

        """
        if parent_id not in self.child_orders:
            self.child_orders[parent_id] = []
        if child_id not in self.child_orders[parent_id]:
            self.child_orders[parent_id].append(child_id)
        self.parent_orders[child_id] = parent_id

    def add_execution(self, execution):
        """Add the specified execution to this handler.

        Keyword arguments:
        execution -- ibapipy.data.execution.Execution instance

        """
        perm_id = execution.perm_id
        exec_id = execution.exec_id
        if perm_id not in self.executions:
            self.executions[perm_id] = {}
        self.executions[perm_id][exec_id] = execution

    def add_order(self, order, profit_offset=0, loss_offset=0):
        """Add the specified order to this handler. If profit_offset or
        loss_offset are non-zero, a corresponding limit or stop order will be
        automatically placed after the parent order has been filled.

        Keyword arguments:
        order         -- ibapipy.data.order.Order instance
        profit_offset -- profit target offset from parent's fill price
                         (default: 0)
        loss_offset   -- loss target offset from parent's fill price
                         (default: 0)

        """
        self.orders[order.order_id] = order
        self.bracket_parms[order.order_id] = (profit_offset, loss_offset)

    def update_order(self, req_id, contract, order):
        """Update the order with the specified request/order id.

        Keyword arguments:
        contract -- contract associated with the order
        order    -- order instance

        """
        # Add/replace the order
        order.contract = contract
        self.orders[req_id] = order
        # Associate any executions
        perm_id = order.perm_id
        order.executions = []
        if perm_id in self.executions:
            for exec_id in self.executions[perm_id]:
                order.executions.append(self.executions[perm_id][exec_id])

    @asyncio.coroutine
    def update_brackets(self, parent):
        """Place a take profit (limit) and stop loss (stop) around the
        specified parent order's fill price.

        Keyword arguments:
        parent -- ibapipy.data.order.Order object for the parent order

        """
        if parent.order_id not in self.bracket_parms:
            return
        if parent.order_id not in self.client.id_contracts:
            return
        if parent.status != 'filled' or parent.avg_fill_price == 0:
            return
        action = 'sell' if parent.action == 'buy' else 'buy'
        direction = 1 if parent.action == 'buy' else -1
        profit_offset, loss_offset = self.bracket_parms.pop(parent.order_id)
        contract = self.client.id_contracts[parent.order_id]
        oca_group = str(parent.perm_id)
        # Profit order
        if profit_offset != 0:
            limit_price = parent.avg_fill_price + \
                abs(profit_offset) * direction
            profit_order = ibo.Order(action, parent.total_quantity, 'lmt',
                                     lmt_price=limit_price)
            profit_order.oca_group = oca_group
            profit_order.oca_type = 2
            profit_order.tif = 'gtc'
            profit_id = yield from self.client.place_order(contract,
                                                           profit_order)
            self.__add_relation__(parent.order_id, profit_id)
        # Loss order
        if loss_offset != 0:
            stop_price = parent.avg_fill_price - abs(loss_offset) * direction
            loss_order = ibo.Order(action, parent.total_quantity, 'stp',
                                   aux_price=stop_price)
            loss_order.oca_group = oca_group
            loss_order.oca_type = 2
            loss_order.tif = 'gtc'
            loss_id = yield from self.client.place_order(contract,
                                                         loss_order)
            self.__add_relation__(parent.order_id, loss_id)


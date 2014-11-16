"""Translate methods from a ibapipy.core.client_socket.ClientSocket to those in
a ibclientpy.client.Client.

"""
from ibapipy.data.account import Account
from ibapipy.data.holding import Holding
from ibapipy.data.order import Order
from ibapipy.data.tick import Tick
import ibapipy.core.client_socket as ibcs
import pyutils.dates as dates


class ClientAdapter(ibcs.ClientSocket):
    """Adapts methods from a ClientSocket."""

    def __init__(self, client):
        """Initialize a new instance of a ClientAdapter."""
        ibcs.ClientSocket.__init__(self)
        self.client = client
        # Current account and tick (by symbol.currency)
        self.account = Account()
        self.tick = {}
        # Contracts, orders, and executions by perm_id
        self.order_contracts = {}
        self.order_orders = {}
        self.order_executions = {}

    def account_download_end(self, account_name):
        pass

    def commission_report(self, report):
        self.client.on_commission(report)

    def contract_details(self, req_id, contract):
        self.client.on_contract(contract)

    def contract_details_end(self, req_id):
        pass

    def error(self, req_id, code, message):
        self.client.on_error(req_id, code, message)

    def exec_details(self, req_id, contract, execution):
        perm_id = execution.perm_id
        if perm_id not in self.order_contracts:
            self.order_contracts[perm_id] = contract
        if perm_id not in self.order_executions:
            self.order_executions[perm_id] = {}
        self.order_executions[perm_id][execution.exec_id] = execution
        order = None if perm_id not in self.order_orders else \
            self.order_orders[perm_id]
        executions = tuple(self.order_executions[perm_id].values())
        self.client.on_order(contract, order, executions)

    def exec_details_end(self, req_id):
        pass

    def historical_data(self, req_id, date, open, high, low, close, volume,
                        bar_count, wap, has_gaps):
        if req_id in self.client.id_contracts:
            contract = self.client.id_contracts[req_id]
        else:
            contract = None
        # Download is complete
        if 'finished' in date:
            self.client.on_history(contract, None, None, None)
        # Still receiving bars from the request
        else:
            milliseconds = int(date) * 1000
            tick = Tick(contract.local_symbol, milliseconds)
            tick.bid = low
            tick.ask = high
            tick.volume = volume * 100
            self.client.on_history(contract, tick, None, None)

    def managed_accounts(self, accounts):
        pass

    def next_valid_id(self, req_id):
        self.client.on_next_id(req_id)

    def open_order(self, req_id, contract, order):
        perm_id = order.perm_id
        if perm_id not in self.order_contracts:
            self.order_contracts[perm_id] = contract
        if perm_id not in self.order_orders:
            self.order_orders[perm_id] = order
        if perm_id in self.order_executions:
            executions = tuple(self.order_executions[perm_id].values())
        else:
            executions = ()
        self.client.on_order(contract, order, executions)

    def open_order_end(self):
        pass

    def order_status(self, req_id, status, filled, remaining, avg_fill_price,
                     perm_id, parent_id, last_fill_price, client_id, why_held):
        if perm_id not in self.order_orders:
            self.order_orders[perm_id] = Order()
        order = self.order_orders[perm_id]
        order.status = status
        order.filled = filled
        order.remaining = remaining
        order.avg_fill_price = avg_fill_price
        order.perm_id = perm_id
        order.parent_id = parent_id
        order.last_fill_price = last_fill_price
        order.client_id = client_id
        order.why_held = why_held
        if perm_id in self.order_contracts:
            contract = self.order_contracts[perm_id]
        elif req_id in self.client.id_contracts:
            contract = self.client.id_contracts[req_id]
        else:
            contract = None
        if perm_id in self.order_executions:
            executions = tuple(self.order_executions[perm_id].values())
        else:
            executions = ()
        self.client.on_order(contract, order, executions)

    def tick_price(self, req_id, tick_type, price, can_auto_execute):
        if req_id in self.client.id_contracts:
            contract = self.client.id_contracts[req_id]
        else:
            contract = None
        local_symbol = self.client.id_contracts[req_id].local_symbol
        if local_symbol not in self.tick:
            self.tick[local_symbol] = Tick(local_symbol, dates.now())
        # Update the last tick
        tick = self.tick[local_symbol]
        tick.volume = 0
        if tick_type == 1:
            tick.milliseconds = dates.now()
            tick.bid = price
        elif tick_type == 2:
            tick.milliseconds = dates.now()
            tick.ask = price
        # We can get ask prices lower than bid prices; don't return those.
        if tick.bid > 0 and tick.ask > tick.bid:
            self.client.on_tick(contract, copy_tick(tick))

    def tick_size(self, req_id, tick_type, size):
        if req_id in self.client.id_contracts:
            contract = self.client.id_contracts[req_id]
        else:
            contract = None
        local_symbol = self.client.id_contracts[req_id].local_symbol
        if local_symbol not in self.tick:
            self.tick[local_symbol] = Tick(local_symbol, dates.now())
        # Update the last tick
        tick = self.tick[local_symbol]
        tick.volume = 0
        if tick_type == 0:
            tick.milliseconds = dates.now()
            tick.bid_size = size
        elif tick_type == 3:
            tick.milliseconds = dates.now()
            tick.ask_size = size
        elif tick_type == 8:
            tick.milliseconds = dates.now()
            tick.volume = size
        # We can get ask prices lower than bid prices; don't return those.
        if tick.bid > 0 and tick.ask > tick.bid:
            self.client.on_tick(contract, copy_tick(tick))

    def update_account_time(self, timestamp):
        partial_date = dates.ms_to_str(dates.now(), dates.local_tz(),
                                       '%Y-%m-%d')
        full_date = '{0} {1}'.format(partial_date, timestamp)
        self.account.milliseconds = dates.str_to_ms(full_date,
                                                    dates.local_tz(),
                                                    '%Y-%m-%d %H:%M')
        self.client.on_account(self.account)

    def update_account_value(self, key, value, currency, account_name):
        self.account.account_name = account_name
        if key == 'netliquidation':
            self.account.net_liquidation = float(value)
        elif key == 'previousdayequitywithloanvalue':
            self.account.previous_equity = float(value)
        elif key == 'equitywithloanvalue':
            self.account.equity = float(value)
        elif key == 'totalcashvalue':
            self.account.cash = float(value)
        elif key == 'initmarginreq':
            self.account.initial_margin = float(value)
        elif key == 'maintmarginreq':
            self.account.maintenance_margin = float(value)
        elif key == 'availablefunds':
            self.account.available_funds = float(value)
        elif key == 'excessliquidity':
            self.account.excess_liquidity = float(value)
        elif key == 'sma':
            self.account.sma = float(value)
        elif key == 'buyingpower':
            self.account.buying_power = float(value)
        self.client.on_account(self.account)

    def update_portfolio(self, contract, position, market_price, market_value,
                         average_cost, unrealized_pnl, realized_pnl,
                         account_name):
        # Sometimes when closing out a position, IB gets confused and will
        # report the P&L as 1.79769313486232e+308.
        if abs(unrealized_pnl) > 1000000000000:
            unrealized_pnl = 0
        if abs(realized_pnl) > 1000000000000:
            realized_pnl = 0
        holding = Holding(account_name, contract.local_symbol)
        holding.milliseconds = dates.now()
        holding.quantity = position
        holding.market_price = market_price
        holding.market_value = market_value
        holding.average_cost = average_cost
        holding.unrealized = unrealized_pnl
        holding.realized = realized_pnl
        self.client.on_holding(contract, holding)

    def update_unknown(self, *args):
        """Callback for updated known data that does not match any existing
        callbacks.

        """
        raise NotImplementedError()


def copy_tick(tick):
    """Returns a copy of the specified tick.

    Keyword arguments:
    tick -- ibapipy.data.tick.Tick object

    """
    result = Tick(tick.local_symbol, tick.milliseconds)
    result.__dict__.update(tick.__dict__)
    return result

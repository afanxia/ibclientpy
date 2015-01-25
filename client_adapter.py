"""Translate methods from a ibapipy.client_socket.ClientSocket to those in a
ibclientpy.client.Client.

"""
from asyncio import Future
from ibapipy.data.account import Account
from ibapipy.data.holding import Holding
from ibapipy.data.order import Order
from ibapipy.data.tick import Tick
import asyncio
import calendar
import pytz
import time
import ibapipy.client_socket as ibcs
import ibapipy.date_support as ds


class ClientAdapter(ibcs.ClientSocket):
    """Adapts methods from a ClientSocket."""

    def __init__(self, client, loop):
        """Initialize a new instance of a ClientAdapter."""
        ibcs.ClientSocket.__init__(self, loop)
        self.client = client
        # Current account and tick (by symbol.currency)
        self.account = Account()
        self.tick = {}
        # Contracts, orders, and executions by perm_id
        self.execs_by_id = {}
        self.orders_by_id = {}
        self.order_contracts = {}
        self.order_orders = {}
        self.order_executions = {}
        # When requesting orders via req_all_open_orders(),
        # the open_orders_end() method will be called after order_status() has
        # been called for each order, but before open_order() has been called
        # for each order. It doesn't seem like it should work that way, but...
        # So, instead of relying on open_orders_end(), we keep track of what
        # orders order_status() has been called for and use that so
        # open_order() knows when to return a result. This dictionary contains
        # the req_ids that have been seen by order_status() -- the value is set
        # from False to True once open_order() has seen it.
        self.open_order_ids = {}
        # Historical ticks
        self.historical_remaining = 0
        self.historical_ticks = []
        # Realtime ticks
        self.is_receiving_ticks = False
        # Futures
        self.contract_fut = None
        self.executions_fut = None
        self.history_fut = None
        self.next_valid_id_fut = None
        self.managed_accounts_fut = None
        self.orders_fut = None
        self.order_cancel_fut = None
        self.ticks_fut = None
        self.update_account_fut = None

    # *************************************************************************
    # Outgoing Requests
    # *************************************************************************

    @asyncio.coroutine
    def req_all_open_orders(self):
        self.open_order_ids.clear()
        self.orders_by_id.clear()
        yield from ibcs.ClientSocket.req_all_open_orders(self)

    @asyncio.coroutine
    def req_executions(self, req_id, exec_filter):
        self.execs_by_id.clear()
        yield from ibcs.ClientSocket.req_executions(self, req_id, exec_filter)

    @asyncio.coroutine
    def req_historical_data(self, req_id, contract, end_date_time,
                            duration_str, bar_size_setting, what_to_show,
                            use_rth, format_date):
        self.history_fut = Future()
        self.historical_ticks.clear()
        yield from ibcs.ClientSocket.req_historical_data(
            self, req_id, contract, end_date_time, duration_str,
            bar_size_setting, what_to_show, use_rth, format_date)
        yield from self.history_fut

    @asyncio.coroutine
    def req_mkt_data(self, req_id, contract, generic_ticklist='',
                     snapshot=False):
        self.is_receiving_ticks = True
        self.ticks_fut = Future()
        yield from ibcs.ClientSocket.req_mkt_data(self, req_id, contract,
                                                  generic_ticklist, snapshot)
        while self.is_receiving_ticks:
            yield from self.ticks_fut
            self.ticks_fut = Future()
        yield from ibcs.ClientSocket.cancel_mkt_data(self, req_id)

    # *************************************************************************
    # Incoming Data
    # *************************************************************************

    @asyncio.coroutine
    def account_download_end(self, account_name):
        fut = self.update_account_fut
        if fut is not None and not fut.done():
            fut.set_result(self.account)

    @asyncio.coroutine
    def contract_details(self, req_id, contract):
        fut = self.contract_fut
        if fut is not None and not fut.done():
            fut.set_result(contract)

    @asyncio.coroutine
    def contract_details_end(self, req_id):
        pass

    @asyncio.coroutine
    def error(self, req_id, code, message):
        self.client.on_error(req_id, code, message)

    @asyncio.coroutine
    def exec_details(self, req_id, contract, execution):
        perm_id = execution.perm_id
        if perm_id not in self.execs_by_id:
            self.execs_by_id[perm_id] = []
        self.execs_by_id[perm_id].append(execution)

    @asyncio.coroutine
    def exec_details_end(self, req_id):
        fut = self.executions_fut
        if fut is not None and not fut.done():
            fut.set_result(True)

    @asyncio.coroutine
    def historical_data(self, req_id, date, open, high, low, close, volume,
                        bar_count, wap, has_gaps):
        if req_id in self.client.id_contracts:
            contract = self.client.id_contracts[req_id]
        else:
            contract = None
        # Download is complete
        if 'finished' in date:
            fut = self.history_fut
            if fut is not None and not fut.done():
                fut.set_result((self.historical_remaining,
                                self.historical_ticks))
        # Still receiving bars from the request
        else:
            milliseconds = int(date) * 1000
            tick = Tick(contract.local_symbol, milliseconds)
            tick.bid = low
            tick.ask = high
            tick.volume = volume * 100
            self.historical_remaining -= 1
            self.historical_ticks.append(tick)

    @asyncio.coroutine
    def managed_accounts(self, account_number):
        fut = self.managed_accounts_fut
        if fut is not None and not fut.done():
            fut.set_result(account_number)

    @asyncio.coroutine
    def next_valid_id(self, req_id):
        fut = self.next_valid_id_fut
        if fut is not None and not fut.done():
            fut.set_result(req_id)

    @asyncio.coroutine
    def open_order(self, req_id, contract, order):
        perm_id = order.perm_id
        order.contract = contract
        order.executions = []
        if perm_id in self.execs_by_id:
            order.executions.extend(self.execs_by_id[perm_id])
        self.orders_by_id[perm_id] = order
        # Update our "open order tracking" dictionary
        if req_id in self.open_order_ids:
            self.open_order_ids[req_id] = True
        # See if we should return a result
        fut = self.orders_fut
        if False not in self.open_order_ids.values() and \
                fut is not None and not fut.done():
            result = []
            for perm_id in self.orders_by_id:
                result.append(self.orders_by_id[perm_id])
            fut.set_result(tuple(result))

    @asyncio.coroutine
    def open_order_end(self):
        pass

    @asyncio.coroutine
    def order_status(self, req_id, status, filled, remaining, avg_fill_price,
                     perm_id, parent_id, last_fill_price, client_id, why_held):
        # Keep track of what orders this has been called for. We wait until we
        # get 'apipending' so that we can't do something like cancel an order
        # before it's even been fulled entered (otherwise we can end up with
        # an order that just stays in 'pendingcancel' forever).
        if req_id not in self.open_order_ids and status == 'apipending':
            self.open_order_ids[req_id] = False
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
        # See if we should set a result on the order cancelled future
        fut = self.order_cancel_fut
        if status == 'cancelled' and fut is not None and not fut.done():
            fut.set_result(req_id)

    @asyncio.coroutine
    def tick_price(self, req_id, tick_type, price, can_auto_execute):
        local_symbol = self.client.id_contracts[req_id].local_symbol
        if local_symbol not in self.tick:
            self.tick[local_symbol] = Tick(local_symbol, ds.now())
        # Update the last tick
        tick = self.tick[local_symbol]
        tick.volume = 0
        if tick_type == 1:
            tick.milliseconds = ds.now()
            tick.bid = price
        elif tick_type == 2:
            tick.milliseconds = ds.now()
            tick.ask = price
        # We can get ask prices lower than bid prices; don't return those.
        if tick.bid > 0 and tick.ask > tick.bid and \
                self.ticks_fut is not None and not self.ticks_fut.done():
            self.ticks_fut.set_result(copy_tick(tick))

    @asyncio.coroutine
    def tick_size(self, req_id, tick_type, size):
        local_symbol = self.client.id_contracts[req_id].local_symbol
        if local_symbol not in self.tick:
            self.tick[local_symbol] = Tick(local_symbol, ds.now())
        # Update the last tick
        tick = self.tick[local_symbol]
        tick.volume = 0
        if tick_type == 0:
            tick.milliseconds = ds.now()
            tick.bid_size = size
        elif tick_type == 3:
            tick.milliseconds = ds.now()
            tick.ask_size = size
        elif tick_type == 8:
            tick.milliseconds = ds.now()
            tick.volume = size

    @asyncio.coroutine
    def update_account_time(self, timestamp):
        """Called when the account time is updated. The 'timestamp' is in the
        form of hh:mm and does not include a day. This converts the time into
        milliseconds (using the current day and UTC timezone) before updating
        the account.

        Keyword arguments:
        timestamp -- last update time for account data

        """
        partial_date = ms_to_str(time.time() * 1000, 'UTC', '%Y-%m-%d')
        full_date = '{0} {1}'.format(partial_date, timestamp)
        self.account.milliseconds = str_to_ms(full_date, 'UTC',
                                              '%Y-%m-%d %H:%M')

    @asyncio.coroutine
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

    @asyncio.coroutine
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
        holding.milliseconds = int(time.time() * 1000)
        holding.quantity = position
        holding.market_price = market_price
        holding.market_value = market_value
        holding.average_cost = average_cost
        holding.unrealized = unrealized_pnl
        holding.realized = realized_pnl
        self.client.on_holding(contract, holding)

    @asyncio.coroutine
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


def ms_to_str(milliseconds, timezone='UTC', formatting='%Y-%m-%d %H:%M:%S.%f'):
    """Converts the specified time in milliseconds to a string.

    Please see the following for formatting directives:
    http://docs.python.org/library/datetime.html#strftime-strptime-behavior

    Keyword arguments:
    milliseconds -- time in milliseconds since the Epoch
    timezone     -- timezone string (default: 'UTC')
    formatting   -- formatting string (default: '%Y-%m-%d %H:%M:%S.%f')

    """
    seconds = int(milliseconds / 1000)
    microseconds = (milliseconds % 1000) * 1000
    gmtime = time.gmtime(seconds)
    dtime = pytz.datetime.datetime(gmtime.tm_year, gmtime.tm_mon,
                                   gmtime.tm_mday, gmtime.tm_hour,
                                   gmtime.tm_min, gmtime.tm_sec, microseconds,
                                   pytz.timezone('UTC'))
    dtime = dtime.astimezone(pytz.timezone(timezone))
    return dtime.strftime(formatting)


def str_to_ms(date, timezone='UTC', formatting='%Y-%m-%d %H:%M:%S.%f'):
    """Converts the specified string to milliseconds since the Epoch.

    Keyword arguments:
    date       -- date string
    timezone   -- timezone string (default: 'UTC')
    formatting -- formatting string (default: '%Y-%m-%d %H:%M:%S.%f')

    """
    dtime = pytz.datetime.datetime.strptime(date, formatting)
    # Note that in order for daylight savings conversions to work, we
    # MUST use localize() here and cannot simply pass a timezone into the
    # datetime constructor. This is because derple, werpy, durp, durp ...
    tzone = pytz.timezone(timezone)
    dtime = pytz.datetime.datetime(dtime.year, dtime.month, dtime.day,
                                   dtime.hour, dtime.minute, dtime.second,
                                   dtime.microsecond)
    dtime = tzone.localize(dtime)
    dtime = dtime.astimezone(pytz.timezone('UTC'))
    milliseconds = calendar.timegm(dtime.utctimetuple()) * 1000
    milliseconds = int(milliseconds + dtime.microsecond / 1000.0)
    return milliseconds

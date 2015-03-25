"""Translate methods from a ibapipy.client_socket.ClientSocket to those in a
ibclientpy.client.Client.

"""
from asyncio import Future, Queue
from ibapipy.data.account import Account
from ibapipy.data.holding import Holding
from ibapipy.data.order import Order
from ibapipy.data.tick import Tick
import asyncio
import calendar
import pytz
import time
import ibapipy.client_socket as ibcs
import ibclientpy.date_support as ds


class ClientAdapter(ibcs.ClientSocket):
    """Adapts methods from a ClientSocket."""

    def __init__(self, client, loop):
        """Initialize a new instance of a ClientAdapter."""
        ibcs.ClientSocket.__init__(self, loop)
        self.client = client
        # Connection
        self.next_valid_id_fut = None
        # Accounts
        # Account information is updated one attribute at a time via multiple
        # calls so we maintain a single instance here (account) that is
        # always updated.
        self.account = Account()
        self.account_fut = Future()
        self.account_name_fut = Future()
        # Contracts
        # Contract asyncio.Futures by 'symbol.currency' key
        self.contract_futs = {}
        # Order call tracking -- see order_status() comments for details
        self.open_order_end_called = False
        self.open_order_ids = {}
        # Historical ticks
        self.history_pending = []
        self.history_queue = Queue()
        # Realtime ticks
        self.tick = {}
        # Market data (tick) requests {'symbol.currency' : req_id, ...}
        self.market_data_ids = {}
        # Incoming tick queue {req_id : Queue(), ...}
        self.tick_queue = {}
        # Futures
        self.executions_fut = None
        self.orders_fut = None
        self.order_cancel_fut = None

    # *************************************************************************
    # Outgoing Requests
    # *************************************************************************

    @asyncio.coroutine
    def cancel_mkt_data(self, req_id):
        yield from ibcs.ClientSocket.cancel_mkt_data(self, req_id)

    @asyncio.coroutine
    def req_account_updates(self, acct_code):
        self.account_fut = Future()
        self.account.account_name = None
        self.account.milliseconds = 0
        self.account.net_liquidation = None
        self.account.previous_equity = None
        self.account.equity = None
        self.account.cash = None
        self.account.initial_margin = None
        self.account.maintenance_margin = None
        self.account.available_funds = None
        self.account.excess_liquidity = None
        self.account.sma = None
        self.account.buying_power = None
        yield from ibcs.ClientSocket.req_account_updates(self, True,
                                                         acct_code)
        return self.account_fut

    @asyncio.coroutine
    def req_all_open_orders(self):
        self.open_order_end_called = False
        self.open_order_ids.clear()
        yield from ibcs.ClientSocket.req_all_open_orders(self)

    @asyncio.coroutine
    def req_contract_details(self, req_id, contract):
        key = '{0}.{1}'.format(contract.symbol, contract.currency)
        self.contract_futs[key] = Future()
        yield from ibcs.ClientSocket.req_contract_details(self, req_id,
                                                          contract)
        return self.contract_futs[key]

    @asyncio.coroutine
    def req_executions(self, req_id, exec_filter):
        yield from ibcs.ClientSocket.req_executions(self, req_id, exec_filter)

    @asyncio.coroutine
    def req_historical_data(self, req_id, contract, end_date_time,
                            duration_str, bar_size_setting, what_to_show,
                            use_rth, format_date):
        self.history_pending.clear()
        yield from ibcs.ClientSocket.req_historical_data(
            self, req_id, contract, end_date_time, duration_str,
            bar_size_setting, what_to_show, use_rth, format_date)

    @asyncio.coroutine
    def req_ids(self, num_ids):
        self.next_valid_id_fut = Future()
        yield from ibcs.ClientSocket.req_ids(self, num_ids)
        return self.next_valid_id_fut

    @asyncio.coroutine
    def req_managed_accts(self):
        self.account_name_fut = Future()
        yield from ibcs.ClientSocket.req_managed_accts(self)
        return self.account_name_fut

    @asyncio.coroutine
    def req_mkt_data(self, req_id, contract, generic_ticklist='',
                     snapshot=False):
        key = '{0}.{1}'.format(contract.symbol, contract.currency)
        self.market_data_ids[key] = req_id
        self.tick_queue[req_id] = Queue()
        yield from ibcs.ClientSocket.req_mkt_data(self, req_id, contract,
                                                  generic_ticklist, snapshot)

    # *************************************************************************
    # Incoming Data
    # *************************************************************************

    @asyncio.coroutine
    def account_download_end(self, account_name):
        pass

    @asyncio.coroutine
    def contract_details(self, req_id, contract):
        key = '{0}.{1}'.format(contract.symbol, contract.currency)
        if is_future_valid(self.contract_futs[key]):
            self.contract_futs[key].set_result(contract)

    @asyncio.coroutine
    def contract_details_end(self, req_id):
        pass

    @asyncio.coroutine
    def error(self, req_id, code, message):
        self.client.on_error(req_id, code, message)

    @asyncio.coroutine
    def exec_details(self, req_id, contract, execution):
        self.client.order_handler.add_execution(execution)

    @asyncio.coroutine
    def exec_details_end(self, req_id):
        fut = self.executions_fut
        if fut is not None and not fut.done():
            fut.set_result(True)

    @asyncio.coroutine
    def historical_data(self, req_id, date, open, high, low, close, volume,
                        bar_count, wap, has_gaps):
        contract = self.client.id_contracts[req_id]
        # Download is complete
        if 'finished' in date:
            yield from self.history_queue.put(tuple(self.history_pending))
            self.history_pending.clear()
        # Still receiving bars from the request
        else:
            milliseconds = int(date) * 1000
            tick = Tick(contract.local_symbol, milliseconds)
            tick.bid = low
            tick.ask = high
            tick.volume = volume * 100
            self.history_pending.append(tick)

    @asyncio.coroutine
    def managed_accounts(self, account_number):
        if is_future_valid(self.account_name_fut):
            self.account_name_fut.set_result(account_number)

    @asyncio.coroutine
    def next_valid_id(self, req_id):
        fut = self.next_valid_id_fut
        if fut is not None and not fut.done():
            fut.set_result(req_id)

    @asyncio.coroutine
    def open_order(self, req_id, contract, order):
        self.client.order_handler.update_order(req_id, contract, order)
        # Update our "open order tracking" dictionary. Being in the dictionary
        # means that open_order has been called or order_status has been called
        # with a status of 'apipending'; having a value of False means that
        # order_status has yet to be called with a non-apipending status. See
        # the order_status() comments for details.
        if req_id not in self.open_order_ids:
            self.open_order_ids[req_id] = False

    @asyncio.coroutine
    def open_order_end(self):
        self.open_order_end_called = True

    @asyncio.coroutine
    def order_status(self, req_id, status, filled, remaining, avg_fill_price,
                     perm_id, parent_id, last_fill_price, client_id, why_held):
        """Called when the order status is updated.

        The IB API appears to work like this:
        - open_order is called for orders that are not apipending
        - order_status is called for all orders
        - open_order_end is called
        - open_order is called for orders that have moved from apipending
        - order_status is called for orders that have moved from apipending

        Since open_order_end does not mark the end of the request process, we
        must use order_status to determine when we have all of the order
        information. This method is always called after open_order for a given
        request id.

        """
        # Track which order order_status has been called on
        self.open_order_ids[req_id] = status != 'apipending'
        if req_id not in self.client.order_handler.orders:
            return
        order = self.client.order_handler.orders[req_id]
        order.order_id = req_id
        order.status = status
        order.filled = filled
        order.remaining = remaining
        order.avg_fill_price = avg_fill_price
        order.perm_id = perm_id
        order.parent_id = parent_id
        order.last_fill_price = last_fill_price
        order.client_id = client_id
        order.why_held = why_held
        # Update any bracketed orders for this order
        if order.status == 'filled':
            self.client.order_handler.update_brackets(order)
        # See if we should set a result on the order cancelled future
        fut = self.order_cancel_fut
        if status == 'cancelled' and fut is not None and not fut.done():
            fut.set_result(req_id)
        # See if we should return a result on the orders future
        fut = self.orders_fut
        if False not in self.open_order_ids.values() and fut is not None and \
                self.open_order_end_called and not fut.done():
            fut.set_result(tuple(self.client.order_handler.orders.values()))

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
        if tick.bid > 0 and tick.ask > tick.bid:
            yield from self.tick_queue[req_id].put(copy_tick(tick))

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
        self.account.milliseconds = int(time.time() * 1000)
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
        # It can take IB a long time to call account_download_end, so we check
        # to see if we have all the data we need and, if so, consider the
        # request completed.
        if self.account.account_name is not None and \
                self.account.milliseconds > 0 and \
                self.account.net_liquidation is not None and \
                self.account.previous_equity is not None and \
                self.account.equity is not None and \
                self.account.cash is not None and \
                self.account.initial_margin is not None and \
                self.account.maintenance_margin is not None and \
                self.account.available_funds is not None and \
                self.account.excess_liquidity is not None and \
                self.account.sma is not None and \
                self.account.buying_power is not None and \
                is_future_valid(self.account_fut):
            yield from ibcs.ClientSocket.req_account_updates(self, False,
                                                             account_name)
            self.account_fut.set_result(self.account)

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


def is_future_valid(future):
    """Return True if the specified future is not None and is not done; False,
    otherwise.

    Keyword arguments:
    future -- asyncio.Future instance

    """
    return future is not None and not future.done()


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

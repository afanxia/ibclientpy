#!/usr/bin/env python3
"""Simple client for downloading historical data from Interactive Brokers."""
import asyncio
import sys
import ibapipy.data.contract as ibdc
import ibclientpy.client as ibc
import ibclientpy.date_support as ds


TIMEZONE='US/Eastern'


def _ms_bounds(local_symbol, day):
    """Return the inclusive starting and exclusive ending time bounds in
    milliseconds for the specified date.

    This is just a sample function. In practice, there would likely be a
    configuration file that specified different time intervals (trading
    windows) for different instruments. The SPY will trade at a different time
    than the USD/JPY pair for instance.

    Keyword arguments:
    local_symbol -- ticker symbol
    day          -- day in 'yyyy-mm-dd' form

    """
    milliseconds = ds.str_to_ms(day, TIMEZONE, '%Y-%m-%d')
    weekday = ds.ms_to_datetime(milliseconds, TIMEZONE).weekday()
    # Don't support weekends
    if weekday > 4:
        return 0, 0
    start_time, end_time = 0, 0
    # For this example, we assume a '.' in the symbol refers to a currency so
    # we trade between 06:00 and 20:00 in the specified timezone
    if '.' in local_symbol:
        start_time = '06:00'
        end_time = '20:00'
    # Otherwise, we'll assume it's an equity and trade 09:30 to 16:00
    else:
        start_time = '09:30'
        end_time = '16:00'
    min_time = '{0} {1}:00.000'.format(day, start_time)
    min_ms = ds.str_to_ms(min_time, TIMEZONE)
    max_time = '{0} {1}:00.000'.format(day, end_time)
    max_ms = ds.str_to_ms(max_time, TIMEZONE)
    return min_ms, max_ms


def _symbol_to_contract(local_symbol):
    """Create a Contract given the specified local symbol. This will return an
    equity contract ('stk') for symbols with no period in the name and a
    Forex ('cash') contract for symbols with a period.

    Keyword arguments:
    local_symbol -- ticker symbol

    """
    # Forex
    if '.' in local_symbol:
        primary, secondary = local_symbol.split('.')
        return ibdc.Contract('cash', primary, secondary, 'idealpro')
    # Equity
    else:
        return ibdc.Contract('stk', local_symbol, 'usd', 'smart')


@asyncio.coroutine
def get_history(local_symbol, start_time, end_time):
    """Download historical prices for the specified symbol.

    Keyword arguments:
    local_symbol -- ticker symbol
    start_time   -- inclusive start time in 'yyyy-mm-dd hh:mm' form
    end_time     -- exclusive end time in 'yyyy-mm-dd hh:mm' form

    """
    result = []
    # Connect to IB
    print('[{0}] Connecting to client.'.format(local_symbol))
    client = ibc.Client()
    yield from client.connect()
    # Get a fully populated contract
    basic_contract = _symbol_to_contract(local_symbol)
    contract = yield from client.get_contract(basic_contract)
    # Fetch the history
    msg = '[{0}] Downloading history from {1} to {2}.'
    print(msg.format(local_symbol, start_time, end_time))
    blocks_count, start_ms = 0, ds.now()
    while True:
        blocks_left, ticks = yield from client.get_next_history_block(
                contract, start_time, end_time, TIMEZONE)
        if ticks is None:
            break
        result.extend(ticks)
        blocks_count += 1
        # Update our time estimate
        mean_time = (ds.now() - start_ms) / blocks_count
        mins_left = blocks_left * mean_time / 60000
        msg = '[{0}] {1:3,d} blocks, {2:5.2f} minutes remainining.'
        print(msg.format(local_symbol, blocks_left, mins_left))
    yield from client.disconnect()
    print('Downloaded {0} ticks'.format(len(result)))


@asyncio.coroutine
def get_history_day(local_symbol, day):
    """Download historical prices for the specified symbol.

    Keyword arguments:
    local_symbol -- ticker symbol
    day          -- day in 'yyyy-mm-dd' form

    """

    start_ms, end_ms = _ms_bounds(local_symbol, day)
    start_time = ds.ms_to_str(start_ms, TIMEZONE, '%Y-%m-%d %H:%M')
    end_time = ds.ms_to_str(end_ms + 1000, TIMEZONE, '%Y-%m-%d %H:%M')
    yield from get_history(local_symbol, start_time, end_time)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print()
        print('Usage:')
        print('{0} <symbol> <day>'.format(sys.argv[0]))
        print()
        print('Days should be specified as yyyy-mm-dd.')
        print()
    else:
        local_symbol = sys.argv[1].lower()
        day = sys.argv[2]
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(get_history_day(local_symbol, day))


"""Provide helper functions that allow for historical data requests to
Interactive Brokers without having a pacing violation.

"""
import ibapipy.data.contract as ibc
import ibclientpy.config as config
import ibclientpy.date_support as ds


# Number of milliseconds in a day
DAY_MS = 86400000


def create_bar_data(start_date, end_date, min_time, max_time, timezone):
    """Create a list of the form [(bar_time, bar_count), ...] with each
    element in the list representing the timeframe for a historical data
    request. This is calculated using the known constraints that will prevent a
    pacing violation.

    Keyword arguments:
    start_date -- start date in yyyy-mm-dd format
    end_date   -- end date in yyyy-mm-dd format
    min_time   -- minimum inclusive session starting time in hh:mm form
    max_time   -- maximum exclusive session ending time in hh:mm form
    timezone   -- time zone

    """
    # Append times to the dates
    min_hour, min_minute = [int(z) for z in min_time.split(':')]
    max_hour, max_minute = [int(z) for z in max_time.split(':')]
    start_time = '{0} {1}:{2}'.format(start_date, min_hour, min_minute)
    end_time = '{0} {1}:{2}'.format(end_date, max_hour, max_minute)
    # Convert our time to milliseconds
    start_ms = ds.str_to_ms(start_time, timezone, '%Y-%m-%d %H:%M')
    end_ms = ds.str_to_ms(end_time, timezone, '%Y-%m-%d %H:%M')
    # Enumerate each day as a start:end pair
    span_min = (max_hour * 60 + max_minute) - (min_hour * 60 + min_minute)
    span_ms = span_min * 60 * 1000
    days = []
    first_ms, last_ms = start_ms, start_ms + span_ms
    while first_ms < end_ms:
        dtime = ds.ms_to_datetime(last_ms, timezone)
        # Only allow Monday through Friday
        if dtime.weekday() < 5:
            days.append((first_ms, last_ms))
        first_ms += DAY_MS
        last_ms += DAY_MS
    # Break each day into blocks of MAX_BLOCK_SIZE
    blocks = []
    for day in days:
        total_span_secs = (day[1] - day[0]) / 1000
        total_bars = total_span_secs / config.BAR_SIZE_SECONDS
        last_ms = day[0]
        while total_bars > 0:
            bars = min(total_bars, config.MAX_BLOCK_SIZE)
            span_ms = int(bars * config.BAR_SIZE_SECONDS * 1000)
            blocks.append((last_ms + span_ms, bars))
            total_bars -= config.MAX_BLOCK_SIZE
            last_ms += span_ms
    return blocks


def create_request(contract, bar_time, bar_count):
    """Return a dictionary containing parameters for a single historical data
    request.

    Keyword arguments:
    contract  -- contract
    bar_time  -- ending time
    bar_count -- number of bars to request

    """
    result = {}
    result['delay'] = config.WAIT_SECONDS
    result['contract'] = contract
    end_date_time = ds.ms_to_str(bar_time, 'UTC', '%Y%m%d %H:%M:%S UTC')
    result['end_date_time'] = end_date_time
    result['duration_str'] = '{0:d} S'.format(int(bar_count *
                                                  config.BAR_SIZE_SECONDS))
    result['bar_size_setting'] = config.BAR_SIZE
    sec_type = contract.sec_type.upper()
    result['what_to_show'] = config.WHAT_TO_SHOW[sec_type]
    result['use_rth'] = config.USE_RTH
    result['format_date'] = config.FORMAT_DATE
    return result


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


def get_parameters(contract, start_date, end_date, timezone):
    """Return a list of tuples where each tuple represents the parameters for
    a single call to req_historical_data().

    Keyword arguments:
    contract   -- ibapipy.data.contract.Contract object
    start_date -- start date in "yyyy-mm-dd hh:mm" format
    end_date   -- end date in "yyyy-mm-dd hh:mm" format
    timezone   -- timezone in "Country/Region" format

    """
    start_date, min_time = start_date.split(' ')
    end_date, max_time = end_date.split(' ')
    bar_data = create_bar_data(start_date, end_date, min_time, max_time,
                                timezone)
    result = []
    for item in bar_data:
        result.append(create_request(contract, item[0], item[1]))
    return result


"""Date and time translation functions."""
import calendar
import pytz
import time


def ms_to_datetime(milliseconds, timezone='UTC'):
    """Converts the specified time in milliseconds to a datetime object in
    the given timezone.

    Keyword arguments:
    milliseconds -- time in milliseconds since the Epoch
    timezone     -- timezone string (default: 'UTC')

    """
    seconds = int(milliseconds / 1000)
    microseconds = (milliseconds % 1000) * 1000
    gmtime = time.gmtime(seconds)
    dtime = pytz.datetime.datetime(gmtime.tm_year, gmtime.tm_mon,
                                   gmtime.tm_mday, gmtime.tm_hour,
                                   gmtime.tm_min, gmtime.tm_sec, microseconds,
                                   pytz.timezone('UTC'))
    return dtime.astimezone(pytz.timezone(timezone))


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


def now():
    """Returns the current time in milliseconds since the Epoch."""
    return int(time.time() * 1000)


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

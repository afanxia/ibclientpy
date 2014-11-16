"""Implementation of an Interactive Brokers Trader's Workstation (TWS)
client designed to encapsulate and simplify the TWS API.

"""
import logging


# Default log level
LOG_LEVEL = logging.INFO


def configure_logging():
    """Configure the default logger for this package."""
    logger = logging.getLogger()
    logger.setLevel(LOG_LEVEL)
    handler = logging.NullHandler()
    line1 = '%(asctime)s %(levelname)s %(name)s %(funcName)s():'
    line2 = '%(message)s'
    formatter = logging.Formatter('{0}\n{1}'.format(line1, line2))
    handler.setFormatter(formatter)
    logger.addHandler(handler)


# Setup logging
configure_logging()

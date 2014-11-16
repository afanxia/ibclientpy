"""Settings and options for the ibclient package."""


# *****************************************************************************
# NETWORKING OPTIONS
# *****************************************************************************

# Host name or IP address of the TWS machine.
HOST = '127.0.0.1'

# Port number on the TWS machine.
PORT = 4001

# Number used to identify a client connection.
CLIENT_ID = 0


# *****************************************************************************
# HISTORICAL DATA
# *****************************************************************************

# Bar size in seconds.
BAR_SIZE_SECONDS = 1

# Bar size in IB-compatible text.
BAR_SIZE = '{0:d} secs'.format(BAR_SIZE_SECONDS)

# Type of data to request, as follows:
# TRADES   -- standard OHLC with volume and count (in 100's)
# MIDPOINT -- open/high same, low/close same, no volume or count
# BID      -- open/high same, low/close same, no volume or count
# ASK      -- open/high same, low/close same, no volume or count
# BID_ASK  -- open is the bid, close is the ask, no volume or count
WHAT_TO_SHOW = {'STK': 'TRADES', 'CASH': 'MIDPOINT'}

# Only use regular trading hours (if True)
USE_RTH = False

# Ask IB to use 'seconds since the Epoch' for dates
FORMAT_DATE = 2

# Maximum number of bars for each request.
MAX_BLOCK_SIZE = 1800

# Number of seconds to wait between requests to avoid a "pacing violation."
WAIT_SECONDS = 18

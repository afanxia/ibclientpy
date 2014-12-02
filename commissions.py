"""Commission calculations."""
import ibclientpy.config as config


def est_comm(contract, price, quantity):
    """Return the estimated commission.

    Keyword arguments:
    contract -- contract
    price    -- current price
    quantity -- quantity (units or shares) to trade

    """
    if contract.sec_type == 'stk':
        return est_equity_comm(price, quantity)
    elif contract.sec_type == 'cash':
        return est_fx_comm(contract.symbol, contract.currency, price, quantity)
    else:
        raise NotImplementedError('Only equities and forex are supported.')


def est_equity_comm(price, quantity):
    """Return the estimated commission for an equity trade.

    Keyword arguments:
    price    -- current price
    quantity -- quantity (units or shares) to trade

    """
    cost = quantity * config.EQUITY_PER_SHARE
    cost = min(config.EQUITY_MAX * quantity * price, cost)
    cost = max(config.EQUITY_MIN, cost)
    return cost


def est_fx_comm(symbol, currency, price, quantity):
    """Return the estimated commission for a forex trade.

    Keyword arguments:
    symbol   -- trade currency
    currency -- base currency
    price    -- current price
    quantity -- quantity (units or shares) to trade

    """
    # Buying with USD
    if currency == 'usd':
        trade_value = quantity * price
        cost = trade_value * config.FX_POINTS
    # Buying USD with something else
    elif symbol == 'usd':
        cost = quantity * config.FX_POINTS
    # We don't support non-USD currency transactions at the moment
    else:
        raise NotImplementedError('Non-USD pairs not supported yet.')
    return max(config.FX_MIN, cost)

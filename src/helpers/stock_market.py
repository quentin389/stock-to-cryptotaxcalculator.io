import re
from logging import warning

from config.types import TickerSuffix


def parse_ticker(ticker: str, translation_table, suffix: TickerSuffix) -> str:
    if ticker in translation_table:
        ticker = translation_table[ticker]
    elif re.match(r'^[a-zA-Z]+$', ticker) is None and ticker not in parse_ticker.already_warned:
        warning(
            f"The ticker '{ticker}' contains characters other than letters. This increases the probability "
            "that this isn't a standardised name, and can lead to shares not being matched between exchanges. "
            "Consider adding a translation in 'src/config/config.py'."
        )
        parse_ticker.already_warned[ticker] = True

    return ticker + suffix


parse_ticker.already_warned = {}

import re

from config.config import translate_tickers
from config.types import TickerSuffix, Exchange
from helpers.validation import show_warning_once


def parse_ticker(ticker: str, exchange: Exchange, suffix: TickerSuffix) -> str:
    if ticker in translate_tickers[exchange]:
        ticker = translate_tickers[exchange][ticker]
    elif re.match(r'^[a-zA-Z]+$', ticker) is None:
        show_warning_once(
            group=f'{exchange} Ticker {ticker}',
            message=f"The ticker '{ticker}' contains characters other than letters.\nThis increases the probability "
                    "that this isn't a standardised name, and can lead to shares not being matched between exchanges.\n"
                    "Consider adding a translation to 'src/config/config.py'."
        )

    return ticker + suffix
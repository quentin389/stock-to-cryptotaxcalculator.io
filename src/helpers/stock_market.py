import re

from config.config import translate_tickers
from config.types import TickerAffix, Exchange
from helpers.validation import show_warning_once


def parse_ticker(ticker: str, exchange: Exchange, suffix: TickerAffix) -> str:
    if ticker in translate_tickers[exchange]:
        ticker = translate_tickers[exchange][ticker]
    elif re.match(r'^[A-Z]+$', ticker) is None:
        show_warning_once(
            group=f'{exchange} Ticker {ticker}',
            message=f"The ticker '{ticker}' contains characters other than capital letters.\nThis increases the "
                    "probability that this isn't a standardised name, and can lead to shares not being matched between "
                    "exchanges.\nConsider adding a translation to 'src/config/config.py'."
        )

    return ticker + suffix

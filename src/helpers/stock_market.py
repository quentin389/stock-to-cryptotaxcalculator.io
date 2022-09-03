import re

from config.config import translate_tickers
from config.types import Exchange, AssetType
from helpers.validation import show_warning_once, validate


def parse_ticker(
        ticker: str, exchange: Exchange, asset_type: AssetType
) -> str:
    if ticker in translate_tickers[exchange]:
        ticker = translate_tickers[exchange][ticker]
    elif asset_type != AssetType.Option and re.match(r'^[A-Z]+$', ticker) is None:
        show_warning_once(
            group=f'{exchange} Ticker {ticker}',
            message=f"The ticker '{ticker}' contains characters other than capital letters.\nThis increases the "
                    "probability that this isn't a standardised name, and can lead to shares not being matched between "
                    "exchanges.\nConsider adding a translation to 'src/config/config.py'."
        )

    if asset_type == AssetType.Crypto:
        return ticker

    if asset_type == AssetType.Stock:
        # There is no functional difference in computing taxes for stocks and some derivatives, like ETF, so for
        # the sake of simplicity I just call of it stock. What's important is that the tickers get a name that will
        # not conflict with crypto.
        return f'{ticker}:STOCK'

    if asset_type == AssetType.Option:
        # Options can be treated as stock, so I prefix each option type in order for the names to be easily
        # recognisable.
        return f'OPT:{ticker}'

    validate(
        condition=False,
        error=f'Asset Type {asset_type} does not have a ticker name implemented.',
        context=asset_type
    )

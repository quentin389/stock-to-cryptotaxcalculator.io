import locale
from abc import ABC
from decimal import Decimal, ROUND_HALF_UP

from src.config.config import translate_tickers
from src.config.types import Exchange, AssetType
from src.helpers.stock_market import parse_ticker
from src.helpers.validation import validate
from src.parsers.AbstractDataParser import AbstractDataParser


class AbstractIgDataParser(AbstractDataParser, ABC):

    def __init__(self, source: str, target: str):
        super().__init__(source, target)
        locale.setlocale(locale.LC_ALL, 'en_GB.UTF-8')

    @staticmethod
    def _parse_ticker(ticker: str) -> str:
        validate(
            condition=ticker in translate_tickers[Exchange.IG],
            error="IG does not have Tickers in the export files\n"
                  "You have to match each name to a correct stock ticker manually.\n"
                  f"Add '{ticker}' to the 'translate_tickers' dict for IG.\n"
                  "Remember to add names in such a way that they match names from other exchanges:\n"
                  "US tickers just as is, for example 'GOOG', and other tickers as custom names.",
            context=ticker
        )
        return parse_ticker(ticker, Exchange.IG, AssetType.Stock)

    @staticmethod
    def _round(number: float, exponent: str = '.00') -> float:
        # IG rounds halves up
        return float(Decimal(number).quantize(Decimal(exponent), ROUND_HALF_UP))

    @staticmethod
    def _format_money(number: float) -> str:
        return locale.format_string('%.2f', number, True)

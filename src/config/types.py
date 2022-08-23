from enum import Enum
from typing import Optional

import numpy
from pandas import Timestamp
from pydantic import BaseModel, validator


class TickerSuffix(str, Enum):
    Empty = ''

    # There is no functional difference in computing taxes for stocks and some derivatives, like ETF, so for the sake
    # of simplicity I just call of it stock. What's important is that the tickers get a name that will not conflict
    # with crypto.
    Stock = ':STOCK'


class Exchange(str, Enum):
    Etoro = 'eToro'
    Bank = 'Bank'

    # This is a special source for easy identification and filtering of dividends income.
    Dividends = 'Dividends'


class OutputType(str, Enum):
    FiatDeposit = 'fiat-deposit'
    FiatWithdrawal = 'fiat-withdrawal'
    Buy = 'buy'
    Sell = 'sell'
    ChainSplit = 'chain-split'
    Interest = 'interest'


# noinspection PyMethodParameters
class OutputRow(BaseModel):
    TimestampUTC: Timestamp
    Type: OutputType
    BaseCurrency: str
    BaseAmount: float
    QuoteCurrency: str = ''
    QuoteAmount: float = None
    FeeCurrency: str = ''
    FeeAmount: float = None
    From: Exchange
    To: Exchange
    ID: str = ''
    Description: str

    @validator('TimestampUTC')
    def convert_timestamp_to_string(cls, timestamp: Timestamp) -> str:
        return timestamp.strftime("%Y-%m-%d %H:%M:%S")

    @validator('BaseAmount', 'QuoteAmount', 'FeeAmount')
    def avoid_scientific_float_notation(cls, value: Optional[float]) -> str:
        if value is None:
            return ''
        return numpy.format_float_positional(value, trim='-')

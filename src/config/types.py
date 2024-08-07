from enum import Enum
from typing import Optional

import numpy
from pandas import Timestamp
from pydantic import BaseModel, validator


class AssetType(str, Enum):
    Stock = 'Stock'
    Option = 'Option'
    Crypto = 'Crypto'


class Exchange(str, Enum):
    Bank = 'Bank'
    Etoro = 'eToro'
    IG = 'IG'
    IG_CFD = 'IG-CFD'
    Schwab = 'Schwab'

    # This is Interactive Brokers, but writing the full name does not make sense as it's shown as "Interactive b...".
    Ibkr = 'IBKR'

    # Special sources for easy identification and filtering of special income types.
    Dividends = 'Dividends'
    CFDs = 'CFDs'

    # This is a special exchange name to flag it in cryptotaxcalculator.io as this indicates missing name.
    Unknown = 'Unknown'


class OutputType(str, Enum):
    FiatDeposit = 'fiat-deposit'
    FiatWithdrawal = 'fiat-withdrawal'
    Buy = 'buy'
    Sell = 'sell'
    ChainSplit = 'chain-split'
    Interest = 'interest'
    RealizedProfit = 'realized-profit'
    RealizedLoss = 'realized-loss'
    Fee = 'fee'
    Receive = 'receive'
    Send = 'send'


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
    Blockchain: str = ''
    ID: str = ''
    Description: str
    ReferencePricePerUnit: float = None
    ReferencePriceCurrency: str = ''

    @validator('TimestampUTC')
    def convert_timestamp_to_string(cls, timestamp: Timestamp) -> str:
        return timestamp.strftime("%Y-%m-%d %H:%M:%S")

    @validator('BaseAmount', 'QuoteAmount', 'FeeAmount')
    def avoid_scientific_float_notation(cls, value: Optional[float]) -> str:
        if value is None:
            return ''
        return numpy.format_float_positional(value, trim='-')

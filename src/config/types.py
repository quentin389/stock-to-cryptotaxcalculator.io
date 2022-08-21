from enum import Enum

from pandas import Timestamp
from pydantic import BaseModel, validator


class Exchange(Enum):
    Etoro = 'eToro'
    Bank = 'Bank'


class OutputType(Enum):
    FiatDeposit = 'fiat-deposit'
    FiatWithdrawal = 'fiat-withdrawal'
    Buy = 'buy'
    Sell = 'sell'


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

    @validator('Type', 'From', 'To')
    def resolve_enum(cls, enum: Enum):
        return enum.value

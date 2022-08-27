from enum import Enum
from typing import NamedTuple

from pandas import Timestamp


class Codes(str, Enum):
    # Ah, yes, as we all know 'L' stands for "margin call"
    L = 'Ordered by IB (Margin Violation)'


class DepositsAndWithdrawalsRow(NamedTuple):
    Header: str
    Currency: str
    Settle_Date: Timestamp
    Description: str
    Amount: float


class FeesRow(NamedTuple):
    Header: str
    Subtitle: str
    Currency: str
    Date: Timestamp
    Description: str
    Amount: float


class ForexTradesRow(NamedTuple):
    Header: str
    DataDiscriminator: str
    Currency: str
    Symbol: str
    Date_Time: Timestamp
    Quantity: float
    T__Price: float
    Proceeds: float
    Comm_in_Base_Currency: float
    MTM_in_Base_Currency: float
    Code: str

from typing import NamedTuple

from pandas import Timestamp


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

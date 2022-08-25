from typing import NamedTuple

from pandas import Timestamp


class DepositsAndWithdrawalsRow(NamedTuple):
    Header: str
    Currency: str
    Settle_Date: Timestamp
    Description: str
    Amount: float

from typing import NamedTuple

from pandas import Timestamp


class TransactionTuple(NamedTuple):
    Index: int
    Date: Timestamp
    Type: str
    Details: str
    Amount: float
    Units: float
    Realized_Equity_Change: float
    Realized_Equity: float
    Balance: float
    Position_ID: str
    Asset_type: str
    NWA: int


class PositionTuple(NamedTuple):
    Index: int
    Action: str
    Amount: float
    Units: float
    Open_Date: Timestamp
    Close_Date: Timestamp
    Leverage: int
    Spread: float
    Profit: float
    Open_Rate: float
    Close_Rate: float
    Take_profit_rate: float
    Stop_lose_rate: float
    Rollover_Fees_and_Dividends: float
    Copied_From: str
    Type: str
    ISIN: str
    Notes: str

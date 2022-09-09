from dataclasses import dataclass
from typing import NamedTuple, Optional

from pandas import Timestamp


class TradeRow(NamedTuple):
    Date: str
    Time: str
    Activity: str
    Market: str
    Direction: str
    Quantity: float
    Price: float
    Currency: str
    Consideration: float
    Commission: float
    Charges: float
    Cost_Proceeds: float
    Conversion_rate: float
    Order_type: str
    Venue_ID: str
    Settled_: bool
    Settlement_date: str
    Order_ID: str


class TransactionRow(NamedTuple):
    Index: int
    Date: str
    Summary: str
    MarketName: str
    Period: str
    ProfitAndLoss: str
    Transaction_type: str
    Reference: int
    Open_level: float
    Close_level: float
    Size: float
    Currency: str
    PL_Amount: float
    Cash_transaction: bool
    DateUtc: str  # TODO: could I use this?
    OpenDateUtc: str
    CurrencyIsoCode: str


@dataclass
class Trade:
    Date: Timestamp
    Trade: TradeRow
    Consideration: Optional[TransactionRow] = None
    Commission: Optional[TransactionRow] = None
    Fee: Optional[TransactionRow] = None

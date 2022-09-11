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
    DateUtc: Timestamp
    OpenDateUtc: str
    CurrencyIsoCode: str


@dataclass
class Trade:
    Date: Timestamp
    Trade: TradeRow
    Consideration: Optional[TransactionRow] = None
    Commission: Optional[TransactionRow] = None
    Fee: Optional[TransactionRow] = None


class CfdClosingTrade(NamedTuple):
    Closing_Ref: str
    Closed: Timestamp
    Opening_Ref: str
    Opened: Timestamp
    Market: str
    Period: str
    Direction: str
    Size: float
    Opening: float
    Closing: float
    Trade_Ccy_: str
    P_L: float
    Funding: float
    Borrowing: float
    Dividends: float
    LR_Prem_: float
    Others: float
    Comm__Ccy_: str
    Comm_: float
    Total: float

from enum import Enum
from typing import NamedTuple

from pandas import Timestamp


class Codes(str, Enum):
    # Ah, yes, as we all know 'L' stands for "margin call"
    L = 'Ordered by IB (Margin Violation)'

    C = 'Closing Trade'
    P = 'Partial Execution'
    O = 'Opening Trade'  # noqa: E741
    Ep = 'Resulted from an Expired Position'

    # ignore those codes
    FPA = ''


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
    Code: list[Codes]


class StocksAndDerivativesTradesRow(NamedTuple):
    Header: str
    DataDiscriminator: str
    Currency: str
    Symbol: str
    Date_Time: Timestamp
    Quantity: float
    T__Price: float
    C__Price: float
    Proceeds: float
    Comm_Fee: float
    Basis: float
    Realized_P_L: float
    Realized_P_L_pct: float
    MTM_P_L: float
    Code: list[Codes]


class InterestRow(NamedTuple):
    Header: str
    Currency: str
    Date: Timestamp
    Description: str
    Amount: float


class CorporateActionsRow(NamedTuple):
    Header: str
    Asset_Category: str
    Currency: str
    Report_Date: Timestamp
    Date_Time: Timestamp
    Description: str
    Quantity: float
    Proceeds: float
    Value: float
    Realized_P_L: float
    Code: list[Codes]


class TransferRow(NamedTuple):
    Header: str
    Asset_Category: str
    Currency: str
    Symbol: str
    Date: Timestamp
    Type: str
    Direction: str
    Xfer_Company: str
    Xfer_Account: str
    Qty: float
    Xfer_Price: float
    Market_Value: float
    Realized_P_L: float
    Cash_Amount: float
    Code: list[Codes]

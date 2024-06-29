from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class BrokerageRow(BaseModel):
    Date: datetime
    Action: str
    Symbol: Optional[str]
    Description: str
    Quantity: Optional[int]
    Price: Optional[float]
    Fees: Optional[float]
    Amount: Optional[float]


class EquityAwardRow(BaseModel):
    Date: datetime
    Action: str
    Symbol: str
    Description: str
    Quantity: int
    AwardDate: datetime
    FairMarketValuePrice: float
    SharesSoldWithheldForTaxes: int
    NetSharesDeposited: int
    Taxes: float

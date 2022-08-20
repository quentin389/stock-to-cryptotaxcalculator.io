from typing import Optional, NamedTuple

import pandas
from pandas import Timestamp
from pydantic import validate_arguments

from helpers.csv import save_output
from helpers.data_frames import remove_column_spaces, get_one_by_key
from helpers.types import OutputRow, OutputType
from helpers.validation import validate


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


class EtoroImport:
    def __init__(self, source: str, target: str):
        self.__source = source
        self.__target = target

    def run(self):
        self.__pre_validate()
        data = self.__parse()
        save_output(self.__target, data)

    def __pre_validate(self):
        account_summary = pandas.read_excel(io=self.__source, sheet_name="Account Summary", header=1, index_col=0)
        validate(
            condition=account_summary.loc['Currency']['Totals'] == "USD",
            error="Only USD accounts are supported. I didn't have any other examples.",
            context=account_summary
        )

    def __parse(self) -> list[OutputRow]:
        transactions = pandas.read_excel(
            io=self.__source, sheet_name="Account Activity", na_values="-", keep_default_na=False, parse_dates=['Date'],
            converters={'Details': lambda x: '' if x == '-' else x}
        )
        remove_column_spaces(transactions)

        positions = pandas.read_excel(
            io=self.__source, sheet_name="Closed Positions", index_col="Position ID", keep_default_na=False,
            parse_dates=['Open Date', 'Close Date']
        )
        remove_column_spaces(positions)

        data: list[OutputRow] = []
        row: TransactionTuple
        for row in transactions.itertuples():
            row_data = self.__parse_transaction(row, get_one_by_key(positions, row.Position_ID))
            if row_data:
                data.append(row_data)

        return data

    @validate_arguments
    def __parse_transaction(
            self, transaction: TransactionTuple, position: Optional[PositionTuple]
    ) -> Optional[OutputRow]:
        validate(
            condition=transaction.NWA == 0,
            error="What is NWA? It's always 0.00 in my case.",
            context=transaction
        )

        if transaction.Type == 'Deposit' and position is None:
            return self.__parse_deposit(transaction)

        return None
        # TODO: raise Exception(f"Row {str(transaction.Index)} of type '{transaction.Type}' cannot be parsed.")

    def __parse_deposit(self, transaction: TransactionTuple) -> OutputRow:
        validate(
            condition=transaction.Amount == transaction.Realized_Equity_Change,
            error="Deposit amount inconsistent.",
            context=transaction
        )

        return OutputRow(
            TimestampUTC=transaction.Date,
            Type=OutputType.FiatDeposit,
            From='Bank',
            To='eToro',
            Description=self.__make_description(transaction),

            # Whatever is the actual transaction.Details currency, it gets converted to USD as the only accepted
            # currency (this is validated in self.__pre_validate). So, we can skip the conversion, which is not even
            # recorded in the eToro file, and use transaction.Amount as the USD amount.
            BaseCurrency='USD',
            BaseAmount=transaction.Amount,

            # Deposits do not have IDs on eToro
            ID='',
        )

    @staticmethod
    def __make_description(transaction: TransactionTuple) -> str:
        return f'eToro {transaction.Type} {transaction.Details}'

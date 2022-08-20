from logging import warning
from typing import Optional

import pandas
from pydantic import validate_arguments

from helpers.csv import save_output
from helpers.data_frames import remove_column_spaces, get_one_by_key
from helpers.types import OutputRow, OutputType
from helpers.validation import validate
from parsers.types import TransactionTuple, PositionTuple


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
            return self.__parse_deposit_or_withdrawal(transaction, is_deposit=True)

        if transaction.Type == 'Withdraw Request' and position is None:
            return self.__parse_deposit_or_withdrawal(transaction, is_deposit=False)

        if transaction.Type == 'Edit Stop Loss':
            return None

        if transaction.Type == 'Withdraw Fee':
            if transaction.Amount != 0:
                warning("It appears that there is a withdrawal fee. Saving this data is NOT IMPLEMENTED.")
            return None

        if transaction.Type in {'Open Position', 'Position closed', 'corp action: Split', 'Dividend', 'Rollover Fee',
                                'Interest Payment'}:
            # warning(f'{transaction.Type} is yet to be implemented')
            return None  # TODO

        raise Exception(f"Row {str(transaction.Index)} of type '{transaction.Type}' cannot be parsed.")

    def __parse_deposit_or_withdrawal(self, transaction: TransactionTuple, is_deposit: bool) -> OutputRow:
        operation_name = "Deposit" if is_deposit else "Withdrawal"
        validate(
            condition=transaction.Amount == transaction.Realized_Equity_Change,
            error=f'{operation_name} amount inconsistent.',
            context=transaction
        )
        validate(
            condition=transaction.Position_ID == '' and transaction.Asset_type == '',
            error=f'{operation_name} cannot have Position ID or Asset type.',
            context=transaction
        )

        return OutputRow(
            TimestampUTC=transaction.Date,
            Type=OutputType.FiatDeposit if is_deposit else OutputType.FiatWithdrawal,
            From='Bank' if is_deposit else 'eToro',
            To='eToro' if is_deposit else 'Bank',
            Description=self.__make_description(transaction),

            # Whatever is the actual transaction.Details currency for a deposit, it gets converted to USD as the
            # only currency I parse (this is validated in self.__pre_validate). So, I can skip the conversion, which
            # is not even recorded in the eToro file, and use transaction.Amount as the USD amount.
            BaseCurrency='USD',
            BaseAmount=transaction.Amount if is_deposit else -transaction.Amount,

            # Deposits and withdrawals do not have IDs on eToro
            ID='',
        )

    @staticmethod
    def __make_description(transaction: TransactionTuple) -> str:
        return f'eToro {transaction.Type} {transaction.Details}'.strip()

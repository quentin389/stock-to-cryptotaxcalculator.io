from typing import Optional

import pandas
from pandas import DataFrame, to_datetime

from config.types import OutputRow
from helpers import data_frames
from helpers.validation import validate
from parsers.AbstractDataParser import AbstractDataParser
from parsers.ig.types import TradeRow, TransactionRow


class IgDataParser(AbstractDataParser):
    __trades: DataFrame
    __transactions: DataFrame

    def __init__(self, source: str, second_source: str, target: str):
        super().__init__(source, target)
        self.__second_source = second_source

    def run(self) -> None:
        self.__parse_files()
        data = self.__parse()
        self._save_output(data)

    def __parse(self) -> list[OutputRow]:
        all_rows = []

        trade: TradeRow
        for trade in self.__trades.itertuples():
            result = self.__parse_trade(trade)
            all_rows.append(result) if result else None

        # TODO: parse currency transactions
        print(self.__transactions)

        return all_rows

    def __parse_trade(self, trade: TradeRow) -> Optional[OutputRow]:  # TODO: is optional needed here?
        date = to_datetime(f'{trade.Date} {trade.Time}', format='%d-%m-%Y %H:%M:%S')
        print(date, trade.Order_ID, trade)
        transaction: TransactionRow
        for transaction in \
                self.__transactions.loc[self.__transactions['MarketName'].str.contains(trade.Order_ID)].itertuples():
            self.__transactions.drop([transaction.Index], inplace=True)
            print("     ", transaction)
        return None

    def __parse_files(self) -> None:
        first_source = pandas.read_csv(self._get_source(), skip_blank_lines=True, na_values=["-"], thousands=',')
        second_source = pandas.read_csv(self.__second_source, skip_blank_lines=True, na_values=["-"], thousands=',')
        data_frames.normalize_column_names(first_source)
        data_frames.normalize_column_names(second_source)
        first_source = first_source[::-1]
        second_source = second_source[::-1]

        if 'Activity' in first_source and 'Summary' in second_source:
            self.__trades = first_source
            self.__transactions = second_source
        elif 'Summary' in first_source and 'Activity' in second_source:
            self.__trades = second_source
            self.__transactions = first_source
        else:
            validate(
                condition=False,
                error="The passed files do not appear to be 'TradeHistory' and 'TransactionHistory'.",
                context=[self._get_source(), self.__second_source]
            )

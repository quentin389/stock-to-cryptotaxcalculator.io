import re
from io import StringIO
from pprint import pprint
from typing import Iterator

import pandas
from pandas import DataFrame

from config.types import OutputRow, OutputType, Exchange
from helpers import data_frames
from parsers.AbstractDataParser import AbstractDataParser
from parsers.ibkr.types import DepositsAndWithdrawalsRow


class IbkrDataParser(AbstractDataParser):
    __date_format = '%Y-%m-%d'

    def run(self) -> None:
        self.__extract_all_tables()
        data = self.__parse()
        pprint(data)
        self._save_output(data)

    def __parse(self) -> list[OutputRow]:
        return [
            *self.__parse_deposits_and_withdrawals(self.__tables['Deposits & Withdrawals'])
        ]
        # wide_print(self.__tables['Fees'])
        #
        # trades_test = self.__tables['Trades'][0]
        # wide_print(trades_test.loc[trades_test['DataDiscriminator'] == 'Order'])
        #
        # trades_test = self.__tables['Trades'][1]
        # wide_print(trades_test.loc[trades_test['DataDiscriminator'] == 'Order'])
        #
        # trades_test = self.__tables['Trades'][2]
        # wide_print(trades_test.loc[trades_test['DataDiscriminator'] == 'Order'])

    def __parse_deposits_and_withdrawals(self, data: DataFrame) -> list[OutputRow]:
        data_frames.remove_column_spaces(data)
        data_frames.parse_date(data, 'Settle_Date', self.__date_format)

        row: DepositsAndWithdrawalsRow
        for row in data.loc[data['Currency'] != 'Total'].itertuples():
            print(row)
            is_deposit = row.Amount > 0
            yield OutputRow(
                TimestampUTC=row.Settle_Date,
                Type=OutputType.FiatDeposit if is_deposit else OutputType.FiatWithdrawal,
                BaseCurrency=row.Currency,
                BaseAmount=abs(row.Amount),
                From=Exchange.Bank if is_deposit else Exchange.Ibkr,
                To=Exchange.Ibkr if is_deposit else Exchange.Bank,
                Description=f'{Exchange.Ibkr} {row.Description}'
            )

        return []

    def __extract_all_tables(self) -> None:
        data = []
        names = []
        for table_name, data_frame in self.__extract_table():
            data.append(data_frame)
            names.append(table_name)

        self.__tables = pandas.Series(data, index=names)

    def __extract_table(self) -> Iterator[tuple[str, DataFrame]]:
        regex_pattern = re.compile(r'^([^,]+),Header,')
        table_lines: str = ''
        table_name: str = ''
        with open(
                self._get_source(),
                encoding='utf-8-sig'  # apparently IBKR uses BOM in their files
        ) as file:
            for line in file:
                regex_match = re.search(regex_pattern, line)
                if regex_match:
                    if table_name:
                        yield self.__parse_table(table_name, table_lines)
                    table_name = regex_match.group(1)
                    table_lines = line
                else:
                    table_lines += line

            if table_name:
                yield self.__parse_table(table_name, table_lines)

    @staticmethod
    def __parse_table(table_name, table_lines) -> tuple[str, DataFrame]:
        # noinspection PyTypeChecker
        data_frame = pandas.read_csv(
            StringIO(table_lines),
            skip_blank_lines=True,
            na_values=["--"]
        )
        data_frame.drop(data_frame.columns[0], axis=1, inplace=True)

        return table_name, data_frame

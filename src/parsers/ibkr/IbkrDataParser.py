import re
from io import StringIO
from typing import Iterator

import pandas
from pandas import DataFrame

from config.types import OutputRow, OutputType, Exchange, TickerAffix
from helpers import data_frames
from helpers.validation import validate, is_nan, is_currency, show_warning_once
from parsers.AbstractDataParser import AbstractDataParser
from parsers.ibkr.types import DepositsAndWithdrawalsRow, FeesRow, ForexTradesRow, Codes, OptionsRow


class IbkrDataParser(AbstractDataParser):
    __base_currency: str

    __date_format = '%Y-%m-%d'
    __datetime_format = '%Y-%m-%d, %H:%M:%S'

    # My IBKR file says: "Trade execution times are displayed in Eastern Time."
    __timezone = 'US/Eastern'

    def run(self) -> None:
        # todo: any validations?
        self.__extract_all_tables()
        data = self.__parse()
        self._save_output(data)

    def __parse(self) -> list[OutputRow]:
        self.__parse_account_information(self.__tables['Account Information'])

        return [
            *(self.__parse_deposits_and_withdrawals(self.__tables['Deposits & Withdrawals'])
              if 'Deposits & Withdrawals' in self.__tables else []),
            *(self.__parse_account_fees(self.__tables['Fees']) if 'Fees' in self.__tables else []),
            *(self.__parse_trades(self.__tables['Trades']) if 'Trades' in self.__tables else []),
        ]

    def __parse_account_information(self, data: DataFrame) -> None:
        base_currency_row = data.loc[data['Field Name'] == 'Base Currency']
        validate(
            condition=base_currency_row.shape[0] == 1,
            error="Account Information has to have Base Currency data.",
            context=data
        )

        self.__base_currency = base_currency_row['Field Value'].iloc[0]
        validate(
            condition=type(self.__base_currency) == str and is_currency(self.__base_currency),
            error="Base currency value has to be correct.",
            context=data
        )

    def __parse_deposits_and_withdrawals(self, data: DataFrame) -> list[OutputRow]:
        data_frames.normalize_column_names(data)
        data_frames.parse_date(data, 'Settle_Date', self.__date_format)

        row: DepositsAndWithdrawalsRow
        for row in data.loc[~data['Currency'].str.startswith('Total')].itertuples():
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

    def __parse_account_fees(self, data: DataFrame) -> list[OutputRow]:
        data_frames.normalize_column_names(data)
        data_frames.parse_date(data, 'Date', self.__date_format)

        row: FeesRow
        for row in data.loc[(data['Header'] == 'Data') & (data['Subtitle'] != 'Total')].itertuples():
            validate(
                condition=row.Subtitle == 'Other Fees',
                error="Only 'Other Fees' with are implemented for 'Fees' table.",
                context=row
            )

            is_fee = row.Amount < 0
            yield OutputRow(
                TimestampUTC=row.Date,
                BaseCurrency=row.Currency,
                BaseAmount=abs(row.Amount),
                From=Exchange.Ibkr,
                To=Exchange.Ibkr,
                Description=f'{Exchange.Ibkr} {row.Subtitle} {"" if is_fee else "(waived)"}: {row.Description}',

                # IBKR can waive a fee under certain conditions, so I think this is a situation where I paid a fee,
                # but the condition for returning the fee were met after the fee was paid. This is classified as 'buy'
                # in order to make it a non-taxable event (I'm receiving back my money after all).
                Type=OutputType.Fee if is_fee else OutputType.Buy,
            )

        return []

    def __parse_trades(self, trade_tables: DataFrame) -> list[OutputRow]:
        for table in trade_tables:
            asset_category_line = table['Asset Category'][0]
            # noinspection PyUnresolvedReferences
            validate(
                condition=(table['Asset Category'] == asset_category_line).all(),
                error="Each Trades category table should have the same asset category for all rows.",
                context=table['Asset Category']
            )

            asset_category = asset_category_line.split(' - ')[0]
            implemented_tables = ['Stocks', 'Equity and Index Options', 'Forex']
            validate(
                condition=asset_category in implemented_tables,
                error=f"Only the following trade tables are implemented: {implemented_tables}",
                context=asset_category_line
            )

            # Not all 'Trades' tables utilize all columns. Empty header (unnamed column) means no data.
            data_frames.remove_columns(table, table.columns[table.columns.str.contains(r'^Unnamed: \d+$')])

            data_frames.remove_columns(table, table.columns[table.columns.get_loc('Asset Category')])
            data_frames.normalize_column_names(table)

            data_frames.parse_date(table, 'Date_Time', self.__datetime_format, self.__timezone)
            table['Code'] = table['Code'].apply(
                lambda code: [] if is_nan(code) or code == ' ' else self.__strings_to_codes(code.split(';'))
            )

            if asset_category == 'Stocks':
                # TODO
                pass
            elif asset_category == 'Equity and Index Options':
                for result in self.__parse_options(table):
                    yield result
            elif asset_category == 'Forex':
                for result in self.__parse_forex_trades(table):
                    yield result

        return []

    def __parse_forex_trades(self, data: DataFrame) -> list[OutputRow]:
        comm_name = f'Comm_in_{self.__base_currency}'
        mtm_name = f'MTM_in_{self.__base_currency}'
        validate(
            condition=comm_name in data.columns and mtm_name in data.columns,
            error="'Comm_in_XXX' and 'MTM_in_XXX' columns have to exist for Forex Trades.",
            context=data.columns
        )
        data.rename(columns={comm_name: 'Comm_in_Base_Currency', mtm_name: 'MTM_in_Base_Currency'}, inplace=True)

        row: ForexTradesRow
        for row in data.loc[(data['Header'] == 'Data')].itertuples():
            validate(
                condition=row.DataDiscriminator == 'Order',
                error="Forex Trade fields should be consistent.",
                context=row
            )
            self.__validate_allowed_codes(row.Code, [Codes.L], 'Forex Trades')

            symbol_values = row.Symbol.split('.')
            validate(
                condition=len(symbol_values) == 2 and is_currency(symbol_values[0]) and is_currency(symbol_values[1]),
                error="Forex Trade 'Symbol' column has to contain two currency values.",
                context=row
            )

            first_currency, second_currency = symbol_values
            validate(
                condition=first_currency != second_currency and second_currency == row.Currency,
                context=row,

                # Tbh, this isn't really consistent. It seems to me that the 'base' currency here is chosen based on
                # some weird rules, and then some transactions are a "buy" of that currency, some "sell". Maybe it's
                # related to the way the trade was initiated or something like that? But this would suggest that
                # currency exchanges are not symmetrical ("buy A => B" == "sell B => A").
                error="Forex Trade currency names have to be consistent.",
            )

            is_buy = row.Quantity > 0
            base_currency = first_currency if is_buy else second_currency
            base_amount = row.Quantity if is_buy else row.Proceeds
            quote_currency = second_currency if is_buy else first_currency
            quote_amount = row.Proceeds if is_buy else row.Quantity
            validate(
                condition=base_amount > 0 > quote_amount,
                error="Buy and sell transactions currency values have to be consistent",
                context=row
            )

            yield OutputRow(
                TimestampUTC=row.Date_Time,
                Type=OutputType.Buy,
                BaseCurrency=base_currency,
                BaseAmount=base_amount,
                QuoteCurrency=quote_currency,
                QuoteAmount=abs(quote_amount),
                FeeCurrency=self.__base_currency,
                FeeAmount=abs(row.Comm_in_Base_Currency),
                From=Exchange.Ibkr,
                To=Exchange.Ibkr,
                Description=f'{Exchange.Ibkr} Forex Trade: '
                            f'from {quote_currency} to {base_currency}{self.__code__values_to_string(row.Code)}'
            )

        return []

    def __parse_options(self, data: DataFrame) -> list[OutputRow]:
        # Please note that the rules below only implement options buying. Writing options is not implemented.
        # On the buy side, the only implemented transactions are: sell and lapse.
        # See https://www.gov.uk/hmrc-internal-manuals/capital-gains-manual/cg55536 for reference.

        row: OptionsRow
        for row in data.loc[(data['Header'] == 'Data')].itertuples():
            validate(
                condition=row.DataDiscriminator == 'Order',
                error="Options Trade fields should be consistent.",
                context=row
            )

            if Codes.O in row.Code:
                yield self.__parse_option_opening_trade(row)
            elif Codes.C in row.Code:
                yield self.__parse_option_closing_trade(row)
            else:
                validate(
                    condition=False,
                    error="Only opening and closing Option Trades are allowed.",
                    context=row
                )

        return []

    def __parse_option_opening_trade(self, row: OptionsRow) -> OutputRow:
        self.__validate_allowed_codes(row.Code, [Codes.O], 'Option Opening Trades')
        validate(
            condition=row.Realized_P_L == 0 and row.Realized_P_L_pct == 0,
            error="Option Opening Trade cannot have Realized Profit",
            context=row
        )
        # noinspection PyChainedComparisons
        validate(
            condition=row.Quantity > 0 and row.Proceeds < 0 and row.Comm_Fee <= 0,
            error="Option Opening Trade has to have values set with expected signs.",
            context=row
        )
        validate(
            condition=-row.Basis == row.Proceeds + row.Comm_Fee,
            error="Option Opening Trade should have its Basis consistent with other fields.",
            context=row
        )

        # Bought option 'sell' and 'expiry' can be both modeled (somewhat?) accurately if we pretend they are stock.
        return OutputRow(
            TimestampUTC=row.Date_Time,
            Type=OutputType.Buy,
            BaseCurrency=self.__get_option_ticker(row.Symbol),
            BaseAmount=row.Quantity,
            QuoteCurrency=row.Currency,
            QuoteAmount=abs(row.Proceeds),
            FeeCurrency=row.Currency,
            FeeAmount=abs(row.Comm_Fee),
            From=Exchange.Ibkr,
            To=Exchange.Ibkr,
            Description=f'{Exchange.Ibkr} Option Trade{self.__code__values_to_string(row.Code)}',
        )

    def __parse_option_closing_trade(self, row: OptionsRow) -> OutputRow:
        print(row)

        self.__validate_allowed_codes(row.Code, [Codes.C, Codes.P, Codes.Ep], 'Option Opening Trades')
        validate(
            condition=round(abs(row.Basis), 4) == round(row.Proceeds - row.Realized_P_L + row.Comm_Fee, 4),
            error="Option Closing Trade should have its Basis consistent with other fields.",
            context=row
        )

        if Codes.Ep in row.Code:  # expired option
            show_warning_once(
                group="Option Lapse",
                message="Lapsed (expired) options are implemented as a sale with value 0.\nThis means that "
                        "cryptotaxcalculator.io thinks that the position value is 0 and will show "
                        "a 'missing market price' warning.\nThose warnings should be dismissed."
            )
            validate(
                condition=row.Quantity < 0 and row.Proceeds == 0 and row.Comm_Fee == 0,
                error="Option Lapse has to have correct values.",
                context=row
            )
        else:  # sold option
            # noinspection PyChainedComparisons
            validate(
                condition=row.Quantity < 0 and row.Proceeds > 0 and row.Comm_Fee <= 0,
                error="Option Closing Trade has to have values set with expected signs.",
                context=row
            )
            validate(
                condition=row.Proceeds + row.Comm_Fee > 0,
                error="Selling an Option for less than the fee is not implemented.",
                context=row
            )

        return OutputRow(
            TimestampUTC=row.Date_Time,
            Type=OutputType.Sell,
            BaseCurrency=self.__get_option_ticker(row.Symbol),
            BaseAmount=abs(row.Quantity),
            FeeCurrency=row.Currency,
            FeeAmount=abs(row.Comm_Fee),
            From=Exchange.Ibkr,
            To=Exchange.Ibkr,
            Description=f'{Exchange.Ibkr} Option Trade{self.__code__values_to_string(row.Code)}',

            # The opening trade FeeAmount is not included in QuoteAmount in cryptotaxcalculator.io import,
            # but the closing trade FeeAmount is. So we have to set the QuoteAmount to a sum of those values.
            QuoteAmount=(row.Proceeds + row.Comm_Fee),
            QuoteCurrency=row.Currency,
        )

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

                # apparently IBKR uses BOM in their files
                encoding='utf-8-sig'
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
            na_values=["--"],
            thousands=','
        )
        data_frames.remove_columns(data_frame, data_frame.columns[0])

        return table_name, data_frame

    def __validate_allowed_codes(self, codes: list[Codes], allowed: list[Codes], context_name: str) -> None:
        validate(
            condition=all(one_code in allowed for one_code in codes),
            error=f"The only codes implemented for {context_name} are: {self.__code__names_to_string(allowed)}.",
            context=codes
        )

    @staticmethod
    def __strings_to_codes(strings: list[str]) -> list[Codes]:
        codes = []
        for one_code in strings:
            try:
                codes.append(Codes[one_code])
            except KeyError:
                validate(
                    condition=False,
                    error=f"The code '{one_code}' is not implemented.",
                    context=strings
                )

        return codes

    @staticmethod
    def __code__names_to_string(codes: list[Codes]) -> str:
        return "'{0}'".format("', '".join([one_code.name for one_code in codes]))

    @staticmethod
    def __code__values_to_string(codes: list[Codes]) -> str:
        important_codes = [
            f"{one_code.name}: '{one_code.value}'" for one_code in codes if one_code.value != ''
        ]
        if len(important_codes) == 0:
            return ''

        return f" ({', '.join(important_codes)})"

    @staticmethod
    def __get_option_ticker(symbol: str) -> str:
        # Note that this only works if the only source of options is IBKR. Otherwise, I may need to normalize the
        # option position name, so options from different exchanges can be pooled together.
        return TickerAffix.Option + symbol

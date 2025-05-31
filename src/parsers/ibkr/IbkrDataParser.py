import re
from io import StringIO
from typing import Iterator, Optional

import pandas
from pandas import DataFrame

from src.config.types import OutputRow, OutputType, Exchange, AssetType
from src.helpers import data_frames
from src.helpers.stock_market import parse_ticker
from src.helpers.validation import validate, is_nan, is_currency
from src.helpers.warnings import show_option_lapse_warning_once, show_dividends_warning_once, \
    show_stock_split_warning_once, show_stock_transfers_warning_once
from src.parsers.AbstractDataParser import AbstractDataParser
from src.parsers.ibkr.types import DepositsAndWithdrawalsRow, FeesRow, ForexTradesRow, StocksAndDerivativesTradesRow, \
    CorporateActionsRow, Codes, TransferRow, InterestRow, WithholdingTaxRow, DividendsRow


class IbkrDataParser(AbstractDataParser):
    __base_currency: str

    __date_format = '%Y-%m-%d'
    __datetime_format = '%Y-%m-%d, %H:%M:%S'

    # My IBKR file says: "Trade execution times are displayed in Eastern Time."
    __timezone = 'US/Eastern'

    def run(self) -> None:
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
            *(self.__parse_interest(self.__tables['Interest']) if 'Interest' in self.__tables else []),
            *(self.__parse_dividends(
                self.__tables['Dividends'],
                self.__tables['Withholding Tax'] if 'Withholding Tax' in self.__tables else None
            ) if 'Dividends' in self.__tables else []),
            *(self.__parse_corporate_actions(self.__tables['Corporate Actions'])
              if 'Corporate Actions' in self.__tables else []),
            *(self.__parse_transfers(self.__tables['Transfers']) if 'Transfers' in self.__tables else []),
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

    def __parse_deposits_and_withdrawals(self, data: DataFrame) -> Iterator[OutputRow]:
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

    def __parse_account_fees(self, data: DataFrame) -> Iterator[OutputRow]:
        data_frames.normalize_column_names(data)
        data_frames.parse_date(data, 'Date', self.__date_format)
        fees_rows = data.loc[(data['Header'] == 'Data') & (data['Subtitle'] != 'Total')]

        # It seems that, at least in my situation, each fee has a 20% tax that is not reported anywhere but
        # in summaries. It's also not refunded if the fee itself is returned.
        sales_tax_value = self.__get_sales_tax()
        fees_taken_sum = fees_rows[fees_rows['Amount'] < 0]['Amount'].sum()
        sales_tax = sales_tax_value / fees_taken_sum if sales_tax_value != 0 and fees_taken_sum != 0 else 0

        row: FeesRow
        for row in fees_rows.itertuples():
            validate(
                condition=row.Subtitle == 'Other Fees',
                error="Only 'Other Fees' with are implemented for 'Fees' table.",
                context=row
            )
            validate(
                condition=row.Currency == self.__base_currency,
                error=f"Only fees in the base currency ({self.__base_currency}) are implemented.",
                context=row
            )

            is_fee = row.Amount < 0
            yield OutputRow(
                TimestampUTC=row.Date,
                BaseCurrency=self.__base_currency,
                BaseAmount=abs(row.Amount),
                From=Exchange.Ibkr,
                To=Exchange.Ibkr,
                Description=f'{Exchange.Ibkr} {row.Subtitle} {"" if is_fee else "(waived)"}: {row.Description}',

                # IBKR can waive a fee under certain conditions, so I think this is a situation where I paid a fee,
                # but the condition for returning the fee were met after the fee was paid. This is classified as 'buy'
                # in order to make it a non-taxable event (I'm receiving back my money after all).
                Type=OutputType.Fee if is_fee else OutputType.Buy,
            )

            if is_fee and sales_tax != 0:
                yield OutputRow(
                    TimestampUTC=row.Date,
                    Type=OutputType.Fee,
                    BaseCurrency=self.__base_currency,
                    BaseAmount=round(abs(row.Amount * sales_tax), 6),
                    From=Exchange.Ibkr,
                    To=Exchange.Ibkr,
                    Description=f'{Exchange.Ibkr} {row.Subtitle} Sales Tax of {sales_tax * 100}%',
                )

    def __parse_trades(self, trade_tables: DataFrame) -> Iterator[OutputRow]:
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
            table = table.sort_values('Date_Time')
            table['Code'] = table['Code'].apply(
                lambda code: [] if is_nan(code) or code == ' ' else self.__strings_to_codes(code.split(';'))
            )

            if asset_category == 'Stocks':
                for result in self.__parse_stocks_and_derivatives(table, AssetType.Stock):
                    yield result
            elif asset_category == 'Equity and Index Options':
                for result in self.__parse_stocks_and_derivatives(table, AssetType.Option):
                    yield result
            elif asset_category == 'Forex':
                for result in self.__parse_forex_trades(table):
                    yield result

    def __parse_forex_trades(self, data: DataFrame) -> Iterator[OutputRow]:
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
            self.__validate_allowed_codes(row.Code, [Codes.L, Codes.AFx], 'Forex Trades')

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
                From=Exchange.Ibkr,
                To=Exchange.Ibkr,
                Description=f'{Exchange.Ibkr} Forex Trade: '
                            f'from {quote_currency} to {base_currency}{self.__code__values_to_string(row.Code)}'
            )

            # To prevent weird "Fee Forwarding" in cryptotaxcalculator.io that results in incorrect base currency
            # total values, the forex fees are added as a separate transaction. This is probably the correct way of
            # treating it anyway, as the those fees are always in the base currency, so they aren't really a part
            # of the main transaction.
            if row.Comm_in_Base_Currency != 0:
                yield OutputRow(
                    TimestampUTC=row.Date_Time,
                    Type=OutputType.Fee,
                    BaseCurrency=self.__base_currency,
                    BaseAmount=abs(row.Comm_in_Base_Currency),
                    From=Exchange.Ibkr,
                    To=Exchange.Ibkr,
                    Description=f'{Exchange.Ibkr} Forex Trade Fee: '
                                f'from {quote_currency} to {base_currency}{self.__code__values_to_string(row.Code)}'
                )

    def __parse_stocks_and_derivatives(self, data: DataFrame, asset_type: AssetType) -> Iterator[OutputRow]:
        # Note for Options:
        # The rules below only implement options buying. Writing options is not implemented.
        # On the buy side, the only implemented transactions are 'sell' (close position) and 'lapse' (expire).
        # See https://www.gov.uk/hmrc-internal-manuals/capital-gains-manual/cg55536 for reference.

        row: StocksAndDerivativesTradesRow
        for row in data.loc[(data['Header'] == 'Data')].itertuples():
            validate(
                condition=row.DataDiscriminator == 'Order',
                error=f"{asset_type} Trade fields should be consistent.",
                context=row
            )
            validate(
                condition=round(abs(row.Basis), 4) == round(abs(row.Proceeds - row.Realized_P_L + row.Comm_Fee), 4),
                error=f"{asset_type} Trade should have its Basis consistent with other fields.",
                context=row
            )

            if Codes.O in row.Code:
                yield self.__parse_stock_or_derivative_opening_trade(row, asset_type)
            elif Codes.C in row.Code:
                yield self.__parse_stock_or_derivative_closing_trade(row, asset_type)
            else:
                validate(
                    condition=False,
                    error=f"Only opening and closing {asset_type} Trades are allowed.",
                    context=row
                )

    def __parse_stock_or_derivative_opening_trade(
            self, row: StocksAndDerivativesTradesRow, asset_type: AssetType
    ) -> OutputRow:
        self.__validate_allowed_codes(row.Code, [Codes.O, Codes.P, Codes.FPA], f'{asset_type} Opening Trades')
        validate(
            condition=row.Realized_P_L == 0 and row.Realized_P_L_pct == 0,
            error=f"{asset_type} Opening Trade cannot have Realized Profit",
            context=row
        )
        # noinspection PyChainedComparisons
        validate(
            condition=row.Quantity > 0 and row.Proceeds < 0 and row.Comm_Fee <= 0 and row.Basis > 0,
            error=f"{asset_type} Opening Trade has to have values set with expected signs.",
            context=row
        )

        # Note for Options:
        # Bought option 'sell' and 'expiry' can be both modeled (somewhat?) accurately if we pretend they are stock.
        return OutputRow(
            TimestampUTC=row.Date_Time,
            Type=OutputType.Buy,
            BaseCurrency=self.__parse_ticker(row.Symbol, asset_type),
            BaseAmount=row.Quantity,
            QuoteCurrency=row.Currency,
            QuoteAmount=abs(row.Proceeds),
            FeeCurrency=row.Currency,
            FeeAmount=abs(row.Comm_Fee),
            From=Exchange.Ibkr,
            To=Exchange.Ibkr,
            Description=f'{Exchange.Ibkr} {asset_type} Trade{self.__code__values_to_string(row.Code)}',
            ReferencePricePerUnit=round(abs(row.Proceeds / row.Quantity), 6),
            ReferencePriceCurrency=row.Currency,
        )

    def __parse_stock_or_derivative_closing_trade(
            self, row: StocksAndDerivativesTradesRow, asset_type: AssetType
    ) -> OutputRow:
        self.__validate_allowed_codes(row.Code, [Codes.C, Codes.P, Codes.Ep, Codes.FPA], f'{asset_type} Closing Trades')

        if Codes.Ep in row.Code:  # expired option
            validate(
                condition=asset_type == AssetType.Option,
                error="Only Option can be expired.",
                context=row
            )
            validate(
                condition=row.Quantity < 0 and row.Proceeds == 0 and row.Comm_Fee == 0 and row.Basis < 0,
                error="Option Lapse has to have correct values.",
                context=row
            )
            show_option_lapse_warning_once()
        else:  # stock or option position close
            # noinspection PyChainedComparisons
            validate(
                condition=row.Quantity < 0 and row.Proceeds > 0 and row.Comm_Fee <= 0 and row.Basis < 0,
                error=f"{asset_type} Closing Trade has to have values set with expected signs.",
                context=row
            )
            validate(
                condition=row.Proceeds + row.Comm_Fee > 0,
                error=f"Selling {asset_type} for less than the fee is not implemented.",
                context=row
            )

        return OutputRow(
            TimestampUTC=row.Date_Time,
            Type=OutputType.Sell,
            BaseCurrency=self.__parse_ticker(row.Symbol, asset_type),
            BaseAmount=abs(row.Quantity),
            FeeCurrency=row.Currency,
            FeeAmount=abs(float(row.Comm_Fee)),
            From=Exchange.Ibkr,
            To=Exchange.Ibkr,
            Description=f'{Exchange.Ibkr} {asset_type} Trade{self.__code__values_to_string(row.Code)}',
            ReferencePricePerUnit=round(abs(row.Proceeds / row.Quantity), 6),
            ReferencePriceCurrency=row.Currency,

            # The opening trade FeeAmount is not included in QuoteAmount in cryptotaxcalculator.io import,
            # but the closing trade FeeAmount is. So we have to set the QuoteAmount to a sum of those values.
            QuoteAmount=(row.Proceeds + row.Comm_Fee),
            QuoteCurrency=row.Currency,
        )

    def __parse_interest(self, data: DataFrame) -> Iterator[OutputRow]:
        data_frames.parse_date(data, 'Date', self.__date_format, self.__timezone)

        row: InterestRow
        for row in data.loc[~data['Currency'].str.startswith('Total')].itertuples():
            validate(
                condition=row.Amount > 0,
                error="Interest amount has to be positive.",
                context=row
            )

            yield OutputRow(
                TimestampUTC=row.Date,
                Type=OutputType.Interest,
                BaseCurrency=row.Currency,
                BaseAmount=row.Amount,
                From=Exchange.Ibkr,
                To=Exchange.Ibkr,
                Description=f'{Exchange.Ibkr} {row.Description}'
            )

    def __parse_dividends(self, data: DataFrame, tax_data: Optional[DataFrame]) -> Iterator[OutputRow]:
        # Because of the dividends taxation rules, the withholding tax should not be a separate item to the dividend
        # itself, so I'm subtracting the tax from the dividend base value
        taxes = {}
        if tax_data is not None:
            tax_row: WithholdingTaxRow
            for tax_row in tax_data.loc[~tax_data['Currency'].str.startswith('Total')].itertuples():
                action_type = re.search(r'^(.*)\(.*?\) Cash Dividend .*$', tax_row.Description)
                validate(
                    condition=action_type is not None and len(action_type.groups()) == 1,
                    error="The only type of Dividend implemented is Cash Dividend (Withholding Tax row).",
                    context=tax_row
                )

                key = f'{action_type.group(1)} {tax_row.Currency} {tax_row.Date}'
                taxes[key] = tax_row.Amount

        data_frames.parse_date(data, 'Date', self.__date_format, self.__timezone)
        row: DividendsRow
        for row in data.loc[~data['Currency'].str.startswith('Total')].itertuples():
            action_type = re.search(r'^(.*)\(.*?\) Cash Dividend .*$', row.Description)
            validate(
                condition=action_type is not None and len(action_type.groups()) == 1,
                error="The only type of Dividend implemented is Cash Dividend.",
                context=row
            )

            raw_ticker = action_type.group(1)
            key = f'{raw_ticker} {row.Currency} {row.Date.strftime(self.__date_format)}'
            tax_value = taxes[key] if key in taxes else 0

            validate(
                condition=row.Amount > 0 >= tax_value and abs(tax_value) < row.Amount,
                error="Dividend and Withholding Tax amounts should be correct.",
                context=[tax_value, row]
            )

            show_dividends_warning_once()

            yield OutputRow(
                TimestampUTC=row.Date,
                Type=OutputType.FiatDeposit,
                BaseCurrency=row.Currency,
                BaseAmount=round(row.Amount + tax_value, 6),
                From=Exchange.Dividends,
                To=Exchange.Ibkr,
                Description=f'{Exchange.Ibkr} {row.Description}'
            )

    def __parse_corporate_actions(self, data: DataFrame) -> Iterator[OutputRow]:
        data_frames.normalize_column_names(data)
        data_frames.parse_date(data, 'Date_Time', self.__datetime_format, self.__timezone)
        data_frames.parse_date(data, 'Report_Date', self.__date_format, self.__timezone)

        row: CorporateActionsRow
        for row in data.loc[~data['Asset_Category'].str.startswith('Total')].itertuples():
            validate(
                condition=row.Asset_Category.startswith('Stocks '),
                error="Only stock Corporate Actions are implemented.",
                context=row
            )

            action_type = re.match(r'^(.*)\(.*?\) Split .*$', row.Description)
            if action_type:
                validate(
                    condition=row.Quantity > 0 and row.Proceeds == row.Value == row.Realized_P_L == 0,
                    error="Corporate Action stock split has to have correct numeric fields values.",
                    context=row
                )
                validate(
                    condition=is_nan(row.Code),
                    error="No Codes are allowed for stock split Corporate Action.",
                    context=row
                )

                show_stock_split_warning_once()

                yield OutputRow(
                    TimestampUTC=row.Date_Time,
                    Type=OutputType.ChainSplit,  # See EtoroDataParser stock split for explanation.
                    BaseCurrency=self.__parse_ticker(action_type.group(1), AssetType.Stock),
                    BaseAmount=row.Quantity,
                    From=Exchange.Ibkr,
                    To=Exchange.Ibkr,
                    Description=f'{Exchange.Ibkr} {row.Description}',
                )
                continue

            action_type = re.match(r'^(.*)\(.*?\) Merged\(Acquisition\) .*$', row.Description)
            if action_type:
                validate(
                    condition=row.Quantity < 0 < row.Proceeds,
                    error="Corporate Action merge/acquisition has to have correct numeric fields values.",
                    context=row
                )
                validate(
                    condition=is_nan(row.Code),
                    error="No Codes are allowed for merge/acquisition Corporate Action.",
                    context=row
                )

                yield OutputRow(
                    TimestampUTC=row.Date_Time,
                    Type=OutputType.Sell,
                    BaseCurrency=self.__parse_ticker(action_type.group(1), AssetType.Stock),
                    BaseAmount=abs(row.Quantity),
                    QuoteCurrency=row.Currency,
                    QuoteAmount=round(row.Proceeds, 6),
                    From=Exchange.Ibkr,
                    To=Exchange.Ibkr,
                    Description=f'{Exchange.Ibkr} {row.Description}',
                    ReferencePricePerUnit=round(abs(row.Proceeds / row.Quantity), 6),
                    ReferencePriceCurrency=row.Currency,
                )
                continue

            validate(
                condition=False,
                error="The only implemented Corporate Actions are 'stock split' and 'merger/acquisition'.",
                context=row
            )

    def __parse_transfers(self, data: DataFrame) -> Iterator[OutputRow]:
        data_frames.normalize_column_names(data)
        data_frames.parse_date(data, 'Date', self.__date_format, self.__timezone)

        show_stock_transfers_warning_once()

        row: TransferRow
        for row in data.loc[~data['Asset_Category'].str.startswith('Total')].itertuples():
            validate(
                condition=row.Asset_Category.startswith('Stocks ') and row.Direction == 'In',
                error="Only incoming stock Transfers are implemented.",
                context=row
            )
            validate(
                condition=(row.Type == 'FOP' or row.Type == 'Internal') and is_nan(row.Xfer_Price),
                error="Only 'FOP (Free Of Payment)' and 'Internal' Transfers are implemented.",
                context=row
            )
            validate(
                condition=row.Qty > 0 and row.Market_Value > 0 and row.Realized_P_L == 0 and row.Cash_Amount == 0,
                error="Transfer numeric fields have to be correct.",
                context=row
            )
            validate(
                condition=is_nan(row.Code),
                error="No Codes are allowed for Transfers.",
                context=row
            )

            yield OutputRow(
                TimestampUTC=row.Date,
                Type=OutputType.Receive,
                BaseCurrency=self.__parse_ticker(row.Symbol, AssetType.Stock),
                BaseAmount=row.Qty,
                From=Exchange.Unknown,
                To=Exchange.Ibkr,
                Description=f'{Exchange.Ibkr} incoming stock transfer from: '
                            f'{"unknown" if is_nan(row.Xfer_Company) else row.Xfer_Company} '
                            f'/ {"unknown" if is_nan(row.Xfer_Account) else row.Xfer_Account}'
            )

    def __get_sales_tax(self) -> float:
        data = self.__tables['Change in NAV']
        sales_tax_fields = data.loc[data['Field Name'] == 'Sales Tax']
        if sales_tax_fields.shape[0] == 0:
            return 0.0

        validate(
            condition=sales_tax_fields.shape[0] == 1,
            error="I have no idea how to parse more than one Sales Tax field.",
            context=sales_tax_fields
        )

        sales_tax_value = sales_tax_fields['Field Value'].iloc[0]
        validate(
            condition=sales_tax_value < 0,
            error="Sales Tax should be number.",
            context=sales_tax_fields
        )

        return sales_tax_value

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
    def __parse_ticker(ticker: str, asset_type: AssetType) -> str:
        # Note that just passing the option name works only if the only source of options is IBKR. Otherwise, I may
        # need to normalize the option position names, so options from different exchanges can be pooled together.
        return parse_ticker(ticker, Exchange.Ibkr, asset_type)

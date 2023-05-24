import re
from typing import Optional, Union

import pandas
from pydantic import validate_arguments

from src.helpers import data_frames, date_time
from src.config.types import OutputRow, OutputType, Exchange, AssetType
from src.helpers.stock_market import parse_ticker
from src.helpers.validation import validate, is_nan
from src.helpers.warnings import show_warning_once, show_stock_split_warning_once, show_dividends_warning_once
from src.parsers.AbstractDataParser import AbstractDataParser
from src.parsers.etoro.types import TransactionRow, PositionRow


class EtoroDataParser(AbstractDataParser):
    # I only allow USD as the base fiat currency, as this is the only file example I had.
    __base_fiat = 'USD'

    # I don't know if other formats are possible in the source file. This is the one that I have.
    __date_format = '%d/%m/%Y %H:%M:%S'

    def run(self):
        self.__pre_validate()
        data = self.__parse()
        self._save_output(data)

    def __pre_validate(self):
        account_summary = pandas.read_excel(io=self._get_source(), sheet_name="Account Summary", header=1, index_col=0)
        validate(
            condition=account_summary.loc['Currency']['Totals'] == self.__base_fiat,
            error=f"Only {self.__base_fiat} accounts are supported. I didn't have any other examples.",
            context=account_summary
        )

    def __parse(self) -> list[OutputRow]:
        self.__transactions = pandas.read_excel(
            io=self._get_source(), sheet_name="Account Activity", na_values="-", keep_default_na=False,
            converters={'Details': lambda x: '' if x == '-' else x}
        )
        data_frames.normalize_column_names(self.__transactions)
        data_frames.parse_date(self.__transactions, 'Date', self.__date_format)

        self.__positions = pandas.read_excel(
            io=self._get_source(), sheet_name="Closed Positions", index_col="Position ID", keep_default_na=False
        )
        data_frames.normalize_column_names(self.__positions)
        data_frames.parse_date(self.__positions, 'Open_Date', self.__date_format)
        data_frames.parse_date(self.__positions, 'Close_Date', self.__date_format)
        self.__validate_positions()

        data: list[OutputRow] = []
        row: TransactionRow
        for row in self.__transactions.itertuples():
            row_data = self.__parse_transaction(row, data_frames.get_one_by_key(self.__positions, row.Position_ID))
            if row_data:
                data.append(row_data)

        return data

    @validate_arguments
    def __parse_transaction(
            self, transaction: TransactionRow, position: Optional[PositionRow]
    ) -> Optional[OutputRow]:
        validate(
            condition=transaction.NWA == 0,
            error="What is NWA? It's always 0.00 in my case.",
            context=transaction
        )
        if transaction.Type == 'Edit Stop Loss':
            return None

        validate(
            condition=position is None or transaction.Asset_type == position.Type,
            error="Asset type from transactions and positions should be the same.",
            context=[transaction, position]
        )
        if transaction.Type == 'Withdraw Fee':
            if transaction.Amount != 0:
                show_warning_once(
                    group="Withdrawal Fees",
                    message="It appears that there is a withdrawal fee. Saving this data is NOT IMPLEMENTED."
                )
            return None

        if position is None:
            if transaction.Type == 'Deposit':
                return self.__parse_deposit_or_withdrawal(transaction, is_deposit=True)
            if transaction.Type == 'Withdraw Request':
                return self.__parse_deposit_or_withdrawal(transaction, is_deposit=False)
            if transaction.Type == 'Interest Payment':
                return self.__parse_interest_payment(transaction)

        if position is not None and transaction.Asset_type != 'CFD':
            if transaction.Type == 'Open Position':
                return self.__parse_open_position(transaction, position)
            if transaction.Type == 'Position closed':
                return self.__parse_close_position(transaction, position)
            if transaction.Type == 'corp action: Split':
                return self.__parse_stock_split(transaction, position)
            if transaction.Type == 'Dividend':
                return self.__parse_dividend(transaction)

        if position is not None and transaction.Asset_type == 'CFD':
            if transaction.Type == 'Open Position':
                return self.__parse_cfd_open_position(transaction)
            if transaction.Type == 'Rollover Fee':
                return self.__parse_cfd_rollover_fee(transaction, position)
            if transaction.Type == 'Position closed':
                return self.__parse_cfd_close_position(transaction, position)

        raise Exception(
            f"Row {str(transaction.Index)} of type '{transaction.Type}' cannot be parsed."
            "This is probably not implemented because I did not encounter it in my file."
        )

    def __parse_deposit_or_withdrawal(self, transaction: TransactionRow, is_deposit: bool) -> OutputRow:
        self.__validate_fiat_transaction(transaction, "Deposit" if is_deposit else "Withdrawal")

        return OutputRow(
            Type=OutputType.FiatDeposit if is_deposit else OutputType.FiatWithdrawal,
            From=Exchange.Bank if is_deposit else Exchange.Etoro,
            To=Exchange.Etoro if is_deposit else Exchange.Bank,
            Description=f'{Exchange.Etoro} {transaction.Type} {transaction.Details}'.strip(),

            # What offset are dates in eToro files? I think it's some US timezone, but I'm not really sure.
            # I treat it as UTC for simplicity though. This could mess up "same day rule" but I have no idea how to
            # interpret "same day" even. Is it UK timezone? My timezone at the time of transaction? Exchange timezone?
            # Broker timezone? ...
            # This problem is not unique to eToro, so I think just using whatever time is supplied is a decent option.
            TimestampUTC=transaction.Date,

            # Whatever is the actual transaction.Details currency for a deposit, it gets converted to USD as the
            # only currency I parse (this is validated in self.__pre_validate). So, I can skip the conversion, which
            # is not even recorded in the eToro file, and use transaction.Amount as the USD amount.
            BaseCurrency=self.__base_fiat,
            BaseAmount=abs(transaction.Amount),

            # Fiat transactions do not have IDs on eToro
            ID='',
        )

    def __parse_interest_payment(self, transaction: TransactionRow) -> OutputRow:
        self.__validate_fiat_transaction(transaction, 'Interest Payment')
        validate(
            condition=transaction.Details == '' and is_nan(transaction.Units),
            error=f"Interest Payment has unexpected data.",
            context=transaction
        )

        return OutputRow(
            TimestampUTC=transaction.Date,
            Type=OutputType.Interest,
            BaseCurrency=self.__base_fiat,
            BaseAmount=transaction.Amount,
            From=Exchange.Etoro,
            To=Exchange.Etoro,
            Description=f'{Exchange.Etoro} {transaction.Type}',

            # Fiat transactions do not have IDs on eToro
            ID='',
        )

    @staticmethod
    def __validate_fiat_transaction(transaction: TransactionRow, operation_name: str) -> None:
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

    def __parse_open_position(self, transaction: TransactionRow, position: PositionRow) -> OutputRow:
        ticker = self.__parse_ticker(transaction.Details, transaction.Asset_type)
        validate(
            # I found a case where a transaction date and position date differed by 1 second. ...
            condition=date_time.almost_identical(transaction.Date, position.Open_Date, offset_sec=1)
                      and transaction.Amount == position.Amount,
            error="Open position data is not consistent between Transaction and Position.",
            context=[transaction, position]
        )
        validate(
            condition=transaction.Realized_Equity_Change == 0,
            error="Stock-like asset Open Transactions cannot change Realized Equity.",
            context=[transaction, position]
        )

        other_partial_amounts = self.__get_all_other_partial_trade_positions(position)['Amount'].sum()

        return OutputRow(
            TimestampUTC=transaction.Date,
            Type=OutputType.Buy,
            BaseCurrency=ticker,
            BaseAmount=transaction.Units,
            From=Exchange.Etoro,
            To=Exchange.Etoro,
            ID=self.__make_id(transaction.Position_ID),
            Description=f'{Exchange.Etoro} {position.Action}: {transaction.Type}',

            # [1] Positions that are closed in more than one transaction require matching the opening trade with other
            # positions that have unrelated Position IDs, in order to get the full open price. This match is just
            # a guess and could be wrong. Alternatively a fake 'open' position could be created. Both of those methods
            # will fail if the opening trades are not in the same file as the closing trades. A third option would be
            # to ignore all opening trades and create them just on close, but this would lead to incorrect HMRC
            # section 104 calculations.
            QuoteAmount=round(transaction.Amount + other_partial_amounts, 4),
            QuoteCurrency=self.__base_fiat,
        )

    def __parse_close_position(self, transaction: TransactionRow, position: PositionRow) -> OutputRow:
        ticker = self.__parse_ticker(transaction.Details, transaction.Asset_type)
        validate(
            condition=date_time.almost_identical(transaction.Date, position.Close_Date, offset_sec=1)
                      and transaction.Amount == position.Profit,
            error="Data is not consistent between Transaction and Position.",
            context=[transaction, position]
        )
        validate(
            condition=transaction.Amount == transaction.Realized_Equity_Change,
            error="Transaction close amount is not equal to equity change.",
            context=transaction
        )

        return OutputRow(
            TimestampUTC=transaction.Date,
            Type=OutputType.Sell,
            BaseCurrency=ticker,
            BaseAmount=position.Units,
            QuoteCurrency=self.__base_fiat,
            From=Exchange.Etoro,
            To=Exchange.Etoro,
            ID=self.__make_id(transaction.Position_ID),
            Description=f'{Exchange.Etoro} {position.Action}: {transaction.Type}',

            # For open transaction "amount" is the amount. But for closing transaction it's actually the profit.
            # I don't understand how this make sense, but it is what it is.
            QuoteAmount=round(position.Amount + transaction.Amount, 4),
        )

    def __parse_stock_split(self, transaction: TransactionRow, position: PositionRow) -> OutputRow:
        validate(
            condition=transaction.Amount == 0 and is_nan(transaction.Units)
                      and transaction.Realized_Equity_Change == 0 and transaction.Balance == 0,
            error="Unexpected data in stock split transaction.",
            context=transaction
        )

        details_split = re.match(r'^(\w+) (\d+):(\d+)$', transaction.Details)
        validate(
            condition=bool(details_split),
            error=f"The transaction information '{transaction.Details}' for a stock split is incorrect.",
            context=transaction
        )

        ticker, split_to, split_from = details_split.groups()
        ticker = self.__parse_ticker(ticker, transaction.Asset_type)
        split_from = int(split_from)
        split_to = int(split_to)
        validate(
            condition=split_from == 1,
            error="Only simple 'x:1' stock splits are supported.",
            context=transaction
        )
        validate(
            condition=self.__get_transactions_of_type(position.Index, 'corp action: Split').shape[0] == 1,
            error="Only one stock split per Position ID is supported.",
            context=transaction
        )

        show_stock_split_warning_once()

        return OutputRow(
            TimestampUTC=transaction.Date,
            From=Exchange.Etoro,
            To=Exchange.Etoro,
            ID=self.__make_id(transaction.Position_ID),
            Description=f'{Exchange.Etoro} {position.Action}: {transaction.Type}',

            # This seems like the best approximation of a stock split. The new shares appear out of nowhere,
            # as a result of a corporate action. Even though cryptotaxcalculator.io says that this transaction type
            # should have 0 cost basis, this is not the case, and requires setting this by hand, by ignoring the
            # 'missing market price' warning after import. At least that's how it works currently.
            # After that, because of HMRC Section 104 rules, this should have the cost basis correctly adjusted,
            # because the 0 cost basis will proportionally 'steal' the cost basis from existing shares. I think.
            # Plus, the stock split actually IS a 0 cost basis share issue, so it fits.
            Type=OutputType.ChainSplit,

            # I have to compute the number of new shares issued during the stock split as this isn't mentioned.
            BaseAmount=round(position.Units - position.Units / split_to, 8),
            BaseCurrency=ticker,

        )

    def __parse_dividend(self, transaction: TransactionRow) -> OutputRow:
        # The 'Dividends' tab contains one additional information, which is the "Withholding Tax Rate",
        # but I'm not sure what to do with it, so I just ignore it along with the whole 'Dividends' sheet.

        validate(
            condition=is_nan(transaction.Units),
            error="Dividend cannot have units.",
            context=transaction
        )

        show_dividends_warning_once()

        return OutputRow(
            TimestampUTC=transaction.Date,
            Type=OutputType.FiatDeposit,
            BaseCurrency=self.__base_fiat,
            BaseAmount=transaction.Amount,
            From=Exchange.Dividends,
            To=Exchange.Etoro,
            ID=self.__make_id(transaction.Position_ID),
            Description=f'{Exchange.Etoro} {transaction.Type}: {transaction.Details}'
        )

    @staticmethod
    def __parse_cfd_open_position(transaction: TransactionRow) -> None:
        validate(
            condition=transaction.Realized_Equity_Change == 0,
            error="CFD Open Transactions with opening Realized Equity Change are not supported at this time.",
            context=transaction
        )

        # Since CFDs are taxed only by profits and losses, without any additional rules, the open trade does
        # not matter, and couldn't even be correctly entered into cryptotaxcalculator.io. Also, since we validate
        # realized equity change above, we probably don't have to care about any initial fees.
        return None

    def __parse_cfd_rollover_fee(self, transaction: TransactionRow, position: PositionRow) -> OutputRow:
        validate(
            condition=transaction.Amount == transaction.Realized_Equity_Change and transaction.Amount < 0
                      and is_nan(transaction.Units),
            error="CFD rollover fee transaction has unexpected values.",
            context=transaction
        )

        return OutputRow(
            Type=OutputType.RealizedLoss,
            BaseCurrency=self.__base_fiat,
            BaseAmount=transaction.Amount,
            From=Exchange.Etoro,
            To=Exchange.CFDs,
            ID=self.__make_id(transaction.Position_ID),
            Description=f'{Exchange.Etoro} {transaction.Asset_type} {transaction.Details} for: '
                        + self.__get_original_cfd_position_info(position),

            # It is not clear to me whether CFDs should be taxed all at the point of closing transaction,
            # or at the point of expense occurring. The latter makes more sense, so I just parse every transaction
            # separately.
            TimestampUTC=transaction.Date
        )

    def __parse_cfd_close_position(self, transaction: TransactionRow, position: PositionRow) -> OutputRow:
        validate(
            condition=transaction.Amount == position.Profit,
            error="CFD closing transaction is not consistent with position entry.",
            context=[transaction, position]
        )
        validate(
            condition=transaction.Amount == transaction.Realized_Equity_Change,
            error="CFD closing position is inconsistent.",
            context=transaction
        )

        is_profit = transaction.Amount > 0
        return OutputRow(
            TimestampUTC=transaction.Date,
            Type=OutputType.RealizedProfit if is_profit else OutputType.RealizedLoss,
            ID=self.__make_id(transaction.Position_ID),
            Description=f'{Exchange.Etoro} {transaction.Asset_type} {transaction.Type} for: '
                        + self.__get_original_cfd_position_info(position),

            # Perhaps somewhat more detailed information about CFDs would be useful, for example with asset name
            # as a "currency" as with stock, instead of just cramming it into description and pretending the currency
            # is just USD. Especially that it shows in a slightly confusing way in the final report that way.
            # However, this "advanced manual CSV" import does not accept field combinations with "realized-profit" and
            # "realized-loss" transactions that would allow this to be exported to cryptotaxcalculator.io.
            # So, in the end, just a simple dollar amount seems like the best option. And it's relatively compatible
            # with what HMRC requires.
            BaseCurrency=self.__base_fiat,
            BaseAmount=abs(transaction.Amount),

            # Another weird thing with this transaction type. This is required for fiat amounts to be counted
            # correctly at exchange, even though both transaction types are disposals.
            From=Exchange.CFDs if is_profit else Exchange.Etoro,
            To=Exchange.Etoro if is_profit else Exchange.CFDs
        )

    def __validate_positions(self):
        position: PositionRow
        for position in self.__positions.itertuples():
            total_dividends = self.__get_transactions_of_type(position.Index, 'Dividend')['Amount'].sum()
            validate(
                condition=position.Type == 'CFD' or position.Rollover_Fees_and_Dividends == total_dividends,
                error="Stock-like assets should have no rollover fees and consistent information about dividends.",
                context=position
            )
            validate(
                condition=position.Type == 'CFD' or position.Leverage == 1,
                error="Only CFDs can be leveraged.",
                context=position
            )
            validate(
                condition=self.__check_partial_positions_consistency(position),
                error=f"Please use a source file with transaction data starting on {position.Open_Date.date()} "
                      f"or earlier in order to include the opening trade for position '{position.Index}'. "
                      "If this is a partial position, this is required to calculate the opening price correctly.",
                context=position
            )

    def __check_partial_positions_consistency(self, position: PositionRow) -> bool:
        # [2] This check is done to prevent issues with situation described in "[1]". It should ensure consistency.
        # I make sure that for any position that does not have an opening trade itself, which means it could be
        # a partial position, an opening trade for the original position exists in the file. This way the opening
        # price can be computed correctly.
        if self.__get_transactions_of_type(position.Index, 'Open Position').shape[0] == 1:
            return True
        for index in self.__get_all_other_partial_trade_positions(position).index:
            if self.__get_transactions_of_type(index, 'Open Position').shape[0] == 1:
                return True
        return False

    def __get_all_other_partial_trade_positions(self, position: PositionRow) -> pandas.DataFrame:
        # This is a terrible way of trying to match partially closed positions, but I don't see a better way,
        # and I have to find the additional records in order to get the real amount paid.
        return self.__positions.loc[
            (self.__positions['Open_Date'] == position.Open_Date)
            & (self.__positions.index != position.Index)
            & (self.__positions['Type'] == position.Type)
            ]

    def __get_transactions_of_type(self, position_id: Union[str, int], transaction_type: str) -> pandas.DataFrame:
        return self.__transactions.loc[
            (self.__transactions['Position_ID'] == str(position_id))
            & (self.__transactions['Type'] == transaction_type)
            ]

    @staticmethod
    def __make_id(base_id: str) -> str:
        return f'{Exchange.Etoro}:{base_id}'

    @staticmethod
    def __parse_ticker(ticker: str, asset_type: str) -> str:
        return parse_ticker(ticker, Exchange.Etoro, AssetType.Crypto if asset_type == 'Crypto' else AssetType.Stock)

    @staticmethod
    def __get_original_cfd_position_info(position) -> str:
        leverage = 'without leverage' if position.Leverage == 1 else f'with {position.Leverage}x leverage'
        return f'{position.Action} on {position.Open_Date} {leverage}'

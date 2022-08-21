from logging import warning
from typing import Optional, Union

import pandas
from pydantic import validate_arguments

from helpers import data_frames
from helpers.csv import save_output
from config.types import OutputRow, OutputType, Exchange, TickerSuffix
from helpers.stock_market import parse_ticker
from helpers.validation import validate
from config.config import translate_tickers_etoro
from parsers.etoro_types import TransactionRow, PositionRow


class EtoroImport:
    # I only allow USD as the base fiat currency, as this is the only file example I had.
    __base_fiat = 'USD'

    # I don't know if other formats are possible in the source file. This is the one that I have.
    __date_format = '%d/%m/%Y %H:%M:%S'

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
            condition=account_summary.loc['Currency']['Totals'] == self.__base_fiat,
            error=f"Only {self.__base_fiat} accounts are supported. I didn't have any other examples.",
            context=account_summary
        )

    def __parse(self) -> list[OutputRow]:
        self.__transactions = pandas.read_excel(
            io=self.__source, sheet_name="Account Activity", na_values="-", keep_default_na=False,
            converters={'Details': lambda x: '' if x == '-' else x}
        )
        data_frames.remove_column_spaces(self.__transactions)
        data_frames.parse_date(self.__transactions, 'Date', self.__date_format)

        self.__positions = pandas.read_excel(
            io=self.__source, sheet_name="Closed Positions", index_col="Position ID", keep_default_na=False
        )
        data_frames.remove_column_spaces(self.__positions)
        data_frames.parse_date(self.__positions, 'Open_Date', self.__date_format)
        data_frames.parse_date(self.__positions, 'Close_Date', self.__date_format)

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
        validate(
            condition=position is None or position.Type == 'CFD' or position.Leverage == 1,
            error="Only CFDs can be leveraged.",
            context=position
        )

        if transaction.Type == 'Deposit' and position is None:
            return self.__parse_deposit_or_withdrawal(transaction, is_deposit=True)

        if transaction.Type == 'Withdraw Request' and position is None:
            return self.__parse_deposit_or_withdrawal(transaction, is_deposit=False)

        if transaction.Type == 'Withdraw Fee':
            if transaction.Amount != 0:
                warning("It appears that there is a withdrawal fee. Saving this data is NOT IMPLEMENTED.")
            return None

        if transaction.Type == 'Open Position' and position is not None and (
                transaction.Asset_type == 'Crypto' or transaction.Asset_type == 'ETF'):
            return self.__parse_open_position(
                transaction, position, TickerSuffix.Empty if transaction.Asset_type == 'Crypto' else TickerSuffix.Stock
            )

        if transaction.Type == 'Position closed' and position is not None and (
                transaction.Asset_type == 'Crypto' or transaction.Asset_type == 'ETF'):
            return self.__parse_close_position(
                transaction, position, TickerSuffix.Empty if transaction.Asset_type == 'Crypto' else TickerSuffix.Stock
            )

        exclusions = {'Open Position', 'Position closed', 'corp action: Split', 'Dividend', 'Rollover Fee',
                      'Interest Payment'}
        if transaction.Asset_type != 'Crypto' and transaction.Asset_type != 'ETF' and transaction.Type in exclusions:
            # warning(f'{transaction.Type} is yet to be implemented')
            return None  # TODO

        raise Exception(
            f"Row {str(transaction.Index)} of type '{transaction.Type}' cannot be parsed."
            "This is probably not implemented because I did not encounter it in my file."
        )

    def __parse_deposit_or_withdrawal(self, transaction: TransactionRow, is_deposit: bool) -> OutputRow:
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
            BaseAmount=transaction.Amount if is_deposit else -transaction.Amount,

            # Deposits and withdrawals do not have IDs on eToro
            ID='',
        )

    def __parse_open_position(self, transaction: TransactionRow, position: PositionRow,
                              ticker_suffix: TickerSuffix) -> OutputRow:
        ticker = parse_ticker(transaction.Details, translate_tickers_etoro, ticker_suffix)
        validate(
            condition=transaction.Date == position.Open_Date and transaction.Amount == position.Amount,
            error="Data is consistent between Transaction and Position.",
            context=[transaction, position]
        )
        validate(
            condition=transaction.Realized_Equity_Change == 0 and position.Rollover_Fees_and_Dividends == 0,
            error="Stock and Crypto Open Transactions do not change Realized Equity and have no Fees.",
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

    def __parse_close_position(self, transaction: TransactionRow, position: PositionRow,
                               ticker_suffix: TickerSuffix) -> OutputRow:
        ticker = parse_ticker(transaction.Details, translate_tickers_etoro, ticker_suffix)
        validate(
            condition=transaction.Date == position.Close_Date and transaction.Amount == position.Profit,
            error="Data is consistent between Transaction and Position.",
            context=[transaction, position]
        )
        validate(
            condition=transaction.Amount == transaction.Realized_Equity_Change,
            error="Transaction close amount is equal to equity change.",
            context=transaction
        )
        validate(
            condition=position.Rollover_Fees_and_Dividends == 0,
            error="Stock and Crypto Closing Transactions have no Fees.",
            context=[transaction, position]
        )
        validate(
            condition=self.__check_partial_positions_consistency(position),
            error=f"Please use a source file with transaction data starting at least on {position.Open_Date.date()} "
                  f"in order to include the opening trade for position '{position.Index}'. If this is a partial "
                  f"position, this is required to calculate the opening price correctly.",
            context=position
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

    def __check_partial_positions_consistency(self, position: PositionRow) -> bool:
        # [2] This check is done to prevent issues with situation described in "[1]". It should ensure consistency.
        # I make sure that for any position that does not have an opening trade itself, which means it could be
        # a partial position, an opening trade for the original position exists in the file. This way the opening
        # price can be computed correctly.
        if self.__get_matching_open_transactions(position.Index).shape[0] == 1:
            return True
        for index in self.__get_all_other_partial_trade_positions(position).index:
            if self.__get_matching_open_transactions(index).shape[0] == 1:
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

    def __get_matching_open_transactions(self, position_id: Union[str, int]) -> pandas.DataFrame:
        return self.__transactions.loc[
            (self.__transactions['Position_ID'] == str(position_id))
            & (self.__transactions['Type'] == 'Open Position')
            ]

    @staticmethod
    def __make_id(base_id: str) -> str:
        return f'{Exchange.Etoro}:{base_id}'

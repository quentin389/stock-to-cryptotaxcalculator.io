import locale
import re
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

import pandas
from pandas import DataFrame, to_datetime

from config.config import translate_tickers
from config.types import OutputRow, Exchange, AssetType, OutputType
from helpers import data_frames
from helpers.stock_market import parse_ticker
from helpers.validation import validate, is_nan
from helpers.warnings import show_stock_transfers_warning_once, show_dividends_warning_once
from parsers.AbstractDataParser import AbstractDataParser
from parsers.ig.types import TradeRow, TransactionRow, Trade


class IgDataParser(AbstractDataParser):
    __trades: DataFrame
    __transactions: DataFrame

    def __init__(self, source: str, second_source: str, target: str):
        super().__init__(source, target)
        self.__second_source = second_source

    def run(self) -> None:
        locale.setlocale(locale.LC_ALL, 'en_GB.UTF-8')
        self.__parse_files()
        data = [*self.__parse()]
        self._save_output(data)

    def __parse(self) -> list[OutputRow]:
        trade_row: TradeRow
        for trade_row in self.__trades.itertuples():
            for result in self.__parse_trade_row(trade_row):
                yield result

        transaction: TransactionRow
        for transaction in self.__transactions.itertuples():
            result = self.__parse_transaction(transaction)
            if result:
                yield result

        return []

    def __parse_trade_row(self, trade_row: TradeRow) -> list[OutputRow]:
        # noinspection PyTypeChecker
        trade_data = Trade(
            Date=to_datetime(f'{trade_row.Date} {trade_row.Time}', format='%d-%m-%Y %H:%M:%S'),
            Trade=trade_row
        )
        self.__add_transactions_to_trade(trade_data)

        if trade_data.Trade.Activity == 'TRADE' and trade_data.Trade.Direction == 'BUY':
            return self.__parse_buy_trade(trade_data)
        if trade_data.Trade.Activity == 'TRADE' and trade_data.Trade.Direction == 'SELL':
            return self.__parse_sell_trade(trade_data)
        if trade_data.Trade.Activity == 'CORPORATE ACTION':
            return self.__parse_corporate_action(trade_data)
        if trade_data.Trade.Activity == 'TRANSFER':
            return self.__parse_transfer(trade_data)

        validate(
            condition=False,
            error=f'The activity "{trade_data.Trade.Activity}" with direction "{trade_data.Trade.Direction}" '
                  'is not implemented.',
            context=trade_data.Trade
        )

    def __parse_buy_trade(self, trade_data: Trade) -> list[OutputRow]:
        trade = trade_data.Trade
        commission = trade_data.Commission
        consideration = trade_data.Consideration
        ticker = self.__parse_ticker(trade.Market)
        self.__validate_trade(trade_data)

        validate(
            condition=trade_data.Fee is None,
            error='Fees ("Charges") for opening trades are not implemented.',
            context=trade_data
        )
        validate(
            condition=0 > trade.Consideration == self.__round(trade.Price * trade.Quantity / -100),
            error="Trade price fields should be internally consistent.",
            context=trade
        )
        validate(
            condition=consideration.Transaction_type == 'WITH' and consideration.ProfitAndLoss ==
                      consideration.Currency + self.__format_money(consideration.PL_Amount),
            error="Consideration fields should be internally consistent.",
            context=consideration
        )

        if trade.Currency != consideration.CurrencyIsoCode:
            yield self.__parse_automatic_currency_conversion(trade_data, True)

        yield OutputRow(
            TimestampUTC=trade_data.Date,
            Type=OutputType.Buy,
            BaseCurrency=ticker,
            BaseAmount=trade.Quantity,
            QuoteCurrency=trade.Currency,
            QuoteAmount=abs(trade.Consideration),
            FeeCurrency='' if commission is None else commission.CurrencyIsoCode,
            FeeAmount=None if commission is None else abs(commission.PL_Amount),
            From=Exchange.IG,
            To=Exchange.IG,
            ID=trade.Order_ID,
            Description=f'{Exchange.IG} {trade.Direction} {trade.Market}'
        )

    def __parse_sell_trade(self, trade_data: Trade) -> list[OutputRow]:
        trade = trade_data.Trade
        consideration = trade_data.Consideration
        commission = trade_data.Commission
        fee = trade_data.Fee
        ticker = self.__parse_ticker(trade.Market)
        self.__validate_trade(trade_data)

        validate(
            condition=0 < trade.Consideration == self.__round(trade.Price * trade.Quantity / -100),
            error="Trade price fields should be internally consistent.",
            context=trade
        )
        validate(
            condition=consideration.Transaction_type == 'DEPO' and consideration.ProfitAndLoss ==
                      consideration.Currency + self.__format_money(consideration.PL_Amount),
            error="Consideration fields should be internally consistent.",
            context=consideration
        )
        validate(
            condition=bool(trade.Charges == 0) != bool(fee is not None),
            error='Fees can only exist if Trade Charges exist',
            context=trade_data
        )
        validate(
            condition=fee is None or (fee.ProfitAndLoss == fee.Currency
                                      + self.__format_money(fee.PL_Amount) and fee.PL_Amount < 0),
            error="Fee fields should be internally consistent",
            context=commission
        )
        validate(
            condition=fee is None or commission is None or fee.CurrencyIsoCode == commission.CurrencyIsoCode,
            error="To merge Commissions and Fees (Charges) into the base Trade, only one, "
                  "common currency is allowed for both Fee and Commission.",
            context=trade_data
        )

        fee_currency = ''
        fee_amount = None
        if fee is not None or commission is not None:
            fee_currency = fee.CurrencyIsoCode if fee is not None else commission.CurrencyIsoCode
            fee_amount = 0 + abs(fee.PL_Amount if fee is not None else 0)
            fee_amount += abs(commission.PL_Amount if commission is not None else 0)

        yield OutputRow(
            TimestampUTC=trade_data.Date,
            Type=OutputType.Sell,
            BaseCurrency=ticker,
            BaseAmount=abs(trade.Quantity),
            QuoteCurrency=trade.Currency,
            QuoteAmount=trade.Consideration,
            FeeCurrency=fee_currency,
            FeeAmount=fee_amount,
            From=Exchange.IG,
            To=Exchange.IG,
            ID=trade.Order_ID,
            Description=f'{Exchange.IG} {trade.Direction} {trade.Market}',
        )

        if trade.Currency != consideration.CurrencyIsoCode:
            yield self.__parse_automatic_currency_conversion(trade_data, False)

    @staticmethod
    def __parse_automatic_currency_conversion(trade_data: Trade, is_buy: bool) -> OutputRow:
        # If the trade and consideration have different currencies, it means that the trade was made using IG's
        # automatic currency conversion. While the trade still occurs in the stock currency, we have to record
        # an additional forex conversion.

        # noinspection PyArgumentList
        # All these dates are consistent with when you have performed the trades, but not consistent with settlement
        # dates. On top of that, I modify the original trade date for the forex conversion, in order to make sure
        # the transactions occur in correct order.
        assumed_time = trade_data.Date + pandas.DateOffset(seconds=-1 if is_buy else 1)

        trade = trade_data.Trade
        consideration = trade_data.Consideration
        parsed_conversion = re.search(r'^.* Converted at ([\d.]+)$', consideration.MarketName)
        validate(
            condition=parsed_conversion is not None and len(parsed_conversion.groups()) == 1,
            error="Trade with automatic currency conversion has to have the conversion rate specified",
            context=[trade, consideration]
        )
        conversion_rate = float(parsed_conversion.group(1))

        return OutputRow(
            TimestampUTC=assumed_time,
            Type=OutputType.Buy if is_buy else OutputType.Sell,
            BaseCurrency=trade.Currency,
            BaseAmount=abs(trade.Consideration),
            QuoteCurrency=consideration.CurrencyIsoCode,
            QuoteAmount=abs(consideration.PL_Amount),
            From=Exchange.IG,
            To=Exchange.IG,
            ID=trade.Order_ID,
            Description=f'{Exchange.IG} {trade.Direction} {trade.Currency} for {consideration.CurrencyIsoCode} '
                        f'at {conversion_rate}'
        )

    @staticmethod
    def __parse_corporate_action(trade_data: Trade) -> list[OutputRow]:
        trade = trade_data.Trade

        validate(
            condition=trade_data.Consideration is None and trade_data.Commission is None and trade_data.Fee is None,
            error="Corporate Actions do not have auxiliary information.",
            context=trade_data
        )

        validate(
            condition=trade.Price == 0 and trade.Consideration == 0 and is_nan(trade.Commission) and trade.Charges == 0
                      and trade.Cost_Proceeds == 0,
            error="No Corporate Actions of consequence are implemented.",
            context=trade
        )

        # This action could be a SPAC merge where one stock is exchanged for another, but the way it's modeled
        # in the files is that there is no information about the old and new name, so you have to manage it manually
        return []

    def __parse_transfer(self, trade_data: Trade) -> list[OutputRow]:
        trade = trade_data.Trade
        ticker = self.__parse_ticker(trade.Market)

        show_stock_transfers_warning_once()

        validate(
            condition=trade_data.Consideration is None and trade_data.Commission is None and trade_data.Fee is None,
            error="Stock Transfers do not have auxiliary information.",
            context=trade_data
        )
        validate(
            condition=trade.Direction == 'SELL' and trade.Quantity < 0,
            error="Only outgoing Stock Transfers are implemented.",
            context=trade,
        )
        validate(
            condition=trade.Price == 0 and is_nan(trade.Commission) and is_nan(trade.Charges)
                      and is_nan(trade.Cost_Proceeds) and is_nan(trade.Conversion_rate),
            error="Stock Transfers have to have certain fields empty.",
            context=trade
        )

        yield OutputRow(
            TimestampUTC=trade_data.Date,
            Type=OutputType.Send,
            BaseCurrency=ticker,
            BaseAmount=abs(trade.Quantity),
            From=Exchange.IG,
            To=Exchange.Unknown,
            ID=trade.Order_ID,
            Description=f'{Exchange.IG} Outgoing Transfer: {trade.Market}',
        )

    def __parse_transaction(self, transaction: TransactionRow) -> Optional[OutputRow]:  # TODO: optional?
        validate(
            condition=transaction.Cash_transaction is False,
            error="This cannot be a Cash Transaction (I don't even know what it is).",
            context=transaction
        )

        # Note that 'Inter Account Transfers' are implemented as deposits and withdrawals, not 'send' and 'receive'.
        # That's because for currency transfers it doesn't really matter if you match it as a transfer, because
        # those are not a taxable events. And it makes it simpler, as you don't have to match anything manually.

        if transaction.Transaction_type == 'DEPO':
            if transaction.Summary == 'Cash In' or transaction.Summary == 'Inter Account Transfers':
                return self.__parse_simple_transaction(transaction, OutputType.FiatDeposit, row_from=Exchange.Bank)
            if transaction.Summary == 'Currency Transfers':
                # print(transaction)
                return self.__parse_simple_transaction(transaction, OutputType.Buy)
                return None  # TODO: implement
            if transaction.Summary == 'Dividend':
                show_dividends_warning_once()
                return self.__parse_simple_transaction(transaction, OutputType.FiatDeposit, row_from=Exchange.Dividends)

        if transaction.Transaction_type == 'WITH':
            # Note that I'm guessing that this transaction type is called 'Cash Out'. This may be incorrect.
            if transaction.Summary == 'Cash Out' or transaction.Summary == 'Inter Account Transfers':
                return self.__parse_simple_transaction(transaction, OutputType.FiatWithdrawal, row_to=Exchange.Bank)
            if transaction.Summary == 'Currency Transfers':
                # print(transaction)
                return self.__parse_simple_transaction(transaction, OutputType.Sell)
                return None  # TODO: implement
            if is_nan(transaction.Summary) and transaction.MarketName.startswith("Custody Fee "):
                return self.__parse_simple_transaction(transaction, OutputType.Fee)

        if transaction.Transaction_type == 'EXCHANGE' and transaction.Summary == 'Exchange Fees':
            return self.__parse_simple_transaction(transaction, OutputType.Fee)

        validate(
            condition=False,
            error=f'Transaction type "{transaction.Transaction_type}" ("{transaction.Summary}") is not implemented.',
            context=transaction
        )

    @staticmethod
    def __parse_simple_transaction(
            transaction: TransactionRow,
            row_type: OutputType,
            row_from: Exchange = Exchange.IG,
            row_to: Exchange = Exchange.IG,
    ) -> OutputRow:

        return OutputRow(
            TimestampUTC=transaction.DateUtc,
            Type=row_type,
            BaseCurrency=transaction.CurrencyIsoCode,
            BaseAmount=abs(transaction.PL_Amount),
            From=row_from,
            To=row_to,
            ID=transaction.Reference,
            Description=f'{Exchange.IG} {transaction.Summary}: {transaction.MarketName}'
        )

    def __add_transactions_to_trade(self, trade: Trade) -> None:
        order_id = trade.Trade.Order_ID
        transaction: TransactionRow
        for transaction in \
                self.__transactions.loc[self.__transactions['MarketName'].str.contains(order_id)].itertuples():
            self.__transactions.drop([transaction.Index], inplace=True)

            validate(
                condition=transaction.Close_level == 0 and transaction.Close_level == 0
                          and transaction.Cash_transaction is False
                          and is_nan(transaction.Size) and is_nan(transaction.Period),
                error="Some transaction fields have specific values and I don't have any examples of different values.",
                context=transaction
            )

            if transaction.Summary == 'Client Consideration' and trade.Consideration is None:
                trade.Consideration = transaction
            elif transaction.Summary == 'Share Dealing Commissions' and trade.Commission is None:
                trade.Commission = transaction

            # Fees (additional fees, as Commissions are also fees) are not named correctly in the Summary field
            elif is_nan(transaction.Summary) and ' Fee ' in transaction.MarketName and trade.Fee is None:
                trade.Fee = transaction

            else:
                validate(
                    condition=False,
                    error="Unrecognised transaction type or duplicated transaction type data.",
                    context=[transaction, trade]
                )

    def __validate_trade(self, trade_data: Trade) -> None:
        trade = trade_data.Trade
        consideration = trade_data.Consideration
        commission = trade_data.Commission
        validate(
            condition=trade.Settled_ is True and trade_data.Consideration is not None
                      and bool(commission is not None) != bool(trade.Commission == 0),
            error="Parsing trades that are not settled or don't have full information is not implemented.",
            context=trade_data
        )
        validate(
            condition=bool(trade.Currency == consideration.CurrencyIsoCode)
                      == bool(trade.Consideration == consideration.PL_Amount),
            error="Trade and Consideration currencies and values should be consistent.",
            context=trade_data
        )

        validate(
            condition=commission is None or (commission.ProfitAndLoss == commission.Currency
                                             + self.__format_money(commission.PL_Amount) and commission.PL_Amount < 0),
            error="Commission fields should be internally consistent",
            context=commission
        )

    def __parse_files(self) -> None:
        config = dict(skip_blank_lines=True, na_values=["-"], true_values=['Y'], false_values=['N'], thousands=',')
        first_source = pandas.read_csv(self._get_source(), **config)
        second_source = pandas.read_csv(self.__second_source, **config)
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

        data_frames.parse_date(self.__transactions, 'DateUtc', '%Y-%m-%dT%H:%M:%S')

    @staticmethod
    def __parse_ticker(ticker: str) -> str:
        validate(
            condition=ticker in translate_tickers[Exchange.IG],
            error="IG does not have Tickers in the export files\n"
                  "You have to match each name to a correct stock ticker manually.\n"
                  f"Add '{ticker}' to the 'translate_tickers' dict for IG.\n"
                  "Remember to add names in such a way that they match names from other exchanges:\n"
                  "US tickers just as is, for example 'GOOG', and other tickers as custom names.",
            context=ticker
        )
        return parse_ticker(ticker, Exchange.IG, AssetType.Stock)

    @staticmethod
    def __round(number: float, exponent: str = '.00') -> float:
        # IG rounds halves up
        return float(Decimal(number).quantize(Decimal(exponent), ROUND_HALF_UP))

    @staticmethod
    def __format_money(number: float) -> str:
        return locale.format_string('%.2f', number, True)

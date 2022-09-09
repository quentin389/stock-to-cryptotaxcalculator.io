import locale
from decimal import Decimal, ROUND_HALF_UP

import pandas
from pandas import DataFrame, to_datetime

from config.config import translate_tickers
from config.types import OutputRow, Exchange, AssetType, OutputType
from helpers import data_frames
from helpers.stock_market import parse_ticker
from helpers.validation import validate, is_nan
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

        # TODO: parse currency transactions
        # print(self.__transactions)

        return []

    def __parse_trade_row(self, trade_row: TradeRow) -> list[OutputRow]:
        # noinspection PyTypeChecker
        trade = Trade(
            Date=to_datetime(f'{trade_row.Date} {trade_row.Time}', format='%d-%m-%Y %H:%M:%S'),
            Trade=trade_row
        )
        self.__add_transactions_to_trade(trade)

        if trade.Trade.Activity == 'TRADE' and trade.Trade.Direction == 'BUY':
            return self.__parse_buy_trade(trade)
        if trade.Trade.Activity == 'TRADE' and trade.Trade.Direction == 'SELL':
            return self.__parse_sell_trade(trade)

        # print("TODO: parse me")  # TODO: parse those records and add a validation at the end

        return []

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

        # TODO: how do we model money conversions here? Maybe we need to parse the Consideration row? Else change yield

        yield OutputRow(
            TimestampUTC=trade_data.Date,
            Type=OutputType.Buy,
            BaseCurrency=ticker,
            BaseAmount=trade.Quantity,
            FeeCurrency='' if commission is None else commission.CurrencyIsoCode,
            FeeAmount=None if commission is None else abs(commission.PL_Amount),
            From=Exchange.IG,
            To=Exchange.IG,
            ID=trade.Order_ID,
            Description=f'{Exchange.IG} {trade.Direction} {trade.Market}',

            # Whether the Consideration row is in the same currency as Trade or not, doesn't matter, because the Trade
            # itself has to have occurred in the stock native currency.
            QuoteCurrency=trade.Currency,
            QuoteAmount=abs(trade.Consideration)
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

        # TODO: how do we model money conversions here? Maybe we need to parse the Consideration row? Else change yield

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
        validate(
            condition=trade_data.Trade.Settled_ is True and trade_data.Consideration is not None
                      and bool(trade_data.Commission is not None) != bool(trade_data.Trade.Commission == 0),
            error="Parsing trades that are not settled or don't have full information is not implemented.",
            context=trade_data
        )

        commission = trade_data.Commission
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

import pandas
from pandas import DataFrame

from config.types import OutputRow, OutputType, Exchange
from helpers import data_frames
from helpers.validation import validate
from helpers.warnings import show_warning_once
from parsers.ig.AbstractIgDataParser import AbstractIgDataParser
from parsers.ig.types import CfdClosingTrade


class IgCfdDataParser(AbstractIgDataParser):
    __trades: DataFrame

    def run(self) -> None:
        self.__parse_file()
        data = [*self.__parse()]
        self._save_output(data)

    def __parse(self) -> list[OutputRow]:
        trade: CfdClosingTrade
        for trade in self.__trades.itertuples():
            yield self.__parse_closing_trade(trade)

        return []

    def __parse_closing_trade(self, trade: CfdClosingTrade) -> OutputRow:
        ticker = self._parse_ticker(trade.Market)

        validate(
            condition=trade.Period == '-' and trade.Borrowing == 0 and trade.Dividends == 0 and trade.LR_Prem_ == 0
                      and trade.Others == 0,
            error="Some advanced fields should be empty or zero, since I didn't have examples for values in them",
            context=trade
        )
        validate(
            condition=self._round(trade.P_L + trade.Comm_ + trade.Funding) == trade.Total,
            error="Trade Total relationship has to be correct.",
            context=trade
        )

        show_warning_once(
            "IG-CFD",
            "IG CFD trades are modeled as pure GBP transactions. This may not be an accurate way to do it."
        )

        # See EtoroDataParser.__parse_cfd_close_position for some more explanations
        is_profit = trade.Total > 0
        return OutputRow(
            TimestampUTC=trade.Closed,
            Type=OutputType.RealizedProfit if is_profit else OutputType.RealizedLoss,
            From=Exchange.CFDs if is_profit else Exchange.IG_CFD,
            To=Exchange.IG_CFD if is_profit else Exchange.CFDs,
            ID=trade.Closing_Ref,
            Description=f'{Exchange.IG_CFD} {ticker}: {trade.Direction} {trade.Market}',

            # There is the problem here that the trades for US stocks are be conducted in USD, but all commissions
            # are in GBP, then, immediately after the trade, all USD values are converted to GBP. At least that's how
            # it works with default "instant currency conversion". To correctly model this, I'd have to create multiple
            # transactions, due to limitations of the cryptotaxcalculator.io format for 'realized-loss/profit' type.
            # Even then, I'm not sure if this would be modeled accurately for HMRC. I could even argue that since
            # CFDs are supposed to be modeled as a simple difference profit and loss, doing it this simple way is more
            # correct than using any more in-depth method, and since the currency conversions for GBP are instant,
            # from my perspective everything *is* traded in GBP. So... maybe this is proper the way to do it?
            BaseCurrency=trade.Comm__Ccy_,
            BaseAmount=abs(trade.Total)
        )

    def __parse_file(self):
        all_data = pandas.read_csv(self._get_source(), skip_blank_lines=True, thousands=',', skiprows=5)

        # Only closing trades are relevant for CFDs. Opening trades can be ignored.
        data = all_data.loc[all_data['Closed'] != '-'].copy()
        data_frames.normalize_column_names(data)
        data_frames.parse_date(data, 'Opened', '%d-%m-%Y %H:%M:%S')
        data_frames.parse_date(data, 'Closed', '%d-%m-%Y %H:%M:%S')

        self.__trades = data

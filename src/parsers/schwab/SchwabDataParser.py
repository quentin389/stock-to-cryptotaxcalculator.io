from datetime import timedelta

import pandas

from helpers.stock_market import parse_ticker
from src.config.types import OutputRow, OutputType, Exchange, AssetType
from src.parsers.schwab.types import BrokerageRow, EquityAwardRow
from src.parsers.AbstractDataParser import AbstractDataParser


class SchwabDataParser(AbstractDataParser):
    __date_format = '%m/%d/%Y'
    __base_fiat = 'USD'

    def __init__(self, source: str, second_source: str, target: str):
        super().__init__(source, target)
        self.__second_source = second_source
        self.__brokerage = []
        self.__equity_awards = []

    def run(self) -> None:
        self.__parse_files()
        data = self.__transform_data()
        self._save_output(data)

    def __transform_data(self) -> list[OutputRow]:
        transactions = []

        # Process equity awards
        for row in self.__equity_awards:
            if row.Action == "Lapse":
                # Create FiatDeposit transaction
                transactions.append(OutputRow(
                    TimestampUTC=row.Date,
                    Type=OutputType.FiatDeposit,
                    BaseCurrency=self.__base_fiat,
                    BaseAmount=row.FairMarketValuePrice * row.NetSharesDeposited,
                    From=Exchange.Bank,
                    To=Exchange.Schwab,
                    Description=f"Modeled from fair market value of {row.NetSharesDeposited} shares of "
                                f"{row.Symbol} at {row.FairMarketValuePrice} per share, to keep fiat total consistent."
                ))

                # Create Buy transaction
                transactions.append(OutputRow(
                    TimestampUTC=row.Date + timedelta(hours=1),
                    Type=OutputType.Buy,
                    BaseCurrency=self.__parse_ticker(row.Symbol),
                    BaseAmount=row.NetSharesDeposited,
                    QuoteCurrency=self.__base_fiat,
                    QuoteAmount=row.FairMarketValuePrice * row.NetSharesDeposited,
                    From=Exchange.Schwab,
                    To=Exchange.Schwab,
                    Description=f"Lapse of {row.NetSharesDeposited} Restricted Stock Units (RSU) of "
                                f"{row.Symbol} at {row.FairMarketValuePrice} per share.",
                    ReferencePricePerUnit=row.FairMarketValuePrice,
                    ReferencePriceCurrency=self.__base_fiat,
                ))

            else:
                raise ValueError(f"Unsupported action '{row.Action}' encountered in equity awards data.")

        # Process brokerage transactions
        for row in self.__brokerage:
            if row.Action == "Sell":
                transactions.append(OutputRow(
                    TimestampUTC=row.Date + timedelta(hours=2),
                    Type=OutputType.Sell,
                    BaseCurrency=self.__parse_ticker(row.Symbol),
                    BaseAmount=row.Quantity,
                    QuoteCurrency=self.__base_fiat,
                    QuoteAmount=row.Amount,
                    FeeCurrency=self.__base_fiat,
                    FeeAmount=row.Fees,
                    From=Exchange.Schwab,
                    To=Exchange.Schwab,
                    Description=row.Description,
                    ReferencePricePerUnit=row.Price,
                    ReferencePriceCurrency=self.__base_fiat
                ))
            elif row.Action == "Service Fee":
                transactions.append(OutputRow(
                    TimestampUTC=row.Date + timedelta(hours=2),
                    Type=OutputType.Fee,
                    BaseCurrency=self.__base_fiat,
                    BaseAmount=abs(row.Amount),
                    From=Exchange.Schwab,
                    To=Exchange.Schwab,
                    Description=row.Description
                ))
            elif row.Action == "Wire Sent" or row.Action == "MoneyLink Transfer":
                transactions.append(OutputRow(
                    TimestampUTC=row.Date + timedelta(hours=3),
                    Type=OutputType.FiatWithdrawal,
                    BaseCurrency=self.__base_fiat,
                    BaseAmount=abs(row.Amount),
                    From=Exchange.Schwab,
                    To=Exchange.Bank,
                    Description=row.Description
                ))
            elif row.Action == "Credit Interest":
                transactions.append(OutputRow(
                    TimestampUTC=row.Date,
                    Type=OutputType.Interest,
                    BaseCurrency=self.__base_fiat,
                    BaseAmount=row.Amount,
                    From=Exchange.Schwab,
                    To=Exchange.Schwab,
                    Description=row.Description
                ))
            elif row.Action == "Stock Plan Activity":
                # Skip Stock Plan Activity as it's covered by Lapse
                continue
            else:
                raise ValueError(f"Unsupported action '{row.Action}' encountered in brokerage data.")

        transactions = sorted(transactions, key=lambda x: x.TimestampUTC)

        return transactions

    def __parse_files(self) -> None:
        self.__brokerage.extend(self.__parse_first_source())
        self.__equity_awards.extend(self.__parse_second_source())

    def __parse_first_source(self) -> list:
        first_source = pandas.read_csv(self._get_source())

        # Convert 'Date' to datetime with explicit format
        first_source['Date'] = pandas.to_datetime(first_source['Date'], format='%m/%d/%Y')

        # Clean and convert other columns
        first_source['Quantity'] = pandas.to_numeric(first_source['Quantity'])
        self.__clean_numeric_columns(first_source, ['Price', 'Fees & Comm', 'Amount'])

        # Rename columns
        first_source.rename(columns={'Fees & Comm': 'Fees'}, inplace=True)

        # Sort by date
        first_source.sort_values(by='Date', inplace=True)

        # Convert rows to BrokerageRow NamedTuple
        brokerage_data = [
            BrokerageRow(
                Date=row['Date'],
                Action=row['Action'],
                Symbol=row['Symbol'] if not pandas.isna(row['Symbol']) else None,
                Description=row['Description'],
                Quantity=int(row['Quantity']) if not pandas.isna(row['Quantity']) else None,
                Price=row['Price'] if not pandas.isna(row['Price']) else None,
                Fees=row['Fees'] if not pandas.isna(row['Fees']) else None,
                Amount=row['Amount'] if not pandas.isna(row['Amount']) else None
            )
            for index, row in first_source.iterrows()
        ]

        return brokerage_data

    def __parse_second_source(self) -> list:
        second_source = pandas.read_csv(self.__second_source)

        # Convert 'Date' and 'AwardDate' to datetime with explicit format
        second_source['Date'] = pandas.to_datetime(second_source['Date'], format='%m/%d/%Y', errors='coerce')
        second_source['AwardDate'] = pandas.to_datetime(second_source['AwardDate'], format='%m/%d/%Y', errors='coerce')

        # Clean and convert other columns
        second_source['Quantity'] = pandas.to_numeric(second_source['Quantity'])
        self.__clean_numeric_columns(second_source, ['FairMarketValuePrice', 'Taxes'])

        equity_awards_data = []
        for i in range(0, len(second_source), 2):
            row1 = second_source.iloc[i]
            row2 = second_source.iloc[i + 1]
            equity_award_row = EquityAwardRow(
                Date=row1['Date'],
                Action=row1['Action'],
                Symbol=row1['Symbol'],
                Description=row1['Description'],
                Quantity=int(row1['Quantity']),
                AwardDate=row2['AwardDate'],
                FairMarketValuePrice=float(row2['FairMarketValuePrice']),
                SharesSoldWithheldForTaxes=int(row2['SharesSoldWithheldForTaxes']),
                NetSharesDeposited=int(row2['NetSharesDeposited']),
                Taxes=float(row2['Taxes'])
            )
            equity_awards_data.append(equity_award_row)

        # Sort by Date ascending
        equity_awards_data = sorted(equity_awards_data, key=lambda x: x.Date)

        return equity_awards_data

    @staticmethod
    def __clean_numeric_columns(df, columns):
        for column in columns:
            df[column] = df[column].replace('[$,]', '', regex=True).astype(float)

    @staticmethod
    def __parse_ticker(ticker: str) -> str:
        return parse_ticker(ticker, Exchange.Schwab, AssetType.Stock)

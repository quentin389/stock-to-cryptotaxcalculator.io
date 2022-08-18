import pandas

from helpers.data_frames import remove_column_spaces
from helpers.validation import validate


class EtoroImport:
    def __init__(self, source: str, target: str):
        self.__source = source
        self.__target = target

    def run(self):
        self.__pre_validate()
        data = self.__parse()
        print(data)

    def __pre_validate(self):
        account_summary = pandas.read_excel(io=self.__source, sheet_name="Account Summary", header=1, index_col=0)
        validate(
            condition=account_summary.loc['Currency']['Totals'] == "USD",
            error="Only USD accounts are supported. I didn't have any other examples."
        )

    def __parse(self):
        account_activity = pandas.read_excel(io=self.__source, sheet_name="Account Activity")
        remove_column_spaces(account_activity)

        print(account_activity)

        for row in account_activity.itertuples():
            print(row)

        return []

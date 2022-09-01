from typing import Optional, Any

from pandas import DataFrame, to_datetime, option_context


def normalize_column_names(data_frame: DataFrame, replace_with: str = '_') -> None:
    data_frame.columns = data_frame.columns.str.replace(r'[ /\.\(\)]', replace_with, regex=True)
    data_frame.columns = data_frame.columns.str.replace('%', 'pct', regex=False)


def remove_columns(data_frame: DataFrame, column: str) -> None:
    data_frame.drop(column, axis=1, inplace=True)


def parse_date(data_frame: DataFrame, column_name: str, date_format: str, timezone: str = None) -> None:
    data_frame[column_name] = to_datetime(data_frame[column_name], format=date_format)
    if timezone:
        data_frame[column_name] = data_frame[column_name].map(
            lambda x: x.tz_localize(timezone).tz_convert('UTC').replace(tzinfo=None)
        )


def get_by_key(data_frame: DataFrame, key: Any) -> Optional[DataFrame]:
    try:
        return data_frame.loc[key:key]
    except KeyError:
        return None


def get_one_by_key(data_frame: DataFrame, key: Any) -> Any:
    if not key:
        return None
    data = get_by_key(data_frame, int(key))
    if data is None:
        return None
    if data.shape[0] != 1:
        raise Exception("The values are not unique!")
    for row in data.itertuples():
        return row


def wide_print(stuff_from_pandas) -> None:
    with option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 0):
        print(stuff_from_pandas)

from typing import Optional, Any

from pandas import DataFrame


def remove_column_spaces(data_frame: DataFrame, replace_with: str = '_') -> None:
    data_frame.columns = data_frame.columns.str.replace(' ', replace_with)


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

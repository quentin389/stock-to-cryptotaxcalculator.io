from pandas import DataFrame


def remove_column_spaces(data_frame: DataFrame, replace_with: str = '_') -> None:
    data_frame.columns = data_frame.columns.str.replace(' ', replace_with)

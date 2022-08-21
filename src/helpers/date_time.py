from pandas import Timestamp


def almost_identical(first_date_time: Timestamp, second_date_time: Timestamp, offset_sec: int) -> bool:
    return abs((first_date_time - second_date_time).total_seconds()) <= offset_sec

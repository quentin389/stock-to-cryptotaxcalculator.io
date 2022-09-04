import math
import re
from logging import warning
from typing import Any


def validate(condition: bool, error: str, context: Any) -> None:
    if not condition:
        raise Exception(f'{error}\ncontext:\n{str(context)}')


def is_nan(value: Any) -> bool:
    return type(value) != str and math.isnan(value)


def is_currency(value: str) -> bool:
    return bool(re.match(r'^[A-Z]{3}$', value))


def show_warning_once(group: str, message: str) -> None:
    if group not in show_warning_once.already_shown:
        warning(f'!!! {group} !!!\n{message}\n')
        show_warning_once.already_shown[group] = True


def show_stock_split_warning_once() -> None:
    show_warning_once(
        group="Stock Splits",
        message="I have categorized stock splits as 'Chain Split' in cryptotaxcalculator.io.\nIn order for this"
                " to work correctly, you have to ignore the 'missing market price' warnings for those transactions."
    )


show_warning_once.already_shown = {}

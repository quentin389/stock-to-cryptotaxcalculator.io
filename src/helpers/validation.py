import math
from logging import warning
from typing import Any


def validate(condition: bool, error: str, context: Any) -> None:
    if not condition:
        raise Exception(f'{error}\ncontext:\n{str(context)}')


def is_nan(value: Any) -> bool:
    return math.isnan(value)


def show_warning_once(group: str, message: str) -> None:
    if group not in show_warning_once.already_shown:
        warning(f'!!! {group} !!!\n{message}\n')
        show_warning_once.already_shown[group] = True


show_warning_once.already_shown = {}

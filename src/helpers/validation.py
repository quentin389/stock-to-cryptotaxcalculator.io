import math
import re
from typing import Any


def validate(condition: bool, error: str, context: Any) -> None:
    if not condition:
        raise Exception(f'{error}\ncontext:\n{str(context)}')


def is_nan(value: Any) -> bool:
    return type(value) != str and math.isnan(value)


def is_currency(value: str) -> bool:
    return bool(re.match(r'^[A-Z]{3}$', value))

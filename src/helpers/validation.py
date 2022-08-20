from typing import Any


def validate(condition: bool, error: str, context: Any) -> None:
    if not condition:
        raise Exception(f'{error}\ncontext:\n{str(context)}')

def validate(condition: bool, error: str) -> None:
    if not condition:
        raise Exception(error)

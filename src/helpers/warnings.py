from logging import warning


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


def show_dividends_warning_once() -> None:
    show_warning_once(
        group='Dividends',
        message="I have categorized dividends as 'Fiat Deposit' in cryptotaxcalculator.io, so they correctly "
                "contribute to your cash balance.\nThey are ignored from tax calculations, as they are taxed "
                "separately by HMRC, and this cannot be computed by cryptotaxcalculator.io.\nIf you want to check "
                "your total GBP balance from dividends, you could temporarily categorize all deposits from the "
                "'Dividends' source as 'Realized Profit'."
    )


def show_option_lapse_warning_once() -> None:
    show_warning_once(
        group="Option Lapse",
        message="Lapsed (expired) options are implemented as a sale with value 0.\nThis means that "
                "cryptotaxcalculator.io thinks that the position value is 0 and will show "
                "a 'missing market price' warning.\nThose warnings should be dismissed."
    )


def show_stock_transfers_warning_once() -> None:
    show_warning_once(
        group="Stock Transfers",
        message="For stock transfers to be recognized correctly by cryptotaxcalculator.io, the 'send' and 'receive' "
                "transactions need to be matched by several criteria.\nIt's important to match those transactions "
                "by manually adjusting them.\nOne important criterion is that they have to occur within one hour, "
                "which for stock transfers may not be the case."
    )


show_warning_once.already_shown = {}

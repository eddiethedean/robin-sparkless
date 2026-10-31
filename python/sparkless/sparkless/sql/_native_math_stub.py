from typing import Any

from sparkless_native._native import pmod as _pmod


def pmod(dividend: Any, divisor: Any) -> Any:
    return _pmod(dividend, divisor)

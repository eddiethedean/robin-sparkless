from typing import Any

from sparkless._native import pmod as _pmod  # type: ignore[attr-defined]


def pmod(dividend: Any, divisor: Any) -> Any:
    return _pmod(dividend, divisor)

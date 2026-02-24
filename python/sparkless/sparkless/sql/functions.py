# PySpark-style: from sparkless.sql.functions import col, lit, when, count, sum, ...
from sparkless import (
    column as col,
    lit_i64,
    lit_str,
    lit_bool,
    lit_f64,
    lit_null,
    upper,
    lower,
    substring,
    trim,
    cast,
    when,
    count,
    sum,
    avg,
    min,
    max,
)

__all__ = [
    "col",
    "lit_i64",
    "lit_str",
    "lit_bool",
    "lit_f64",
    "lit_null",
    "upper",
    "lower",
    "substring",
    "trim",
    "cast",
    "when",
    "count",
    "sum",
    "avg",
    "min",
    "max",
]

# Alias for PySpark compatibility
def lit(value):
    """Return a Column with the given value. Use lit_i64, lit_str, lit_bool, lit_f64 for typed literals."""
    if isinstance(value, bool):
        return lit_bool(value)
    if isinstance(value, int):
        return lit_i64(value)
    if isinstance(value, float):
        return lit_f64(value)
    if value is None:
        return lit_null("string")
    if isinstance(value, str):
        return lit_str(value)
    return lit_str(str(value))

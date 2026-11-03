"""Private nulls SQL function implementations."""

from __future__ import annotations

from ._shared import (
    _as_col,
    _col_result,
    _native_fn,
    lit,
)

from ._wrappers import coalesce


def isnull(col_or_name):
    """Return a boolean column that is true when the column is null. PySpark: F.isnull(col)."""
    col = _as_col(col_or_name)
    # Use getattr to avoid relying on stubbed attributes on PyColumn.
    return _col_result(getattr(col, "isNull")())


def isnotnull(col_or_name):
    """Return a boolean column that is true when the column is not null. PySpark: F.isnotnull(col)."""
    col = _as_col(col_or_name)
    return _col_result(getattr(col, "isNotNull")())


def nvl(col_or_name, replacement):
    """Replace null with replacement. PySpark: F.nvl(col, replacement)."""
    c = _as_col(col_or_name)
    # PySpark: string second arg is column name (value of that column), not literal (#1476).
    rep = (
        lit(replacement)
        if isinstance(replacement, (int, float, bool, type(None)))
        else _as_col(replacement)
    )
    return coalesce(c, rep)


def nullif(col1, col2):
    """Return null if col1 equals col2, else col1. PySpark: F.nullif(col1, col2)."""
    return _col_result(_native_fn("nullif")(_as_col(col1), _as_col(col2)))


__all__ = ["isnotnull", "isnull", "nullif", "nvl"]

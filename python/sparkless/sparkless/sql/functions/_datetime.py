"""Private datetime SQL function implementations."""

from __future__ import annotations

from ._shared import (
    ColumnOrName,
    _ColumnType,
    _as_col,
    _col_result,
    _native,
    _native_fn,
)


def add_months(start, months):
    return _native.native_add_months(_as_col(start), months)


def months_between(date1, date2, roundOff=True):
    return _native_fn("months_between")(_as_col(date1), _as_col(date2), roundOff)


def next_day(date, dayOfWeek):
    return _native.native_next_day(_as_col(date), dayOfWeek)


def last_day(date):
    return _native_fn("last_day")(_as_col(date))


def trunc(date, fmt):
    return _native.native_trunc(_as_col(date), fmt)


def date_trunc(fmt, timestamp):
    return _native_fn("date_trunc")(fmt, _as_col(timestamp))


def hour(col_or_name):
    return _native_fn("hour")(_as_col(col_or_name))


def minute(col_or_name):
    return _native_fn("minute")(_as_col(col_or_name))


def second(col_or_name):
    return _native.native_second(_as_col(col_or_name))


def quarter(col_or_name):
    return _native_fn("quarter")(_as_col(col_or_name))


def dayofyear(col_or_name):
    return _native_fn("dayofyear")(_as_col(col_or_name))


def weekofyear(col_or_name):
    return _native_fn("weekofyear")(_as_col(col_or_name))


def make_date(year, month, day):
    """Build date from year, month, day (PySpark make_date)."""
    fn = getattr(_native, "make_date")
    return _col_result(fn(_as_col(year), _as_col(month), _as_col(day)))


def typeof(col_or_name: ColumnOrName) -> _ColumnType:
    """Return the data type of a column as a string (PySpark typeof)."""
    return _col_result(_native.native_typeof(_as_col(col_or_name)))


__all__ = [
    "add_months",
    "date_trunc",
    "dayofyear",
    "hour",
    "last_day",
    "make_date",
    "minute",
    "months_between",
    "next_day",
    "quarter",
    "second",
    "trunc",
    "typeof",
    "weekofyear",
]

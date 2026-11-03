"""Private session SQL function implementations."""

from __future__ import annotations

from ._shared import (
    Any,
    ColumnOrName,
    PySparkValueError,
    _ColumnType,
    _as_col,
    getpass,
    lit,
    tcast,
)


def _active_session():
    from sparkless.sql import SparkSession

    sess = None
    if hasattr(SparkSession, "getActiveSession"):
        sess = SparkSession.getActiveSession()
    if sess is None:
        sess = getattr(SparkSession, "_singleton_session", None)
    if sess is None:
        raise PySparkValueError("No active SparkSession")
    return sess


def current_database():
    return lit(_active_session().catalog.currentDatabase())


def current_schema():
    return current_database()


def current_catalog():
    return lit("spark_catalog")


def current_user():
    return lit(getpass.getuser())


def create_map(*cols: Any) -> _ColumnType:
    """Build a map column from alternating key/value expressions (PySpark create_map).

    With no args or create_map([]), returns a column of empty maps per row.
    """
    import sparkless_native._native as _native

    # PySpark: create_map() or create_map([]) -> empty map
    expanded: list[object] = []
    for c in cols:
        if isinstance(c, (list, tuple)):
            expanded.extend(c)
        else:
            expanded.append(c)
    if len(expanded) % 2 != 0:
        raise ValueError(
            "create_map requires an even number of arguments (key-value pairs)"
        )
    key_values = [_as_col(tcast(ColumnOrName, c)) for c in expanded]
    return _native.create_map(key_values)


__all__ = [
    "_active_session",
    "create_map",
    "current_catalog",
    "current_database",
    "current_schema",
    "current_user",
]

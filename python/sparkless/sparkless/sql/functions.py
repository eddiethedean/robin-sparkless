# PySpark-style: from sparkless.sql.functions import col, lit, when, count, sum, ...
import getpass

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
    count as _count,
    sum as _sum,
    avg as _avg,
    min as _min,
    max as _max,
    regexp_replace as _regexp_replace,
    regexp_extract_all as _regexp_extract_all,
    regexp_like as _regexp_like,
    to_timestamp as _to_timestamp,
    to_date as _to_date,
    current_date as _current_date,
    datediff as _datediff,
    unix_timestamp as _unix_timestamp,
    from_unixtime as _from_unixtime,
    year as _year,
    month as _month,
    dayofmonth as _dayofmonth,
    dayofweek as _dayofweek,
    date_add as _date_add,
    date_sub as _date_sub,
    date_format as _date_format,
)
from sparkless.errors import PySparkValueError

def _not_implemented(name):
    def _fn(*args, **kwargs):
        raise NotImplementedError(f"{name} is not yet implemented in robin-sparkless")

    _fn.__name__ = name
    return _fn


# Still missing (tracked by later todos)
floor = _not_implemented("floor")


def udf(*args, **kwargs):
    """UDF not yet implemented in robin-sparkless."""
    raise NotImplementedError("udf is not yet implemented in robin-sparkless")


__all__ = [
    "col",
    "lit",
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
    "to_timestamp",
    "to_date",
    "datediff",
    "current_date",
    "unix_timestamp",
    "from_unixtime",
    "year",
    "month",
    "dayofmonth",
    "dayofweek",
    "date_add",
    "date_sub",
    "date_format",
    "floor",
    "regexp_replace",
    "regexp_extract_all",
    "regexp_like",
    "expr",
    "current_database",
    "current_schema",
    "current_catalog",
    "current_user",
    "row_number",
    "create_map",
    "asc",
    "desc",
    "udf",
]

# --- Argument coercions (PySpark parity) ---
def _as_col(c):
    return col(c) if isinstance(c, str) else c


def count(c="*"):
    return _count(_as_col(c)) if c != "*" else _count(col("*"))


def sum(c):
    return _sum(_as_col(c))


def avg(c):
    return _avg(_as_col(c))


def min(c):
    return _min(_as_col(c))


def max(c):
    return _max(_as_col(c))


def regexp_replace(column, pattern, replacement):
    return _regexp_replace(_as_col(column), pattern, replacement)


def regexp_extract_all(column, pattern, idx=0):
    return _regexp_extract_all(_as_col(column), pattern, idx)


def regexp_like(column, pattern):
    return _regexp_like(_as_col(column), pattern)


def expr(sql_expr: str):
    """
    Minimal F.expr() support for REGEXP/RLIKE predicates used by upstream tests.
    Examples:
      - "Value REGEXP 'sales|tech'"
      - "Value RLIKE 'sales|tech'"
    """
    import re

    m = re.match(
        r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s+(REGEXP|RLIKE)\s+'([^']*)'\s*$",
        sql_expr,
        re.IGNORECASE,
    )
    if not m:
        raise NotImplementedError(f"expr() is not yet implemented for: {sql_expr!r}")
    col_name = m.group(1)
    pattern = m.group(3)
    return col(col_name).rlike(pattern)


def to_timestamp(column, fmt=None):
    return _to_timestamp(_as_col(column), fmt)


def to_date(column, fmt=None):
    return _to_date(_as_col(column), fmt)


def datediff(end, start):
    return _datediff(_as_col(end), _as_col(start))


def current_date():
    return _current_date()


def unix_timestamp(column=None, fmt=None):
    return _unix_timestamp(_as_col(column) if column is not None else None, fmt)


def from_unixtime(column, fmt=None):
    return _from_unixtime(_as_col(column), fmt)


def year(column):
    return _year(_as_col(column))


def month(column):
    return _month(_as_col(column))


def dayofmonth(column):
    return _dayofmonth(_as_col(column))


def dayofweek(column):
    return _dayofweek(_as_col(column))


def date_add(column, n):
    return _date_add(_as_col(column), n)


def date_sub(column, n):
    return _date_sub(_as_col(column), n)


def date_format(column, fmt):
    return _date_format(_as_col(column), fmt)

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


def create_map(*cols):
    """Build a map column from alternating key/value expressions (PySpark create_map).

    With no args, returns a column of empty maps per row.
    """
    import sparkless._native as _native

    key_values = [_as_col(c) for c in cols]
    # Native create_map expects a list of Columns; empty list -> empty map literal
    return _native.create_map(key_values)


class _RowNumberExpr:
    def over(self, window):
        import sparkless._native as _native

        partition_by, encoded, _ = _window_spec_to_partition_order(window)
        return _native.row_number_window(partition_by, encoded)


def row_number():
    """Window row_number() expression; use with .over(Window.partitionBy(...).orderBy(...))."""
    return _RowNumberExpr()


class _SortKey:
    def __init__(self, name: str, ascending: bool):
        self.name = name
        self.ascending = ascending


def _col_name(arg):
    from sparkless import Column as _Column

    if isinstance(arg, str):
        return arg
    if isinstance(arg, _Column):
        return arg.name
    raise TypeError(f"Unsupported sort key: {type(arg)!r}")


def _window_spec_to_partition_order(window, require_order=True):
    """Extract partition_by and order_by from WindowSpec for window functions.
    If require_order=False, order_by can be empty (for partition-only aggregate windows).
    """
    if not hasattr(window, "_partition_by") or not hasattr(window, "_order_by"):
        raise PySparkValueError("window function .over() expects a WindowSpec")
    partition_by = list(getattr(window, "_partition_by", []) or [])
    order_keys = list(getattr(window, "_order_by", []) or [])
    if not order_keys and require_order:
        raise PySparkValueError("window function .over() requires Window.orderBy(...)")
    partition_names = [_col_name(c) if not isinstance(c, str) else c for c in partition_by]
    flat_keys = []
    for k in order_keys:
        if isinstance(k, (list, tuple)):
            flat_keys.extend(k)
        else:
            flat_keys.append(k)
    if not flat_keys:
        return partition_names, [], False
    order_col_names = []
    encoded = []
    for k in flat_keys:
        if hasattr(k, "name") and hasattr(k, "ascending"):
            name = k.name
            ascending = k.ascending
        elif hasattr(k, "column_name") and hasattr(k, "descending"):
            name = k.column_name
            ascending = not k.descending
        else:
            name = _col_name(k)
            ascending = True
        order_col_names.append(name)
        encoded.append(name if ascending else f"-{name}")
    use_running = not all(oc in partition_names for oc in order_col_names)
    return partition_names, encoded, use_running


def desc(col_or_name):
    """Sort key for descending order in Window.orderBy."""
    name = _col_name(col_or_name)
    return _SortKey(name, ascending=False)


def asc(col_or_name):
    """Sort key for ascending order in Window.orderBy."""
    name = _col_name(col_or_name)
    return _SortKey(name, ascending=True)

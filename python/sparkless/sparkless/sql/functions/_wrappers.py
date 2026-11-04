"""Private wrappers SQL function implementations."""

from __future__ import annotations

from ._shared import (
    ColumnOrName,
    Tuple,
    Union,
    _Column,
    _ColumnType,
    _array,
    _array_contains,
    _array_distinct,
    _as_col,
    _asinh,
    _atanh,
    _avg,
    _coalesce,
    _concat,
    _concat_ws,
    _cosh,
    _count,
    _current_date,
    _current_timestamp,
    _date_add,
    _date_format,
    _date_sub,
    _date_trunc,
    _datediff,
    _dayofmonth,
    _dayofweek,
    _floor,
    _format_string,
    _from_unixtime,
    _get_json_object,
    _greatest,
    _hour,
    _input_file_name,
    _last_day,
    _least,
    _length,
    _levenshtein,
    _ltrim,
    _max,
    _min,
    _minute,
    _monotonically_increasing_id,
    _month,
    _months_between,
    _native,
    _native_when,
    _posexplode,
    _regexp_extract,
    _regexp_extract_all,
    _regexp_like,
    _regexp_replace,
    _repeat,
    _round,
    _sinh,
    _size,
    _soundex,
    _spark_partition_id,
    _split,
    _struct,
    _substring_index,
    _sum,
    _timestamp_seconds,
    _to_date,
    _to_timestamp,
    _to_timestamp_ltz,
    _to_timestamp_ntz,
    _to_utc_timestamp,
    _translate,
    _try_add,
    _try_cast,
    _unix_timestamp,
    _year,
    col,
    lit,
    tcast,
)

from ._session import _active_session


def when(condition, value):
    """PySpark-compatible when(condition, value). Both arguments required. Requires active SparkSession."""
    _active_session()
    cond = _as_col(condition)
    # Python None is omitted by PyO3 optional args; map to null literal so .otherwise() chains work (#1573).
    if value is None:
        val = lit(None)
    elif isinstance(value, (int, float, bool, str)):
        val = lit(value)
    else:
        val = _as_col(value)
    return _native_when(cond, val)


def count(c="*"):
    _active_session()
    return _count(_as_col(c)) if c != "*" else _count(col("*"))


def sum(c):
    _active_session()
    return _sum(_as_col(c))


def avg(c):
    _active_session()
    return _avg(_as_col(c))


def min(c):
    _active_session()
    return _min(_as_col(c))


def max(c):
    _active_session()
    return _max(_as_col(c))


def regexp_replace(column, pattern, replacement):
    return _regexp_replace(_as_col(column), pattern, replacement)


def regexp_extract(column, pattern, idx=0):
    """Extract first regex match (PySpark regexp_extract). idx=0 is full match, 1+ is capture group."""
    return _regexp_extract(_as_col(column), pattern, idx)


def regexp_extract_all(column, pattern, idx=0):
    return _regexp_extract_all(_as_col(column), pattern, idx)


def regexp_like(column, pattern):
    return _regexp_like(_as_col(column), pattern)


def split(column, pattern, limit=-1):
    """Split string by delimiter (PySpark split). limit=-1 means no limit."""
    return _split(_as_col(column), pattern, limit)


def coalesce(*cols):
    """Return first non-null value from columns (PySpark coalesce)."""
    if not cols:
        raise ValueError("coalesce requires at least one column")
    return _coalesce(*[_as_col(c) for c in cols])


def format_string(fmt, *cols):
    """Printf-style format (PySpark format_string)."""
    if not cols:
        raise ValueError("format_string requires at least one column")
    return _format_string(fmt, *[_as_col(c) for c in cols])


printf = format_string  # PySpark alias


def greatest(*cols):
    """Greatest of columns per row (PySpark greatest)."""
    if not cols:
        raise ValueError("greatest requires at least one column")
    return _greatest(*[_as_col(c) for c in cols])


def least(*cols):
    """Least of columns per row (PySpark least)."""
    if not cols:
        raise ValueError("least requires at least one column")
    return _least(*[_as_col(c) for c in cols])


def _array_distinct_column(col):
    """Internal helper: distinct elements in array column."""
    return _array_distinct(_as_col(col))


def posexplode(col):
    """Explode array with position (PySpark posexplode). Returns (pos_col, val_col) or wrapper with .alias()."""
    return _posexplode(col)


def expr(sql_expr: str):
    """
    F.expr() support: SQL expression string resolved in select() context (PySpark parity).
    - REGEXP/RLIKE: "col REGEXP 'pat'" -> col(col).rlike(pat)
    - Other expressions (e.g. "upper(x) as up") -> expr_str; resolved when used in df.select().
    Requires active SparkSession.
    """
    _active_session()
    import re

    m = re.match(
        r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s+(REGEXP|RLIKE)\s+'([^']*)'\s*$",
        sql_expr,
        re.IGNORECASE,
    )
    if m:
        col_name = m.group(1)
        pattern = m.group(3)
        return col(col_name).rlike(pattern)
    return _native.expr_str(sql_expr)


def _wrap_expr_str_in_sql_fn(column: object, fn: str, fmt: str | None = None) -> object:
    """Compose F.expr() SQL when a function wraps an expr() argument (#1574)."""
    sql = getattr(column, "sql", None)
    if not isinstance(sql, str):
        return column
    if fmt is not None:
        esc = fmt.replace("'", "''")
        return expr(f"{fn}({sql}, '{esc}')")
    return expr(f"{fn}({sql})")


def to_timestamp(column, fmt=None):
    wrapped = _wrap_expr_str_in_sql_fn(column, "to_timestamp", fmt)
    if getattr(wrapped, "sql", None) is not None:
        return wrapped
    return _to_timestamp(_as_col(column), fmt)


def to_timestamp_ltz(column, fmt=None):
    wrapped = _wrap_expr_str_in_sql_fn(column, "to_timestamp_ltz", fmt)
    if getattr(wrapped, "sql", None) is not None:
        return wrapped
    return _to_timestamp_ltz(_as_col(column), fmt)


def to_timestamp_ntz(column, fmt=None):
    wrapped = _wrap_expr_str_in_sql_fn(column, "to_timestamp_ntz", fmt)
    if getattr(wrapped, "sql", None) is not None:
        return wrapped
    return _to_timestamp_ntz(_as_col(column), fmt)


def to_date(column, fmt=None):
    wrapped = _wrap_expr_str_in_sql_fn(column, "to_date", fmt)
    if getattr(wrapped, "sql", None) is not None:
        return wrapped
    return _to_date(_as_col(column), fmt)


def datediff(end, start):
    return _datediff(_as_col(end), _as_col(start))


def current_date():
    """Return current date as column expression (PySpark parity: no session required at build time)."""
    return _current_date()


def current_timestamp():
    """Return current timestamp as column expression (PySpark parity: no session required at build time)."""
    return _current_timestamp()  # noqa: F821 - _current_timestamp from sparkless import


def input_file_name():
    return _input_file_name()


def monotonically_increasing_id():
    return _monotonically_increasing_id()


def spark_partition_id():
    return _spark_partition_id()


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


def length(column):
    """String length in characters (PySpark length)."""
    return _length(_as_col(column))


def floor(column):
    """Floor to nearest integer (PySpark floor)."""
    return _floor(_as_col(column))


def round(column, scale=0):
    """Round to given decimal places (PySpark round). scale=0 by default."""
    return _round(_as_col(column), scale)


def ltrim(column):
    """Trim leading whitespace (PySpark ltrim)."""
    return _ltrim(_as_col(column))


def hour(column):
    """Extract hour from datetime (PySpark hour)."""
    return _hour(_as_col(column))


def minute(column):
    """Extract minute from datetime (PySpark minute)."""
    return _minute(_as_col(column))


def soundex(column):
    """American Soundex code (PySpark soundex)."""
    return _soundex(_as_col(column))


def repeat(column, n):
    """Repeat string n times (PySpark repeat)."""
    return _repeat(_as_col(column), n)


def levenshtein(column, other):
    """Levenshtein distance between two strings (PySpark levenshtein)."""
    return _levenshtein(_as_col(column), _as_col(other))


def try_cast(column, type_name):
    """Cast to type, null on invalid (PySpark try_cast)."""
    return _try_cast(_as_col(column), type_name)


def try_add(left, right):
    """Add that returns null on overflow (PySpark try_add)."""
    return _try_add(_as_col(left), _as_col(right))


def concat(*cols):
    """Concatenate columns as strings (PySpark concat)."""
    if not cols:
        raise ValueError("concat requires at least one column")
    return _concat(*[_as_col(c) for c in cols])


def concat_ws(separator, *cols):
    """Concatenate columns with separator (PySpark concat_ws)."""
    if not cols:
        raise ValueError("concat_ws requires at least one column")
    return _concat_ws(separator, *[_as_col(c) for c in cols])


def array(*cols):
    """Create array column from columns (PySpark array)."""
    return _array(*[_as_col(c) for c in cols])


def struct(*cols):
    """Create struct from columns (PySpark struct)."""
    if not cols:
        raise ValueError("struct requires at least one column")
    return _struct(*[_as_col(c) for c in cols])


def asinh(column):
    """Inverse hyperbolic sine (PySpark asinh)."""
    return _asinh(_as_col(column))


def atanh(column):
    """Inverse hyperbolic tangent (PySpark atanh)."""
    return _atanh(_as_col(column))


def cosh(column):
    """Hyperbolic cosine (PySpark cosh)."""
    return _cosh(_as_col(column))


def sinh(column):
    """Hyperbolic sine (PySpark sinh)."""
    return _sinh(_as_col(column))


def last_day(column):
    """Last day of month for date column (PySpark last_day)."""
    return _last_day(_as_col(column))


def months_between(end, start, round_off=True):
    """Months between end and start dates (PySpark months_between)."""
    return _months_between(_as_col(end), _as_col(start), round_off)


def timestamp_seconds(column):
    """Convert seconds since epoch to timestamp (PySpark timestamp_seconds)."""
    return _timestamp_seconds(_as_col(column))


def to_utc_timestamp(column, tz):
    """Interpret timestamp as in tz, convert to UTC (PySpark to_utc_timestamp)."""
    return _to_utc_timestamp(_as_col(column), tz)


def date_trunc(format, column):
    """Truncate date/timestamp to unit (PySpark date_trunc)."""
    return _date_trunc(format, _as_col(column))


def translate(column, from_str, to_str):
    """Character-by-character translation (PySpark translate)."""
    return _translate(_as_col(column), from_str, to_str)


def substring_index(column, delimiter, count):
    """Substring before/after nth delimiter (PySpark substring_index). count > 0: before nth from left; count < 0: after nth from right."""
    return _substring_index(_as_col(column), delimiter, count)


def get_json_object(column, path):
    """Extract JSON path from string column (PySpark get_json_object)."""
    return _get_json_object(_as_col(column), path)


def json_tuple(
    column: ColumnOrName, *keys: str
) -> Union[_ColumnType, Tuple[_ColumnType, ...]]:
    """Extract keys from JSON as columns (PySpark json_tuple).

    Returns one string column per key, named c0, c1, ... in the order of keys.
    With a single key, returns one Column (for withColumn); multiple keys return a tuple
    that select() flattens into separate columns.
    """
    if not keys:
        raise ValueError("json_tuple requires at least one key")
    from typing import List

    cols: List[_ColumnType] = []
    for idx, key in enumerate(keys):
        # Use get_json_object to extract each key as a separate string column, then
        # alias to PySpark-style c0, c1, ... so df.select(F.json_tuple(...)) yields
        # the expected unnamed columns.
        c = tcast(_ColumnType, get_json_object(column, f"$.{key}"))
        cols.append(c.alias(f"c{idx}"))
    if len(cols) == 1:
        return cols[0]
    # df.select() flattens tuples/lists of Columns, so returning a tuple here
    # produces multiple top-level columns matching PySpark's json_tuple behavior.
    return tuple(cols)


def size(column):
    """Number of elements in array column (PySpark size)."""
    return _size(_as_col(column))


def array_contains(column, value):
    """True if array contains value (PySpark array_contains).

    - Literal values (int/float/bool/str) are compared directly to array elements.
    - Column arguments (Column) use arrays_overlap(array_col, array(value_col)) so join
      conditions like df.filter(F.array_contains(\"tags\", other_col)) work.
    """
    # Column argument (e.g. join condition)
    if isinstance(value, _Column):
        return arrays_overlap(_as_col(column), array(value))
    # Literal or column name: follow PySpark and treat plain strings as literals here.
    v = _as_col(value) if not isinstance(value, (int, float, bool, str)) else lit(value)
    return _array_contains(_as_col(column), v)


def arrays_overlap(col1, col2):
    """True if two array columns have any element in common (PySpark arrays_overlap)."""
    return _native.arrays_overlap(_as_col(col1), _as_col(col2))


__all__ = [
    "_array_distinct_column",
    "_wrap_expr_str_in_sql_fn",
    "array",
    "array_contains",
    "arrays_overlap",
    "asinh",
    "atanh",
    "avg",
    "coalesce",
    "concat",
    "concat_ws",
    "cosh",
    "count",
    "current_date",
    "current_timestamp",
    "date_add",
    "date_format",
    "date_sub",
    "date_trunc",
    "datediff",
    "dayofmonth",
    "dayofweek",
    "expr",
    "floor",
    "format_string",
    "from_unixtime",
    "get_json_object",
    "greatest",
    "hour",
    "input_file_name",
    "json_tuple",
    "last_day",
    "least",
    "length",
    "levenshtein",
    "ltrim",
    "max",
    "min",
    "minute",
    "monotonically_increasing_id",
    "month",
    "months_between",
    "posexplode",
    "printf",
    "regexp_extract",
    "regexp_extract_all",
    "regexp_like",
    "regexp_replace",
    "repeat",
    "round",
    "sinh",
    "size",
    "soundex",
    "spark_partition_id",
    "split",
    "struct",
    "substring_index",
    "sum",
    "timestamp_seconds",
    "to_date",
    "to_timestamp",
    "to_timestamp_ltz",
    "to_timestamp_ntz",
    "to_utc_timestamp",
    "translate",
    "try_add",
    "try_cast",
    "unix_timestamp",
    "when",
    "year",
]

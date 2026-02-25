# PySpark-style: from sparkless.sql.functions import col, lit, when, count, sum, ...
import getpass

from sparkless import (
    column as col,
    lit,
    lit_i64,
    lit_str,
    lit_bool,
    lit_f64,
    lit_null,
    format_string as _format_string,
    printf as _printf,
    greatest as _greatest,
    least as _least,
    array_distinct as _array_distinct,
    posexplode as _posexplode,
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
    regexp_extract as _regexp_extract,
    regexp_extract_all as _regexp_extract_all,
    regexp_like as _regexp_like,
    split as _split,
    coalesce as _coalesce,
    to_timestamp as _to_timestamp,
    to_date as _to_date,
    current_date as _current_date,
    input_file_name as _input_file_name,
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
    length as _length,
    floor as _floor,
    round as _round,
    ltrim as _ltrim,
    hour as _hour,
    minute as _minute,
    soundex as _soundex,
    repeat as _repeat,
    levenshtein as _levenshtein,
    try_cast as _try_cast,
    try_add as _try_add,
    concat as _concat,
    concat_ws as _concat_ws,
    array as _array,
    struct as _struct,
    asinh as _asinh,
    atanh as _atanh,
    cosh as _cosh,
    sinh as _sinh,
    last_day as _last_day,
    months_between as _months_between,
    timestamp_seconds as _timestamp_seconds,
    to_utc_timestamp as _to_utc_timestamp,
    approx_count_distinct as _approx_count_distinct,
    date_trunc as _date_trunc,
    first as _first_agg,
    translate as _translate,
    substring_index as _substring_index,
    crc32 as _crc32,
    xxhash64 as _xxhash64,
    get_json_object as _get_json_object,
    json_tuple as _json_tuple,
    size as _size,
    array_contains as _array_contains,
    explode as _explode,
)
from sparkless.errors import PySparkValueError

# Registry for Python UDFs: udf_name -> (callable, return_type). Populated by udf() / @udf.
_PYTHON_UDF_REGISTRY = {}

# Default return type when @udf() is used without arguments (PySpark uses StringType).
_DEFAULT_UDF_RETURN_TYPE = None  # Set below after importing types


def _ensure_udf_executor_registered():
    """Register the Python UDF executor with the native module once (for with_column UDF handling)."""
    from sparkless import _native

    if getattr(_ensure_udf_executor_registered, "_registered", False):
        return
    _native.set_python_udf_executor(_python_udf_executor)
    _ensure_udf_executor_registered._registered = True


def _python_udf_executor(df, column_name, udf_name, arg_names, arg_literal_jsons=None):
    """Run a registered Python UDF over the DataFrame: collect, apply per row, create new DataFrame. Called from Rust with_column.
    arg_literal_jsons: optional list of Optional[str]; if arg_literal_jsons[i] is not None, use json.loads(...) as the arg value (literal column)."""
    import json as _json

    import sparkless.sql.types as T

    if udf_name not in _PYTHON_UDF_REGISTRY:
        raise PySparkValueError(f"Unknown UDF name: {udf_name}")
    func, return_type = _PYTHON_UDF_REGISTRY[udf_name]
    session = _active_session()
    rows = df.collect()
    if not rows:
        # Empty DataFrame: replace or append column (same as non-empty path)
        existing_names = [f.name for f in df.schema.fields]
        if column_name in existing_names:
            extended_fields = [
                T.StructField(f.name, return_type if f.name == column_name else f.dataType, f.nullable if f.name != column_name else True)
                for f in df.schema.fields
            ]
        else:
            extended_fields = list(df.schema.fields) + [T.StructField(column_name, return_type, True)]
        extended_schema = T.StructType(extended_fields)
        return session.createDataFrame([], schema=extended_schema)
    field_names = [f.name for f in df.schema.fields]
    # Build dicts from schema + row values. Row.__iter__ yields keys (_fields), so use positional indexing.
    row_dicts = [{field_names[i]: row[i] for i in range(len(field_names))} for row in rows]
    arg_names_list = list(arg_names)
    literals = (arg_literal_jsons if arg_literal_jsons is not None else [None] * len(arg_names_list))
    if len(literals) < len(arg_names_list):
        literals = list(literals) + [None] * (len(arg_names_list) - len(literals))

    def arg_value(d, i):
        if literals[i] is not None:
            return _json.loads(literals[i])
        return d[arg_names_list[i]]

    new_vals = [func(*[arg_value(d, i) for i in range(len(arg_names_list))]) for d in row_dicts]
    for i, d in enumerate(row_dicts):
        d[column_name] = new_vals[i]
    # Replace existing column if column_name already in schema, else append (PySpark withColumn semantics).
    existing_names = [f.name for f in df.schema.fields]
    if column_name in existing_names:
        extended_fields = [
            T.StructField(f.name, return_type if f.name == column_name else f.dataType, f.nullable if f.name != column_name else True)
            for f in df.schema.fields
        ]
    else:
        extended_fields = list(df.schema.fields) + [T.StructField(column_name, return_type, True)]
    extended_schema = T.StructType(extended_fields)
    # Pass rows as list-of-lists in schema order so createDataFrame does not reorder by sorted dict keys.
    row_lists = [[d[f.name] for f in extended_schema.fields] for d in row_dicts]
    return session.createDataFrame(row_lists, schema=extended_schema)


def udf(f=None, returnType=None):
    """Create a Python UDF. Use as F.udf(func, returnType) or @udf(returnType) def func ...

    When the returned callable is used in withColumn with column(s), the UDF runs row-by-row
    (e.g. df.withColumn("y", my_udf(F.col("x")))).
    """
    import uuid

    from sparkless import _native

    # Lazy default: avoid circular import
    global _DEFAULT_UDF_RETURN_TYPE
    if _DEFAULT_UDF_RETURN_TYPE is None:
        import sparkless.sql.types as T

        _DEFAULT_UDF_RETURN_TYPE = T.StringType()

    def register_and_wrap(func, return_type):
        from sparkless.sql.functions import col as _col

        _ensure_udf_executor_registered()
        udf_name = "udf_" + uuid.uuid4().hex
        _PYTHON_UDF_REGISTRY[udf_name] = (func, return_type)

        def wrapper(*cols):
            # When used as instance method (e.g. self.add(F.col('a'), F.col('b'))), first arg is self; drop it.
            # When descriptor is accessed on the class (e.g. C.add during class build), first arg can be the class; return wrapper so the attribute stays callable.
            if cols and not isinstance(cols[0], str) and not getattr(cols[0], "get_udf_call_info", None):
                first = cols[0]
                if isinstance(first, type):
                    return wrapper
                cols = cols[1:]
            # If we were called with no column args (e.g. wrapper() or wrapper(owner) after strip), do not return a UDF column; return wrapper so decorator leaves the name as the callable.
            if not cols:
                return wrapper
            if len(cols) == 1 and callable(cols[0]) and not getattr(cols[0], "get_udf_call_info", None):
                raise TypeError(
                    "UDF must be called with Column(s), e.g. my_udf(F.col('a'), F.col('b')). "
                    "Got a single callable - ensure you call the UDF with column arguments."
                )
            resolved = [_col(c) if isinstance(c, str) else c for c in cols]
            return _native.create_udf_column(udf_name, resolved)

        return wrapper

    # @udf(returnType) or @udf() — decorator with optional return type
    if f is None:
        return_type = returnType if returnType is not None else _DEFAULT_UDF_RETURN_TYPE

        def decorator(func):
            return register_and_wrap(func, return_type)

        return decorator
    # @udf(returnType) with single arg: that arg is the return type (decorator). Distinguish from F.udf(func).
    if returnType is None and f is not None:
        # Type-like: a class or an instance of a type (e.g. IntegerType(), StringType())
        is_return_type = isinstance(f, type) or getattr(type(f), "__name__", "").endswith("Type")
        if is_return_type:
            return_type = f

            def decorator(func):
                return register_and_wrap(func, return_type)

            return decorator
    # F.udf(f, returnType)
    return_type = returnType if returnType is not None else _DEFAULT_UDF_RETURN_TYPE
    return register_and_wrap(f, return_type)


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
    "input_file_name",
    "unix_timestamp",
    "from_unixtime",
    "year",
    "month",
    "dayofmonth",
    "dayofweek",
    "date_add",
    "date_sub",
    "date_format",
    "length",
    "floor",
    "round",
    "ltrim",
    "hour",
    "minute",
    "soundex",
    "repeat",
    "levenshtein",
    "try_cast",
    "try_add",
    "concat",
    "concat_ws",
    "array",
    "struct",
    "asinh",
    "atanh",
    "cosh",
    "sinh",
    "last_day",
    "months_between",
    "timestamp_seconds",
    "to_utc_timestamp",
    "regexp_replace",
    "regexp_extract",
    "regexp_extract_all",
    "regexp_like",
    "split",
    "coalesce",
    "format_string",
    "printf",
    "greatest",
    "least",
    "array_distinct",
    "posexplode",
    "approx_count_distinct",
    "date_trunc",
    "first",
    "translate",
    "substring_index",
    "crc32",
    "xxhash64",
    "get_json_object",
    "json_tuple",
    "size",
    "array_contains",
    "explode",
    "expr",
    "current_database",
    "current_schema",
    "current_catalog",
    "current_user",
    "row_number",
    "percent_rank",
    "rank",
    "dense_rank",
    "ntile",
    "lag",
    "lead",
    "first_value",
    "last_value",
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


def array_distinct(col):
    """Distinct elements in array column (PySpark array_distinct)."""
    return _array_distinct(_as_col(col))


def posexplode(col):
    """Explode array with position (PySpark posexplode). Returns (pos_col, val_col) or wrapper with .alias()."""
    return _posexplode(col)


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


def input_file_name():
    return _input_file_name()


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


def approx_count_distinct(col, rsd=None):
    """Approximate distinct count (PySpark approx_count_distinct); use in groupBy().agg()."""
    return _approx_count_distinct(_as_col(col), rsd)


def date_trunc(format, column):
    """Truncate date/timestamp to unit (PySpark date_trunc)."""
    return _date_trunc(format, _as_col(column))


def first(col, ignorenulls=True):
    """First value in group (PySpark first); use in groupBy().agg()."""
    return _first_agg(_as_col(col), ignorenulls)


def translate(column, from_str, to_str):
    """Character-by-character translation (PySpark translate)."""
    return _translate(_as_col(column), from_str, to_str)


def substring_index(column, delimiter, count):
    """Substring before/after nth delimiter (PySpark substring_index). count > 0: before nth from left; count < 0: after nth from right."""
    return _substring_index(_as_col(column), delimiter, count)


def crc32(column):
    """CRC32 checksum of string bytes (PySpark crc32)."""
    return _crc32(_as_col(column))


def xxhash64(column):
    """XXH64 hash of string (PySpark xxhash64)."""
    return _xxhash64(_as_col(column))


def get_json_object(column, path):
    """Extract JSON path from string column (PySpark get_json_object)."""
    return _get_json_object(_as_col(column), path)


def json_tuple(column, *keys):
    """Extract keys from JSON as struct (PySpark json_tuple)."""
    return _json_tuple(_as_col(column), keys)


def size(column):
    """Number of elements in array column (PySpark size)."""
    return _size(_as_col(column))


def array_contains(column, value):
    """True if array contains value (PySpark array_contains). value can be column name, Column, or literal."""
    if isinstance(value, str):
        v = col(value)
    elif hasattr(value, "inner"):  # PyColumn
        v = value
    else:
        v = lit(value)
    return _array_contains(_as_col(column), v)


def explode(column):
    """Explode array into one row per element (PySpark explode)."""
    return _explode(_as_col(column))


# lit is imported from sparkless (native polymorphic implementation)


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

    With no args or create_map([]), returns a column of empty maps per row.
    """
    import sparkless._native as _native

    # PySpark: create_map() or create_map([]) -> empty map
    expanded = []
    for c in cols:
        if isinstance(c, (list, tuple)):
            expanded.extend(c)
        else:
            expanded.append(c)
    if len(expanded) % 2 != 0:
        raise ValueError("create_map requires an even number of arguments (key-value pairs)")
    key_values = [_as_col(x) for x in expanded]
    return _native.create_map(key_values)


class _RowNumberExpr:
    def over(self, window):
        import sparkless._native as _native

        partition_by, encoded, _ = _window_spec_to_partition_order(window)
        return _native.row_number_window(partition_by, encoded)


def row_number():
    """Window row_number() expression; use with .over(Window.partitionBy(...).orderBy(...))."""
    return _RowNumberExpr()


class _PercentRankExpr:
    def over(self, window):
        import sparkless._native as _native

        partition_by, encoded, _ = _window_spec_to_partition_order(window)
        return _native.percent_rank_window(partition_by, encoded)


def percent_rank():
    """Window percent_rank() expression; use with .over(Window.partitionBy(...).orderBy(...))."""
    return _PercentRankExpr()


class _RankExpr:
    def over(self, window):
        import sparkless._native as _native

        partition_by, encoded, _ = _window_spec_to_partition_order(window)
        return _native.rank_window(partition_by, encoded)


def rank():
    """Window rank() expression; use with .over(Window.partitionBy(...).orderBy(...))."""
    return _RankExpr()


class _DenseRankExpr:
    def over(self, window):
        import sparkless._native as _native

        partition_by, encoded, _ = _window_spec_to_partition_order(window)
        return _native.dense_rank_window(partition_by, encoded)


def dense_rank():
    """Window dense_rank() expression; use with .over(Window.partitionBy(...).orderBy(...))."""
    return _DenseRankExpr()


class _NtileExpr:
    def __init__(self, n):
        self._n = n

    def over(self, window):
        import sparkless._native as _native

        partition_by, encoded, _ = _window_spec_to_partition_order(window)
        return _native.ntile_window(self._n, partition_by, encoded)


def ntile(n):
    """Window ntile(n) expression; use with .over(Window.partitionBy(...).orderBy(...))."""
    return _NtileExpr(n)


class _LagExpr:
    def __init__(self, col_or_name, offset=1):
        self._col_or_name = col_or_name
        self._offset = offset

    def over(self, window):
        import sparkless._native as _native

        partition_by, encoded, _ = _window_spec_to_partition_order(window)
        name = _col_name(self._col_or_name)
        return _native.lag_window(name, self._offset, partition_by, encoded)


def lag(col_or_name, offset=1):
    """Window lag(col, offset) expression; use with .over(Window.partitionBy(...).orderBy(...))."""
    return _LagExpr(col_or_name, offset)


class _LeadExpr:
    def __init__(self, col_or_name, offset=1):
        self._col_or_name = col_or_name
        self._offset = offset

    def over(self, window):
        import sparkless._native as _native

        partition_by, encoded, _ = _window_spec_to_partition_order(window)
        name = _col_name(self._col_or_name)
        return _native.lead_window(name, self._offset, partition_by, encoded)


def lead(col_or_name, offset=1):
    """Window lead(col, offset) expression; use with .over(Window.partitionBy(...).orderBy(...))."""
    return _LeadExpr(col_or_name, offset)


class _FirstValueExpr:
    def __init__(self, col_or_name):
        self._col_or_name = col_or_name

    def over(self, window):
        from sparkless import column as col

        c = col(self._col_or_name) if isinstance(self._col_or_name, str) else self._col_or_name
        return c.first_value().over(window)


def first_value(col_or_name):
    """Window first_value(col) expression; use with .over(Window.partitionBy(...))."""
    return _FirstValueExpr(col_or_name)


class _LastValueExpr:
    def __init__(self, col_or_name):
        self._col_or_name = col_or_name

    def over(self, window):
        from sparkless import column as col

        c = col(self._col_or_name) if isinstance(self._col_or_name, str) else self._col_or_name
        return c.last_value().over(window)


def last_value(col_or_name):
    """Window last_value(col) expression; use with .over(Window.partitionBy(...))."""
    return _LastValueExpr(col_or_name)


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
    Accepts a list of column names as shorthand for partition-only window (e.g. .over(["dept"])).
    """
    if isinstance(window, (list, tuple)):
        partition_names = [c if isinstance(c, str) else _col_name(c) for c in window]
        return partition_names, [], False
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

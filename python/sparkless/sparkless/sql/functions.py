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
    _native_when,
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
    concat as _concat,
)
from sparkless.errors import PySparkValueError

def _not_implemented(name):
    def _fn(*args, **kwargs):
        raise NotImplementedError(f"{name} is not yet implemented in robin-sparkless")

    _fn.__name__ = name
    return _fn


import sparkless._native as _native

def _as_col(c):
    return col(c) if isinstance(c, str) else c

def _ni(name):
    return _not_implemented(name)

# --- Math functions (native-backed) ---
def floor(c): return _native.native_floor(_as_col(c))
def ceil(c): return _native.native_ceil(_as_col(c))
ceiling = ceil
def abs(c): return _native.native_abs(_as_col(c))
def sqrt(c): return _native.native_sqrt(_as_col(c))
def log(c, base=None): return _native.native_log(_as_col(c))
def exp(c): return _native.native_exp(_as_col(c))
def pow(col1, col2): return _native.native_pow(_as_col(col1), int(col2))
power = pow
def round(c, scale=0): return _native.native_round(_as_col(c), scale)
def signum(c): return _native.native_signum(_as_col(c))
def sin(c): return _native.native_sin(_as_col(c))
def cos(c): return _native.native_cos(_as_col(c))
def tan(c): return _native.native_tan(_as_col(c))
def asin(c): return _native.native_asin(_as_col(c))
def acos(c): return _native.native_acos(_as_col(c))
def atan(c): return _native.native_atan(_as_col(c))
def atan2(y, x): return _native.native_atan2(_as_col(y), _as_col(x))
def degrees(c): return _native.native_degrees(_as_col(c))
def radians(c): return _native.native_radians(_as_col(c))
def log2(c): return _native.native_log2(_as_col(c))
def log10(c): return _native.native_log10(_as_col(c))
def greatest(*cols): return _native.native_greatest([_as_col(c) for c in cols])
def least(*cols): return _native.native_least([_as_col(c) for c in cols])
def coalesce(*cols): return _native.native_coalesce([_as_col(c) for c in cols])

nanvl = _ni("nanvl")
isnan = _ni("isnan")
isnull = _ni("isnull")
monotonically_increasing_id = _ni("monotonically_increasing_id")
input_file_name = _ni("input_file_name")
spark_partition_id = _ni("spark_partition_id")
broadcast = _ni("broadcast")
hash = _ni("hash")

# --- Hash / encoding functions (native-backed) ---
def xxhash64(c): return _native.native_xxhash64(_as_col(c))
def md5(c): return _native.native_md5(_as_col(c))
def sha1(c): return _native.native_sha1(_as_col(c))
def sha2(c, numBits): return _native.native_sha2(_as_col(c), numBits)
def crc32(c): return _native.native_crc32(_as_col(c))
def base64(c): return _native.native_base64(_as_col(c))
def unbase64(c): return _native.native_unbase64(_as_col(c))
def ascii(c): return _native.native_ascii(_as_col(c))
def hex(c): return _native.native_hex(_as_col(c))
def unhex(c): return _native.native_unhex(_as_col(c))
def bin(c): return _native.native_bin(_as_col(c))
def conv(c, fromBase, toBase): return _native.native_conv(_as_col(c), fromBase, toBase)
def format_number(c, d): return _native.native_format_number(_as_col(c), d)

# --- Array / collection functions (native-backed) ---
array = _ni("array")
struct = _ni("struct")
def explode(col_or_name): return _native.native_explode(_as_col(col_or_name))
def explode_outer(col_or_name): return _native.native_explode_outer(_as_col(col_or_name))
def posexplode(col_or_name): return _native.native_posexplode(_as_col(col_or_name))
posexplode_outer = _ni("posexplode_outer")
def flatten(col_or_name): return _native.native_flatten(_as_col(col_or_name))
def split(str_col, pattern, limit=-1):
    lim = limit if limit != -1 else None
    return _native.native_split(_as_col(str_col), pattern, lim)
def format_string(fmt, *cols): return _native.native_format_string(fmt, [_as_col(c) for c in cols])
concat_ws = _ni("concat_ws")

# --- Aggregate functions (native-backed) ---
def mean(col_or_name): return avg(col_or_name)
def first(col_or_name, ignorenulls=False): return _native.native_first(_as_col(col_or_name), ignorenulls)
last = _ni("last")
def collect_list(col_or_name): return _native.native_collect_list(_as_col(col_or_name))
def collect_set(col_or_name): return _native.native_collect_set(_as_col(col_or_name))
def array_contains(col_or_name, value):
    v = _as_col(value) if not isinstance(value, (int, float, bool, str)) else lit(value)
    return _native.native_array_contains(_as_col(col_or_name), v)
def array_distinct(col_or_name): return _native.native_array_distinct(_as_col(col_or_name))
def array_sort(col_or_name): return _native.native_array_sort(_as_col(col_or_name))
def array_join(col_or_name, delimiter, null_replacement=None): return _native.native_array_join(_as_col(col_or_name), delimiter)
def array_max(col_or_name): return _native.native_array_max(_as_col(col_or_name))
def array_min(col_or_name): return _native.native_array_min(_as_col(col_or_name))
def element_at(col_or_name, extraction): return _native.native_element_at(_as_col(col_or_name), extraction)
def size(col_or_name): return _native.native_size(_as_col(col_or_name))
slice = _ni("slice")
def sort_array(col_or_name, asc=True): return _native.native_array_sort(_as_col(col_or_name))
def array_union(col1, col2): return _native.native_array_union(_as_col(col1), _as_col(col2))
def array_intersect(col1, col2): return _native.native_array_intersect(_as_col(col1), _as_col(col2))
def array_except(col1, col2): return _native.native_array_except(_as_col(col1), _as_col(col2))
def map_keys(col_or_name): return _native.native_map_keys(_as_col(col_or_name))
def map_values(col_or_name): return _native.native_map_values(_as_col(col_or_name))
map_from_arrays = _ni("map_from_arrays")
sumDistinct = _ni("sumDistinct")
def count_distinct(*cols):
    if len(cols) == 1:
        return _native.native_count_distinct(_as_col(cols[0]))
    raise NotImplementedError("count_distinct with multiple columns is not yet implemented")
countDistinct = count_distinct
def approx_count_distinct(col_or_name, rsd=0.05): return _native.native_approx_count_distinct(_as_col(col_or_name), rsd)
def stddev(col_or_name): return _native.native_stddev(_as_col(col_or_name))
def stddev_pop(col_or_name): return _native.native_stddev_pop(_as_col(col_or_name))
def stddev_samp(col_or_name): return _native.native_stddev_samp(_as_col(col_or_name))
def variance(col_or_name): return _native.native_variance(_as_col(col_or_name))
def var_pop(col_or_name): return _native.native_var_pop(_as_col(col_or_name))
def var_samp(col_or_name): return _native.native_var_samp(_as_col(col_or_name))
def corr(col1, col2): return _native.native_corr(_as_col(col1), _as_col(col2))
percentile_approx = _ni("percentile_approx")

# --- String functions (native-backed) ---
def lpad(col_or_name, len, pad): return _native.native_lpad(_as_col(col_or_name), len, pad)
def rpad(col_or_name, len, pad): return _native.native_rpad(_as_col(col_or_name), len, pad)
def ltrim(col_or_name): return _native.native_ltrim(_as_col(col_or_name))
def rtrim(col_or_name): return _native.native_rtrim(_as_col(col_or_name))
def initcap(col_or_name): return _native.native_initcap(_as_col(col_or_name))
def soundex(col_or_name): return _native.native_soundex(_as_col(col_or_name))
def length(col_or_name): return _native.native_length(_as_col(col_or_name))
def reverse(col_or_name): return _native.native_reverse(_as_col(col_or_name))
def translate(col_or_name, matching, replace): return _native.native_translate(_as_col(col_or_name), matching, replace)
def instr(col_or_name, substr): return _native.native_instr(_as_col(col_or_name), substr)
def locate(substr, col_or_name, pos=1): return _native.native_locate(substr, _as_col(col_or_name), pos)
def repeat(col_or_name, n): return _native.native_repeat(_as_col(col_or_name), n)
def regexp_extract(col_or_name, pattern, idx=0): return _native.native_regexp_extract(_as_col(col_or_name), pattern, idx)
overlay = _ni("overlay")

# --- Window functions (stubs) ---
dense_rank = _ni("dense_rank")
rank = _ni("rank")
percent_rank = _ni("percent_rank")
cume_dist = _ni("cume_dist")
ntile = _ni("ntile")
lag = _ni("lag")
lead = _ni("lead")
window = _ni("window")

# --- JSON functions (stubs) ---
from_json = _ni("from_json")
to_json = _ni("to_json")
schema_of_json = _ni("schema_of_json")
get_json_object = _ni("get_json_object")
json_tuple = _ni("json_tuple")
decode = _ni("decode")
encode = _ni("encode")
char = _ni("char")
factorial = _ni("factorial")
cbrt = _ni("cbrt")
hypot = _ni("hypot")

# --- Date/time functions (native-backed) ---
def add_months(start, months): return _native.native_add_months(_as_col(start), months)
def months_between(date1, date2, roundOff=True): return _native.native_months_between(_as_col(date1), _as_col(date2), roundOff)
def next_day(date, dayOfWeek): return _native.native_next_day(_as_col(date), dayOfWeek)
def last_day(date): return _native.native_last_day(_as_col(date))
def trunc(date, fmt): return _native.native_trunc(_as_col(date), fmt)
def date_trunc(fmt, timestamp): return _native.native_date_trunc(fmt, _as_col(timestamp))
def hour(col_or_name): return _native.native_hour(_as_col(col_or_name))
def minute(col_or_name): return _native.native_minute(_as_col(col_or_name))
def second(col_or_name): return _native.native_second(_as_col(col_or_name))
def quarter(col_or_name): return _native.native_quarter(_as_col(col_or_name))
def dayofyear(col_or_name): return _native.native_dayofyear(_as_col(col_or_name))
def weekofyear(col_or_name): return _native.native_weekofyear(_as_col(col_or_name))
make_date = _ni("make_date")
typeof = _ni("typeof")

def udf(*args, **kwargs):
    """UDF not yet implemented in robin-sparkless."""
    raise NotImplementedError("udf is not yet implemented in robin-sparkless")


__all__ = [
    "col", "lit", "lit_i64", "lit_str", "lit_bool", "lit_f64", "lit_null",
    "upper", "lower", "substring", "trim", "cast", "when",
    "count", "sum", "avg", "min", "max", "mean",
    "to_timestamp", "to_date", "datediff", "current_date",
    "unix_timestamp", "from_unixtime",
    "year", "month", "dayofmonth", "dayofweek", "date_add", "date_sub", "date_format",
    "floor", "ceil", "abs", "sqrt", "log", "exp", "pow", "round",
    "greatest", "least", "coalesce", "nanvl", "isnan", "isnull",
    "regexp_replace", "regexp_extract_all", "regexp_like", "regexp_extract",
    "expr", "concat",
    "current_database", "current_schema", "current_catalog", "current_user",
    "row_number", "dense_rank", "rank", "percent_rank", "cume_dist", "ntile",
    "lag", "lead",
    "create_map", "asc", "desc", "udf",
    "array", "struct", "explode", "explode_outer", "posexplode", "posexplode_outer",
    "flatten", "split", "concat_ws",
    "format_string", "format_number",
    "first", "last", "collect_list", "collect_set",
    "array_contains", "array_distinct", "array_sort", "array_join",
    "array_max", "array_min", "element_at", "size", "slice", "sort_array",
    "array_union", "array_intersect", "array_except",
    "map_keys", "map_values", "map_from_arrays",
    "sumDistinct", "count_distinct", "countDistinct",
    "approx_count_distinct", "corr",
    "stddev", "stddev_pop", "stddev_samp",
    "variance", "var_pop", "var_samp",
    "percentile_approx",
    "lpad", "rpad", "ltrim", "rtrim", "initcap", "soundex", "length",
    "reverse", "translate", "instr", "locate", "repeat", "overlay",
    "from_json", "to_json", "schema_of_json", "get_json_object", "json_tuple",
    "decode", "encode", "base64", "unbase64",
    "conv", "hex", "unhex", "ascii", "char", "bin",
    "signum", "factorial", "cbrt", "degrees", "radians",
    "sin", "cos", "tan", "asin", "acos", "atan", "atan2", "hypot",
    "log2", "log10",
    "add_months", "months_between", "next_day", "last_day", "trunc", "date_trunc",
    "hour", "minute", "second", "quarter", "dayofyear", "weekofyear",
    "make_date", "typeof",
    "monotonically_increasing_id", "input_file_name", "spark_partition_id",
    "broadcast", "hash", "xxhash64", "md5", "sha1", "sha2", "crc32",
    "window",
]

def when(condition, value=None):
    """PySpark-compatible when(). Accepts Column or str condition, optional value."""
    cond = _as_col(condition)
    if value is not None:
        val = _as_col(value) if not isinstance(value, (int, float, bool, str)) else lit(value)
        return _native_when(cond, val)
    return _native_when(cond)


def concat(*columns):
    """Concatenate columns as strings (PySpark concat)."""
    return _concat(*columns)


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
    """Build a map column from alternating key/value expressions (PySpark create_map)."""
    import sparkless._native as _native

    flat = []
    for c in cols:
        if isinstance(c, (list, tuple)):
            flat.extend(c)
        else:
            flat.append(c)
    key_values = [_as_col(c) for c in flat]
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
    if hasattr(arg, 'name') and hasattr(arg, 'ascending'):
        return arg.name
    if hasattr(arg, 'column_name'):
        return arg.column_name
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

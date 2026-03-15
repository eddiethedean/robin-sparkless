# PySpark-style: from sparkless.sql.functions import col, lit, when, count, sum, ...
# mypy: disable-error-code=no-redef
# ruff: noqa: F811
from __future__ import annotations

import getpass

from sparkless import _native, _Column
from sparkless._native import PyColumn as _ColumnType  # for type hints only
from sparkless import (
    column as col,
    lit,
    lit_i64,
    lit_str,
    lit_bool,
    lit_f64,
    lit_null,
    format_string as _format_string,
    greatest as _greatest,
    least as _least,
    array_distinct as _array_distinct,
    posexplode as _posexplode,
    posexplode_outer as _posexplode_outer,
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
    regexp_extract as _regexp_extract,
    regexp_extract_all as _regexp_extract_all,
    regexp_like as _regexp_like,
    split as _split,
    coalesce as _coalesce,
    to_timestamp as _to_timestamp,
    to_date as _to_date,
    current_date as _current_date,
    current_timestamp as _current_timestamp,
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
    size as _size,
    array_contains as _array_contains,
    explode as _explode,
)
from sparkless.errors import PySparkValueError
from sparkless import DataFrame
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Tuple, Union, cast

if TYPE_CHECKING:
    # Window spec used by window expressions (partitionBy/orderBy/rowsBetween/rangeBetween).
    from sparkless.sql.window import WindowSpec as _WindowSpec

# Column or column name (str); used for function params that accept either.
ColumnOrName = Union[_ColumnType, str]


def _col_result(x: Any) -> _ColumnType:
    """Cast native/column call result to _ColumnType for mypy no-any-return."""
    out: _ColumnType = cast(_ColumnType, x)
    return out


# Registry for Python UDFs: udf_name -> (callable, return_type). Populated by udf() / @udf.
_PYTHON_UDF_REGISTRY: Dict[str, Tuple[Callable[..., object], object]] = {}

# Default return type when @udf() is used without arguments (PySpark uses StringType).
_DEFAULT_UDF_RETURN_TYPE = None  # Set below after importing types


def _ensure_udf_executor_registered() -> None:
    """Register the Python UDF executor with the native module once (for with_column UDF handling)."""
    from sparkless import _native

    if getattr(_ensure_udf_executor_registered, "_registered", False):
        return
    _native.set_python_udf_executor(_python_udf_executor)
    _ensure_udf_executor_registered._registered = True  # type: ignore[attr-defined]


def _as_col(c: ColumnOrName) -> _ColumnType:
    result: _ColumnType = cast(_ColumnType, col(c) if isinstance(c, str) else c)
    return result


def _native_fn(name: str) -> Callable[..., _ColumnType]:
    """Get function from _native: try native_<name> first (e.g. native_floor), then <name> (e.g. floor)."""
    fn = getattr(_native, "native_" + name, None) or getattr(_native, name, None)
    out: Callable[..., _ColumnType] = cast(Callable[..., _ColumnType], fn)
    return out


def _not_implemented(name: str) -> Callable[..., None]:
    """Return a callable that raises NotImplementedError when called (for stub functions)."""

    def _raiser(*args: object, **kwargs: object) -> None:
        raise NotImplementedError(f"{name!r} is not yet implemented")

    return _raiser


def _ni(name: str) -> Callable[..., None]:
    return _not_implemented(name)


# --- Math functions (native-backed) ---
def floor(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("floor")(_as_col(c)))


def ceil(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("ceil")(_as_col(c)))


ceiling = ceil


def abs(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("abs")(_as_col(c)))


def sqrt(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("sqrt")(_as_col(c)))


def log(
    col_or_base: ColumnOrName,
    base_or_col: Optional[Union[ColumnOrName, int, float]] = None,
) -> _ColumnType:
    """Natural log, or log with base. PySpark: log(column) or log(base, column)."""
    if base_or_col is None:
        return _col_result(_native_fn("log")(_as_col(col_or_base)))
    # Two args: PySpark uses log(base, column); accept (base, column) or (column, base)
    if isinstance(base_or_col, (int, float)):
        return _col_result(
            _native_fn("log_with_base")(_as_col(col_or_base), float(base_or_col))
        )
    if isinstance(col_or_base, (int, float)):
        return _col_result(
            _native_fn("log_with_base")(_as_col(base_or_col), float(col_or_base))
        )
    raise TypeError(
        "log(base, column) or log(column, base): one argument must be a numeric base (int/float), the other a Column"
    )


def exp(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("exp")(_as_col(c)))


def pow(col1: ColumnOrName, col2: Union[int, float, _ColumnType]) -> _ColumnType:
    """Power: col1 ** col2. col2 can be int, float, or Column (e.g. F.lit(2))."""
    return _col_result(_native_fn("pow")(_as_col(col1), col2))


power = pow


def round(c: ColumnOrName, scale: int = 0) -> _ColumnType:
    return _col_result(_native_fn("round")(_as_col(c), scale))


def signum(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("signum")(_as_col(c)))


def sin(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("sin")(_as_col(c)))


def cos(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("cos")(_as_col(c)))


def tan(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("tan")(_as_col(c)))


def asin(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("asin")(_as_col(c)))


def acos(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("acos")(_as_col(c)))


def atan(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("atan")(_as_col(c)))


def pmod(dividend: ColumnOrName, divisor: ColumnOrName) -> _ColumnType:
    """Positive modulus (PySpark pmod)."""
    return _col_result(_native_fn("pmod")(_as_col(dividend), _as_col(divisor)))


def atan2(y: ColumnOrName, x: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("atan2")(_as_col(y), _as_col(x)))


def degrees(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("degrees")(_as_col(c)))


def radians(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("radians")(_as_col(c)))


def log2(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("log2")(_as_col(c)))


def log10(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("log10")(_as_col(c)))


def asc_nulls_last(c: ColumnOrName) -> _ColumnType:
    """Ascending sort order with nulls last (PySpark functions.asc_nulls_last)."""
    return _as_col(c).asc_nulls_last()


def desc_nulls_first(c: ColumnOrName) -> _ColumnType:
    """Descending sort order with nulls first (PySpark functions.desc_nulls_first)."""
    return _as_col(c).desc_nulls_first()


def desc_nulls_last(c: ColumnOrName) -> _ColumnType:
    """Descending sort order with nulls last (PySpark functions.desc_nulls_last)."""
    return _as_col(c).desc_nulls_last()


def greatest(*cols: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("greatest")(tuple([_as_col(c) for c in cols])))


def least(*cols: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("least")(tuple([_as_col(c) for c in cols])))


def coalesce(*cols: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("coalesce")(tuple([_as_col(c) for c in cols])))


def nanvl(col1: ColumnOrName, col2: ColumnOrName) -> _ColumnType:
    """Replace NaN with value. PySpark: F.nanvl(col1, col2)."""
    return _col_result(_native_fn("nanvl")(_as_col(col1), _as_col(col2)))


def isnan(c: ColumnOrName) -> _ColumnType:
    """True where the float value is NaN. PySpark: F.isnan(col).

    PySpark semantics:
    - Numeric columns: only real NaN => True; None/null => False.
    - String columns: \"NaN\" (any case/whitespace) => True; other strings/None => False.
    - Other types: always False.

    The native implementation follows these rules but can still yield nulls in some edge
    paths; coalesce with False to ensure the result is always non-null (matches PySpark).
    """
    base = _native_fn("isnan")(_as_col(c))
    # _coalesce is sparkless.coalesce imported above; lit_bool(False) is a native bool literal.
    return _col_result(_coalesce(base, lit_bool(False)))


# isnull defined above (before __all__)
monotonically_increasing_id = _ni("monotonically_increasing_id")
input_file_name = _ni("input_file_name")
spark_partition_id = _ni("spark_partition_id")
broadcast = _ni("broadcast")
hash = _ni("hash")


# --- Hash / encoding functions (native-backed) ---
def xxhash64(c: ColumnOrName) -> _ColumnType:
    return _native.native_xxhash64(_as_col(c))


def md5(c: ColumnOrName) -> _ColumnType:
    return _native.native_md5(_as_col(c))


def sha1(c: ColumnOrName) -> _ColumnType:
    return _native.native_sha1(_as_col(c))


def sha2(c: ColumnOrName, numBits: int) -> _ColumnType:
    return _native.native_sha2(_as_col(c), numBits)


def crc32(c: ColumnOrName) -> _ColumnType:
    return _native.native_crc32(_as_col(c))


def base64(c: ColumnOrName) -> _ColumnType:
    return _native.native_base64(_as_col(c))


def unbase64(c: ColumnOrName) -> _ColumnType:
    return _native.native_unbase64(_as_col(c))


def ascii(c: ColumnOrName) -> _ColumnType:
    return _native.native_ascii(_as_col(c))


def hex(c: ColumnOrName) -> _ColumnType:
    return _native.native_hex(_as_col(c))


def unhex(c: ColumnOrName) -> _ColumnType:
    return _native.native_unhex(_as_col(c))


def bin(c: ColumnOrName) -> _ColumnType:
    return _native.native_bin(_as_col(c))


def conv(c: ColumnOrName, fromBase: int, toBase: int) -> _ColumnType:
    return _native.native_conv(_as_col(c), fromBase, toBase)


def format_number(c: ColumnOrName, d: int) -> _ColumnType:
    return _native.native_format_number(_as_col(c), d)


# --- Array / collection functions (native-backed) ---
array = _ni("array")
struct = _ni("struct")


def explode(col_or_name: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("explode")(_as_col(col_or_name)))


def explode_outer(col_or_name: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("explode_outer")(_as_col(col_or_name)))


def posexplode(col_or_name: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("posexplode")(_as_col(col_or_name)))


def posexplode_outer(col_or_name: ColumnOrName) -> _ColumnType:
    """Explode array with position; null/empty yields one row (PySpark posexplode_outer)."""
    return _col_result(_posexplode_outer(col_or_name))


def flatten(col_or_name: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("flatten")(_as_col(col_or_name)))


def split(str_col: ColumnOrName, pattern: str, limit: int = -1) -> _ColumnType:
    lim = limit if limit != -1 else -1  # Rust uses -1 for "no limit"
    return _col_result(_native_fn("split")(_as_col(str_col), pattern, lim))


def format_string(fmt: str, *cols: ColumnOrName) -> _ColumnType:
    return _col_result(
        _native_fn("format_string")(fmt, tuple([_as_col(c) for c in cols]))
    )


concat_ws = _ni("concat_ws")


# --- Aggregate functions (native-backed) ---
def mean(col_or_name: ColumnOrName) -> _ColumnType:
    return _col_result(avg(col_or_name))


def first(col_or_name: ColumnOrName, ignorenulls: bool = False) -> _ColumnType:
    return _col_result(_native_fn("first")(_as_col(col_or_name), ignorenulls))


def last(col_or_name: ColumnOrName, ignorenulls: bool = False) -> _ColumnType:
    """Last value aggregate/window function (PySpark last).

    Returns a Column that supports .alias() and .over() for parity with PySpark.
    GroupBy: df.groupBy().agg(F.last("col").alias("last_touch")).
    Window: F.last("col").over(Window.partitionBy(...).orderBy(...)).
    """
    return _col_result(_native_fn("last")(_as_col(col_or_name), ignorenulls))


def collect_list(col_or_name: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("collect_list")(_as_col(col_or_name)))


def collect_set(col_or_name: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("collect_set")(_as_col(col_or_name)))


def array_contains(
    col_or_name: ColumnOrName, value: Union[ColumnOrName, int, float, bool, str]
) -> _ColumnType:
    v = _as_col(value) if not isinstance(value, (int, float, bool, str)) else lit(value)
    return _col_result(_native_fn("array_contains")(_as_col(col_or_name), v))


def array_distinct(col_or_name: ColumnOrName) -> _ColumnType:
    """Distinct elements in array (PySpark array_distinct). Output column name matches PySpark: array_distinct(col)."""
    col_obj = _as_col(col_or_name)
    base_name = getattr(col_obj, "name", None)
    out = _native_fn("array_distinct")(col_obj)
    if base_name is not None:
        return out.alias(f"array_distinct({base_name})")
    return out


def array_sort(col_or_name: ColumnOrName) -> _ColumnType:
    """Sort array elements in ascending order (PySpark array_sort).

    Note: Use sort_array(col, asc=False) for descending order.
    """
    return _col_result(_native_fn("array_sort")(_as_col(col_or_name), True))


def array_join(
    col_or_name: ColumnOrName, delimiter: str, null_replacement: Optional[str] = None
) -> _ColumnType:
    """Join array elements with delimiter. If null_replacement is provided, null elements are replaced with it."""
    return _col_result(
        _native_fn("array_join")(_as_col(col_or_name), delimiter, null_replacement)
    )


def array_max(col_or_name: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("array_max")(_as_col(col_or_name)))


def array_min(col_or_name: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("array_min")(_as_col(col_or_name)))


def array_position(
    col_or_name: ColumnOrName,
    value: Union[ColumnOrName, int, float, bool, str],
) -> _ColumnType:
    """1-based index of first occurrence of value in list, or 0 if not found. PySpark: F.array_position(col, value)."""
    v = _as_col(value) if not isinstance(value, (int, float, bool, str)) else lit(value)
    return _col_result(_native_fn("array_position")(_as_col(col_or_name), v))


def array_remove(
    col_or_name: ColumnOrName,
    value: Union[ColumnOrName, int, float, bool, str],
) -> _ColumnType:
    """Remove all elements equal to value from the array. PySpark: F.array_remove(col, element)."""
    v = _as_col(value) if not isinstance(value, (int, float, bool, str)) else lit(value)
    return _col_result(_native.array_remove(_as_col(col_or_name), v))


def element_at(col_or_name: ColumnOrName, extraction: int) -> _ColumnType:
    return _col_result(_native_fn("element_at")(_as_col(col_or_name), extraction))


def size(col_or_name: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("size")(_as_col(col_or_name)))


def slice(col_or_name: ColumnOrName, start: int, length: int) -> _ColumnType:
    """Slice array from 1-based start with given length (PySpark slice)."""
    return _col_result(_native_fn("array_slice")(_as_col(col_or_name), start, length))


def sort_array(col_or_name: ColumnOrName, asc: bool = True) -> _ColumnType:
    """Sort array elements. When asc=True (default), ascending. When asc=False, descending."""
    return _col_result(_native_fn("array_sort")(_as_col(col_or_name), asc))


def array_union(col1: ColumnOrName, col2: ColumnOrName) -> _ColumnType:
    return _col_result(_native.native_array_union(_as_col(col1), _as_col(col2)))


def array_intersect(col1: ColumnOrName, col2: ColumnOrName) -> _ColumnType:
    return _col_result(_native.native_array_intersect(_as_col(col1), _as_col(col2)))


def array_except(col1, col2):
    return _native.native_array_except(_as_col(col1), _as_col(col2))


def map_keys(col_or_name):
    return _native.native_map_keys(_as_col(col_or_name))


def map_values(col_or_name):
    return _native.native_map_values(_as_col(col_or_name))


map_from_arrays = _ni("map_from_arrays")
sumDistinct = _ni("sumDistinct")


def count_distinct(*cols):
    if len(cols) == 1:
        return _col_result(_native_fn("count_distinct")(_as_col(cols[0])))
    raise NotImplementedError(
        "count_distinct with multiple columns is not yet implemented"
    )


countDistinct = count_distinct


def approx_count_distinct(col_or_name, rsd=0.05):
    """Approximate distinct count (PySpark approx_count_distinct). rsd is optional; when omitted, behaves like exact count_distinct with PySpark-style column naming."""
    return _approx_count_distinct(_as_col(col_or_name), rsd)


def stddev(col_or_name):
    return _col_result(_native_fn("stddev")(_as_col(col_or_name)))


def stddev_pop(col_or_name):
    return _col_result(_native_fn("stddev_pop")(_as_col(col_or_name)))


def stddev_samp(col_or_name):
    return _col_result(_native_fn("stddev_samp")(_as_col(col_or_name)))


def variance(col_or_name):
    return _col_result(_native_fn("variance")(_as_col(col_or_name)))


def var_pop(col_or_name):
    return _col_result(_native_fn("var_pop")(_as_col(col_or_name)))


def var_samp(col_or_name):
    return _col_result(_native_fn("var_samp")(_as_col(col_or_name)))


def corr(col1, col2):
    return _col_result(_native_fn("corr")(_as_col(col1), _as_col(col2)))


percentile_approx = _ni("percentile_approx")


# --- String functions (native-backed) ---
def lpad(col_or_name, len, pad):
    return _native.native_lpad(_as_col(col_or_name), len, pad)


def rpad(col_or_name, len, pad):
    return _native.native_rpad(_as_col(col_or_name), len, pad)


def ltrim(col_or_name):
    return _native.native_ltrim(_as_col(col_or_name))


def rtrim(col_or_name):
    return _native.native_rtrim(_as_col(col_or_name))


def initcap(col_or_name):
    return _native.native_initcap(_as_col(col_or_name))


def soundex(col_or_name):
    return _native.native_soundex(_as_col(col_or_name))


def length(col_or_name):
    return _native.native_length(_as_col(col_or_name))


def reverse(col_or_name):
    return _native.native_reverse(_as_col(col_or_name))


def translate(col_or_name, matching, replace):
    return _native.native_translate(_as_col(col_or_name), matching, replace)


def instr(col_or_name, substr):
    return _native.native_instr(_as_col(col_or_name), substr)


def locate(substr, col_or_name, pos=1):
    return _native.native_locate(substr, _as_col(col_or_name), pos)


def repeat(col_or_name, n):
    return _native.native_repeat(_as_col(col_or_name), n)


def regexp_extract(col_or_name, pattern, idx=0):
    return _native.native_regexp_extract(_as_col(col_or_name), pattern, idx)


overlay = _ni("overlay")

# --- Window functions (stubs) ---
dense_rank = _ni("dense_rank")
rank = _ni("rank")
# percent_rank defined below as real implementation returning _PercentRankExpr()
# cume_dist defined below as real implementation returning _CumeDistExpr()
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
def add_months(start, months):
    return _native.native_add_months(_as_col(start), months)


def months_between(date1, date2, roundOff=True):
    return _native.native_months_between(_as_col(date1), _as_col(date2), roundOff)


def next_day(date, dayOfWeek):
    return _native.native_next_day(_as_col(date), dayOfWeek)


def last_day(date):
    return _native.native_last_day(_as_col(date))


def trunc(date, fmt):
    return _native.native_trunc(_as_col(date), fmt)


def date_trunc(fmt, timestamp):
    return _native.native_date_trunc(fmt, _as_col(timestamp))


def hour(col_or_name):
    return _native.native_hour(_as_col(col_or_name))


def minute(col_or_name):
    return _native.native_minute(_as_col(col_or_name))


def second(col_or_name):
    return _native.native_second(_as_col(col_or_name))


def quarter(col_or_name):
    return _native.native_quarter(_as_col(col_or_name))


def dayofyear(col_or_name):
    return _native.native_dayofyear(_as_col(col_or_name))


def weekofyear(col_or_name):
    return _native.native_weekofyear(_as_col(col_or_name))


def make_date(year, month, day):
    """Build date from year, month, day (PySpark make_date)."""
    fn = getattr(_native, "make_date")
    return _col_result(fn(_as_col(year), _as_col(month), _as_col(day)))


typeof = _ni("typeof")


def _python_udf_executor(df, column_name, udf_name, arg_names, arg_literal_jsons=None):
    """Run a registered Python UDF over the DataFrame: collect, apply per row, create new DataFrame. Called from Rust with_column.
    arg_literal_jsons: optional list of Optional[str]; if arg_literal_jsons[i] is not None, use json.loads(...) as the arg value (literal column)."""
    import json as _json

    import sparkless.sql.types as T

    if udf_name not in _PYTHON_UDF_REGISTRY:
        raise PySparkValueError(f"Unknown UDF name: {udf_name}")
    func, return_type = _PYTHON_UDF_REGISTRY[udf_name]
    rt = cast("T.DataType", return_type)
    session = _active_session()
    rows = df.collect()
    if not rows:
        # Empty DataFrame: replace or append column (same as non-empty path)
        existing_names = [f.name for f in df.schema.fields]
        if column_name in existing_names:
            extended_fields = [
                T.StructField(
                    f.name,
                    rt if f.name == column_name else f.dataType,
                    f.nullable if f.name != column_name else True,
                )
                for f in df.schema.fields
            ]
        else:
            extended_fields = list(df.schema.fields) + [
                T.StructField(column_name, rt, True)
            ]
        extended_schema = T.StructType(extended_fields)
        return session.createDataFrame([], schema=extended_schema)
    field_names = [f.name for f in df.schema.fields]
    # Build dicts from schema + row values. Row.__iter__ yields keys (_fields), so use positional indexing.
    row_dicts = [
        {field_names[i]: row[i] for i in range(len(field_names))} for row in rows
    ]
    arg_names_list = list(arg_names)
    literals = (
        arg_literal_jsons
        if arg_literal_jsons is not None
        else [None] * len(arg_names_list)
    )
    if len(literals) < len(arg_names_list):
        literals = list(literals) + [None] * (len(arg_names_list) - len(literals))

    def arg_value(d, i):
        if literals[i] is not None:
            return _json.loads(literals[i])
        return d[arg_names_list[i]]

    new_vals = [
        func(*[arg_value(d, i) for i in range(len(arg_names_list))]) for d in row_dicts
    ]
    for i, d in enumerate(row_dicts):
        d[column_name] = new_vals[i]
    # Replace existing column if column_name already in schema, else append (PySpark withColumn semantics).
    existing_names = [f.name for f in df.schema.fields]
    if column_name in existing_names:
        extended_fields = [
            T.StructField(
                f.name,
                rt if f.name == column_name else f.dataType,
                f.nullable if f.name != column_name else True,
            )
            for f in df.schema.fields
        ]
    else:
        extended_fields = list(df.schema.fields) + [
            T.StructField(column_name, rt, True)
        ]
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
            if (
                cols
                and not isinstance(cols[0], str)
                and not getattr(cols[0], "get_udf_call_info", None)
            ):
                first = cols[0]
                if isinstance(first, type):
                    return wrapper
                cols = cols[1:]
            # If we were called with no column args (e.g. wrapper() or wrapper(owner) after strip), do not return a UDF column; return wrapper so decorator leaves the name as the callable.
            if not cols:
                return wrapper
            if (
                len(cols) == 1
                and callable(cols[0])
                and not getattr(cols[0], "get_udf_call_info", None)
            ):
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
        is_return_type = isinstance(f, type) or getattr(
            type(f), "__name__", ""
        ).endswith("Type")
        if is_return_type:
            return_type = f

            def decorator(func):
                return register_and_wrap(func, return_type)

            return decorator
    # F.udf(f, returnType)
    return_type = returnType if returnType is not None else _DEFAULT_UDF_RETURN_TYPE
    return register_and_wrap(f, return_type)


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


__all__ = [
    "col",
    "lit",
    "mean",
    "isnull",
    "isnotnull",
    "nvl",
    "nullif",
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
    "current_timestamp",
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
    "arrays_overlap",
    "array_position",
    "array_remove",
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
    "stddev",
    "cume_dist",
    "countDistinct",
    "DataFrame",
]


def when(condition, value):
    """PySpark-compatible when(condition, value). Both arguments required. Requires active SparkSession."""
    _active_session()
    cond = _as_col(condition)
    val = (
        _as_col(value) if not isinstance(value, (int, float, bool, str)) else lit(value)
    )
    return _native_when(cond, val)


def concat(*columns):
    """Concatenate columns as strings (PySpark concat)."""
    return _concat(*columns)


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


def to_timestamp(column, fmt=None):
    return _to_timestamp(_as_col(column), fmt)


def to_date(column, fmt=None):
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


def first(col, ignorenulls=False):
    """First value in group (PySpark first); use in groupBy().agg(). Default ignorenulls=False."""
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
    """Extract keys from JSON as columns (PySpark json_tuple).

    Returns one string column per key, named c0, c1, ... in the order of keys.
    """
    if not keys:
        raise ValueError("json_tuple requires at least one key")
    cols: list[_ColumnType] = []
    for idx, key in enumerate(keys):
        # Use get_json_object to extract each key as a separate string column, then
        # alias to PySpark-style c0, c1, ... so df.select(F.json_tuple(...)) yields
        # the expected unnamed columns.
        c = cast(_ColumnType, get_json_object(column, f"$.{key}"))
        cols.append(c.alias(f"c{idx}"))
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
    key_values = [_as_col(cast(ColumnOrName, c)) for c in expanded]
    return _native.create_map(key_values)


class _RowNumberExpr:
    def over(self, window: _WindowSpec) -> _ColumnType:
        import sparkless._native as _native

        partition_by, encoded, _, _ = _window_spec_to_partition_order(window)
        return _native.row_number_window(partition_by, encoded)


def row_number() -> _RowNumberExpr:
    """Window row_number() expression; use with .over(Window.partitionBy(...).orderBy(...)). Requires active SparkSession."""
    _active_session()
    return _RowNumberExpr()


class _PercentRankExpr:
    def over(self, window: _WindowSpec) -> _ColumnType:
        import sparkless._native as _native

        partition_by, encoded, _, _ = _window_spec_to_partition_order(window)
        return _native.percent_rank_window(partition_by, encoded)


def percent_rank() -> _PercentRankExpr:
    """Window percent_rank() expression; use with .over(Window.partitionBy(...).orderBy(...)). Requires active SparkSession."""
    _active_session()
    return _PercentRankExpr()


class _RankExpr:
    def over(self, window: _WindowSpec) -> _ColumnType:
        import sparkless._native as _native
        from sparkless import Column as _Column
        from sparkless.sql.window import Window

        # Special-case Window.orderBy(F.lit(1)) and similar literal-only sort keys.
        # In PySpark this is accepted (order is arbitrary but defined). In our engine,
        # literals become anonymous Columns named "<expr>", which should not be resolved
        # as input columns. When the window's order_by consists only of such anonymous
        # literal expressions and we have a partition key, rewrite the WindowSpec to
        # order by the partition column(s) instead. This gives a stable, schema-backed
        # sort key and avoids "not found: <expr>" resolution errors.
        part_keys = getattr(window, "_partition_by", None) or []
        order_keys = list(getattr(window, "_order_by", []) or [])
        if part_keys and order_keys:
            flat: list[object] = []
            for k in order_keys:
                if isinstance(k, (list, tuple)):
                    flat.extend(k)
                else:
                    flat.append(k)
            if flat and all(
                isinstance(k, _Column) and getattr(k, "name", None) == "<expr>"
                for k in flat
            ):
                window = Window.partitionBy(*part_keys).orderBy(*part_keys)

        partition_by, encoded, _, _ = _window_spec_to_partition_order(window)
        return _native.rank_window(partition_by, encoded)


def rank() -> _RankExpr:
    """Window rank() expression; use with .over(Window.partitionBy(...).orderBy(...)). Requires active SparkSession."""
    _active_session()
    return _RankExpr()


class _DenseRankExpr:
    def over(self, window: _WindowSpec) -> _ColumnType:
        import sparkless._native as _native

        partition_by, encoded, _, _ = _window_spec_to_partition_order(window)
        return _native.dense_rank_window(partition_by, encoded)


def dense_rank() -> _DenseRankExpr:
    """Window dense_rank() expression; use with .over(Window.partitionBy(...).orderBy(...)). Requires active SparkSession."""
    _active_session()
    return _DenseRankExpr()


class _CumeDistExpr:
    def over(self, window: _WindowSpec) -> _ColumnType:
        import sparkless._native as _native

        partition_by, encoded, _, _ = _window_spec_to_partition_order(window)
        return _native.cume_dist_window(partition_by, encoded)


def cume_dist() -> _CumeDistExpr:
    """Window cume_dist() expression; use with .over(Window.partitionBy(...).orderBy(...)). Requires active SparkSession."""
    _active_session()
    return _CumeDistExpr()


class _NtileExpr:
    def __init__(self, n: int) -> None:
        self._n = n

    def over(self, window: _WindowSpec) -> _ColumnType:
        import sparkless._native as _native

        partition_by, encoded, _, _ = _window_spec_to_partition_order(window)
        return _native.ntile_window(self._n, partition_by, encoded)


def ntile(n: int) -> _NtileExpr:
    """Window ntile(n) expression; use with .over(Window.partitionBy(...).orderBy(...)). Requires active SparkSession."""
    _active_session()
    return _NtileExpr(n)


class _LagExpr:
    def __init__(self, col_or_name, offset=1):
        self._col_or_name = col_or_name
        self._offset = offset

    def over(self, window):
        import sparkless._native as _native

        partition_by, encoded, _, _ = _window_spec_to_partition_order(window)
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

        partition_by, encoded, _, _ = _window_spec_to_partition_order(window)
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

        c = (
            col(self._col_or_name)
            if isinstance(self._col_or_name, str)
            else self._col_or_name
        )
        return c.first_value().over(window)


def first_value(col_or_name):
    """Window first_value(col) expression; use with .over(Window.partitionBy(...))."""
    return _FirstValueExpr(col_or_name)


class _LastValueExpr:
    def __init__(self, col_or_name):
        self._col_or_name = col_or_name

    @property
    def _pivot_last_column(self) -> str:
        """Column name for pivot.agg(F.last(\"col\")); used by native pivot.agg()."""
        return _col_name(self._col_or_name)

    def over(self, window):
        import sparkless._native as _native

        partition_by, order_by, _, _ = _window_spec_to_partition_order(
            window, require_order=False
        )
        from sparkless import column as col

        c = (
            col(self._col_or_name)
            if isinstance(self._col_or_name, str)
            else self._col_or_name
        )
        # Full partition frame (unbounded to unbounded): last = value at last row in order (#1145).
        if order_by and _is_full_partition_frame(window):
            return c.last_value().over(window)
        # With orderBy and default frame, last_value = current row (issue #1052).
        if order_by:
            name = _col_name(self._col_or_name)
            return _native.column_value_over_window(name, partition_by, order_by)
        return c.last_value().over(window)


def last_value(col_or_name):
    """Window last_value(col) expression; use with .over(Window.partitionBy(...))."""
    return _LastValueExpr(col_or_name)


class _SortKey:
    def __init__(self, name: str, ascending: bool):
        self.name = name
        self.ascending = ascending


def _col_name(arg: object) -> str:
    from sparkless import Column as _Column

    if isinstance(arg, str):
        return arg
    if isinstance(arg, _Column):
        return str(arg.name)
    if hasattr(arg, "name") and hasattr(arg, "ascending"):
        return str(getattr(arg, "name"))
    if hasattr(arg, "column_name"):
        return str(getattr(arg, "column_name"))
    raise TypeError(f"Unsupported sort key: {type(arg)!r}")


def _is_full_partition_frame(window):
    """True if window frame is UNBOUNDED PRECEDING to UNBOUNDED FOLLOWING (whole partition).
    In that case first_value/last_value mean first/last by window order (#1145).
    """
    frame = getattr(window, "_frame", None)
    if frame is None:
        return False
    from sparkless.sql.window import Window

    _kind, start, end = frame
    return start == Window.unboundedPreceding and end == Window.unboundedFollowing


def _window_spec_to_partition_order(window, require_order=True):
    """Extract partition_by and order_by from WindowSpec for window functions.
    If require_order=False, order_by can be empty (for partition-only aggregate windows).
    Accepts a list of column names as shorthand for partition-only window (e.g. .over(["dept"])).
    """
    if isinstance(window, (list, tuple)):
        partition_names = [c if isinstance(c, str) else _col_name(c) for c in window]
        return partition_names, [], False, False
    if not hasattr(window, "_partition_by") or not hasattr(window, "_order_by"):
        raise PySparkValueError("window function .over() expects a WindowSpec")
    partition_by = list(getattr(window, "_partition_by", []) or [])
    order_keys = list(getattr(window, "_order_by", []) or [])
    if not order_keys and require_order:
        raise ValueError(
            "At least one column must be specified for orderBy(...) in window function .over()"
        )
    partition_names = [
        _col_name(c) if not isinstance(c, str) else c for c in partition_by
    ]
    flat_keys: list[object] = []
    for k in order_keys:
        if isinstance(k, (list, tuple)):
            flat_keys.extend(k)
        else:
            flat_keys.append(k)
    if not flat_keys:
        return partition_names, [], False, False
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
        # Window.orderBy(F.lit(1)) and similar literal-only sort keys produce a synthetic
        # Column name "<expr>" (default display name for anonymous expressions).
        # PySpark accepts literal sort keys for window order (order is arbitrary but defined),
        # and does not attempt to resolve "<expr>" as an input column. To avoid spurious
        # "not found: <expr>" errors when building window specs, map such anonymous sort
        # keys onto the first partition column (if any) so the native backend sees a real
        # column name.
        if name == "<expr>" and partition_names:
            name = partition_names[0]
        order_col_names.append(name)
        encoded.append(name if ascending else f"-{name}")
    use_running = not all(oc in partition_names for oc in order_col_names)
    is_full_partition_frame = (
        _is_full_partition_frame(window)
        if getattr(window, "_frame", None) is not None
        else False
    )
    return partition_names, encoded, use_running, is_full_partition_frame


def desc(col_or_name):
    """Sort key for descending order in Window.orderBy."""
    name = _col_name(col_or_name)
    return _SortKey(name, ascending=False)


def asc(col_or_name):
    """Sort key for ascending order in Window.orderBy."""
    name = _col_name(col_or_name)
    return _SortKey(name, ascending=True)

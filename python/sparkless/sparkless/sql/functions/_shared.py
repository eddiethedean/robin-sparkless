# PySpark-style: from sparkless.sql.functions import col, lit, when, count, sum, ...
# mypy: disable-error-code=no-redef
# ruff: noqa: F811
from __future__ import annotations

import getpass

import sparkless_native._native as _native
from sparkless import _Column
from sparkless_native._native import PyColumn as _ColumnType  # for type hints only
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
    cast as _native_cast,
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
    to_timestamp_ltz as _to_timestamp_ltz,
    to_timestamp_ntz as _to_timestamp_ntz,
    to_date as _to_date,
    current_date as _current_date,
    current_timestamp as _current_timestamp,
    input_file_name as _input_file_name,
    monotonically_increasing_id as _monotonically_increasing_id,
    spark_partition_id as _spark_partition_id,
    percentile_approx as _percentile_approx,
    schema_of_json as _schema_of_json,
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
    translate as _translate,
    substring_index as _substring_index,
    get_json_object as _get_json_object,
    size as _size,
    array_contains as _array_contains,
)
from sparkless.errors import PySparkValueError
from sparkless import DataFrame
from typing import Any, Callable, Dict, Optional, Tuple, Union
from typing import cast as tcast

# Column or column name (str); used for function params that accept either.
ColumnOrName = Union[_ColumnType, str]


def _col_result(x: Any) -> _ColumnType:
    """Cast native/column call result to _ColumnType for mypy no-any-return."""
    out: _ColumnType = tcast(_ColumnType, x)
    return out


# Registry for Python UDFs: udf_name -> (callable, return_type). Populated by udf() / @udf.
_PYTHON_UDF_REGISTRY: Dict[str, Tuple[Callable[..., object], object]] = {}

# Default return type when @udf() is used without arguments (PySpark uses StringType).
_DEFAULT_UDF_RETURN_TYPE: Any = None  # Set below after importing types


def _ensure_udf_executor_registered() -> None:
    """Register the Python UDF executor with the native module once (for with_column UDF handling)."""
    import sparkless_native._native as _native

    global _UDF_EXECUTOR_REGISTERED
    if _UDF_EXECUTOR_REGISTERED:
        return
    from ._udf import _python_udf_executor

    _native.set_python_udf_executor(_python_udf_executor)
    _UDF_EXECUTOR_REGISTERED = True


_UDF_EXECUTOR_REGISTERED = False


def _as_col(c: ColumnOrName) -> _ColumnType:
    result: _ColumnType = tcast(_ColumnType, col(c) if isinstance(c, str) else c)
    return result


def _native_fn(name: str) -> Callable[..., _ColumnType]:
    """Get function from _native: try native_<name> first (e.g. native_floor), then <name> (e.g. floor)."""
    fn = getattr(_native, "native_" + name, None) or getattr(_native, name, None)
    out: Callable[..., _ColumnType] = tcast(Callable[..., _ColumnType], fn)
    return out


def cast(col_or_name: ColumnOrName, data_type: object) -> _ColumnType:
    """Module-level cast for PySpark parity.

    In real PySpark, `pyspark.sql.functions.cast` does not accept a `DataType` object
    (only strings / SQL type names). Some tests assert that passing a `DataType`
    fails, so we explicitly raise here to match that behavior.
    """
    if isinstance(data_type, str):
        return _col_result(_native_cast(_as_col(col_or_name), data_type))
    raise AttributeError("DataType object has no attribute 'alias'")


def _not_implemented(name: str) -> Callable[..., None]:
    """Return a callable that raises NotImplementedError when called (for stub functions)."""

    def _raiser(*args: object, **kwargs: object) -> None:
        raise NotImplementedError(f"{name!r} is not yet implemented")

    return _raiser


def _ni(name: str) -> Callable[..., None]:
    return _not_implemented(name)


# --- Math functions (native-backed) ---

__all__ = [
    "Any",
    "Callable",
    "ColumnOrName",
    "DataFrame",
    "Dict",
    "Optional",
    "PySparkValueError",
    "Tuple",
    "Union",
    "_Column",
    "_ColumnType",
    "_DEFAULT_UDF_RETURN_TYPE",
    "_PYTHON_UDF_REGISTRY",
    "_UDF_EXECUTOR_REGISTERED",
    "_approx_count_distinct",
    "_array",
    "_array_contains",
    "_array_distinct",
    "_as_col",
    "_asinh",
    "_atanh",
    "_avg",
    "_coalesce",
    "_col_result",
    "_concat",
    "_concat_ws",
    "_cosh",
    "_count",
    "_current_date",
    "_current_timestamp",
    "_date_add",
    "_date_format",
    "_date_sub",
    "_date_trunc",
    "_datediff",
    "_dayofmonth",
    "_dayofweek",
    "_ensure_udf_executor_registered",
    "_floor",
    "_format_string",
    "_from_unixtime",
    "_get_json_object",
    "_greatest",
    "_hour",
    "_input_file_name",
    "_last_day",
    "_least",
    "_length",
    "_levenshtein",
    "_ltrim",
    "_max",
    "_min",
    "_minute",
    "_monotonically_increasing_id",
    "_month",
    "_months_between",
    "_native",
    "_native_cast",
    "_native_fn",
    "_native_when",
    "_ni",
    "_not_implemented",
    "_percentile_approx",
    "_posexplode",
    "_posexplode_outer",
    "_regexp_extract",
    "_regexp_extract_all",
    "_regexp_like",
    "_regexp_replace",
    "_repeat",
    "_round",
    "_schema_of_json",
    "_sinh",
    "_size",
    "_soundex",
    "_spark_partition_id",
    "_split",
    "_struct",
    "_substring_index",
    "_sum",
    "_timestamp_seconds",
    "_to_date",
    "_to_timestamp",
    "_to_timestamp_ltz",
    "_to_timestamp_ntz",
    "_to_utc_timestamp",
    "_translate",
    "_try_add",
    "_try_cast",
    "_unix_timestamp",
    "_year",
    "cast",
    "col",
    "getpass",
    "lit",
    "lit_bool",
    "lit_f64",
    "lit_i64",
    "lit_null",
    "lit_str",
    "lower",
    "substring",
    "tcast",
    "trim",
    "upper",
]

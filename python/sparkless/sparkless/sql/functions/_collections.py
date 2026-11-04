"""Private collections SQL function implementations."""

from __future__ import annotations

from ._shared import (
    ColumnOrName,
    Optional,
    Union,
    _ColumnType,
    _approx_count_distinct,
    _as_col,
    _col_result,
    _native,
    _native_fn,
    _percentile_approx,
    _posexplode_outer,
    lit,
)

from ._session import _active_session

from ._wrappers import avg


def explode(col_or_name: ColumnOrName) -> _ColumnType:
    """Explode array into one row per element (PySpark explode)."""
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


def mean(col_or_name: ColumnOrName) -> _ColumnType:
    return _col_result(avg(col_or_name))


def first(col_or_name: ColumnOrName, ignorenulls: bool = False) -> _ColumnType:
    """First value in group (PySpark first); use in groupBy().agg(). Default ignorenulls=False."""
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


def array_distinct(col_or_name: ColumnOrName) -> _ColumnType:
    """Distinct elements in array (PySpark array_distinct). Output column name matches PySpark: array_distinct(col)."""
    col_obj = _as_col(col_or_name)
    base_name = getattr(col_obj, "name", None)
    out = _native_fn("array_distinct")(col_obj)
    if base_name is not None:
        return out.alias(f"array_distinct({base_name})")
    return out


def array_compact(col_or_name: ColumnOrName) -> _ColumnType:
    """Remove null elements from array (PySpark array_compact). Output column name matches PySpark: array_compact(col)."""
    col_obj = _as_col(col_or_name)
    base_name = getattr(col_obj, "name", None)
    out = _native_fn("array_compact")(col_obj)
    if base_name is not None:
        return out.alias(f"array_compact({base_name})")
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
    return _native_fn("map_keys")(_as_col(col_or_name))


def map_values(col_or_name):
    return _native_fn("map_values")(_as_col(col_or_name))


def sumDistinct(col_or_name: ColumnOrName) -> _ColumnType:
    """Sum of distinct values (PySpark sumDistinct); use in groupBy().agg()."""
    _active_session()
    return _col_result(_native_fn("sum_distinct")(_as_col(col_or_name)))


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


def covar_pop(col1, col2):
    return _col_result(_native_fn("covar_pop")(_as_col(col1), _as_col(col2)))


def covar_samp(col1, col2):
    return _col_result(_native_fn("covar_samp")(_as_col(col1), _as_col(col2)))


def broadcast(df):
    """Broadcast hint for DataFrame joins (PySpark parity). No-op on DataFrame."""
    return df


def percentile_approx(col_or_name, percentage, accuracy=None):
    """Approximate percentile (PySpark percentile_approx). Accepts column name or Column."""
    return _percentile_approx(_as_col(col_or_name), percentage, accuracy)


__all__ = [
    "approx_count_distinct",
    "array_compact",
    "array_distinct",
    "array_except",
    "array_intersect",
    "array_join",
    "array_max",
    "array_min",
    "array_position",
    "array_remove",
    "array_sort",
    "array_union",
    "broadcast",
    "collect_list",
    "collect_set",
    "corr",
    "countDistinct",
    "count_distinct",
    "covar_pop",
    "covar_samp",
    "element_at",
    "explode",
    "explode_outer",
    "first",
    "flatten",
    "format_string",
    "last",
    "map_keys",
    "map_values",
    "mean",
    "percentile_approx",
    "posexplode",
    "posexplode_outer",
    "size",
    "slice",
    "sort_array",
    "split",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "sumDistinct",
    "var_pop",
    "var_samp",
    "variance",
]

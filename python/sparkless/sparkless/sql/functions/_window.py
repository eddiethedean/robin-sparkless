"""Private window SQL function implementations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ._shared import (
    Any,
    Optional,
    PySparkValueError,
    Tuple,
    _ColumnType,
)

from ._session import _active_session

if TYPE_CHECKING:
    from sparkless.sql.window import WindowSpec as _WindowSpec


class _RowNumberExpr:
    def over(self, window: _WindowSpec) -> _ColumnType:
        import sparkless_native._native as _native

        partition_by, encoded, _, _, _ = _window_spec_to_partition_order(window)
        return _native.row_number_window(partition_by, encoded)


def row_number() -> _RowNumberExpr:
    """Window row_number() expression; use with .over(Window.partitionBy(...).orderBy(...)). Requires active SparkSession."""
    _active_session()
    return _RowNumberExpr()


class _PercentRankExpr:
    def over(self, window: _WindowSpec) -> _ColumnType:
        import sparkless_native._native as _native

        partition_by, encoded, _, _, _ = _window_spec_to_partition_order(window)
        return _native.percent_rank_window(partition_by, encoded)


def percent_rank() -> _PercentRankExpr:
    """Window percent_rank() expression; use with .over(Window.partitionBy(...).orderBy(...)). Requires active SparkSession."""
    _active_session()
    return _PercentRankExpr()


class _RankExpr:
    def over(self, window: _WindowSpec) -> _ColumnType:
        import sparkless_native._native as _native
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

        partition_by, encoded, _, _, _ = _window_spec_to_partition_order(window)
        return _native.rank_window(partition_by, encoded)


def rank() -> _RankExpr:
    """Window rank() expression; use with .over(Window.partitionBy(...).orderBy(...)). Requires active SparkSession."""
    _active_session()
    return _RankExpr()


class _DenseRankExpr:
    def over(self, window: _WindowSpec) -> _ColumnType:
        import sparkless_native._native as _native

        partition_by, encoded, _, _, _ = _window_spec_to_partition_order(window)
        return _native.dense_rank_window(partition_by, encoded)


def dense_rank() -> _DenseRankExpr:
    """Window dense_rank() expression; use with .over(Window.partitionBy(...).orderBy(...)). Requires active SparkSession."""
    _active_session()
    return _DenseRankExpr()


class _CumeDistExpr:
    def over(self, window: _WindowSpec) -> _ColumnType:
        import sparkless_native._native as _native

        partition_by, encoded, _, _, _ = _window_spec_to_partition_order(window)
        return _native.cume_dist_window(partition_by, encoded)


def cume_dist() -> _CumeDistExpr:
    """Window cume_dist() expression; use with .over(Window.partitionBy(...).orderBy(...)). Requires active SparkSession."""
    _active_session()
    return _CumeDistExpr()


class _NtileExpr:
    def __init__(self, n: int) -> None:
        self._n = n

    def over(self, window: _WindowSpec) -> _ColumnType:
        import sparkless_native._native as _native

        partition_by, encoded, _, _, _ = _window_spec_to_partition_order(window)
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
        import sparkless_native._native as _native

        partition_by, encoded, _, _, _ = _window_spec_to_partition_order(window)
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
        import sparkless_native._native as _native

        partition_by, encoded, _, _, _ = _window_spec_to_partition_order(window)
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
        import sparkless_native._native as _native

        partition_by, order_by, _, _, _ = _window_spec_to_partition_order(
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


def _window_spec_to_partition_order(
    window: Any, require_order: bool = True
) -> Tuple[list[str], list[str], bool, bool, Optional[Any]]:
    """Extract partition_by and order_by from WindowSpec for window functions.
    If require_order=False, order_by can be empty (for partition-only aggregate windows).
    Accepts a list of column names as shorthand for partition-only window (e.g. .over(["dept"])).
    Returns a 5-tuple: (partition_names, encoded_order, use_running, is_full_partition_frame, frame).
    frame is None or (kind, start, end) with kind in ("rows", "range") and start/end as ints.
    """
    _no_frame = None
    if isinstance(window, (list, tuple)):
        partition_names = [c if isinstance(c, str) else _col_name(c) for c in window]
        return partition_names, [], False, False, _no_frame
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
        return partition_names, [], False, False, _no_frame
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
    frame = getattr(window, "_frame", None)
    is_full_partition_frame = (
        _is_full_partition_frame(window) if frame is not None else False
    )
    return partition_names, encoded, use_running, is_full_partition_frame, frame


def desc(col_or_name):
    """Sort key for descending order in Window.orderBy."""
    name = _col_name(col_or_name)
    return _SortKey(name, ascending=False)


def asc(col_or_name):
    """Sort key for ascending order in Window.orderBy."""
    name = _col_name(col_or_name)
    return _SortKey(name, ascending=True)


__all__ = [
    "_CumeDistExpr",
    "_DenseRankExpr",
    "_FirstValueExpr",
    "_LagExpr",
    "_LastValueExpr",
    "_LeadExpr",
    "_NtileExpr",
    "_PercentRankExpr",
    "_RankExpr",
    "_RowNumberExpr",
    "_SortKey",
    "_col_name",
    "_is_full_partition_frame",
    "_window_spec_to_partition_order",
    "asc",
    "cume_dist",
    "dense_rank",
    "desc",
    "first_value",
    "lag",
    "last_value",
    "lead",
    "ntile",
    "percent_rank",
    "rank",
    "row_number",
]

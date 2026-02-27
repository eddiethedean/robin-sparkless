from __future__ import annotations

from typing import Sequence, Union


class _SortKey:
    def __init__(self, name: str, ascending: bool) -> None:
        self.name = name
        self.ascending = ascending


# Partition/order spec: sequence of column names or _SortKey. Column (from sparkless) accepted at runtime.
PartitionOrOrderSpec = Sequence[Union[str, _SortKey]]


def _partition_names(cols: PartitionOrOrderSpec) -> list[str]:
    # Lazy import to avoid circular imports at module import time
    from sparkless import Column as _Column

    out = []
    for c in cols:
        if isinstance(c, (list, tuple)):
            out.extend(_partition_names(c))
        elif isinstance(c, str):
            out.append(c)
        elif isinstance(c, _Column):
            out.append(c.name)
        else:  # pragma: no cover - defensive
            raise TypeError(f"Unsupported partition column type: {type(c)!r}")
    return out


def _normalize_sort_keys(cols: PartitionOrOrderSpec) -> list[_SortKey]:
    # Lazy import inside to avoid cycles
    from sparkless.sql.functions import _SortKey as _FnSortKey
    from sparkless import (
        Column as _Column,
    )  # avoid circular import at module import time

    keys = []
    for c in cols:
        if isinstance(c, (list, tuple)):
            keys.extend(_normalize_sort_keys(c))
        elif isinstance(c, _FnSortKey):
            keys.append(_SortKey(c.name, c.ascending))
        elif isinstance(c, str):
            keys.append(_SortKey(c, True))
        elif isinstance(c, _Column):
            keys.append(_SortKey(c.name, True))
        elif hasattr(c, "column_name") and hasattr(c, "descending"):
            # PySortOrder from col.asc() / col.desc()
            keys.append(_SortKey(c.column_name, not c.descending))
        else:
            raise TypeError(f"Unsupported orderBy key: {type(c)!r}")
    return keys


class WindowSpec:
    def __init__(
        self,
        partition_by: list[str] | None = None,
        order_by: list[_SortKey] | None = None,
        frame: tuple[str, int, int] | None = None,
    ) -> None:
        self._partition_by = list(partition_by or [])
        self._order_by = list(order_by or [])
        self._frame = frame

    def partitionBy(self, *cols: Union[str, _SortKey]) -> WindowSpec:
        names = _partition_names(cols)
        return WindowSpec(
            partition_by=names, order_by=self._order_by, frame=self._frame
        )

    def orderBy(self, *cols: Union[str, _SortKey]) -> WindowSpec:
        keys = _normalize_sort_keys(cols)
        return WindowSpec(
            partition_by=self._partition_by, order_by=keys, frame=self._frame
        )

    def rowsBetween(self, start: int, end: int) -> WindowSpec:
        return WindowSpec(
            partition_by=self._partition_by,
            order_by=self._order_by,
            frame=("rows", start, end),
        )

    def rangeBetween(self, start: int, end: int) -> WindowSpec:
        return WindowSpec(
            partition_by=self._partition_by,
            order_by=self._order_by,
            frame=("range", start, end),
        )


class Window:
    # PySpark constants (Long.MIN_VALUE / Long.MAX_VALUE)
    unboundedPreceding: int = -(2**63)
    currentRow: int = 0
    unboundedFollowing: int = 2**63 - 1

    @staticmethod
    def partitionBy(*cols: Union[str, _SortKey]) -> WindowSpec:
        return WindowSpec().partitionBy(*cols)

    @staticmethod
    def orderBy(*cols: Union[str, _SortKey]) -> WindowSpec:
        return WindowSpec().orderBy(*cols)

    @staticmethod
    def rowsBetween(start: int, end: int) -> WindowSpec:
        return WindowSpec().rowsBetween(start, end)

    @staticmethod
    def rangeBetween(start: int, end: int) -> WindowSpec:
        return WindowSpec().rangeBetween(start, end)

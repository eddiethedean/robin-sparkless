"""Implementation of PySpark-style window specifications.

`WindowSpec` describes a logical window over which window functions operate:

* `Window.partitionBy(...).orderBy(...)`
* `.rowsBetween(...)` / `.rangeBetween(...)`

This module is used internally by `sparkless.sql.functions` and re-exported
via `sparkless.window` so that user code can construct window expressions
in the same way as with PySpark.
"""

from __future__ import annotations

from typing import List, Optional, Sequence, Tuple


class _SortKey:
    def __init__(self, name: str, ascending: bool) -> None:
        self.name = name
        self.ascending = ascending


# Partition/order spec is intentionally permissive: we accept strings, Columns, sort keys,
# and nested lists/tuples of those (matching PySpark call sites).
PartitionOrOrderSpec = Sequence[object]


def _partition_names(cols: PartitionOrOrderSpec) -> List[str]:
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


def _normalize_sort_keys(cols: PartitionOrOrderSpec) -> List[_SortKey]:
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
        partition_by: Optional[List[str]] = None,
        order_by: Optional[List[_SortKey]] = None,
        frame: Optional[Tuple[str, int, int]] = None,
    ) -> None:
        self._partition_by = list(partition_by or [])
        self._order_by = list(order_by or [])
        self._frame = frame

    def partitionBy(self, *cols: object) -> WindowSpec:
        names = _partition_names(cols)
        return WindowSpec(
            partition_by=names, order_by=self._order_by, frame=self._frame
        )

    def orderBy(self, *cols: object) -> WindowSpec:
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
    def partitionBy(*cols: object) -> WindowSpec:
        return WindowSpec().partitionBy(*cols)

    @staticmethod
    def orderBy(*cols: object) -> WindowSpec:
        return WindowSpec().orderBy(*cols)

    @staticmethod
    def rowsBetween(start: int, end: int) -> WindowSpec:
        return WindowSpec().rowsBetween(start, end)

    @staticmethod
    def rangeBetween(start: int, end: int) -> WindowSpec:
        return WindowSpec().rangeBetween(start, end)

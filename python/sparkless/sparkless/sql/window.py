from __future__ import annotations


class _SortKey:
    def __init__(self, name: str, ascending: bool):
        self.name = name
        self.ascending = ascending


def _partition_names(cols):
    # Lazy import to avoid circular imports at module import time
    from sparkless import Column as _Column

    out = []
    for c in cols:
        if isinstance(c, str):
            out.append(c)
        elif isinstance(c, _Column):
            out.append(c.name)
        else:  # pragma: no cover - defensive
            raise TypeError(f"Unsupported partition column type: {type(c)!r}")
    return out


def _normalize_sort_keys(cols):
    # Lazy import inside to avoid cycles
    from sparkless.sql.functions import _SortKey as _FnSortKey  # type: ignore[attr-defined]
    from sparkless import Column as _Column  # avoid circular import at module import time

    keys = []
    for c in cols:
        if isinstance(c, _FnSortKey):
            keys.append(_SortKey(c.name, c.ascending))
        elif isinstance(c, str):
            keys.append(_SortKey(c, True))
        elif isinstance(c, _Column):
            keys.append(_SortKey(c.name, True))
        else:
            raise TypeError(f"Unsupported orderBy key: {type(c)!r}")
    return keys


class WindowSpec:
    def __init__(self, partition_by=None, order_by=None, frame=None):
        self._partition_by = list(partition_by or [])
        self._order_by = list(order_by or [])
        self._frame = frame

    def partitionBy(self, *cols):
        names = _partition_names(cols)
        return WindowSpec(partition_by=names, order_by=self._order_by, frame=self._frame)

    def orderBy(self, *cols):
        keys = _normalize_sort_keys(cols)
        return WindowSpec(partition_by=self._partition_by, order_by=keys, frame=self._frame)

    def rowsBetween(self, start, end):
        return WindowSpec(
            partition_by=self._partition_by,
            order_by=self._order_by,
            frame=("rows", start, end),
        )

    def rangeBetween(self, start, end):
        return WindowSpec(
            partition_by=self._partition_by,
            order_by=self._order_by,
            frame=("range", start, end),
        )


class Window:
    # PySpark constants (Long.MIN_VALUE / Long.MAX_VALUE)
    unboundedPreceding = -(2**63)
    currentRow = 0
    unboundedFollowing = 2**63 - 1

    @staticmethod
    def partitionBy(*cols):
        return WindowSpec().partitionBy(*cols)

    @staticmethod
    def orderBy(*cols):
        return WindowSpec().orderBy(*cols)

    @staticmethod
    def rowsBetween(start, end):
        return WindowSpec().rowsBetween(start, end)

    @staticmethod
    def rangeBetween(start, end):
        return WindowSpec().rangeBetween(start, end)


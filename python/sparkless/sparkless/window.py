# PySpark-style Window for window functions. Minimal stub so tests can import and construct.


class WindowSpec:
    """Stub for partitionBy().orderBy() chain; actual window execution in engine."""

    def __init__(self):
        self._partition_by = []
        self._order_by = []

    def partitionBy(self, *cols):
        self._partition_by = list(cols)
        return self

    def orderBy(self, *cols):
        self._order_by = list(cols)
        return self

    def rowsBetween(self, start, end):
        return self

    def rangeBetween(self, start, end):
        return self


class Window:
    """Window builder: Window.partitionBy(...).orderBy(...)."""

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


__all__ = ["Window", "WindowSpec"]

# PySpark-style Window for window functions. Minimal stub so tests can import and construct.


# Re-export full implementation from sql.window (includes unboundedPreceding, rowsBetween, etc.)
from sparkless.sql.window import WindowSpec, Window


__all__ = ["Window", "WindowSpec"]

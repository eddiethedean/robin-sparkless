"""Top-level `Window` entrypoint for PySpark-style window functions.

User code mirrors PySpark usage patterns such as:

* `from sparkless import Window`
* `from sparkless.window import Window, WindowSpec`

This module simply re-exports the full implementation from
`sparkless.sql.window`, including `Window.unboundedPreceding`,
`rowsBetween`, and `rangeBetween`.
"""

# Re-export full implementation from sql.window (includes unboundedPreceding, rowsBetween, etc.)
from sparkless.sql.window import WindowSpec, Window


__all__ = ["Window", "WindowSpec"]

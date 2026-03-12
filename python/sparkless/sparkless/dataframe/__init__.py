"""DataFrame-related convenience imports.

This module exists primarily so that patterns like:

* `from sparkless.dataframe import DataFrameReader`

continue to work, matching upstream Sparkless and PySpark-style APIs.
"""

from sparkless.sql import DataFrameReader

__all__ = ["DataFrameReader"]

"""PySpark-style SQL entrypoint: `from sparkless.sql import SparkSession, DataFrame, Column, Row`.

This module wires the high-level SQL namespace used by tests and user code:

* Re-exports core classes from `sparkless` (`SparkSession`, `DataFrame`, `Column`,
  `GroupedData`, readers/writers, and `SparklessError`).
* Exposes the `Row` type from `sparkless.sql.types`.
* Ensures the `SparkSession` class has mutable `_active_sessions` and
  `_singleton_session` attributes for compatibility with upstream logic.
"""

from sparkless import (
    SparkSession,
    SparkSessionBuilder,
    DataFrame,
    Column,
    GroupedData,
    DataFrameReader,
    DataFrameWriter,
    SparklessError,
)
from sparkless.sql.types import Row

# Compatibility: upstream sparkless relies on these mutable class attributes for active-session logic.
if not hasattr(SparkSession, "_active_sessions"):
    SparkSession._active_sessions = []
if not hasattr(SparkSession, "_singleton_session"):
    SparkSession._singleton_session = None

__all__ = [
    "SparkSession",
    "SparkSessionBuilder",
    "DataFrame",
    "Column",
    "GroupedData",
    "DataFrameReader",
    "DataFrameWriter",
    "SparklessError",
    "Row",
]

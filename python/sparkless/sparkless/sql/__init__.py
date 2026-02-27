# PySpark-style: from sparkless.sql import SparkSession, DataFrame, Column, Row, etc.
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

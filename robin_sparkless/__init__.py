"""
Compatibility shim: expose the PySpark-like API under the ``robin_sparkless``
module name, backed by the local ``sparkless`` package.

Tests in this repository import ``robin_sparkless as rs`` (matching the
upstream project), but the engine we control lives in ``sparkless``. This
module simply re-exports the public ``sparkless`` API so tests exercise the
current workspace build.
"""

from __future__ import annotations

from sparkless import (  # type: ignore[F401]
    Column,
    DataFrame,
    DataFrameReader,
    DataFrameWriter,
    GroupedData,
    SparkSession,
    SparkSessionBuilder,
    SparklessError,
    col,
)
from sparkless import *  # noqa: F401,F403
from sparkless import sql  # type: ignore[F401]
from sparkless.sql.types import Row  # type: ignore[F401]

__all__ = [
    # Core classes
    "SparkSession",
    "SparkSessionBuilder",
    "DataFrame",
    "Column",
    "GroupedData",
    "DataFrameReader",
    "DataFrameWriter",
    "SparklessError",
    "Row",
    # Common helpers
    "col",
    "sql",
] + [name for name in globals().keys() if not name.startswith("_")]

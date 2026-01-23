"""
Robin Sparkless - A Rust-backed Python package with PySpark API parity
"""

from _robin_sparkless import (
    SparkSession,
    SparkSessionBuilder,
    DataFrame,
    Column,
    StructType,
    StructField,
)

# Import functions module
from _robin_sparkless import functions

# Re-export commonly used functions
from _robin_sparkless.functions import (
    col,
    lit,
    count,
    sum,
    avg,
    max,
    min,
    when,
    coalesce,
)

__version__ = "0.1.0"
__all__ = [
    "SparkSession",
    "SparkSessionBuilder",
    "DataFrame",
    "Column",
    "StructType",
    "StructField",
    "functions",
    "col",
    "lit",
    "count",
    "sum",
    "avg",
    "max",
    "min",
    "when",
    "coalesce",
]

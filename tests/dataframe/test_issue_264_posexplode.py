"""
Tests for issue #264: F.posexplode() missing from robin_sparkless module (PySpark parity).

PySpark has F.posexplode() to explode an array column into position + value columns.
Robin now exposes module-level posexplode and column.posexplode(); F.posexplode(col) returns (pos_col, val_col).
"""

from __future__ import annotations

from tests.fixtures.spark_imports import get_spark_imports


_imports = get_spark_imports()
SparkSession = _imports.SparkSession
F = _imports.F


def test_posexplode_module_exists() -> None:
    """F.posexplode exists and is callable."""
    assert hasattr(F, "posexplode")
    assert callable(F.posexplode)


def test_posexplode_returns_two_columns() -> None:
    """F.posexplode(column) returns (pos_column, value_column) or single struct column (PySpark)."""
    spark = SparkSession.builder.appName("test_264").getOrCreate()
    df = spark.createDataFrame(
        [{"Name": "Alice", "Values": [10, 20]}, {"Name": "Bob", "Values": [30, 40]}],
        "Name string, Values array<int>",
    )
    out = F.posexplode(F.col("Values"))
    if isinstance(out, tuple):
        pos_col, val_col = out
        assert pos_col is not None and val_col is not None
        pos_col.alias("pos")
        val_col.alias("val")
    else:
        # PySpark: posexplode returns two columns; use .alias("pos", "val")
        exploded = df.select(F.posexplode("Values").alias("pos", "val"))
        rows = exploded.collect()
        assert len(rows) >= 1


def test_explode_module_exists() -> None:
    """F.explode exists (also added for parity with posexplode)."""
    assert hasattr(F, "explode")
    assert callable(F.explode)

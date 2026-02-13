"""
Tests for issue #264: F.posexplode() missing from robin_sparkless module (PySpark parity).

PySpark has F.posexplode() to explode an array column into position + value columns.
Robin now exposes module-level posexplode and column.posexplode(); F.posexplode(col) returns (pos_col, val_col).
"""

from __future__ import annotations

import robin_sparkless as rs

F = rs


def test_posexplode_module_exists() -> None:
    """F.posexplode exists and is callable."""
    assert hasattr(F, "posexplode")
    assert callable(F.posexplode)


def test_posexplode_returns_two_columns() -> None:
    """F.posexplode(column) returns (pos_column, value_column); both support .alias()."""
    spark = F.SparkSession.builder().app_name("test_264").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    df = create_df(
        [{"Name": "Alice", "Values": [10, 20]}, {"Name": "Bob", "Values": [30, 40]}],
        [("Name", "string"), ("Values", "list")],
    )
    pos_col, val_col = F.posexplode(F.col("Values"))
    assert pos_col is not None and val_col is not None
    # Both support .alias() for use in select/with_column
    pos_col.alias("pos")
    val_col.alias("val")
    # Column method form also works
    pos2, val2 = F.col("Values").posexplode()
    assert pos2 is not None and val2 is not None


def test_explode_module_exists() -> None:
    """F.explode exists (also added for parity with posexplode)."""
    assert hasattr(F, "explode")
    assert callable(F.explode)

"""
Tests for issue #280: posexplode() should accept column name (str) for PySpark compatibility.

PySpark F.posexplode("Values") and F.posexplode(F.col("Values")) both work.
Robin previously required Column only; now accepts string column name.
"""

from __future__ import annotations

import robin_sparkless as rs

F = rs


def test_posexplode_accepts_column_name_string() -> None:
    """posexplode("Values") with string column name works (PySpark parity)."""
    spark = F.SparkSession.builder().app_name("test_280").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    data = [
        {"Name": "Alice", "Values": [10, 20]},
        {"Name": "Bob", "Values": [30, 40]},
    ]
    df = create_df(data, [("Name", "string"), ("Values", "array")])
    pos_col, val_col = F.posexplode("Values")
    assert pos_col is not None and val_col is not None
    # Use exploded columns in select; result has 4 rows (2 rows x 2 elements)
    out = df.select(pos_col.alias("pos"), val_col.alias("val")).collect()
    assert len(out) == 4
    assert all("pos" in r and "val" in r for r in out)
    vals = [r["val"] for r in out]
    assert vals == [10, 20, 30, 40]


def test_posexplode_column_still_works() -> None:
    """posexplode(F.col("Values")) still works."""
    spark = F.SparkSession.builder().app_name("test_280").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    data = [{"Name": "Alice", "Values": [1, 2, 3]}]
    df = create_df(data, [("Name", "string"), ("Values", "array")])
    pos_col, val_col = F.posexplode(F.col("Values"))
    out = df.select(pos_col.alias("pos"), val_col.alias("val")).collect()
    assert len(out) == 3
    assert [r["val"] for r in out] == [1, 2, 3]

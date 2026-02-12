"""
Tests for issue #250: Column.between(low, high) missing for range filter.

PySpark's Column has between(low, high) for inclusive range filters. Robin Column
now exposes between so df.filter(col("v").between(lit(20), lit(30))) works.
"""

from __future__ import annotations

import robin_sparkless as rs


def test_column_has_between() -> None:
    """Column has between method (PySpark parity)."""
    c = rs.col("v")
    assert hasattr(c, "between")


def test_filter_between_returns_matching_row() -> None:
    """df.filter(col("v").between(lit(20), lit(30))) returns only the row where v is in [20, 30]."""
    spark = rs.SparkSession.builder().app_name("between").get_or_create()
    data = [{"v": 10}, {"v": 25}, {"v": 50}]
    schema = [("v", "int")]
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    df = create_df(data, schema)
    out = df.filter(rs.col("v").between(rs.lit(20), rs.lit(30))).collect()
    assert len(out) == 1
    assert out[0]["v"] == 25


def test_filter_between_inclusive_boundaries() -> None:
    """between is inclusive: v between 10 and 10 returns the row with v=10."""
    spark = rs.SparkSession.builder().app_name("between").get_or_create()
    data = [{"v": 10}, {"v": 20}]
    schema = [("v", "int")]
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    df = create_df(data, schema)
    out = df.filter(rs.col("v").between(rs.lit(10), rs.lit(10))).collect()
    assert len(out) == 1
    assert out[0]["v"] == 10

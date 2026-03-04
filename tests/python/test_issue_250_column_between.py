"""
Tests for issue #250: Column.between(low, high) missing for range filter.

PySpark's Column has between(low, high) for inclusive range filters. Robin Column
now exposes between so df.filter(col("v").between(lit(20), lit(30))) works.
"""

from __future__ import annotations

from tests.python.utils import get_spark, get_functions


def test_column_has_between() -> None:
    """Column has between method (PySpark parity)."""
    F = get_functions()
    c = F.col("v")
    assert hasattr(c, "between")


def test_filter_between_returns_matching_row() -> None:
    """df.filter(col("v").between(lit(20), lit(30))) returns only the row where v is in [20, 30]."""
    spark = get_spark("issue_250_between")
    F = get_functions()
    data = [{"v": 10}, {"v": 25}, {"v": 50}]
    df = spark.createDataFrame(data, ["v"])
    out = df.filter(F.col("v").between(F.lit(20), F.lit(30))).collect()
    assert len(out) == 1
    assert out[0]["v"] == 25


def test_filter_between_inclusive_boundaries() -> None:
    """between is inclusive: v between 10 and 10 returns the row with v=10."""
    spark = get_spark("issue_250_between")
    F = get_functions()
    data = [{"v": 10}, {"v": 20}]
    df = spark.createDataFrame(data, ["v"])
    out = df.filter(F.col("v").between(F.lit(10), F.lit(10))).collect()
    assert len(out) == 1
    assert out[0]["v"] == 10

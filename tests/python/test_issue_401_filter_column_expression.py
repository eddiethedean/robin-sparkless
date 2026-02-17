"""Tests for issue #401: filter condition accept Column expression."""

from __future__ import annotations

import robin_sparkless as rs


def test_filter_column_gt_literal() -> None:
    """df.filter(col('x') > 1) - PySpark accepts Column expression."""
    spark = rs.SparkSession.builder().app_name("issue_401").get_or_create()
    df = spark.createDataFrame([(1,), (2,)], ["x"])
    result = df.filter(rs.col("x") > 1).collect()
    assert result == [{"x": 2}]


def test_filter_column_lt_literal() -> None:
    """df.filter(col('x') < 2)."""
    spark = rs.SparkSession.builder().app_name("issue_401").get_or_create()
    df = spark.createDataFrame([(1,), (2,)], ["x"])
    result = df.filter(rs.col("x") < 2).collect()
    assert result == [{"x": 1}]


def test_filter_column_eq_literal() -> None:
    """df.filter(col('x') == 2)."""
    spark = rs.SparkSession.builder().app_name("issue_401").get_or_create()
    df = spark.createDataFrame([(1,), (2,)], ["x"])
    result = df.filter(rs.col("x") == 2).collect()
    assert result == [{"x": 2}]

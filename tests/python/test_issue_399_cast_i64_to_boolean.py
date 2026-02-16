"""Tests for issue #399: cast integer to boolean (0 -> false, non-zero -> true)."""

from __future__ import annotations

import robin_sparkless as rs


def test_cast_i64_to_boolean() -> None:
    """cast i64 to boolean: 0 -> false, non-zero -> true (PySpark parity)."""
    spark = rs.SparkSession.builder().app_name("issue_399").get_or_create()
    df = spark.createDataFrame([(0,), (1,)], ["x"])
    result = df.select(rs.col("x").cast("boolean")).collect()
    assert result[0]["x"] is False
    assert result[1]["x"] is True


def test_try_cast_i64_to_boolean() -> None:
    """try_cast i64 to boolean also works."""
    spark = rs.SparkSession.builder().app_name("issue_399").get_or_create()
    df = spark.createDataFrame([(0,), (1,), (-1,)], ["x"])
    result = df.select(rs.col("x").try_cast("boolean").alias("b")).collect()
    assert result[0]["b"] is False
    assert result[1]["b"] is True
    assert result[2]["b"] is True


def test_cast_float_to_boolean() -> None:
    """cast float to boolean: 0.0 -> false, non-zero -> true."""
    spark = rs.SparkSession.builder().app_name("issue_399").get_or_create()
    df = spark.createDataFrame([(0.0,), (1.5,)], ["x"])
    result = df.select(rs.col("x").cast("boolean")).collect()
    assert result[0]["x"] is False
    assert result[1]["x"] is True

"""Tests for issue #399: cast integer to boolean (0 -> false, non-zero -> true)."""

from __future__ import annotations

from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
F = _imports.F


def test_cast_i64_to_boolean(spark) -> None:
    """cast i64 to boolean: 0 -> false, non-zero -> true (PySpark parity)."""
    df = spark.createDataFrame([(0,), (1,)], ["x"])
    result = df.select(F.col("x").cast("boolean")).collect()
    assert result[0]["x"] is False
    assert result[1]["x"] is True


def test_cast_i64_to_boolean_multiple(spark) -> None:
    """cast i64 to boolean: 0 -> false, non-zero -> true (same as try_cast for these inputs)."""
    df = spark.createDataFrame([(0,), (1,), (-1,)], ["x"])
    result = df.select(F.col("x").cast("boolean").alias("b")).collect()
    assert result[0]["b"] is False
    assert result[1]["b"] is True
    assert result[2]["b"] is True


def test_cast_float_to_boolean(spark) -> None:
    """cast float to boolean: 0.0 -> false, non-zero -> true."""
    df = spark.createDataFrame([(0.0,), (1.5,)], ["x"])
    result = df.select(F.col("x").cast("boolean")).collect()
    assert result[0]["x"] is False
    assert result[1]["x"] is True

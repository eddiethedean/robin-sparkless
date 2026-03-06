"""
Tests for #402: format_string accepts variadic columns (PySpark parity).
"""

from __future__ import annotations

import pytest

from tests.fixtures.spark_imports import get_spark_imports


_imports = get_spark_imports()
SparkSession = _imports.SparkSession
F = _imports.F


def _spark() -> SparkSession:
    return SparkSession.builder.appName("issue_402").getOrCreate()


def test_format_string_variadic_columns() -> None:
    """format_string(format, col1, col2, ...) accepts multiple columns."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"a": 1, "b": 2}, {"a": 10, "b": 20}],
        schema=["a", "b"],
    )
    result = df.select(
        F.format_string("a=%d b=%d", F.col("a"), F.col("b")).alias("fmt")
    ).collect()
    assert len(result) == 2
    assert result[0]["fmt"] == "a=1 b=2"
    assert result[1]["fmt"] == "a=10 b=20"


@pytest.mark.skip(reason="Issue #1234: unskip when fixing")
def test_format_string_string_int() -> None:
    """format_string with %s and %d (PySpark format_string supports variadic columns)."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"x": "hello", "y": 42}],
        schema=["x", "y"],
    )
    result = df.select(
        F.format_string("%s: %d", F.col("x"), F.col("y")).alias("out")
    ).collect()
    assert len(result) == 1
    assert result[0]["out"] == "hello: 42"

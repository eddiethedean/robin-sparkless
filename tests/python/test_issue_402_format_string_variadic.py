"""
Tests for #402: format_string accepts variadic columns (PySpark parity).
"""

from __future__ import annotations

import robin_sparkless as rs


def _spark() -> rs.SparkSession:
    return rs.SparkSession.builder().app_name("issue_402").get_or_create()


def test_format_string_variadic_columns() -> None:
    """format_string(format, col1, col2, ...) accepts multiple columns."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"a": 1, "b": 2}, {"a": 10, "b": 20}],
        schema=[("a", "int"), ("b", "int")],
    )
    result = df.select(
        rs.format_string("a=%d b=%d", rs.col("a"), rs.col("b")).alias("fmt")
    ).collect()
    assert len(result) == 2
    assert result[0]["fmt"] == "a=1 b=2"
    assert result[1]["fmt"] == "a=10 b=20"


def test_printf_variadic_columns() -> None:
    """printf(format, col1, col2, ...) also accepts variadic columns."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"x": "hello", "y": 42}],
        schema=[("x", "string"), ("y", "int")],
    )
    result = df.select(
        rs.printf("%s: %d", rs.col("x"), rs.col("y")).alias("out")
    ).collect()
    assert len(result) == 1
    assert result[0]["out"] == "hello: 42"

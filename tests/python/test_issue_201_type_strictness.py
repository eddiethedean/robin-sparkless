"""
Tests for issue #201: PySpark-style type coercion in arithmetic (string vs numeric).

PySpark coerces string to numeric in arithmetic (e.g. col("a") + col("b") when a is
string "10" and b is bigint). Robin-sparkless supports the same via Python Column
operators (+, -, *, /) and plan interpreter add/subtract/multiply/divide.
"""

from __future__ import annotations


def test_string_plus_numeric_with_column_no_cast() -> None:
    """with_column('x', col('a') + col('b')) with a=string, b=bigint works (issue #201)."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.createDataFrame(
        [{"a": "10", "b": 2}],
        [("a", "string"), ("b", "bigint")],
    )
    result = df.with_column("x", rs.col("a") + rs.col("b"))
    rows = result.collect()
    assert rows == [{"a": "10", "b": 2, "x": 12.0}]


def test_string_arithmetic_ops_implicit_coercion() -> None:
    """All arithmetic ops coerce string to numeric (add, sub, mul, div)."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.createDataFrame(
        [{"a": "10", "b": 2}, {"a": "20", "b": 3}],
        [("a", "string"), ("b", "bigint")],
    )
    assert df.with_column("x", rs.col("a") + rs.col("b")).collect() == [
        {"a": "10", "b": 2, "x": 12.0},
        {"a": "20", "b": 3, "x": 23.0},
    ]
    assert df.with_column("x", rs.col("a") - rs.col("b")).collect() == [
        {"a": "10", "b": 2, "x": 8.0},
        {"a": "20", "b": 3, "x": 17.0},
    ]
    assert df.with_column("x", rs.col("a") * rs.col("b")).collect() == [
        {"a": "10", "b": 2, "x": 20.0},
        {"a": "20", "b": 3, "x": 60.0},
    ]
    rows_div = df.with_column("x", rs.col("a") / rs.col("b")).collect()
    assert rows_div[0]["x"] == 5.0 and rows_div[1]["x"] == 20.0 / 3.0


def test_invalid_string_becomes_null() -> None:
    """Invalid numeric strings in arithmetic yield null (PySpark semantics)."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.createDataFrame(
        [{"s": "10"}, {"s": "bad"}, {"s": "5"}],
        [("s", "string")],
    )
    result = df.with_column("x", rs.col("s") + rs.lit(1)).collect()
    assert result[0]["x"] == 11.0
    assert result[1]["x"] is None
    assert result[2]["x"] == 6.0

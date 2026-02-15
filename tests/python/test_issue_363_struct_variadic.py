"""
Tests for #363: struct() accept multiple Column arguments (PySpark parity).

PySpark F.struct(col1, col2, ...) accepts variadic Column arguments.
Robin-sparkless struct() now accepts the same: struct(col1, col2, ...).
"""

from __future__ import annotations


def test_struct_multiple_columns_issue_repro() -> None:
    """struct(col("a"), col("b")) works (issue repro)."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("issue_363").get_or_create()
    df = spark.createDataFrame([{"a": 1, "b": 2}], [("a", "int"), ("b", "int")])
    rows = df.select(rs.struct(rs.col("a"), rs.col("b")).alias("s")).collect()
    assert list(rows[0].keys()) == ["s"]
    assert rows[0]["s"] == {"a": 1, "b": 2}


def test_struct_single_column() -> None:
    """struct(col("x")) produces struct with one field."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("issue_363").get_or_create()
    df = spark.createDataFrame([{"x": 10}], [("x", "int")])
    rows = df.select(rs.struct(rs.col("x")).alias("s")).collect()
    assert rows[0]["s"] == {"x": 10}


def test_struct_three_columns() -> None:
    """struct(col1, col2, col3) produces struct with three fields."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("issue_363").get_or_create()
    df = spark.createDataFrame([(1, "a", 3.0)], ["i", "s", "f"])
    rows = df.select(
        rs.struct(rs.col("i"), rs.col("s"), rs.col("f")).alias("s")
    ).collect()
    assert rows[0]["s"] == {"i": 1, "s": "a", "f": 3.0}

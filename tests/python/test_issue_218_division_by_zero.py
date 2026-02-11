"""Tests for issue #218: Division by zero returns null (Spark parity).

Polars returns inf for division by zero; Spark/PySpark returns null.
Sparkless tests expect null.
"""

import robin_sparkless as rs


def test_division_by_zero_literal_over_column() -> None:
    """Exact scenario from #218: lit(1) / col('x') with x=0 -> null."""
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [{"x": 1}, {"x": 0}],
        [("x", "bigint")],
    )
    result = df.with_column("q", rs.lit(1) / rs.col("x"))
    rows = result.collect()
    assert len(rows) == 2
    assert rows[0]["q"] == 1.0
    assert rows[1]["q"] is None


def test_division_by_zero_column_over_literal() -> None:
    """col('x') / lit(0) -> null."""
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [{"x": 10}, {"x": 20}],
        [("x", "bigint")],
    )
    result = df.with_column("q", rs.col("x") / rs.lit(0))
    rows = result.collect()
    assert len(rows) == 2
    assert rows[0]["q"] is None
    assert rows[1]["q"] is None


def test_division_by_zero_column_over_column() -> None:
    """col('a') / col('b') with b=0 -> null."""
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [{"a": 5, "b": 1}, {"a": 5, "b": 0}],
        [("a", "bigint"), ("b", "bigint")],
    )
    result = df.with_column("q", rs.col("a") / rs.col("b"))
    rows = result.collect()
    assert rows[0]["q"] == 5.0
    assert rows[1]["q"] is None

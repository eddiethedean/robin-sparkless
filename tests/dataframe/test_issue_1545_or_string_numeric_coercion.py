"""Tests for issue #1545: string literal coercion under logical OR in filter."""

from __future__ import annotations

from sparkless.testing import get_imports

F = get_imports().F


def test_filter_or_numeric_column_eq_string_literal(spark) -> None:
    """(col == '123') | (col == '123') with integer column (issue #1545 repro)."""
    df = spark.createDataFrame([["123"]], ["A"]).withColumn(
        "A", F.col("A").cast("integer")
    )
    out = df.filter((F.col("A") == "123") | (F.col("A") == "123")).collect()
    assert len(out) == 1
    assert out[0]["A"] == 123


def test_filter_and_numeric_column_eq_string_literal(spark) -> None:
    """Same coercion when comparisons are joined with &."""
    df = spark.createDataFrame([["123"]], ["A"]).withColumn(
        "A", F.col("A").cast("integer")
    )
    out = df.filter((F.col("A") == "123") & (F.col("A") == "123")).collect()
    assert len(out) == 1
    assert out[0]["A"] == 123

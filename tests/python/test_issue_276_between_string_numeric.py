"""
Tests for issue #276: between() with string column and numeric bounds (PySpark parity).

PySpark col("col").between(1, 20) when col is string coerces for comparison.
Robin previously raised: RuntimeError: cannot compare string with numeric type (i32).
"""

from __future__ import annotations

import robin_sparkless as rs

F = rs


def test_between_string_column_numeric_bounds_with_column() -> None:
    """df.with_column("between", col("col").between(1, 20)) when col is string coerces."""
    spark = F.SparkSession.builder().app_name("test_276").get_or_create()
    data = [{"col": "5"}, {"col": "10"}, {"col": "15"}]
    df = spark.createDataFrame(data, [("col", "str")])
    df = df.with_column("between", F.col("col").between(1, 20))
    rows = df.collect()
    assert len(rows) == 3
    # All "5", "10", "15" are in [1, 20] when coerced to number
    assert rows[0]["between"] is True
    assert rows[1]["between"] is True
    assert rows[2]["between"] is True


def test_between_string_column_numeric_bounds_filter() -> None:
    """df.filter(col("col").between(1, 20)) when col is string also coerces."""
    spark = F.SparkSession.builder().app_name("test_276").get_or_create()
    data = [{"col": "5"}, {"col": "10"}, {"col": "25"}]
    df = spark.createDataFrame(data, [("col", "str")])
    out = df.filter(F.col("col").between(1, 20)).collect()
    assert len(out) == 2  # "5" and "10" in range, "25" not
    assert {r["col"] for r in out} == {"5", "10"}

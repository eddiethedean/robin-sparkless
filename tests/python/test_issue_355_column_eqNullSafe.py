"""
Tests for #355: Column.eqNullSafe (PySpark parity).

PySpark Column has .eqNullSafe(other) for null-safe equality. Robin-sparkless now
exposes eqNullSafe as an alias for eq_null_safe so PySpark-style code works.
"""

from __future__ import annotations

import robin_sparkless as rs


def test_column_eqNullSafe_select_lit() -> None:
    """df.select(col("a").eqNullSafe(lit(1))) returns boolean column (issue repro)."""
    spark = rs.SparkSession.builder().app_name("issue_355").get_or_create()
    df = spark.createDataFrame([{"a": 1}, {"a": None}], [("a", "int")])
    out = df.select(rs.col("a").eqNullSafe(rs.lit(1)).alias("eq")).collect()
    assert len(out) == 2
    # a=1 eq 1 -> True; a=None eq 1 -> False (null-safe: one null -> false)
    assert out[0]["eq"] is True
    assert out[1]["eq"] is False


def test_column_eqNullSafe_same_as_eq_null_safe() -> None:
    """eqNullSafe and eq_null_safe produce the same result."""
    spark = rs.SparkSession.builder().app_name("issue_355").get_or_create()
    df = spark.createDataFrame(
        [{"a": 1}, {"a": None}, {"a": 2}],
        [("a", "int")],
    )
    out_camel = df.select(rs.col("a").eqNullSafe(rs.lit(1)).alias("eq")).collect()
    out_snake = df.select(rs.col("a").eq_null_safe(rs.lit(1)).alias("eq")).collect()
    assert out_camel == out_snake

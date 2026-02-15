"""
Tests for issue #260: F.lit(None).cast("double") and F.lit(None).cast("date") (PySpark parity).

PySpark allows casting a null literal to double or date for schema evolution and typed null columns.
Robin (Polars) previously raised RuntimeError; we now support it.
"""

from __future__ import annotations

import robin_sparkless as rs

F = rs


def test_lit_none_cast_double() -> None:
    """with_column with F.lit(None).cast('double') succeeds and yields null double column."""
    spark = F.SparkSession.builder().app_name("test_260").get_or_create()
    df = spark.createDataFrame([{"a": 1}], [("a", "int")])
    df = df.with_column("null_double", F.lit(None).cast("double"))
    out = df.collect()
    assert len(out) == 1
    assert out[0]["a"] == 1
    assert out[0]["null_double"] is None


def test_lit_none_cast_date() -> None:
    """with_column with F.lit(None).cast('date') succeeds and yields null date column."""
    spark = F.SparkSession.builder().app_name("test_260").get_or_create()
    df = spark.createDataFrame([{"a": 1}], [("a", "int")])
    df = df.with_column("null_date", F.lit(None).cast("date"))
    out = df.collect()
    assert len(out) == 1
    assert out[0]["a"] == 1
    assert out[0]["null_date"] is None


def test_lit_none_cast_double_and_date() -> None:
    """Multiple null casts (double and date) in one DataFrame."""
    spark = F.SparkSession.builder().app_name("test_260").get_or_create()
    df = spark.createDataFrame([{"id": 1}, {"id": 2}], [("id", "bigint")])
    df = df.with_column("nd", F.lit(None).cast("double")).with_column(
        "ndate", F.lit(None).cast("date")
    )
    out = df.collect()
    assert len(out) == 2
    for row in out:
        assert row["nd"] is None
        assert row["ndate"] is None

"""
Tests for issue #260: F.lit(None).cast("double") and F.lit(None).cast("date") (PySpark parity).

PySpark allows casting a null literal to double or date for schema evolution and typed null columns.
Robin (Polars) previously raised RuntimeError; we now support it.
"""

from __future__ import annotations

from sparkless.testing import get_imports

_imports = get_imports()
F = _imports.F


def test_lit_none_cast_double(spark) -> None:
    """withColumn with F.lit(None).cast('double') succeeds and yields null double column."""
    df = spark.createDataFrame([{"a": 1}], schema=["a"])
    df = df.withColumn("null_double", F.lit(None).cast("double"))
    out = df.collect()
    assert len(out) == 1
    assert out[0]["a"] == 1
    assert out[0]["null_double"] is None


def test_lit_none_cast_date(spark) -> None:
    """withColumn with F.lit(None).cast('date') succeeds and yields null date column."""
    df = spark.createDataFrame([{"a": 1}], schema=["a"])
    df = df.withColumn("null_date", F.lit(None).cast("date"))
    out = df.collect()
    assert len(out) == 1
    assert out[0]["a"] == 1
    assert out[0]["null_date"] is None


def test_lit_none_cast_double_and_date(spark) -> None:
    """Multiple null casts (double and date) in one DataFrame."""
    df = spark.createDataFrame([{"id": 1}, {"id": 2}], schema=["id"])
    df = df.withColumn("nd", F.lit(None).cast("double")).withColumn(
        "ndate", F.lit(None).cast("date")
    )
    out = df.collect()
    assert len(out) == 2
    for row in out:
        assert row["nd"] is None
        assert row["ndate"] is None

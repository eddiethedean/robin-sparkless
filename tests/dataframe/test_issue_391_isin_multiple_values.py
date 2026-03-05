"""Tests for issue #391: Column.isin multiple values (variadic and list)."""

from __future__ import annotations

from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
F = _imports.F


def test_isin_list(spark) -> None:
    """col.isin([1, 2, 3]) works as before."""
    df = spark.createDataFrame([(1,), (2,), (3,), (4,)], ["x"])
    out = df.filter(F.col("x").isin([1, 2, 3]))
    rows = out.collect()
    assert len(rows) == 3
    assert {r["x"] for r in rows} == {1, 2, 3}


def test_isin_variadic(spark) -> None:
    """col.isin(1, 2, 3) works (PySpark *values)."""
    df = spark.createDataFrame([(1,), (2,), (3,), (4,)], ["x"])
    out = df.filter(F.col("x").isin(1, 2, 3))
    rows = out.collect()
    assert len(rows) == 3
    assert {r["x"] for r in rows} == {1, 2, 3}


def test_isin_single_value(spark) -> None:
    """col.isin(2) returns rows where x == 2."""
    df = spark.createDataFrame([(1,), (2,), (2,)], ["x"])
    out = df.filter(F.col("x").isin(2))
    rows = out.collect()
    assert len(rows) == 2
    assert all(r["x"] == 2 for r in rows)


def test_isin_str_list_and_variadic(spark) -> None:
    """col.isin with strings: list and variadic."""
    df = spark.createDataFrame([("a",), ("b",), ("c",)], ["label"])
    out = df.filter(F.col("label").isin("a", "c"))
    rows = out.collect()
    assert len(rows) == 2
    assert {r["label"] for r in rows} == {"a", "c"}

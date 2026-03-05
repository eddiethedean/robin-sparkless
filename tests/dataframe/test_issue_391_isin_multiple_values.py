"""Tests for issue #391: Column.isin multiple values (variadic and list)."""

from __future__ import annotations

from tests.utils import get_functions, get_spark

F = get_functions()


def test_isin_list() -> None:
    """col.isin([1, 2, 3]) works as before."""
    spark = get_spark("issue_391")
    df = spark.createDataFrame([(1,), (2,), (3,), (4,)], ["x"])
    out = df.filter(F.col("x").isin([1, 2, 3]))
    rows = out.collect()
    assert len(rows) == 3
    assert {r["x"] for r in rows} == {1, 2, 3}


def test_isin_variadic() -> None:
    """col.isin(1, 2, 3) works (PySpark *values)."""
    spark = get_spark("issue_391")
    df = spark.createDataFrame([(1,), (2,), (3,), (4,)], ["x"])
    out = df.filter(F.col("x").isin(1, 2, 3))
    rows = out.collect()
    assert len(rows) == 3
    assert {r["x"] for r in rows} == {1, 2, 3}


def test_isin_single_value() -> None:
    """col.isin(2) returns rows where x == 2."""
    spark = get_spark("issue_391")
    df = spark.createDataFrame([(1,), (2,), (2,)], ["x"])
    out = df.filter(F.col("x").isin(2))
    rows = out.collect()
    assert len(rows) == 2
    assert all(r["x"] == 2 for r in rows)


def test_isin_str_list_and_variadic() -> None:
    """col.isin with strings: list and variadic."""
    spark = get_spark("issue_391")
    df = spark.createDataFrame([("a",), ("b",), ("c",)], ["label"])
    out = df.filter(F.col("label").isin("a", "c"))
    rows = out.collect()
    assert len(rows) == 2
    assert {r["label"] for r in rows} == {"a", "c"}

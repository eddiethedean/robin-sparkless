"""Tests for #378: orderBy/sort with .asc()/.desc() (PySpark parity).

PySpark does not support orderBy(..., ascending=...). Use col("x").asc() or col("x").desc().
"""

from __future__ import annotations

from sparkless.testing import get_imports

_imports = get_imports()
F = _imports.F


def test_order_by_single_column_ascending_false(spark) -> None:
    """orderBy(col("a").desc()) sorts descending."""
    df = spark.createDataFrame([(3,), (1,), (2,)], ["a"])
    out = df.orderBy(F.col("a").desc())
    rows = out.collect()
    assert [r["a"] for r in rows] == [3, 2, 1]


def test_order_by_column_list_ascending_single_bool(spark) -> None:
    """orderBy(col("a").desc(), col("b").desc()) sorts both descending."""
    df = spark.createDataFrame([(1, 10), (2, 20), (1, 30)], ["a", "b"])
    out = df.orderBy(F.col("a").desc(), F.col("b").desc())
    rows = out.collect()
    # Both desc: (2,20), (1,30), (1,10)
    assert [r["a"] for r in rows] == [2, 1, 1]
    assert [r["b"] for r in rows] == [20, 30, 10]


def test_order_by_column_names_ascending_single_bool(spark) -> None:
    """orderBy(col("a").desc(), col("b").desc()) with column expressions."""
    df = spark.createDataFrame([(1, 10), (2, 20)], ["a", "b"])
    out = df.orderBy(F.col("a").desc(), F.col("b").desc())
    rows = out.collect()
    assert [r["a"] for r in rows] == [2, 1]


def test_order_by_ascending_list_of_bool(spark) -> None:
    """orderBy(col("a").asc(), col("b").desc()) first asc, second desc."""
    df = spark.createDataFrame([(1, 30), (1, 10), (2, 20)], ["a", "b"])
    out = df.orderBy(F.col("a").asc(), F.col("b").desc())
    rows = out.collect()
    # a asc, b desc within a: (1,30), (1,10), (2,20)
    assert [r["a"] for r in rows] == [1, 1, 2]
    assert [r["b"] for r in rows] == [30, 10, 20]

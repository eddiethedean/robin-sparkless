"""Tests for #378: orderBy/sort ascending parameter accept bool (PySpark parity)."""

from __future__ import annotations

from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
F = _imports.F


def test_order_by_single_column_ascending_false(spark) -> None:
    """orderBy(col("a"), ascending=False) sorts descending."""
    df = spark.createDataFrame([(3,), (1,), (2,)], ["a"])
    out = df.orderBy(F.col("a"), ascending=False)
    rows = out.collect()
    assert [r["a"] for r in rows] == [3, 2, 1]


def test_order_by_column_list_ascending_single_bool(spark) -> None:
    """orderBy(col("a"), col("b"), ascending=False) sorts both descending."""
    df = spark.createDataFrame([(1, 10), (2, 20), (1, 30)], ["a", "b"])
    out = df.orderBy(F.col("a"), F.col("b"), ascending=False)
    rows = out.collect()
    # Both desc: (2,20), (1,30), (1,10)
    assert [r["a"] for r in rows] == [2, 1, 1]
    assert [r["b"] for r in rows] == [20, 30, 10]


def test_order_by_column_names_ascending_single_bool(spark) -> None:
    """orderBy(["a", "b"], ascending=False) sorts both descending."""
    df = spark.createDataFrame([(1, 10), (2, 20)], ["a", "b"])
    out = df.orderBy(["a", "b"], ascending=False)
    rows = out.collect()
    assert [r["a"] for r in rows] == [2, 1]


def test_order_by_ascending_list_of_bool(spark) -> None:
    """orderBy(["a", "b"], ascending=[True, False]) first asc, second desc."""
    df = spark.createDataFrame([(1, 30), (1, 10), (2, 20)], ["a", "b"])
    out = df.orderBy(["a", "b"], ascending=[True, False])
    rows = out.collect()
    # a asc, b desc within a: (1,30), (1,10), (2,20)
    assert [r["a"] for r in rows] == [1, 1, 2]
    assert [r["b"] for r in rows] == [30, 10, 20]

"""Tests for #378: orderBy/sort ascending parameter accept bool (PySpark parity)."""

from __future__ import annotations

import robin_sparkless as rs
from robin_sparkless import col


def _spark():
    return rs.SparkSession.builder().app_name("issue_378").get_or_create()


def test_order_by_single_column_ascending_false() -> None:
    """order_by(col("x"), ascending=False) sorts descending."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"a": 3}, {"a": 1}, {"a": 2}],
        schema=[("a", "int")],
    )
    out = df.order_by(col("a"), ascending=False)
    rows = out.collect()
    assert [r["a"] for r in rows] == [3, 2, 1]


def test_order_by_column_list_ascending_single_bool() -> None:
    """order_by([col("a"), col("b")], ascending=False) sorts both descending."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"a": 1, "b": 10}, {"a": 2, "b": 20}, {"a": 1, "b": 30}],
        schema=[("a", "int"), ("b", "int")],
    )
    out = df.order_by([col("a"), col("b")], ascending=False)
    rows = out.collect()
    # Both desc: (2,20), (1,30), (1,10)
    assert [r["a"] for r in rows] == [2, 1, 1]
    assert [r["b"] for r in rows] == [20, 30, 10]


def test_order_by_column_names_ascending_single_bool() -> None:
    """order_by(["a", "b"], ascending=False) sorts both descending."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"a": 1, "b": 10}, {"a": 2, "b": 20}],
        schema=[("a", "int"), ("b", "int")],
    )
    out = df.order_by(["a", "b"], ascending=False)
    rows = out.collect()
    assert [r["a"] for r in rows] == [2, 1]


def test_order_by_ascending_list_of_bool() -> None:
    """order_by(["a", "b"], ascending=[True, False]) first asc, second desc."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"a": 1, "b": 30}, {"a": 1, "b": 10}, {"a": 2, "b": 20}],
        schema=[("a", "int"), ("b", "int")],
    )
    out = df.order_by(["a", "b"], ascending=[True, False])
    rows = out.collect()
    # a asc, b desc within a: (1,30), (1,10), (2,20)
    assert [r["a"] for r in rows] == [1, 1, 2]
    assert [r["b"] for r in rows] == [30, 10, 20]

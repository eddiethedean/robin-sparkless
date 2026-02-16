"""Tests for #408: greatest() and least() variadic arguments."""

from __future__ import annotations

import robin_sparkless as rs


def _spark():
    return rs.SparkSession.builder().app_name("issue_408").get_or_create()


def test_greatest_variadic() -> None:
    """greatest(col("a"), col("b"), col("c")) returns max per row."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"a": 1, "b": 5, "c": 3}, {"a": 10, "b": 2, "c": 8}],
        schema=[("a", "int"), ("b", "int"), ("c", "int")],
    )
    out = df.select(
        rs.greatest(rs.col("a"), rs.col("b"), rs.col("c")).alias("m")
    ).collect()
    rows = list(out)
    assert len(rows) == 2
    assert rows[0]["m"] == 5
    assert rows[1]["m"] == 10


def test_least_variadic() -> None:
    """least(col("a"), col("b"), col("c")) returns min per row."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"a": 1, "b": 5, "c": 3}, {"a": 10, "b": 2, "c": 8}],
        schema=[("a", "int"), ("b", "int"), ("c", "int")],
    )
    out = df.select(
        rs.least(rs.col("a"), rs.col("b"), rs.col("c")).alias("m")
    ).collect()
    rows = list(out)
    assert len(rows) == 2
    assert rows[0]["m"] == 1
    assert rows[1]["m"] == 2

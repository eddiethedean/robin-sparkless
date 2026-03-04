"""Tests for #408: greatest() and least() variadic arguments."""

from __future__ import annotations

from tests.fixtures.spark_imports import get_spark_imports


_imports = get_spark_imports()
SparkSession = _imports.SparkSession
F = _imports.F


def _spark():
    return SparkSession.builder.appName("issue_408").getOrCreate()


def test_greatest_variadic() -> None:
    """greatest(col("a"), col("b"), col("c")) returns max per row."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"a": 1, "b": 5, "c": 3}, {"a": 10, "b": 2, "c": 8}],
        schema=["a", "b", "c"],
    )
    out = df.select(
        F.greatest(F.col("a"), F.col("b"), F.col("c")).alias("m")
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
        schema=["a", "b", "c"],
    )
    out = df.select(
        F.least(F.col("a"), F.col("b"), F.col("c")).alias("m")
    ).collect()
    rows = list(out)
    assert len(rows) == 2
    assert rows[0]["m"] == 1
    assert rows[1]["m"] == 2

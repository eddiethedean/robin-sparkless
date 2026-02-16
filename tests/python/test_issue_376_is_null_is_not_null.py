"""Tests for #376: Column.isNull() and isNotNull() (PySpark naming)."""

from __future__ import annotations

import robin_sparkless as rs
from robin_sparkless import col


def _spark():
    return rs.SparkSession.builder().app_name("issue_376").get_or_create()


def test_is_null_and_is_not_null_snake() -> None:
    """is_null() and is_not_null() work and can be used in filter."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"a": 1, "b": "x"}, {"a": None, "b": "y"}, {"a": 3, "b": None}],
        schema=[("a", "int"), ("b", "string")],
    )
    null_a = df.filter(col("a").is_null())
    rows = null_a.collect()
    assert len(rows) == 1
    assert rows[0]["a"] is None
    non_null_a = df.filter(col("a").is_not_null())
    assert len(non_null_a.collect()) == 2


def test_is_null_and_is_not_null_camel() -> None:
    """isNull() and isNotNull() (PySpark naming) work the same."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"a": 1}, {"a": None}],
        schema=[("a", "int")],
    )
    null_a = df.filter(col("a").isNull())
    assert len(null_a.collect()) == 1
    non_null_a = df.filter(col("a").isNotNull())
    assert len(non_null_a.collect()) == 1

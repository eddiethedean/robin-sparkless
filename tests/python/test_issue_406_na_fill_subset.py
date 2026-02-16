"""Tests for #406: na.fill(value, subset=[list of str])."""

from __future__ import annotations

import robin_sparkless as rs


def _spark():
    return rs.SparkSession.builder().app_name("issue_406").get_or_create()


def test_na_fill_subset_list_of_str() -> None:
    """df.na.fill(0, subset=["b"]) fills nulls only in "b", leaves "a" unchanged."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"a": 1, "b": None}, {"a": None, "b": 2}],
        schema=[("a", "int"), ("b", "int")],
    )
    result = df.na().fill(0, subset=["b"]).collect()
    rows = list(result)
    assert len(rows) == 2
    # First row: a=1, b was null -> filled with 0
    assert rows[0]["a"] == 1
    assert rows[0]["b"] == 0
    # Second row: a was null (unchanged), b=2
    assert rows[1]["a"] is None
    assert rows[1]["b"] == 2

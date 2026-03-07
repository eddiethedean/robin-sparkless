"""Tests for #375: flatMap behavior (PySpark)."""

from __future__ import annotations
import pytest


@pytest.mark.skip(reason="Issue #1231: unskip when fixing")
def test_flat_map(spark) -> None:
    """PySpark: use RDD.flatMap for row expansion."""
    df = spark.createDataFrame([("a b",), ("c d e",)], ["word"])
    words = df.rdd.flatMap(lambda row: row["word"].split()).collect()
    assert words == ["a", "b", "c", "d", "e"]


@pytest.mark.skip(reason="Issue #1231: unskip when fixing")
def test_flat_map_empty(spark) -> None:
    """PySpark: flatMap can return empty output."""
    df = spark.createDataFrame([(1,)], ["x"])
    out = df.rdd.flatMap(lambda row: []).collect()
    assert out == []

"""Tests for #375: DataFrame.flatMap() (PySpark parity)."""

from __future__ import annotations

import robin_sparkless as rs


def _spark():
    return rs.SparkSession.builder().app_name("issue_375").get_or_create()


def test_flat_map() -> None:
    """df.flat_map(func) applies func to each row and flattens; func returns iterable of dicts."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"word": "a b"}, {"word": "c d e"}],
        schema=[("word", "string")],
    )
    # Return list of one-dict rows per word so we get one row per word
    out = df.flat_map(lambda row: [{"word": w} for w in row["word"].split()])
    rows = out.collect()
    assert len(rows) == 5  # a, b, c, d, e
    words = [r["word"] for r in rows]
    assert words == ["a", "b", "c", "d", "e"]


def test_flat_map_empty() -> None:
    """flat_map with func that returns empty iterable yields empty DataFrame."""
    spark = _spark()
    df = spark.createDataFrame([{"x": 1}], schema=[("x", "int")])
    out = df.flat_map(lambda row: [])
    rows = out.collect()
    assert len(rows) == 0

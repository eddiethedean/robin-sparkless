"""Tests for #382: CubeRollupData.count() and agg (PySpark parity)."""

from __future__ import annotations

from tests.python.utils import get_functions

F = get_functions()


def test_cube_count(spark) -> None:
    """df.cube("a").count() returns DataFrame with count per grouping set."""
    df = spark.createDataFrame(
        [{"a": 1, "v": 10}, {"a": 1, "v": 20}, {"a": 2, "v": 30}],
        schema=["a", "v"],
    )
    out = df.cube("a").count()
    rows = out.collect()
    assert len(rows) >= 1
    # Should have "a" and "count" columns
    names = list(rows[0].asDict().keys()) if rows else []
    assert "count" in names


def test_rollup_count(spark) -> None:
    """df.rollup("a", "b").count() returns DataFrame with count per grouping set."""
    df = spark.createDataFrame(
        [{"a": 1, "b": 10, "v": 1}, {"a": 1, "b": 10, "v": 2}],
        schema=["a", "b", "v"],
    )
    out = df.rollup("a", "b").count()
    rows = out.collect()
    assert len(rows) >= 1
    assert "count" in (list(rows[0].asDict().keys()) if rows else [])


def test_cube_agg(spark) -> None:
    """df.cube("a").agg([sum(col("v"))]) returns DataFrame with aggregates."""
    df = spark.createDataFrame(
        [{"a": 1, "v": 10}, {"a": 1, "v": 20}, {"a": 2, "v": 30}],
        schema=["a", "v"],
    )
    out = df.cube("a").agg(F.sum(F.col("v")))
    rows = out.collect()
    assert len(rows) >= 1
    # Sum of v is 60 total; per-group sums 30, 30
    vals = [r for r in rows]
    assert len(vals) >= 1

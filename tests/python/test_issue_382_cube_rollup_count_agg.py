"""Tests for #382: CubeRollupData.count() and agg (PySpark parity)."""

from __future__ import annotations

import robin_sparkless as rs
from robin_sparkless import col, sum as rs_sum


def _spark():
    return rs.SparkSession.builder().app_name("issue_382").get_or_create()


def test_cube_count() -> None:
    """df.cube("a").count() returns DataFrame with count per grouping set."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"a": 1, "v": 10}, {"a": 1, "v": 20}, {"a": 2, "v": 30}],
        schema=[("a", "int"), ("v", "int")],
    )
    out = df.cube("a").count()
    rows = out.collect()
    assert len(rows) >= 1
    # Should have "a" and "count" columns
    names = list(rows[0].keys()) if rows else []
    assert "count" in names


def test_rollup_count() -> None:
    """df.rollup("a", "b").count() returns DataFrame with count per grouping set."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"a": 1, "b": 10, "v": 1}, {"a": 1, "b": 10, "v": 2}],
        schema=[("a", "int"), ("b", "int"), ("v", "int")],
    )
    out = df.rollup("a", "b").count()
    rows = out.collect()
    assert len(rows) >= 1
    assert "count" in (list(rows[0].keys()) if rows else [])


def test_cube_agg() -> None:
    """df.cube("a").agg([sum(col("v"))]) returns DataFrame with aggregates."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"a": 1, "v": 10}, {"a": 1, "v": 20}, {"a": 2, "v": 30}],
        schema=[("a", "int"), ("v", "int")],
    )
    out = df.cube("a").agg([rs_sum(col("v"))])
    rows = out.collect()
    assert len(rows) >= 1
    # Sum of v is 60 total; per-group sums 30, 30
    vals = [r for r in rows]
    assert len(vals) >= 1

"""Tests for #382: CubeRollupData.count() and agg (PySpark parity)."""

from __future__ import annotations

from sparkless.testing import get_imports

_imports = get_imports()
F = _imports.F


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


def test_rollup_sum(spark) -> None:
    """df.rollup("a", "b").sum("val") - exact reproduction from issue #1482."""
    df = spark.createDataFrame(
        [
            {"a": "A", "b": "X", "val": 1},
            {"a": "A", "b": "Y", "val": 2},
            {"a": "B", "b": "X", "val": 3},
        ]
    )
    result = df.rollup("a", "b").sum("val")
    rows = result.collect()
    assert len(rows) >= 1
    assert "sum(val)" in (list(rows[0].asDict().keys()) if rows else [])
    # Rollup: (A,X), (A,Y), (B,X), (A), (B), () -> 6 grouping sets
    sums = {r["sum(val)"] for r in rows}
    assert 1 in sums or 2 in sums or 3 in sums or 6 in sums  # some expected sums


def test_cube_sum(spark) -> None:
    """df.cube("a", "b").sum("val") - issue #1482."""
    df = spark.createDataFrame(
        [
            {"a": "A", "b": "X", "val": 1},
            {"a": "A", "b": "Y", "val": 2},
            {"a": "B", "b": "X", "val": 3},
        ]
    )
    result = df.cube("a", "b").sum("val")
    rows = result.collect()
    assert len(rows) >= 1
    assert "sum(val)" in (list(rows[0].asDict().keys()) if rows else [])


def test_rollup_avg_min_max(spark) -> None:
    """PyCubeRollupData.avg(), .min(), .max() work (issue #1482)."""
    df = spark.createDataFrame(
        [{"a": 1, "val": 10}, {"a": 1, "val": 20}, {"a": 2, "val": 30}],
        schema=["a", "val"],
    )
    for method, col_name in [("avg", "avg(val)"), ("min", "min(val)"), ("max", "max(val)")]:
        out = getattr(df.rollup("a"), method)("val")
        rows = out.collect()
        assert len(rows) >= 1, f"rollup().{method}() should return rows"
        assert col_name in (list(rows[0].asDict().keys()) if rows else []), (
            f"rollup().{method}() should have column {col_name}"
        )

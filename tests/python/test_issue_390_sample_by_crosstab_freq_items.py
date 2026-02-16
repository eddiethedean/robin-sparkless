"""Tests for issue #390: DataFrame.sampleBy, crosstab, freqItems."""

from __future__ import annotations

import robin_sparkless as rs


def test_sample_by() -> None:
    """sampleBy(col, fractions, seed) stratified sample by column value."""
    spark = rs.SparkSession.builder().app_name("issue_390").get_or_create()
    # col "x" values 1,2,3; sample 100% of 1, 50% of 2, 0% of 3
    df = spark.createDataFrame(
        [(1, "a"), (1, "b"), (2, "c"), (2, "d"), (3, "e")], ["x", "y"]
    )
    out = df.sampleBy("x", {1: 1.0, 2: 0.5, 3: 0.0}, seed=42)
    rows = out.collect()
    # All x=1 (2 rows), ~half of x=2 (1 or 2 rows), no x=3
    assert len(rows) >= 2 and len(rows) <= 4
    xs = [r["x"] for r in rows]
    assert 3 not in xs
    assert all(x in (1, 2) for x in xs)


def test_crosstab() -> None:
    """crosstab(col1, col2) returns col1, col2, count."""
    spark = rs.SparkSession.builder().app_name("issue_390").get_or_create()
    df = spark.createDataFrame(
        [("a", "x"), ("a", "x"), ("a", "y"), ("b", "x")],
        ["c1", "c2"],
    )
    out = df.crosstab("c1", "c2")
    rows = out.collect()
    assert len(rows) >= 1
    names = list(rows[0].keys())
    assert "c1" in names or "c2" in names
    assert "count" in names


def test_freq_items() -> None:
    """freqItems(columns, support) returns one row with {col}_freqItems arrays."""
    spark = rs.SparkSession.builder().app_name("issue_390").get_or_create()
    df = spark.createDataFrame([(1, "a"), (1, "a"), (2, "b")], ["x", "y"])
    out = df.freqItems(["x", "y"], support=0.3)
    rows = out.collect()
    assert len(rows) == 1
    # Columns should be x_freqItems, y_freqItems (or similar)
    keys = list(rows[0].keys())
    assert any("freqItems" in k for k in keys)

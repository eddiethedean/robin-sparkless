"""
Tests for #361: dropDuplicates(subset=[...]) (PySpark parity).

PySpark: df.dropDuplicates(subset=None) / df.dropDuplicates(subset=[...]).
"""

from __future__ import annotations

def test_drop_duplicates_subset_issue_repro(spark) -> None:
    """df.dropDuplicates(subset=["a"]).collect() keeps one row per a."""
    df = spark.createDataFrame([(1, 1), (1, 2), (2, 1)], ["a", "b"])
    rows = df.dropDuplicates(["a"]).collect()
    assert len(rows) == 2
    a_vals = {r["a"] for r in rows}
    assert a_vals == {1, 2}


def test_drop_duplicates_no_subset(spark) -> None:
    """dropDuplicates() with no subset = distinct on all columns."""
    df = spark.createDataFrame([(1, 1), (1, 1), (2, 1)], ["a", "b"])
    rows = df.dropDuplicates().collect()
    assert len(rows) == 2


def test_drop_duplicates_subset_multiple_columns(spark) -> None:
    """dropDuplicates(subset=["a", "b"]) keeps one row per (a, b)."""
    df = spark.createDataFrame([(1, 1), (1, 2), (1, 1)], ["a", "b"])
    rows = df.dropDuplicates(["a", "b"]).collect()
    assert len(rows) == 2


def test_drop_duplicates_camel_case(spark) -> None:
    """dropDuplicates(subset=[...]) works (PySpark camelCase)."""
    df = spark.createDataFrame([(1, 1), (1, 2), (2, 1)], ["a", "b"])
    rows = df.dropDuplicates(subset=["a"]).collect()
    assert len(rows) == 2
    assert {r["a"] for r in rows} == {1, 2}

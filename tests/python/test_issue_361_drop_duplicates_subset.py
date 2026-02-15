"""
Tests for #361: dropDuplicates(subset=[...]) (PySpark parity).

PySpark: df.dropDuplicates(subset=None) / df.drop_duplicates(subset=[...]). Robin-sparkless now supports both.
"""

from __future__ import annotations

import robin_sparkless as rs


def _spark() -> rs.SparkSession:
    return rs.SparkSession.builder().app_name("issue_361").get_or_create()


def test_drop_duplicates_subset_issue_repro() -> None:
    """df.drop_duplicates(subset=["a"]).collect() (issue repro)."""
    spark = _spark()
    create_df = getattr(spark, "create_dataframe_from_rows", spark.createDataFrame)
    df = create_df(
        [{"a": 1, "b": 1}, {"a": 1, "b": 2}, {"a": 2, "b": 1}],
        [("a", "int"), ("b", "int")],
    )
    rows = df.drop_duplicates(subset=["a"]).collect()
    assert len(rows) == 2
    a_vals = {r["a"] for r in rows}
    assert a_vals == {1, 2}


def test_drop_duplicates_no_subset() -> None:
    """drop_duplicates() with no subset = distinct on all columns."""
    spark = _spark()
    create_df = getattr(spark, "create_dataframe_from_rows", spark.createDataFrame)
    df = create_df(
        [{"a": 1, "b": 1}, {"a": 1, "b": 1}, {"a": 2, "b": 1}],
        [("a", "int"), ("b", "int")],
    )
    rows = df.drop_duplicates().collect()
    assert len(rows) == 2


def test_drop_duplicates_subset_multiple_columns() -> None:
    """drop_duplicates(subset=["a", "b"]) keeps one row per (a, b)."""
    spark = _spark()
    create_df = getattr(spark, "create_dataframe_from_rows", spark.createDataFrame)
    df = create_df(
        [{"a": 1, "b": 1}, {"a": 1, "b": 2}, {"a": 1, "b": 1}],
        [("a", "int"), ("b", "int")],
    )
    rows = df.drop_duplicates(subset=["a", "b"]).collect()
    assert len(rows) == 2


def test_drop_duplicates_camel_case() -> None:
    """dropDuplicates(subset=[...]) works (PySpark camelCase)."""
    spark = _spark()
    create_df = getattr(spark, "create_dataframe_from_rows", spark.createDataFrame)
    df = create_df(
        [{"a": 1, "b": 1}, {"a": 1, "b": 2}, {"a": 2, "b": 1}],
        [("a", "int"), ("b", "int")],
    )
    rows = df.dropDuplicates(subset=["a"]).collect()
    assert len(rows) == 2
    assert {r["a"] for r in rows} == {1, 2}

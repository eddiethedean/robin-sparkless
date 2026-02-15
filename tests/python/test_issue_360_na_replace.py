"""
Tests for #360: DataFrame.na.replace() (PySpark parity).

PySpark: df.na.replace(to_replace, value, subset=None). Robin-sparkless: df.na().replace(...) (na is a method).
"""

from __future__ import annotations

import robin_sparkless as rs


def _spark() -> rs.SparkSession:
    return rs.SparkSession.builder().app_name("issue_360").get_or_create()


def test_na_replace_issue_repro() -> None:
    """df.na.replace("a", "A", subset=["x"]).collect() (issue repro)."""
    spark = _spark()
    create_df = getattr(
        spark, "create_dataframe_from_rows", spark.createDataFrame
    )
    df = create_df(
        [{"x": "a"}, {"x": "b"}],
        [("x", "string")],
    )
    rows = df.na().replace("a", "A", subset=["x"]).collect()
    assert len(rows) == 2
    assert rows[0]["x"] == "A"
    assert rows[1]["x"] == "b"


def test_na_replace_without_subset() -> None:
    """na.replace applies to all columns when subset is None."""
    spark = _spark()
    create_df = getattr(
        spark, "create_dataframe_from_rows", spark.createDataFrame
    )
    df = create_df(
        [{"a": "x", "b": "x"}, {"a": "y", "b": "y"}],
        [("a", "string"), ("b", "string")],
    )
    rows = df.na().replace("x", "X").collect()
    assert len(rows) == 2
    assert rows[0]["a"] == "X" and rows[0]["b"] == "X"
    assert rows[1]["a"] == "y" and rows[1]["b"] == "y"


def test_na_replace_subset_one_column() -> None:
    """na.replace with subset only touches specified columns."""
    spark = _spark()
    create_df = getattr(
        spark, "create_dataframe_from_rows", spark.createDataFrame
    )
    df = create_df(
        [{"a": 1, "b": 1}, {"a": 2, "b": 1}],
        [("a", "int"), ("b", "int")],
    )
    rows = df.na().replace(1, 10, subset=["a"]).collect()
    assert rows[0]["a"] == 10 and rows[0]["b"] == 1
    assert rows[1]["a"] == 2 and rows[1]["b"] == 1


def test_na_replace_with_none() -> None:
    """na.replace can replace with None (null)."""
    spark = _spark()
    create_df = getattr(
        spark, "create_dataframe_from_rows", spark.createDataFrame
    )
    df = create_df(
        [{"x": "a"}, {"x": "b"}],
        [("x", "string")],
    )
    rows = df.na().replace("a", None, subset=["x"]).collect()
    assert len(rows) == 2
    assert rows[0]["x"] is None
    assert rows[1]["x"] == "b"

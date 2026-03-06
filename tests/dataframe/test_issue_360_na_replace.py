"""
Tests for #360: DataFrame.na.replace() (PySpark parity).

PySpark: df.na.replace(to_replace, value, subset=None). Robin-sparkless: df.na().replace(...) (na is a method).
"""

from __future__ import annotations
import pytest

@pytest.mark.skip(reason="Issue #1223: unskip when fixing")

def test_na_replace_issue_repro(spark) -> None:
    """df.na.replace(\"a\", \"A\", subset=[\"x\"]).collect() (issue repro)."""
    create_df = getattr(spark, "create_dataframe_from_rows", spark.createDataFrame)
    df = create_df(
        [{"x": "a"}, {"x": "b"}],
        ["x"],
    )
    rows = df.na.replace("a", "A", subset=["x"]).collect()
    assert len(rows) == 2
    assert rows[0]["x"] == "A"
    assert rows[1]["x"] == "b"

@pytest.mark.skip(reason="Issue #1223: unskip when fixing")

def test_na_replace_without_subset(spark) -> None:
    """na.replace applies to all columns when subset is None."""
    create_df = getattr(spark, "create_dataframe_from_rows", spark.createDataFrame)
    df = create_df(
        [{"a": "x", "b": "x"}, {"a": "y", "b": "y"}],
        ["a", "b"],
    )
    rows = df.na.replace("x", "X").collect()
    assert len(rows) == 2
    assert rows[0]["a"] == "X" and rows[0]["b"] == "X"
    assert rows[1]["a"] == "y" and rows[1]["b"] == "y"


def test_na_replace_subset_one_column(spark) -> None:
    """na.replace with subset only touches specified columns."""
    create_df = getattr(spark, "create_dataframe_from_rows", spark.createDataFrame)
    df = create_df(
        [{"a": 1, "b": 1}, {"a": 2, "b": 1}],
        ["a", "b"],
    )
    rows = df.na.replace(1, 10, subset=["a"]).collect()
    assert rows[0]["a"] == 10 and rows[0]["b"] == 1
    assert rows[1]["a"] == 2 and rows[1]["b"] == 1

@pytest.mark.skip(reason="Issue #1223: unskip when fixing")

def test_na_replace_with_none(spark) -> None:
    """na.replace can replace with None (null)."""
    create_df = getattr(spark, "create_dataframe_from_rows", spark.createDataFrame)
    df = create_df(
        [{"x": "a"}, {"x": "b"}],
        ["x"],
    )
    rows = df.na.replace("a", None, subset=["x"]).collect()
    assert len(rows) == 2
    assert rows[0]["x"] is None
    assert rows[1]["x"] == "b"

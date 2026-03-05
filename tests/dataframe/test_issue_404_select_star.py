"""Tests for #404: select("*") and select("*", col) expand to all columns (PySpark)."""

from __future__ import annotations


def test_select_star(spark) -> None:
    """df.select("*") returns all columns."""
    df = spark.createDataFrame([(1, 2, 3)], ["a", "b", "c"])
    result = df.select("*").collect()
    rows = list(result)
    assert len(rows) == 1
    # Row may be tuple or dict-like; check we have 3 values (a=1, b=2, c=3)
    row = rows[0]
    assert len(row) == 3
    assert row[0] == 1 and row[1] == 2 and row[2] == 3
    # Column set
    out_df = df.select("*")
    names = out_df.columns
    assert names == ["a", "b", "c"]


def test_select_star_plus_column(spark) -> None:
    """df.select("*", "a") expands "*" then adds the extra column (duplicate a)."""
    df = spark.createDataFrame([(10, 20)], ["a", "b"])
    result = df.select("*", "a").collect()
    rows = list(result)
    assert len(rows) == 1
    # Order: all columns then "a" again (a=10, b=20, a=10)
    row = rows[0]
    assert row[0] == 10 and row[1] == 20 and row[2] == 10
    # Second "a" may appear as last; PySpark keeps order so we expect a, b, a
    out_df = df.select("*", "a")
    names = out_df.columns
    assert "a" in names and "b" in names
    assert len(names) == 3  # a, b, a

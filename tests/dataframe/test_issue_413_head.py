"""
Tests for #413: DataFrame.head(n) — default n=1 (PySpark parity).
"""

from __future__ import annotations


def test_head_default_one_row(spark) -> None:
    """df.head() returns first row (default n=1)."""
    from sparkless.testing import get_imports

    Row = get_imports().Row
    df = spark.createDataFrame([{"x": 1}, {"x": 2}, {"x": 3}])
    row = df.head()
    assert isinstance(row, Row)
    assert row["x"] == 1
    assert row[0] == 1


def test_head_n(spark) -> None:
    """df.head(n) returns first n rows as list of Row (PySpark parity)."""
    df = spark.createDataFrame([{"x": 1}, {"x": 2}, {"x": 3}])
    rows = df.head(2)
    assert len(rows) == 2
    assert rows[0]["x"] == 1
    assert rows[1]["x"] == 2

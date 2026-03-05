"""
Tests for #413: DataFrame.head(n) — default n=1 (PySpark parity).
"""

from __future__ import annotations

from tests.utils import get_spark


def _spark():
    return get_spark("issue_413")


def test_head_default_one_row() -> None:
    """df.head() returns first row (default n=1)."""
    spark = _spark()
    df = spark.createDataFrame([{"x": 1}, {"x": 2}, {"x": 3}])
    row = df.head()
    assert row is not None
    assert row["x"] == 1


def test_head_n() -> None:
    """df.head(n) returns first n rows as list of Row (PySpark parity)."""
    spark = _spark()
    df = spark.createDataFrame([{"x": 1}, {"x": 2}, {"x": 3}])
    rows = df.head(2)
    assert len(rows) == 2
    assert rows[0]["x"] == 1
    assert rows[1]["x"] == 2


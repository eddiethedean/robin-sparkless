"""
Tests for #413: DataFrame.head(n) — default n=1 (PySpark parity).
"""

from __future__ import annotations

import robin_sparkless as rs


def _spark() -> rs.SparkSession:
    return rs.SparkSession.builder().app_name("issue_413").get_or_create()


def test_head_default_one_row() -> None:
    """df.head() returns first row (default n=1)."""
    spark = _spark()
    df = spark.createDataFrame([{"x": 1}, {"x": 2}, {"x": 3}])
    out = df.head()
    assert out is not None
    rows = out.collect()
    assert len(rows) == 1
    assert rows[0] == {"x": 1}


def test_head_n() -> None:
    """df.head(n) returns first n rows."""
    spark = _spark()
    df = spark.createDataFrame([{"x": 1}, {"x": 2}, {"x": 3}])
    out = df.head(2)
    rows = out.collect()
    assert len(rows) == 2
    assert rows[0] == {"x": 1}
    assert rows[1] == {"x": 2}

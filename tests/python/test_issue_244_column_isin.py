"""
Tests for issue #244: Column.isin() not found.

With robin-sparkless 0.7.0, col("id").isin([]) raised AttributeError.
PySpark supports col.isin([]) (empty list yields 0 rows). We now expose isin on Column.
"""

from __future__ import annotations

import robin_sparkless as rs


def test_column_isin_empty_list_returns_zero_rows() -> None:
    """col("id").isin([]) filters to 0 rows (PySpark parity)."""
    spark = rs.SparkSession.builder().app_name("isin_empty").get_or_create()
    data = [{"id": 1}, {"id": 2}, {"id": 3}]
    schema = [("id", "int")]
    df = spark._create_dataframe_from_rows(data, schema)
    out = df.filter(rs.col("id").isin([])).collect()
    assert len(out) == 0


def test_column_isin_non_empty_int_list() -> None:
    """col("id").isin([1, 3]) keeps matching rows."""
    spark = rs.SparkSession.builder().app_name("isin_int").get_or_create()
    data = [{"id": 1}, {"id": 2}, {"id": 3}]
    schema = [("id", "int")]
    df = spark._create_dataframe_from_rows(data, schema)
    out = df.filter(rs.col("id").isin([1, 3])).collect()
    assert len(out) == 2
    ids = {r["id"] for r in out}
    assert ids == {1, 3}


def test_column_isin_string_list() -> None:
    """col("name").isin(list of str) works."""
    spark = rs.SparkSession.builder().app_name("isin_str").get_or_create()
    data = [{"name": "a"}, {"name": "b"}, {"name": "c"}]
    schema = [("name", "string")]
    df = spark._create_dataframe_from_rows(data, schema)
    out = df.filter(rs.col("name").isin(["a", "c"])).collect()
    assert len(out) == 2
    names = {r["name"] for r in out}
    assert names == {"a", "c"}

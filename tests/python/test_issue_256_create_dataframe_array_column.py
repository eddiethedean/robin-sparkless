"""
Tests for issue #256: create_dataframe_from_rows accepts schema ("col", "list") and ("col", "array").

PySpark accepts DataFrame creation with list/array column data, e.g.:
  spark.createDataFrame([{"name": "a", "vals": [1,2,3]}])
Robin's create_dataframe_from_rows now accepts schema dtype "list" or "array" for parity.
"""

from __future__ import annotations

import robin_sparkless as rs


def test_create_dataframe_from_rows_schema_list() -> None:
    """create_dataframe_from_rows with ("vals", "list") succeeds and vals is list column."""
    spark = rs.SparkSession.builder().app_name("test_256").get_or_create()
    data = [{"name": "a", "vals": [1, 2, 3]}]
    schema = [("name", "string"), ("vals", "list")]
    df = spark.createDataFrame(data, schema)
    out = df.collect()
    assert len(out) == 1
    assert out[0]["name"] == "a"
    assert out[0]["vals"] == [1, 2, 3]


def test_create_dataframe_from_rows_schema_array() -> None:
    """create_dataframe_from_rows with ("vals", "array") succeeds and vals is list column."""
    spark = rs.SparkSession.builder().app_name("test_256").get_or_create()
    data = [{"name": "b", "vals": [10, 20]}]
    schema = [("name", "string"), ("vals", "array")]
    df = spark.createDataFrame(data, schema)
    out = df.collect()
    assert len(out) == 1
    assert out[0]["name"] == "b"
    assert out[0]["vals"] == [10, 20]


def test_create_dataframe_from_rows_list_null_and_multiple_rows() -> None:
    """list/array column supports null and multiple rows."""
    spark = rs.SparkSession.builder().app_name("test_256").get_or_create()
    data = [
        {"id": 1, "vals": [1, 2]},
        {"id": 2, "vals": None},
        {"id": 3, "vals": []},
    ]
    schema = [("id", "bigint"), ("vals", "list")]
    df = spark.createDataFrame(data, schema)
    out = df.collect()
    assert len(out) == 3
    assert out[0]["vals"] == [1, 2]
    assert out[1]["vals"] is None
    assert out[2]["vals"] == []

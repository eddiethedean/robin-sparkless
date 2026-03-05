"""
Tests for issue #256: create_dataframe_from_rows accepts schema ("col", "list") and ("col", "array").

PySpark accepts DataFrame creation with list/array column data, e.g.:
  spark.createDataFrame([{"name": "a", "vals": [1,2,3]}])
Robin's create_dataframe_from_rows now accepts schema dtype "list" or "array" for parity.
"""

from __future__ import annotations

from tests.utils import get_spark


def test_create_dataframe_from_rows_schema_list() -> None:
    """create_dataframe_from_rows with (\"vals\", \"list\") succeeds and vals is list column."""
    spark = get_spark("test_256")
    data = [{"name": "a", "vals": [1, 2, 3]}]
    df = spark.createDataFrame(data, "name string, vals array<int>")
    out = df.collect()
    assert len(out) == 1
    assert out[0]["name"] == "a"
    assert out[0]["vals"] == [1, 2, 3]


def test_create_dataframe_from_rows_schema_array() -> None:
    """create_dataframe_from_rows with (\"vals\", \"array\") succeeds and vals is list column."""
    spark = get_spark("test_256")
    data = [{"name": "b", "vals": [10, 20]}]
    df = spark.createDataFrame(data, "name string, vals array<int>")
    out = df.collect()
    assert len(out) == 1
    assert out[0]["name"] == "b"
    assert out[0]["vals"] == [10, 20]


def test_create_dataframe_from_rows_list_null_and_multiple_rows() -> None:
    """list/array column supports null and multiple rows."""
    spark = get_spark("test_256")
    data = [
        {"id": 1, "vals": [1, 2]},
        {"id": 2, "vals": None},
        {"id": 3, "vals": []},
    ]
    df = spark.createDataFrame(data, "id bigint, vals array<int>")
    out = df.collect()
    assert len(out) == 3
    assert out[0]["vals"] == [1, 2]
    assert out[1]["vals"] is None
    assert out[2]["vals"] == []

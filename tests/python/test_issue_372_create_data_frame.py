"""
Tests for PySpark-style createDataFrame(data, schema=None). Closes #372.
"""

from __future__ import annotations

import robin_sparkless as rs


def test_create_data_frame_list_of_dicts_schema_inferred() -> None:
    """createDataFrame([dict, ...], schema=None) infers column names and types."""
    spark = rs.SparkSession.builder().app_name("issue_372").get_or_create()
    data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
    df = spark.createDataFrame(data)
    rows = df.collect()
    assert len(rows) == 2
    assert rows[0]["name"] == "Alice" and rows[0]["age"] == 25
    assert rows[1]["name"] == "Bob" and rows[1]["age"] == 30


def test_create_data_frame_list_of_tuples_with_column_names() -> None:
    """createDataFrame([(a, b), ...], ["col1", "col2"]) uses names and infers types."""
    spark = rs.SparkSession.builder().app_name("issue_372").get_or_create()
    data = [("Alice", 25), ("Bob", 30)]
    df = spark.createDataFrame(data, ["name", "age"])
    rows = df.collect()
    assert len(rows) == 2
    assert rows[0]["name"] == "Alice" and rows[0]["age"] == 25
    assert rows[1]["name"] == "Bob" and rows[1]["age"] == 30


def test_create_data_frame_list_of_tuples_schema_inferred_default_names() -> None:
    """createDataFrame([(a, b), ...], schema=None) uses _1, _2, ... and infers types."""
    spark = rs.SparkSession.builder().app_name("issue_372").get_or_create()
    data = [("Alice", 25), ("Bob", 30)]
    df = spark.createDataFrame(data)
    rows = df.collect()
    assert len(rows) == 2
    assert rows[0]["_1"] == "Alice" and rows[0]["_2"] == 25
    assert rows[1]["_1"] == "Bob" and rows[1]["_2"] == 30


def test_create_data_frame_schema_list_of_tuples_name_type() -> None:
    """createDataFrame(data, [(name, dtype_str), ...]) uses full schema."""
    spark = rs.SparkSession.builder().app_name("issue_372").get_or_create()
    data = [("Alice", 25), ("Bob", 30)]
    schema = [("name", "string"), ("age", "bigint")]
    df = spark.createDataFrame(data, schema)
    rows = df.collect()
    assert len(rows) == 2
    assert rows[0]["name"] == "Alice" and rows[0]["age"] == 25


def test_create_data_frame_empty_data_no_schema() -> None:
    """createDataFrame([], schema=None) returns empty DataFrame."""
    spark = rs.SparkSession.builder().app_name("issue_372").get_or_create()
    df = spark.createDataFrame([])
    assert df.collect() == []


def test_create_data_frame_empty_data_with_schema() -> None:
    """createDataFrame([], schema=[names]) returns empty DataFrame with columns."""
    spark = rs.SparkSession.builder().app_name("issue_372").get_or_create()
    df = spark.createDataFrame([], ["name", "age"])
    assert df.collect() == []
    assert df.columns() == ["name", "age"]


def test_create_data_frame_dict_order_from_schema() -> None:
    """When schema is list of names, dict rows are extracted in that order."""
    spark = rs.SparkSession.builder().app_name("issue_372").get_or_create()
    data = [{"age": 25, "name": "Alice"}, {"age": 30, "name": "Bob"}]
    df = spark.createDataFrame(data, ["name", "age"])
    rows = df.collect()
    assert rows[0]["name"] == "Alice" and rows[0]["age"] == 25


def test_create_data_frame_parity_with_create_dataframe() -> None:
    """createDataFrame([(1, 25, 'Alice')], ['id', 'age', 'name']) matches create_dataframe."""
    spark = rs.SparkSession.builder().app_name("issue_372").get_or_create()
    data = [(1, 25, "Alice"), (2, 30, "Bob")]
    df1 = spark.createDataFrame(data, ["id", "age", "name"])
    df2 = spark.createDataFrame(data, ["id", "age", "name"])
    assert df1.collect() == df2.collect()

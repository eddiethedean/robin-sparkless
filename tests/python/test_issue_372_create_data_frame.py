"""
Tests for PySpark-style createDataFrame(data, schema=None, ...). Closes #372.

Covers common usage: dict/list rows, schema as None/list/DDL/StructType,
verifySchema, samplingRatio, all supported value types, edge cases, and errors.
"""

from __future__ import annotations

from datetime import date, datetime

import robin_sparkless as rs


def _spark() -> rs.SparkSession:
    return rs.SparkSession.builder().app_name("createDataFrame_tests").get_or_create()


# ---- Basic: schema inference and column names ----


def test_create_data_frame_list_of_dicts_schema_inferred() -> None:
    """createDataFrame([dict, ...], schema=None) infers column names and types."""
    spark = _spark()
    data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
    df = spark.createDataFrame(data)
    rows = df.collect()
    assert len(rows) == 2
    assert rows[0]["name"] == "Alice" and rows[0]["age"] == 25
    assert rows[1]["name"] == "Bob" and rows[1]["age"] == 30


def test_create_data_frame_list_of_tuples_with_column_names() -> None:
    """createDataFrame([(a, b), ...], ["col1", "col2"]) uses names and infers types."""
    spark = _spark()
    data = [("Alice", 25), ("Bob", 30)]
    df = spark.createDataFrame(data, ["name", "age"])
    rows = df.collect()
    assert len(rows) == 2
    assert rows[0]["name"] == "Alice" and rows[0]["age"] == 25
    assert rows[1]["name"] == "Bob" and rows[1]["age"] == 30


def test_create_data_frame_list_of_tuples_schema_inferred_default_names() -> None:
    """createDataFrame([(a, b), ...], schema=None) uses _1, _2, ... and infers types."""
    spark = _spark()
    data = [("Alice", 25), ("Bob", 30)]
    df = spark.createDataFrame(data)
    rows = df.collect()
    assert len(rows) == 2
    assert rows[0]["_1"] == "Alice" and rows[0]["_2"] == 25
    assert rows[1]["_1"] == "Bob" and rows[1]["_2"] == 30


def test_create_data_frame_schema_list_of_tuples_name_type() -> None:
    """createDataFrame(data, [(name, dtype_str), ...]) uses full schema."""
    spark = _spark()
    data = [("Alice", 25), ("Bob", 30)]
    schema = [("name", "string"), ("age", "bigint")]
    df = spark.createDataFrame(data, schema)
    rows = df.collect()
    assert len(rows) == 2
    assert rows[0]["name"] == "Alice" and rows[0]["age"] == 25


def test_create_data_frame_empty_data_no_schema() -> None:
    """createDataFrame([], schema=None) returns empty DataFrame."""
    spark = _spark()
    df = spark.createDataFrame([])
    assert df.collect() == []


def test_create_data_frame_empty_data_with_schema() -> None:
    """createDataFrame([], schema=[names]) returns empty DataFrame with columns."""
    spark = _spark()
    df = spark.createDataFrame([], ["name", "age"])
    assert df.collect() == []
    assert df.columns() == ["name", "age"]


def test_create_data_frame_dict_order_from_schema() -> None:
    """When schema is list of names, dict rows are extracted in that order."""
    spark = _spark()
    data = [{"age": 25, "name": "Alice"}, {"age": 30, "name": "Bob"}]
    df = spark.createDataFrame(data, ["name", "age"])
    rows = df.collect()
    assert rows[0]["name"] == "Alice" and rows[0]["age"] == 25


def test_create_data_frame_tuples_with_column_names() -> None:
    """createDataFrame([(1, 25, 'Alice'), ...], ['id', 'age', 'name']) returns expected rows."""
    spark = _spark()
    data = [(1, 25, "Alice"), (2, 30, "Bob")]
    df = spark.createDataFrame(data, ["id", "age", "name"])
    assert df.collect() == [
        {"id": 1, "age": 25, "name": "Alice"},
        {"id": 2, "age": 30, "name": "Bob"},
    ]


# ---- DDL schema (PySpark "name: string, age: int") ----


def test_create_data_frame_ddl_schema_with_data() -> None:
    """createDataFrame(data, "name: string, age: int") uses DDL schema."""
    spark = _spark()
    data = [("Alice", 25), ("Bob", 30)]
    df = spark.createDataFrame(data, "name: string, age: int")
    rows = df.collect()
    assert len(rows) == 2
    assert rows[0]["name"] == "Alice" and rows[0]["age"] == 25
    assert rows[1]["name"] == "Bob" and rows[1]["age"] == 30


def test_create_data_frame_ddl_schema_space_separated() -> None:
    """DDL with space between name and type (name string, age int) works."""
    spark = _spark()
    data = [("x", 1)]
    df = spark.createDataFrame(data, "a string, b int")
    rows = df.collect()
    assert rows[0]["a"] == "x" and rows[0]["b"] == 1


def test_create_data_frame_empty_data_with_ddl_schema() -> None:
    """createDataFrame([], "name: string, age: int") returns empty DataFrame with columns."""
    spark = _spark()
    df = spark.createDataFrame([], "name: string, age: int")
    assert df.collect() == []
    assert df.columns() == ["name", "age"]


# ---- Optional args: samplingRatio, verifySchema ----


def test_create_data_frame_accepts_sampling_ratio_none() -> None:
    """createDataFrame(..., sampling_ratio=None) succeeds (ignored for list data)."""
    spark = _spark()
    data = [("a", 1)]
    df = spark.createDataFrame(data, ["x", "y"], sampling_ratio=None)
    assert df.collect() == [{"x": "a", "y": 1}]


def test_create_data_frame_accepts_verify_schema_true() -> None:
    """createDataFrame(..., verify_schema=True) succeeds."""
    spark = _spark()
    data = [("Alice", 25)]
    df = spark.createDataFrame(data, ["name", "age"], verify_schema=True)
    assert df.collect() == [{"name": "Alice", "age": 25}]


def test_create_data_frame_accepts_verify_schema_false() -> None:
    """createDataFrame(..., verify_schema=False) succeeds."""
    spark = _spark()
    data = [("Bob", 30)]
    df = spark.createDataFrame(data, ["name", "age"], verify_schema=False)
    assert df.collect() == [{"name": "Bob", "age": 30}]


# ---- Single row ----


def test_create_data_frame_single_row_dict() -> None:
    """createDataFrame([{...}]) with single dict row works."""
    spark = _spark()
    df = spark.createDataFrame([{"id": 1, "name": "Only"}])
    assert df.collect() == [{"id": 1, "name": "Only"}]


def test_create_data_frame_single_row_list() -> None:
    """createDataFrame([(a, b)]) with single list/tuple row works."""
    spark = _spark()
    df = spark.createDataFrame([(10, "x")], ["num", "label"])
    assert df.collect() == [{"num": 10, "label": "x"}]


# ---- All supported value types ----


def test_create_data_frame_types_none_int_float_bool_str() -> None:
    """createDataFrame with None, int, float, bool, str in rows."""
    spark = _spark()
    data = [
        {"a": None, "b": 42, "c": 3.14, "d": True, "e": "hello"},
        {"a": 1, "b": None, "c": None, "d": False, "e": ""},
    ]
    df = spark.createDataFrame(data)
    rows = df.collect()
    assert rows[0]["a"] is None and rows[0]["b"] == 42 and rows[0]["c"] == 3.14
    assert rows[0]["d"] is True and rows[0]["e"] == "hello"
    assert rows[1]["a"] == 1 and rows[1]["b"] is None and rows[1]["e"] == ""


def test_create_data_frame_types_date_and_datetime() -> None:
    """createDataFrame with datetime.date and datetime.datetime in rows."""
    spark = _spark()
    data = [
        {"id": 1, "d": date(2025, 2, 10), "ts": datetime(2025, 2, 10, 12, 0, 0)},
        {"id": 2, "d": None, "ts": None},
    ]
    schema = [("id", "bigint"), ("d", "date"), ("ts", "timestamp")]
    df = spark.createDataFrame(data, schema)
    rows = df.collect()
    assert len(rows) == 2
    assert rows[0]["id"] == 1 and rows[0]["d"] is not None and rows[0]["ts"] is not None
    assert rows[1]["id"] == 2 and rows[1]["d"] is None and rows[1]["ts"] is None


def test_create_data_frame_types_list_and_struct() -> None:
    """createDataFrame with list (array) and dict (struct) in rows."""
    spark = _spark()
    data = [
        {"id": 1, "arr": [1, 2, 3], "nested": {"k": "v"}},
        {"id": 2, "arr": [], "nested": None},
    ]
    schema = [
        ("id", "bigint"),
        ("arr", "array<bigint>"),
        ("nested", "struct<k:string>"),
    ]
    df = spark.createDataFrame(data, schema)
    rows = df.collect()
    assert len(rows) == 2
    assert (
        rows[0]["id"] == 1
        and rows[0]["arr"] == [1, 2, 3]
        and rows[0]["nested"] == {"k": "v"}
    )
    assert rows[1]["id"] == 2 and rows[1]["arr"] == []


# ---- Dict rows: missing key, duplicate key order ----


def test_create_data_frame_dict_row_missing_key_fills_none() -> None:
    """Dict row missing a key yields None for that column when schema provided."""
    spark = _spark()
    data = [{"name": "Alice", "age": 25}, {"name": "Bob"}]
    df = spark.createDataFrame(data, ["name", "age"])
    rows = df.collect()
    assert rows[0]["name"] == "Alice" and rows[0]["age"] == 25
    assert rows[1]["name"] == "Bob" and rows[1]["age"] is None


def test_create_data_frame_dict_column_order_from_schema_not_insertion() -> None:
    """When schema is list of names, column order follows schema not dict insertion."""
    spark = _spark()
    data = [{"z": 3, "a": 1, "m": 2}]
    df = spark.createDataFrame(data, ["a", "m", "z"])
    row = df.collect()[0]
    assert list(row.keys()) == ["a", "m", "z"]
    assert row["a"] == 1 and row["m"] == 2 and row["z"] == 3


# ---- Many columns and unicode ----


def test_create_data_frame_many_columns() -> None:
    """createDataFrame with many columns (e.g. 15) works."""
    spark = _spark()
    n = 15
    names = [f"c{i}" for i in range(n)]
    row = tuple(i for i in range(n))
    df = spark.createDataFrame([row], names)
    out = df.collect()[0]
    for i in range(n):
        assert out[names[i]] == i


def test_create_data_frame_unicode_column_names() -> None:
    """createDataFrame with unicode in column names works."""
    spark = _spark()
    data = [{"name": "Alice", "âge": 25}]
    df = spark.createDataFrame(data, ["name", "âge"])
    assert df.collect() == [{"name": "Alice", "âge": 25}]


# ---- Error cases ----


def test_create_data_frame_invalid_row_type_raises() -> None:
    """Row that is not dict or list/tuple raises TypeError."""
    spark = _spark()
    data = [{"a": 1}, 42]
    try:
        spark.createDataFrame(data)
        assert False, "expected TypeError"
    except (TypeError, Exception) as e:
        assert (
            "dict" in str(e).lower()
            or "list" in str(e).lower()
            or "tuple" in str(e).lower()
        )


def test_create_data_frame_mixed_dict_and_list_rows_raises() -> None:
    """First row dict, second row list raises TypeError (all must be same shape)."""
    spark = _spark()
    data = [{"a": 1}, (2,)]
    try:
        spark.createDataFrame(data)
        assert False, "expected TypeError"
    except TypeError:
        pass


def test_create_data_frame_invalid_schema_type_raises() -> None:
    """Schema that is not None, str, list, or StructType raises TypeError."""
    spark = _spark()
    data = [("a", 1)]
    try:
        spark.createDataFrame(data, schema=123)
        assert False, "expected TypeError"
    except TypeError:
        pass

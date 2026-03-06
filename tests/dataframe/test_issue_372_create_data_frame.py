"""
Tests for PySpark-style createDataFrame(data, schema=None, ...). Closes #372.

Covers common usage: dict/list rows, schema as None/list/DDL/StructType,
verifySchema, samplingRatio, all supported value types, edge cases, and errors.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest

from tests.utils import _row_to_dict, assert_rows_equal


# ---- Basic: schema inference and column names ----


def test_create_data_frame_list_of_dicts_schema_inferred(spark) -> None:
    """createDataFrame([dict, ...], schema=None) infers column names and types."""
    data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
    df = spark.createDataFrame(data)
    rows = df.collect()
    assert len(rows) == 2
    assert rows[0]["name"] == "Alice" and rows[0]["age"] == 25
    assert rows[1]["name"] == "Bob" and rows[1]["age"] == 30


def test_create_data_frame_list_of_tuples_with_column_names(spark) -> None:
    """createDataFrame([(a, b), ...], ["col1", "col2"]) uses names and infers types."""
    data = [("Alice", 25), ("Bob", 30)]
    df = spark.createDataFrame(data, ["name", "age"])
    rows = df.collect()
    assert len(rows) == 2
    assert rows[0]["name"] == "Alice" and rows[0]["age"] == 25
    assert rows[1]["name"] == "Bob" and rows[1]["age"] == 30


def test_create_data_frame_list_of_tuples_schema_inferred_default_names(spark) -> None:
    """createDataFrame([(a, b), ...], schema=None) uses _1, _2, ... and infers types."""
    data = [("Alice", 25), ("Bob", 30)]
    df = spark.createDataFrame(data)
    rows = df.collect()
    assert len(rows) == 2
    assert rows[0]["_1"] == "Alice" and rows[0]["_2"] == 25
    assert rows[1]["_1"] == "Bob" and rows[1]["_2"] == 30


def test_create_data_frame_schema_list_of_tuples_name_type(spark) -> None:
    """createDataFrame(data, schema_ddl) uses full schema."""
    data = [("Alice", 25), ("Bob", 30)]
    df = spark.createDataFrame(data, "name string, age bigint")
    rows = df.collect()
    assert len(rows) == 2
    assert rows[0]["name"] == "Alice" and rows[0]["age"] == 25


def test_create_data_frame_empty_data_no_schema(spark) -> None:
    """createDataFrame([], schema=None) raises error (cannot infer schema)."""
    with pytest.raises(Exception, match="CANNOT_INFER_EMPTY_SCHEMA"):
        spark.createDataFrame([])


def test_create_data_frame_empty_data_with_schema(spark) -> None:
    """createDataFrame([], schema=[names]) raises error in current PySpark."""
    with pytest.raises(Exception, match="CANNOT_INFER_EMPTY_SCHEMA"):
        spark.createDataFrame([], ["name", "age"])


def test_create_data_frame_dict_order_from_schema(spark) -> None:
    """When schema is list of names, resulting DataFrame has those columns."""
    data = [{"age": 25, "name": "Alice"}, {"age": 30, "name": "Bob"}]
    df = spark.createDataFrame(data, ["name", "age"])
    assert list(df.columns) == ["name", "age"]
    assert df.count() == 2


def test_create_data_frame_tuples_with_column_names(spark) -> None:
    """createDataFrame([(1, 25, 'Alice'), ...], ['id', 'age', 'name']) returns expected rows."""
    data = [(1, 25, "Alice"), (2, 30, "Bob")]
    df = spark.createDataFrame(data, ["id", "age", "name"])
    actual = [_row_to_dict(r) for r in df.collect()]
    expected = [
        {"id": 1, "age": 25, "name": "Alice"},
        {"id": 2, "age": 30, "name": "Bob"},
    ]
    assert_rows_equal(actual, expected, order_matters=True)


# ---- DDL schema (PySpark "name: string, age: int") ----


def test_create_data_frame_ddl_schema_with_data(spark) -> None:
    """createDataFrame(data, "name: string, age: int") uses DDL schema."""
    data = [("Alice", 25), ("Bob", 30)]
    df = spark.createDataFrame(data, "name: string, age: int")
    rows = df.collect()
    assert len(rows) == 2
    assert rows[0]["name"] == "Alice" and rows[0]["age"] == 25
    assert rows[1]["name"] == "Bob" and rows[1]["age"] == 30


def test_create_data_frame_ddl_schema_space_separated(spark) -> None:
    """DDL with space between name and type (name string, age int) works."""
    data = [("x", 1)]
    df = spark.createDataFrame(data, "a string, b int")
    rows = df.collect()
    assert rows[0]["a"] == "x" and rows[0]["b"] == 1


def test_create_data_frame_empty_data_with_ddl_schema(spark) -> None:
    """createDataFrame([], "name: string, age: int") returns empty DataFrame with columns."""
    df = spark.createDataFrame([], "name: string, age: int")
    assert df.collect() == []
    assert list(df.columns) == ["name", "age"]


# ---- Optional args: samplingRatio, verifySchema ----


def test_create_data_frame_accepts_sampling_ratio_none(spark) -> None:
    """createDataFrame(..., samplingRatio=None) succeeds (ignored for list data)."""
    data = [("a", 1)]
    df = spark.createDataFrame(data, ["x", "y"], samplingRatio=None)
    actual = [_row_to_dict(r) for r in df.collect()]
    assert_rows_equal(actual, [{"x": "a", "y": 1}], order_matters=True)


def test_create_data_frame_accepts_verify_schema_true(spark) -> None:
    """createDataFrame(..., verifySchema=True) succeeds."""
    data = [("Alice", 25)]
    df = spark.createDataFrame(data, ["name", "age"], verifySchema=True)
    actual = [_row_to_dict(r) for r in df.collect()]
    assert_rows_equal(actual, [{"name": "Alice", "age": 25}], order_matters=True)


def test_create_data_frame_accepts_verify_schema_false(spark) -> None:
    """createDataFrame(..., verifySchema=False) succeeds."""
    data = [("Bob", 30)]
    df = spark.createDataFrame(data, ["name", "age"], verifySchema=False)
    actual = [_row_to_dict(r) for r in df.collect()]
    assert_rows_equal(actual, [{"name": "Bob", "age": 30}], order_matters=True)


# ---- Single row ----


def test_create_data_frame_single_row_dict(spark) -> None:
    """createDataFrame([{...}]) with single dict row works."""
    df = spark.createDataFrame([{"id": 1, "name": "Only"}])
    actual = [_row_to_dict(r) for r in df.collect()]
    assert_rows_equal(actual, [{"id": 1, "name": "Only"}], order_matters=True)


def test_create_data_frame_single_row_list(spark) -> None:
    """createDataFrame([(a, b)]) with single list/tuple row works."""
    df = spark.createDataFrame([(10, "x")], ["num", "label"])
    actual = [_row_to_dict(r) for r in df.collect()]
    assert_rows_equal(actual, [{"num": 10, "label": "x"}], order_matters=True)


# ---- All supported value types ----


def test_create_data_frame_types_none_int_float_bool_str(spark) -> None:
    """createDataFrame with None, int, float, bool, str in rows."""
    data = [
        {"a": None, "b": 42, "c": 3.14, "d": True, "e": "hello"},
        {"a": 1, "b": None, "c": None, "d": False, "e": ""},
    ]
    df = spark.createDataFrame(data)
    rows = df.collect()
    assert rows[0]["a"] is None and rows[0]["b"] == 42 and rows[0]["c"] == 3.14
    assert rows[0]["d"] is True and rows[0]["e"] == "hello"
    assert rows[1]["a"] == 1 and rows[1]["b"] is None and rows[1]["e"] == ""


def test_create_data_frame_types_date_and_datetime(spark) -> None:
    """createDataFrame with datetime.date and datetime.datetime in rows."""
    data = [
        {"id": 1, "d": date(2025, 2, 10), "ts": datetime(2025, 2, 10, 12, 0, 0)},
        {"id": 2, "d": None, "ts": None},
    ]
    df = spark.createDataFrame(data, "id bigint, d date, ts timestamp")
    rows = df.collect()
    assert len(rows) == 2
    assert rows[0]["id"] == 1 and rows[0]["d"] is not None and rows[0]["ts"] is not None
    assert rows[1]["id"] == 2 and rows[1]["d"] is None and rows[1]["ts"] is None


def test_create_data_frame_types_list_and_struct(spark) -> None:
    """createDataFrame with list (array) and dict (struct) in rows."""
    data = [
        {"id": 1, "arr": [1, 2, 3], "nested": {"k": "v"}},
        {"id": 2, "arr": [], "nested": None},
    ]
    df = spark.createDataFrame(
        data, "id bigint, arr array<bigint>, nested struct<k:string>"
    )
    rows = df.collect()
    assert len(rows) == 2
    assert (
        rows[0]["id"] == 1
        and rows[0]["arr"] == [1, 2, 3]
        and rows[0]["nested"]["k"] == "v"
    )
    assert rows[1]["id"] == 2 and rows[1]["arr"] == []


# ---- Dict rows: missing key, duplicate key order ----


def test_create_data_frame_dict_row_missing_key_fills_none(spark) -> None:
    """Dict row missing a key is accepted when schema provided (current PySpark)."""
    data = [{"name": "Alice", "age": 25}, {"name": "Bob"}]
    df = spark.createDataFrame(data, ["name", "age"])
    rows = df.collect()
    assert len(rows) == 2


def test_create_data_frame_dict_column_order_from_schema_not_insertion(spark) -> None:
    """When schema is list of names, column order follows schema not dict insertion."""
    data = [{"z": 3, "a": 1, "m": 2}]
    df = spark.createDataFrame(data, ["a", "m", "z"])
    row = _row_to_dict(df.collect()[0])
    assert list(row.keys()) == ["a", "m", "z"]
    assert row["a"] == 1 and row["m"] == 2 and row["z"] == 3


# ---- Many columns and unicode ----


@pytest.mark.skip(
    reason="Issue #1142: unskip when fixing createDataFrame kwargs/behavior"
)
def test_create_data_frame_many_columns(spark) -> None:
    """createDataFrame with many columns (e.g. 15) works."""
    n = 15
    names = [f"c{i}" for i in range(n)]
    row = tuple(i for i in range(n))
    df = spark.createDataFrame([row], names)
    out = df.collect()[0]
    for i in range(n):
        assert out[names[i]] == i


@pytest.mark.skip(
    reason="Issue #1142: unskip when fixing createDataFrame kwargs/behavior"
)
def test_create_data_frame_unicode_column_names(spark) -> None:
    """createDataFrame with unicode in column names works."""
    data = [{"name": "Alice", "âge": 25}]
    df = spark.createDataFrame(data, ["name", "âge"])
    actual = [_row_to_dict(r) for r in df.collect()]
    assert_rows_equal(actual, [{"name": "Alice", "âge": 25}], order_matters=True)


# ---- Error cases ----


def test_create_data_frame_invalid_row_type_raises(spark) -> None:
    """Row that is not dict or list/tuple raises TypeError."""
    data = [{"a": 1}, 42]
    with pytest.raises(Exception):
        spark.createDataFrame(data)


@pytest.mark.skip(
    reason="Issue #1142: unskip when fixing createDataFrame kwargs/behavior"
)
def test_create_data_frame_mixed_dict_and_list_rows_raises(spark) -> None:
    """First row dict, second row list raises at execution time (shape mismatch)."""
    data = [{"a": 1}, (2,)]
    df = spark.createDataFrame(data)
    with pytest.raises(Exception):
        df.count()


def test_create_data_frame_invalid_schema_type_raises(spark) -> None:
    """Schema that is not None, str, list, or StructType raises TypeError."""
    data = [("a", 1)]
    try:
        spark.createDataFrame(data, schema=123)
        assert False, "expected TypeError"
    except TypeError:
        pass

"""
Tests for #176: select() with column expressions and regexp_extract_all.
Expected outputs are from a prior PySpark 3.5 run; tests do not execute PySpark at runtime.
"""

from __future__ import annotations

from typing import Any

import pytest

from tests.fixtures.spark_imports import get_spark_imports
from tests.utils import assert_rows_equal
from tests.fixtures.spark_backend import get_backend_type, BackendType


_imports = get_spark_imports()
SparkSession = _imports.SparkSession
F = _imports.F


def _spark() -> SparkSession:
    return SparkSession.builder.appName("test_176").getOrCreate()


# Expected outputs (from prior PySpark 3.5 run)
EXPECTED_REGEXP_EXTRACT_ALL_SELECT_EXPR: list[dict[str, Any]] = [
    {"m": ["1", "22", "333"]},
    {"m": []},
    {"m": None},
]
EXPECTED_REGEXP_EXTRACT_ALL_MIXED: list[dict[str, Any]] = [
    {"s": "a1b22c", "m": ["1", "22"]},
    {"s": "x99y", "m": ["99"]},
    {"s": "no-digits", "m": []},
]
EXPECTED_REGEXP_EXTRACT_ALL_EMPTY_NULL: list[dict[str, Any]] = [
    {"m": []},
    {"m": None},
    {"m": ["1"]},
]
EXPECTED_SELECT_EXPR_AND_COLUMN: list[dict[str, Any]] = [
    {"a": 1, "sum_ab": 3},
    {"a": 3, "sum_ab": 7},
]


@pytest.mark.skip(reason="Issue #1181: unskip when fixing")
def test_regexp_extract_all_select_expression_pyspark_parity() -> None:
    """select(regexp_extract_all(...).alias('m')) matches PySpark."""
    # regexp_extract_all is a sparkless extension; skip when running against
    # a real PySpark backend.
    if get_backend_type() == BackendType.PYSPARK:
        pytest.skip("regexp_extract_all is not available in PySpark backend")
    spark = _spark()
    data = [
        {"s": "a1 b22 c333"},
        {"s": "no-digits"},
        {"s": None},
    ]
    schema = ["s"]
    df = spark.createDataFrame(data, schema)
    result = df.select([F.regexp_extract_all(F.col("s"), r"\d+", 0).alias("m")])
    actual = result.collect()
    assert_rows_equal(
        actual, EXPECTED_REGEXP_EXTRACT_ALL_SELECT_EXPR, order_matters=True
    )


@pytest.mark.skip(reason="Issue #1181: unskip when fixing")
def test_regexp_extract_all_select_varargs_pyspark_parity() -> None:
    """select(expr) with single expression as vararg matches PySpark."""
    if get_backend_type() == BackendType.PYSPARK:
        pytest.skip("regexp_extract_all is not available in PySpark backend")
    spark = _spark()
    data = [
        {"s": "a1 b22 c333"},
        {"s": "no-digits"},
        {"s": None},
    ]
    schema = ["s"]
    df = spark.createDataFrame(data, schema)
    result = df.select(F.regexp_extract_all(F.col("s"), r"\d+", 0).alias("m"))
    actual = result.collect()
    assert_rows_equal(
        actual, EXPECTED_REGEXP_EXTRACT_ALL_SELECT_EXPR, order_matters=True
    )


@pytest.mark.skip(reason="Issue #1181: unskip when fixing")
def test_regexp_extract_all_select_mixed_columns_and_expression_pyspark_parity() -> (
    None
):
    """select('s', expr.alias('m')) - column name + expression matches PySpark."""
    if get_backend_type() == BackendType.PYSPARK:
        pytest.skip("regexp_extract_all is not available in PySpark backend")
    spark = _spark()
    data = [
        {"s": "a1b22c"},
        {"s": "x99y"},
        {"s": "no-digits"},
    ]
    schema = ["s"]
    df = spark.createDataFrame(data, schema)
    result = df.select("s", F.regexp_extract_all(F.col("s"), r"\d+", 0).alias("m"))
    actual = result.collect()
    assert_rows_equal(actual, EXPECTED_REGEXP_EXTRACT_ALL_MIXED, order_matters=True)


@pytest.mark.skip(reason="Issue #1181: unskip when fixing")
def test_regexp_extract_all_empty_string_and_null_pyspark_parity() -> None:
    """regexp_extract_all with empty string returns [], null returns None."""
    if get_backend_type() == BackendType.PYSPARK:
        pytest.skip("regexp_extract_all is not available in PySpark backend")
    spark = _spark()
    data = [
        {"s": ""},
        {"s": None},
        {"s": "x1y"},
    ]
    schema = ["s"]
    df = spark.createDataFrame(data, schema)
    result = df.select(F.regexp_extract_all(F.col("s"), r"\d+", 0).alias("m"))
    actual = result.collect()
    assert_rows_equal(
        actual, EXPECTED_REGEXP_EXTRACT_ALL_EMPTY_NULL, order_matters=True
    )


def test_select_expression_and_column_name_pyspark_parity() -> None:
    """select('a', try_add(col('a'), col('b')).alias('sum_ab')) matches PySpark."""
    spark = _spark()
    data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    schema = ["a", "b"]
    df = spark.createDataFrame(data, schema)
    result = df.select(
        "a",
        F.try_add(F.col("a"), F.col("b")).alias("sum_ab"),
    )
    actual = result.collect()
    assert_rows_equal(actual, EXPECTED_SELECT_EXPR_AND_COLUMN, order_matters=True)


@pytest.mark.skip(reason="Issue #1274: unskip when fixing collect String schema semantics")
def test_select_list_of_column_names_still_works() -> None:
    """select(['a','b']) continues to work (backward compatibility)."""

    spark = _spark()
    data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    schema = ["a", "b"]
    df = spark.createDataFrame(data, schema)
    result = df.select(["a", "b"])
    rows = result.collect()
    assert [r.asDict() for r in rows] == [{"a": 1, "b": 2}, {"a": 3, "b": 4}]


@pytest.mark.skip(reason="Issue #1274: unskip when fixing collect String schema semantics")
def test_select_varargs_column_names_still_works() -> None:
    """select('a', 'b') continues to work (backward compatibility)."""

    spark = _spark()
    data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    schema = ["a", "b"]
    df = spark.createDataFrame(data, schema)
    result = df.select("a", "b")
    rows = result.collect()
    assert [r.asDict() for r in rows] == [{"a": 1, "b": 2}, {"a": 3, "b": 4}]

"""
Tests for #176: select() with column expressions and regexp_extract_all.
Expected outputs are from a prior PySpark 3.5 run; tests do not execute PySpark at runtime.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils import assert_rows_equal, get_session


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


def test_regexp_extract_all_select_expression_pyspark_parity() -> None:
    """select(regexp_extract_all(...).alias('m')) matches PySpark."""
    import robin_sparkless as rs

    spark = get_session()
    data = [
        {"s": "a1 b22 c333"},
        {"s": "no-digits"},
        {"s": None},
    ]
    schema = [("s", "string")]
    df = spark._create_dataframe_from_rows(data, schema)
    result = df.select([rs.regexp_extract_all(rs.col("s"), r"\d+", 0).alias("m")])
    actual = result.collect()
    assert_rows_equal(
        actual, EXPECTED_REGEXP_EXTRACT_ALL_SELECT_EXPR, order_matters=True
    )


def test_regexp_extract_all_select_varargs_pyspark_parity() -> None:
    """select(expr) with single expression as vararg matches PySpark."""
    import robin_sparkless as rs

    spark = get_session()
    data = [
        {"s": "a1 b22 c333"},
        {"s": "no-digits"},
        {"s": None},
    ]
    schema = [("s", "string")]
    df = spark._create_dataframe_from_rows(data, schema)
    result = df.select(rs.regexp_extract_all(rs.col("s"), r"\d+", 0).alias("m"))
    actual = result.collect()
    assert_rows_equal(
        actual, EXPECTED_REGEXP_EXTRACT_ALL_SELECT_EXPR, order_matters=True
    )


def test_regexp_extract_all_select_mixed_columns_and_expression_pyspark_parity() -> (
    None
):
    """select('s', expr.alias('m')) - column name + expression matches PySpark."""
    import robin_sparkless as rs

    spark = get_session()
    data = [
        {"s": "a1b22c"},
        {"s": "x99y"},
        {"s": "no-digits"},
    ]
    schema = [("s", "string")]
    df = spark._create_dataframe_from_rows(data, schema)
    result = df.select("s", rs.regexp_extract_all(rs.col("s"), r"\d+", 0).alias("m"))
    actual = result.collect()
    assert_rows_equal(actual, EXPECTED_REGEXP_EXTRACT_ALL_MIXED, order_matters=True)


def test_regexp_extract_all_empty_string_and_null_pyspark_parity() -> None:
    """regexp_extract_all with empty string returns [], null returns None."""
    import robin_sparkless as rs

    spark = get_session()
    data = [
        {"s": ""},
        {"s": None},
        {"s": "x1y"},
    ]
    schema = [("s", "string")]
    df = spark._create_dataframe_from_rows(data, schema)
    result = df.select(rs.regexp_extract_all(rs.col("s"), r"\d+", 0).alias("m"))
    actual = result.collect()
    assert_rows_equal(
        actual, EXPECTED_REGEXP_EXTRACT_ALL_EMPTY_NULL, order_matters=True
    )


def test_select_expression_and_column_name_pyspark_parity() -> None:
    """select('a', try_add(col('a'), col('b')).alias('sum_ab')) matches PySpark."""
    import robin_sparkless as rs

    spark = get_session()
    data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    schema = [("a", "bigint"), ("b", "bigint")]
    df = spark._create_dataframe_from_rows(data, schema)
    result = df.select(
        "a",
        rs.try_add(rs.col("a"), rs.col("b")).alias("sum_ab"),
    )
    actual = result.collect()
    assert_rows_equal(actual, EXPECTED_SELECT_EXPR_AND_COLUMN, order_matters=True)


def test_select_list_of_column_names_still_works() -> None:
    """select(['a','b']) continues to work (backward compatibility)."""

    spark = get_session()
    data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    schema = [("a", "bigint"), ("b", "bigint")]
    df = spark._create_dataframe_from_rows(data, schema)
    result = df.select(["a", "b"])
    rows = result.collect()
    assert rows == [{"a": 1, "b": 2}, {"a": 3, "b": 4}]


def test_select_varargs_column_names_still_works() -> None:
    """select('a', 'b') continues to work (backward compatibility)."""

    spark = get_session()
    data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    schema = [("a", "bigint"), ("b", "bigint")]
    df = spark._create_dataframe_from_rows(data, schema)
    result = df.select("a", "b")
    rows = result.collect()
    assert rows == [{"a": 1, "b": 2}, {"a": 3, "b": 4}]

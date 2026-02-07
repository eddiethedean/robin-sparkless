"""
Robust tests for issue #176: select() with column expressions and regexp_extract_all.

These tests use PySpark expectations: when PySpark is available (pip install pyspark,
Java 17+), we run the same operations in both engines and compare. When PySpark is
unavailable, we use predetermined expected output derived from PySpark 3.5.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils import assert_rows_equal, get_session, run_with_pyspark_expected


def _pyspark_regexp_extract_all_select_expr(spark, F):
    """PySpark: df.select(F.regexp_extract_all(col('s'), r'\\d+', 0).alias('m'))."""
    data = [
        {"s": "a1 b22 c333"},
        {"s": "no-digits"},
        {"s": None},
    ]
    df = spark.createDataFrame([(r["s"],) for r in data], ["s"])
    result = df.select(F.regexp_extract_all("s", r"\d+", 0).alias("m"))
    return result.collect()


def _pyspark_regexp_extract_all_with_mixed_select(spark, F):
    """PySpark: select original column + regexp_extract_all expression."""
    data = [
        {"s": "a1b22c"},
        {"s": "x99y"},
        {"s": "no-digits"},
    ]
    df = spark.createDataFrame([(r["s"],) for r in data], ["s"])
    result = df.select("s", F.regexp_extract_all("s", r"\d+", 0).alias("m"))
    return result.collect()


def _pyspark_regexp_extract_all_empty_and_null(spark, F):
    """PySpark: regexp_extract_all with empty string and null."""
    data = [
        {"s": ""},
        {"s": None},
        {"s": "x1y"},
    ]
    df = spark.createDataFrame([(r["s"],) for r in data], ["s"])
    result = df.select(F.regexp_extract_all("s", r"\d+", 0).alias("m"))
    return result.collect()


def _pyspark_select_expression_and_column_name(spark, F):
    """PySpark: select with both column name and expression."""
    data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    df = spark.createDataFrame([(r["a"], r["b"]) for r in data], ["a", "b"])
    result = df.select("a", (F.col("a") + F.col("b")).alias("sum_ab"))
    return result.collect()


# Predetermined expected (from PySpark 3.5) for when PySpark is unavailable
FALLBACK_REGEXP_EXTRACT_ALL_SELECT_EXPR: list[dict[str, Any]] = [
    {"m": ["1", "22", "333"]},
    {"m": []},
    {"m": None},
]
FALLBACK_REGEXP_EXTRACT_ALL_MIXED: list[dict[str, Any]] = [
    {"s": "a1b22c", "m": ["1", "22"]},
    {"s": "x99y", "m": ["99"]},
    {"s": "no-digits", "m": []},
]
FALLBACK_REGEXP_EXTRACT_ALL_EMPTY_NULL: list[dict[str, Any]] = [
    {"m": []},
    {"m": None},
    {"m": ["1"]},
]
FALLBACK_SELECT_EXPR_AND_COLUMN: list[dict[str, Any]] = [
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

    expected = run_with_pyspark_expected(
        _pyspark_regexp_extract_all_select_expr,
        FALLBACK_REGEXP_EXTRACT_ALL_SELECT_EXPR,
    )
    assert_rows_equal(actual, expected, order_matters=True)


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

    expected = run_with_pyspark_expected(
        _pyspark_regexp_extract_all_select_expr,
        FALLBACK_REGEXP_EXTRACT_ALL_SELECT_EXPR,
    )
    assert_rows_equal(actual, expected, order_matters=True)


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

    expected = run_with_pyspark_expected(
        _pyspark_regexp_extract_all_with_mixed_select,
        FALLBACK_REGEXP_EXTRACT_ALL_MIXED,
    )
    assert_rows_equal(actual, expected, order_matters=True)


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

    expected = run_with_pyspark_expected(
        _pyspark_regexp_extract_all_empty_and_null,
        FALLBACK_REGEXP_EXTRACT_ALL_EMPTY_NULL,
    )
    assert_rows_equal(actual, expected, order_matters=True)


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

    expected = run_with_pyspark_expected(
        _pyspark_select_expression_and_column_name,
        FALLBACK_SELECT_EXPR_AND_COLUMN,
    )
    assert_rows_equal(actual, expected, order_matters=True)


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

"""
Column–Column comparison parity tests (#184). Expected outputs are from a prior PySpark 3.5 run;
tests do not execute PySpark at runtime.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils import assert_rows_equal, get_session


# Expected outputs (from prior PySpark 3.5 run)
EXPECTED_FILTER_A_GT_B: list[dict[str, Any]] = [
    {"a": 3, "b": 1},
    {"a": 4, "b": 2},
    {"a": 5, "b": 1},
]
EXPECTED_FILTER_A_LT_B: list[dict[str, Any]] = [
    {"a": 1, "b": 5},
    {"a": 2, "b": 4},
]
EXPECTED_FILTER_X_EQ_Y: list[dict[str, Any]] = [
    {"x": 2, "y": 2},
]
EXPECTED_FILTER_X_NEQ_Y: list[dict[str, Any]] = [
    {"x": 3, "y": 1},
    {"x": 1, "y": 3},
    {"x": 0, "y": 5},
]
EXPECTED_FILTER_COMBINED: list[dict[str, Any]] = [
    {"a": 3, "b": 1},
    {"a": 4, "b": 2},
    {"a": 5, "b": 1},
]
EXPECTED_WITH_COLUMN_P_GT_Q: list[dict[str, Any]] = [
    {"p": 10, "q": 5, "p_gt_q": True},
    {"p": 3, "q": 7, "p_gt_q": False},
    {"p": 0, "q": 0, "p_gt_q": False},
]
EXPECTED_FILTER_S1_GT_S2_STRINGS: list[dict[str, Any]] = [
    {"s1": "banana", "s2": "apple"},
]
EXPECTED_FILTER_A_EQ_B_EMPTY: list[dict[str, Any]] = []
EXPECTED_FILTER_A_NEQ_B_ALL: list[dict[str, Any]] = [
    {"a": 1, "b": 2},
    {"a": 3, "b": 4},
]


def test_filter_col_gt_col_pyspark_parity() -> None:
    """filter(col('a') > col('b')) matches PySpark (#184)."""
    import robin_sparkless as rs

    spark = get_session()
    data = [[1, 5], [2, 4], [3, 1], [4, 2], [5, 1]]
    schema = [("a", "bigint"), ("b", "bigint")]
    df = spark.createDataFrame(data, schema)
    actual = df.filter(rs.col("a") > rs.col("b")).collect()
    assert_rows_equal(actual, EXPECTED_FILTER_A_GT_B, order_matters=True)


def test_filter_col_lt_col_pyspark_parity() -> None:
    """filter(col('a') < col('b')) matches PySpark (#184)."""
    import robin_sparkless as rs

    spark = get_session()
    data = [[1, 5], [2, 4], [3, 1], [4, 2], [5, 1]]
    schema = [("a", "bigint"), ("b", "bigint")]
    df = spark.createDataFrame(data, schema)
    actual = df.filter(rs.col("a") < rs.col("b")).collect()
    assert_rows_equal(actual, EXPECTED_FILTER_A_LT_B, order_matters=True)


def test_filter_col_eq_col_pyspark_parity() -> None:
    """filter(col('x') == col('y')) matches PySpark (#184)."""
    import robin_sparkless as rs

    spark = get_session()
    data = [[3, 1], [1, 3], [2, 2], [0, 5]]
    schema = [("x", "bigint"), ("y", "bigint")]
    df = spark.createDataFrame(data, schema)
    actual = df.filter(rs.col("x") == rs.col("y")).collect()
    assert_rows_equal(actual, EXPECTED_FILTER_X_EQ_Y, order_matters=True)


def test_filter_col_neq_col_pyspark_parity() -> None:
    """filter(col('x') != col('y')) matches PySpark (#184)."""
    import robin_sparkless as rs

    spark = get_session()
    data = [[3, 1], [1, 3], [2, 2], [0, 5]]
    schema = [("x", "bigint"), ("y", "bigint")]
    df = spark.createDataFrame(data, schema)
    actual = df.filter(rs.col("x") != rs.col("y")).collect()
    assert_rows_equal(actual, EXPECTED_FILTER_X_NEQ_Y, order_matters=False)


def test_filter_combined_col_col_and_literal_pyspark_parity() -> None:
    """filter((col('a')>col('b')) & (col('a')>2)) matches PySpark (#184)."""
    import robin_sparkless as rs

    spark = get_session()
    data = [[1, 5], [2, 4], [3, 1], [4, 2], [5, 1]]
    schema = [("a", "bigint"), ("b", "bigint")]
    df = spark.createDataFrame(data, schema)
    actual = df.filter((rs.col("a") > rs.col("b")) & (rs.col("a") > 2)).collect()
    assert_rows_equal(actual, EXPECTED_FILTER_COMBINED, order_matters=True)


def test_with_column_col_gt_col_pyspark_parity() -> None:
    """with_column('p_gt_q', col('p') > col('q')) matches PySpark (#184)."""
    import robin_sparkless as rs

    spark = get_session()
    data = [[10, 5], [3, 7], [0, 0]]
    schema = [("p", "bigint"), ("q", "bigint")]
    df = spark.createDataFrame(data, schema)
    actual = df.with_column("p_gt_q", rs.col("p") > rs.col("q")).collect()
    assert_rows_equal(actual, EXPECTED_WITH_COLUMN_P_GT_Q, order_matters=True)


def test_filter_col_gt_col_strings_pyspark_parity() -> None:
    """filter(col('s1') > col('s2')) on strings matches PySpark (#184)."""
    import robin_sparkless as rs

    spark = get_session()
    data = [["apple", "banana"], ["banana", "apple"], ["x", "x"]]
    schema = [("s1", "string"), ("s2", "string")]
    df = spark.createDataFrame(data, schema)
    actual = df.filter(rs.col("s1") > rs.col("s2")).collect()
    assert_rows_equal(actual, EXPECTED_FILTER_S1_GT_S2_STRINGS, order_matters=True)


def test_filter_col_eq_col_empty_pyspark_parity() -> None:
    """filter(col('a')==col('b')) with no matches returns [] like PySpark (#184)."""
    import robin_sparkless as rs

    spark = get_session()
    data = [[1, 2], [3, 4]]
    schema = [("a", "bigint"), ("b", "bigint")]
    df = spark.createDataFrame(data, schema)
    actual = df.filter(rs.col("a") == rs.col("b")).collect()
    assert_rows_equal(actual, EXPECTED_FILTER_A_EQ_B_EMPTY, order_matters=True)


def test_filter_col_neq_col_all_match_pyspark_parity() -> None:
    """filter(col('a')!=col('b')) with all matching matches PySpark (#184)."""
    import robin_sparkless as rs

    spark = get_session()
    data = [[1, 2], [3, 4]]
    schema = [("a", "bigint"), ("b", "bigint")]
    df = spark.createDataFrame(data, schema)
    actual = df.filter(rs.col("a") != rs.col("b")).collect()
    assert_rows_equal(actual, EXPECTED_FILTER_A_NEQ_B_ALL, order_matters=False)

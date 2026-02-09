"""
PySpark parity tests for Column–Column comparisons (fixes #184).

When PySpark is available (pip install pyspark, Java 17+), we run the same
operations in PySpark and compare. When unavailable, we use predetermined
expected output derived from PySpark 3.5.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils import assert_rows_equal, get_session, run_with_pyspark_expected


# --- PySpark helpers: same data + same operation, return .collect() ---


def _pyspark_filter_a_gt_b(spark, F):
    """PySpark: filter(col('a') > col('b')) on [(1,5),(2,4),(3,1),(4,2),(5,1)]."""
    data = [(1, 5), (2, 4), (3, 1), (4, 2), (5, 1)]
    df = spark.createDataFrame(data, ["a", "b"])
    return df.filter(F.col("a") > F.col("b")).collect()


def _pyspark_filter_a_lt_b(spark, F):
    """PySpark: filter(col('a') < col('b'))."""
    data = [(1, 5), (2, 4), (3, 1), (4, 2), (5, 1)]
    df = spark.createDataFrame(data, ["a", "b"])
    return df.filter(F.col("a") < F.col("b")).collect()


def _pyspark_filter_a_eq_b(spark, F):
    """PySpark: filter(col('a') == col('b'))."""
    data = [(3, 1), (1, 3), (2, 2), (0, 5)]
    df = spark.createDataFrame(data, ["x", "y"])
    return df.filter(F.col("x") == F.col("y")).collect()


def _pyspark_filter_a_neq_b(spark, F):
    """PySpark: filter(col('x') != col('y'))."""
    data = [(3, 1), (1, 3), (2, 2), (0, 5)]
    df = spark.createDataFrame(data, ["x", "y"])
    return df.filter(F.col("x") != F.col("y")).collect()


def _pyspark_filter_combined_col_col_and_literal(spark, F):
    """PySpark: filter((col('a') > col('b')) & (col('a') > 2))."""
    data = [(1, 5), (2, 4), (3, 1), (4, 2), (5, 1)]
    df = spark.createDataFrame(data, ["a", "b"])
    return df.filter((F.col("a") > F.col("b")) & (F.col("a") > 2)).collect()


def _pyspark_with_column_p_gt_q(spark, F):
    """PySpark: withColumn('p_gt_q', col('p') > col('q'))."""
    data = [(10, 5), (3, 7), (0, 0)]
    df = spark.createDataFrame(data, ["p", "q"])
    return df.withColumn("p_gt_q", F.col("p") > F.col("q")).collect()


def _pyspark_filter_s1_gt_s2_strings(spark, F):
    """PySpark: filter(col('s1') > col('s2')) on strings."""
    data = [("apple", "banana"), ("banana", "apple"), ("x", "x")]
    df = spark.createDataFrame(data, ["s1", "s2"])
    return df.filter(F.col("s1") > F.col("s2")).collect()


def _pyspark_filter_a_eq_b_empty(spark, F):
    """PySpark: filter(col('a') == col('b')) where no row matches."""
    data = [(1, 2), (3, 4)]
    df = spark.createDataFrame(data, ["a", "b"])
    return df.filter(F.col("a") == F.col("b")).collect()


def _pyspark_filter_a_neq_b_all_match(spark, F):
    """PySpark: filter(col('a') != col('b')) where all rows match."""
    data = [(1, 2), (3, 4)]
    df = spark.createDataFrame(data, ["a", "b"])
    return df.filter(F.col("a") != F.col("b")).collect()


# Fallback expected (from PySpark 3.5) when PySpark is unavailable
FALLBACK_FILTER_A_GT_B: list[dict[str, Any]] = [
    {"a": 3, "b": 1},
    {"a": 4, "b": 2},
    {"a": 5, "b": 1},
]
FALLBACK_FILTER_A_LT_B: list[dict[str, Any]] = [
    {"a": 1, "b": 5},
    {"a": 2, "b": 4},
]
FALLBACK_FILTER_X_EQ_Y: list[dict[str, Any]] = [
    {"x": 2, "y": 2},
]
FALLBACK_FILTER_X_NEQ_Y: list[dict[str, Any]] = [
    {"x": 3, "y": 1},
    {"x": 1, "y": 3},
    {"x": 0, "y": 5},
]
FALLBACK_FILTER_COMBINED: list[dict[str, Any]] = [
    {"a": 3, "b": 1},
    {"a": 4, "b": 2},
    {"a": 5, "b": 1},
]
FALLBACK_WITH_COLUMN_P_GT_Q: list[dict[str, Any]] = [
    {"p": 10, "q": 5, "p_gt_q": True},
    {"p": 3, "q": 7, "p_gt_q": False},
    {"p": 0, "q": 0, "p_gt_q": False},
]
FALLBACK_FILTER_S1_GT_S2_STRINGS: list[dict[str, Any]] = [
    {"s1": "banana", "s2": "apple"},
]
FALLBACK_FILTER_A_EQ_B_EMPTY: list[dict[str, Any]] = []
FALLBACK_FILTER_A_NEQ_B_ALL: list[dict[str, Any]] = [
    {"a": 1, "b": 2},
    {"a": 3, "b": 4},
]


def test_filter_col_gt_col_pyspark_parity() -> None:
    """filter(col('a') > col('b')) matches PySpark (#184)."""
    import robin_sparkless as rs

    spark = get_session()
    data = [[1, 5], [2, 4], [3, 1], [4, 2], [5, 1]]
    schema = [("a", "bigint"), ("b", "bigint")]
    df = spark._create_dataframe_from_rows(data, schema)
    actual = df.filter(rs.col("a") > rs.col("b")).collect()
    expected = run_with_pyspark_expected(_pyspark_filter_a_gt_b, FALLBACK_FILTER_A_GT_B)
    assert_rows_equal(actual, expected, order_matters=True)


def test_filter_col_lt_col_pyspark_parity() -> None:
    """filter(col('a') < col('b')) matches PySpark (#184)."""
    import robin_sparkless as rs

    spark = get_session()
    data = [[1, 5], [2, 4], [3, 1], [4, 2], [5, 1]]
    schema = [("a", "bigint"), ("b", "bigint")]
    df = spark._create_dataframe_from_rows(data, schema)
    actual = df.filter(rs.col("a") < rs.col("b")).collect()
    expected = run_with_pyspark_expected(_pyspark_filter_a_lt_b, FALLBACK_FILTER_A_LT_B)
    assert_rows_equal(actual, expected, order_matters=True)


def test_filter_col_eq_col_pyspark_parity() -> None:
    """filter(col('x') == col('y')) matches PySpark (#184)."""
    import robin_sparkless as rs

    spark = get_session()
    data = [[3, 1], [1, 3], [2, 2], [0, 5]]
    schema = [("x", "bigint"), ("y", "bigint")]
    df = spark._create_dataframe_from_rows(data, schema)
    actual = df.filter(rs.col("x") == rs.col("y")).collect()
    expected = run_with_pyspark_expected(_pyspark_filter_a_eq_b, FALLBACK_FILTER_X_EQ_Y)
    assert_rows_equal(actual, expected, order_matters=True)


def test_filter_col_neq_col_pyspark_parity() -> None:
    """filter(col('x') != col('y')) matches PySpark (#184)."""
    import robin_sparkless as rs

    spark = get_session()
    data = [[3, 1], [1, 3], [2, 2], [0, 5]]
    schema = [("x", "bigint"), ("y", "bigint")]
    df = spark._create_dataframe_from_rows(data, schema)
    actual = df.filter(rs.col("x") != rs.col("y")).collect()
    expected = run_with_pyspark_expected(
        _pyspark_filter_a_neq_b, FALLBACK_FILTER_X_NEQ_Y
    )
    assert_rows_equal(actual, expected, order_matters=False)


def test_filter_combined_col_col_and_literal_pyspark_parity() -> None:
    """filter((col('a')>col('b')) & (col('a')>2)) matches PySpark (#184)."""
    import robin_sparkless as rs

    spark = get_session()
    data = [[1, 5], [2, 4], [3, 1], [4, 2], [5, 1]]
    schema = [("a", "bigint"), ("b", "bigint")]
    df = spark._create_dataframe_from_rows(data, schema)
    actual = df.filter((rs.col("a") > rs.col("b")) & (rs.col("a") > 2)).collect()
    expected = run_with_pyspark_expected(
        _pyspark_filter_combined_col_col_and_literal, FALLBACK_FILTER_COMBINED
    )
    assert_rows_equal(actual, expected, order_matters=True)


def test_with_column_col_gt_col_pyspark_parity() -> None:
    """with_column('p_gt_q', col('p') > col('q')) matches PySpark (#184)."""
    import robin_sparkless as rs

    spark = get_session()
    data = [[10, 5], [3, 7], [0, 0]]
    schema = [("p", "bigint"), ("q", "bigint")]
    df = spark._create_dataframe_from_rows(data, schema)
    actual = df.with_column("p_gt_q", rs.col("p") > rs.col("q")).collect()
    expected = run_with_pyspark_expected(
        _pyspark_with_column_p_gt_q, FALLBACK_WITH_COLUMN_P_GT_Q
    )
    assert_rows_equal(actual, expected, order_matters=True)


def test_filter_col_gt_col_strings_pyspark_parity() -> None:
    """filter(col('s1') > col('s2')) on strings matches PySpark (#184)."""
    import robin_sparkless as rs

    spark = get_session()
    data = [["apple", "banana"], ["banana", "apple"], ["x", "x"]]
    schema = [("s1", "string"), ("s2", "string")]
    df = spark._create_dataframe_from_rows(data, schema)
    actual = df.filter(rs.col("s1") > rs.col("s2")).collect()
    expected = run_with_pyspark_expected(
        _pyspark_filter_s1_gt_s2_strings, FALLBACK_FILTER_S1_GT_S2_STRINGS
    )
    assert_rows_equal(actual, expected, order_matters=True)


def test_filter_col_eq_col_empty_pyspark_parity() -> None:
    """filter(col('a')==col('b')) with no matches returns [] like PySpark (#184)."""
    import robin_sparkless as rs

    spark = get_session()
    data = [[1, 2], [3, 4]]
    schema = [("a", "bigint"), ("b", "bigint")]
    df = spark._create_dataframe_from_rows(data, schema)
    actual = df.filter(rs.col("a") == rs.col("b")).collect()
    expected = run_with_pyspark_expected(
        _pyspark_filter_a_eq_b_empty, FALLBACK_FILTER_A_EQ_B_EMPTY
    )
    assert_rows_equal(actual, expected, order_matters=True)


def test_filter_col_neq_col_all_match_pyspark_parity() -> None:
    """filter(col('a')!=col('b')) with all matching matches PySpark (#184)."""
    import robin_sparkless as rs

    spark = get_session()
    data = [[1, 2], [3, 4]]
    schema = [("a", "bigint"), ("b", "bigint")]
    df = spark._create_dataframe_from_rows(data, schema)
    actual = df.filter(rs.col("a") != rs.col("b")).collect()
    expected = run_with_pyspark_expected(
        _pyspark_filter_a_neq_b_all_match, FALLBACK_FILTER_A_NEQ_B_ALL
    )
    assert_rows_equal(actual, expected, order_matters=False)

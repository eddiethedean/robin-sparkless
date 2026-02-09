"""
PySpark parity tests for lit(datetime.date) and lit(datetime.datetime) (Fixes #186).

When PySpark is available (pip install pyspark, Java 17+), we run the same
operations in PySpark and robin-sparkless and compare row counts and row values.
When unavailable, we use predetermined expected output derived from PySpark 3.5.
"""

from __future__ import annotations

import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils import assert_rows_equal, get_session, run_with_pyspark_expected


# --- PySpark runners: same data + same operation, return .collect() as list of dicts ---


def _pyspark_lit_date_and_datetime_with_column(spark, F):
    """PySpark: with_column lit(date) and lit(datetime)."""
    data = [(1, 10, "a"), (2, 20, "b"), (3, 30, "c")]
    df = spark.createDataFrame(data, ["id", "x", "name"])
    df = df.withColumn("const_date", F.lit(datetime.date(2025, 6, 15)))
    df = df.withColumn(
        "const_ts",
        F.lit(datetime.datetime(2025, 6, 15, 12, 30, 45, 123456)),
    )
    return df.collect()


def _pyspark_filter_date_gt_lit(spark, F):
    """PySpark: add date column d, filter d > lit(past_date) -> all rows."""
    data = [(1, 10, "a"), (2, 20, "b"), (3, 30, "c")]
    df = spark.createDataFrame(data, ["id", "x", "name"])
    df = df.withColumn("d", F.lit(datetime.date(2025, 5, 15)))
    threshold = datetime.date(2026, 1, 1)
    return df.filter(F.col("d") < F.lit(threshold)).collect()


def _pyspark_filter_date_eq_lit(spark, F):
    """PySpark: add date column d, filter d == lit(same_date) -> all rows."""
    data = [(1, 10, "a"), (2, 20, "b"), (3, 30, "c")]
    df = spark.createDataFrame(data, ["id", "x", "name"])
    df = df.withColumn("d", F.lit(datetime.date(2025, 5, 15)))
    threshold = datetime.date(2025, 5, 15)
    return df.filter(F.col("d") == F.lit(threshold)).collect()


def _pyspark_when_then_lit_date_otherwise_lit_date(spark, F):
    """PySpark: when(x < 5).then(lit(date1)).otherwise(lit(date2))."""
    data = [(1, 1, "a"), (2, 2, "b"), (3, 10, "c")]
    df = spark.createDataFrame(data, ["id", "x", "name"])
    df = df.withColumn(
        "bucket",
        F.when(F.col("x") < 5, F.lit(datetime.date(2025, 1, 1))).otherwise(
            F.lit(datetime.date(2025, 12, 31))
        ),
    )
    return df.collect()


# Fallback expected (from PySpark 3.5 when PySpark not available at run time)
FALLBACK_LIT_DATE_DATETIME = [
    {"id": 1, "x": 10, "name": "a", "const_date": datetime.date(2025, 6, 15), "const_ts": datetime.datetime(2025, 6, 15, 12, 30, 45, 123456)},
    {"id": 2, "x": 20, "name": "b", "const_date": datetime.date(2025, 6, 15), "const_ts": datetime.datetime(2025, 6, 15, 12, 30, 45, 123456)},
    {"id": 3, "x": 30, "name": "c", "const_date": datetime.date(2025, 6, 15), "const_ts": datetime.datetime(2025, 6, 15, 12, 30, 45, 123456)},
]

FALLBACK_FILTER_DATE_LT_LIT = [
    {"id": 1, "x": 10, "name": "a", "d": datetime.date(2025, 5, 15)},
    {"id": 2, "x": 20, "name": "b", "d": datetime.date(2025, 5, 15)},
    {"id": 3, "x": 30, "name": "c", "d": datetime.date(2025, 5, 15)},
]

FALLBACK_FILTER_DATE_EQ_LIT = [
    {"id": 1, "x": 10, "name": "a", "d": datetime.date(2025, 5, 15)},
    {"id": 2, "x": 20, "name": "b", "d": datetime.date(2025, 5, 15)},
    {"id": 3, "x": 30, "name": "c", "d": datetime.date(2025, 5, 15)},
]

FALLBACK_WHEN_THEN_LIT_DATE = [
    {"id": 1, "x": 1, "name": "a", "bucket": datetime.date(2025, 1, 1)},
    {"id": 2, "x": 2, "name": "b", "bucket": datetime.date(2025, 1, 1)},
    {"id": 3, "x": 10, "name": "c", "bucket": datetime.date(2025, 12, 31)},
]


# --- Parity tests ---
# run_with_pyspark_expected returns list of dicts (PySpark rows via _row_to_dict, or fallback).


def test_lit_date_and_datetime_with_column_pyspark_parity() -> None:
    """with_column lit(date) and lit(datetime) matches PySpark (#186)."""
    import robin_sparkless as rs

    spark = get_session()
    df = spark.create_dataframe(
        [(1, 10, "a"), (2, 20, "b"), (3, 30, "c")], ["id", "x", "name"]
    )
    df = df.with_column("const_date", rs.lit(datetime.date(2025, 6, 15)))
    df = df.with_column(
        "const_ts",
        rs.lit(datetime.datetime(2025, 6, 15, 12, 30, 45, 123456)),
    )
    actual = df.collect()
    expected = run_with_pyspark_expected(
        _pyspark_lit_date_and_datetime_with_column, FALLBACK_LIT_DATE_DATETIME
    )
    assert_rows_equal(actual, expected, order_matters=True)


def test_filter_date_lt_lit_pyspark_parity() -> None:
    """filter(col('d') < lit(date)) with constant date column matches PySpark (#186)."""
    import robin_sparkless as rs

    spark = get_session()
    df = spark.create_dataframe(
        [(1, 10, "a"), (2, 20, "b"), (3, 30, "c")], ["id", "x", "name"]
    )
    df = df.with_column("d", rs.lit(datetime.date(2025, 5, 15)))
    actual = df.filter(rs.col("d").lt(rs.lit(datetime.date(2026, 1, 1)))).collect()
    expected = run_with_pyspark_expected(
        _pyspark_filter_date_gt_lit, FALLBACK_FILTER_DATE_LT_LIT
    )
    assert_rows_equal(actual, expected, order_matters=True)


def test_filter_date_eq_lit_pyspark_parity() -> None:
    """filter(col('d') == lit(date)) matches PySpark (#186)."""
    import robin_sparkless as rs

    spark = get_session()
    df = spark.create_dataframe(
        [(1, 10, "a"), (2, 20, "b"), (3, 30, "c")], ["id", "x", "name"]
    )
    df = df.with_column("d", rs.lit(datetime.date(2025, 5, 15)))
    actual = df.filter(rs.col("d").eq(rs.lit(datetime.date(2025, 5, 15)))).collect()
    expected = run_with_pyspark_expected(
        _pyspark_filter_date_eq_lit, FALLBACK_FILTER_DATE_EQ_LIT
    )
    assert_rows_equal(actual, expected, order_matters=True)


def test_when_then_lit_date_otherwise_pyspark_parity() -> None:
    """when(x < 5).then(lit(date)).otherwise(lit(date)) matches PySpark (#186)."""
    import robin_sparkless as rs

    spark = get_session()
    df = spark.create_dataframe(
        [(1, 1, "a"), (2, 2, "b"), (3, 10, "c")], ["id", "x", "name"]
    )
    df = df.with_column(
        "bucket",
        rs.when(rs.col("x").lt(rs.lit(5)))
        .then(rs.lit(datetime.date(2025, 1, 1)))
        .otherwise(rs.lit(datetime.date(2025, 12, 31))),
    )
    actual = df.collect()
    expected = run_with_pyspark_expected(
        _pyspark_when_then_lit_date_otherwise_lit_date, FALLBACK_WHEN_THEN_LIT_DATE
    )
    assert_rows_equal(actual, expected, order_matters=True)

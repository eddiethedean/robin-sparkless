"""
Lit date/datetime parity tests (#186). Expected outputs are from a prior PySpark 3.5 run;
tests do not execute PySpark at runtime.
"""

from __future__ import annotations

import datetime

from sparkless.testing import get_imports
from tests.utils import assert_rows_equal

_imports = get_imports()
F = _imports.F


# Expected outputs (from prior PySpark 3.5 run)
EXPECTED_LIT_DATE_DATETIME = [
    {
        "id": 1,
        "x": 10,
        "name": "a",
        "const_date": datetime.date(2025, 6, 15),
        "const_ts": datetime.datetime(2025, 6, 15, 12, 30, 45, 123456),
    },
    {
        "id": 2,
        "x": 20,
        "name": "b",
        "const_date": datetime.date(2025, 6, 15),
        "const_ts": datetime.datetime(2025, 6, 15, 12, 30, 45, 123456),
    },
    {
        "id": 3,
        "x": 30,
        "name": "c",
        "const_date": datetime.date(2025, 6, 15),
        "const_ts": datetime.datetime(2025, 6, 15, 12, 30, 45, 123456),
    },
]

EXPECTED_FILTER_DATE_LT_LIT = [
    {"id": 1, "x": 10, "name": "a", "d": datetime.date(2025, 5, 15)},
    {"id": 2, "x": 20, "name": "b", "d": datetime.date(2025, 5, 15)},
    {"id": 3, "x": 30, "name": "c", "d": datetime.date(2025, 5, 15)},
]

EXPECTED_FILTER_DATE_EQ_LIT = [
    {"id": 1, "x": 10, "name": "a", "d": datetime.date(2025, 5, 15)},
    {"id": 2, "x": 20, "name": "b", "d": datetime.date(2025, 5, 15)},
    {"id": 3, "x": 30, "name": "c", "d": datetime.date(2025, 5, 15)},
]

EXPECTED_WHEN_THEN_LIT_DATE = [
    {"id": 1, "x": 1, "name": "a", "bucket": datetime.date(2025, 1, 1)},
    {"id": 2, "x": 2, "name": "b", "bucket": datetime.date(2025, 1, 1)},
    {"id": 3, "x": 10, "name": "c", "bucket": datetime.date(2025, 12, 31)},
]


def test_lit_date_and_datetime_with_column_pyspark_parity(spark) -> None:
    """with_column lit(date) and lit(datetime) matches expected from PySpark (#186)."""
    df = spark.createDataFrame(
        [(1, 10, "a"), (2, 20, "b"), (3, 30, "c")], ["id", "x", "name"]
    )
    df = df.withColumn("const_date", F.lit(datetime.date(2025, 6, 15)))
    df = df.withColumn(
        "const_ts",
        F.lit(datetime.datetime(2025, 6, 15, 12, 30, 45, 123456)),
    )
    actual = df.collect()
    assert_rows_equal(actual, EXPECTED_LIT_DATE_DATETIME, order_matters=True)


def test_filter_date_lt_lit_pyspark_parity(spark) -> None:
    """filter(col('d') < lit(date)) matches expected from PySpark (#186)."""
    df = spark.createDataFrame(
        [(1, 10, "a"), (2, 20, "b"), (3, 30, "c")], ["id", "x", "name"]
    )
    df = df.withColumn("d", F.lit(datetime.date(2025, 5, 15)))
    actual = df.filter(F.col("d") < F.lit(datetime.date(2026, 1, 1))).collect()
    assert_rows_equal(actual, EXPECTED_FILTER_DATE_LT_LIT, order_matters=True)


def test_filter_date_eq_lit_pyspark_parity(spark) -> None:
    """filter(col('d') == lit(date)) matches expected from PySpark (#186)."""
    df = spark.createDataFrame(
        [(1, 10, "a"), (2, 20, "b"), (3, 30, "c")], ["id", "x", "name"]
    )
    df = df.withColumn("d", F.lit(datetime.date(2025, 5, 15)))
    actual = df.filter(F.col("d") == F.lit(datetime.date(2025, 5, 15))).collect()
    assert_rows_equal(actual, EXPECTED_FILTER_DATE_EQ_LIT, order_matters=True)


def test_when_then_lit_date_otherwise_pyspark_parity(spark) -> None:
    """when(x < 5).then(lit(date)).otherwise(lit(date)) matches expected from PySpark (#186)."""
    df = spark.createDataFrame(
        [(1, 1, "a"), (2, 2, "b"), (3, 10, "c")], ["id", "x", "name"]
    )
    df = df.withColumn(
        "bucket",
        F.when(F.col("x") < 5, datetime.date(2025, 1, 1)).otherwise(
            datetime.date(2025, 12, 31)
        ),
    )
    actual = df.collect()
    assert_rows_equal(actual, EXPECTED_WHEN_THEN_LIT_DATE, order_matters=True)

"""Tests for issue #216: Date cast from datetime string (PySpark parity).

Casting a string like '2025-01-01 10:30:00' to date must work; PySpark accepts
datetime strings and truncates to date.
"""

import datetime

from tests.utils import get_functions

F = get_functions()


def test_cast_datetime_string_to_date(spark) -> None:
    """Exact scenario from #216: withColumn('d', col('date_str').cast('date'))."""
    df = spark.createDataFrame(
        [{"date_str": "2025-01-01 10:30:00"}],
        schema=["date_str"],
    )
    result = df.withColumn("d", F.col("date_str").cast("date"))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["date_str"] == "2025-01-01 10:30:00"
    # PySpark returns datetime.date for DateType columns
    assert rows[0]["d"] == datetime.date(2025, 1, 1)


def test_cast_date_only_string_to_date(spark) -> None:
    """Date-only string still works."""
    df = spark.createDataFrame(
        [{"date_str": "2025-01-01"}],
        schema=["date_str"],
    )
    result = df.withColumn("d", F.col("date_str").cast("date"))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["d"] == datetime.date(2025, 1, 1)


def test_try_cast_datetime_string_to_date_invalid_null(spark) -> None:
    """Invalid datetime string cast to date yields null."""
    df = spark.createDataFrame(
        [{"s": "2025-01-01 10:30:00"}, {"s": "not-a-date"}],
        schema=["s"],
    )
    result = df.select(F.col("s").cast("date").alias("d"))
    rows = result.collect()
    assert len(rows) == 2
    assert rows[0]["d"] == datetime.date(2025, 1, 1)
    assert rows[1]["d"] is None


"""Tests for issue #216: Date cast from datetime string.

Casting a string like '2025-01-01 10:30:00' to date must work (Spark accepts
datetime strings and truncates to date). Polars' default parser expects date-only format.
"""

import robin_sparkless as rs


def test_cast_datetime_string_to_date() -> None:
    """Exact scenario from #216: with_column('d', col('date_str').cast('date'))."""
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.createDataFrame(
        [{"date_str": "2025-01-01 10:30:00"}],
        [("date_str", "string")],
    )
    result = df.with_column("d", rs.col("date_str").cast("date"))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["date_str"] == "2025-01-01 10:30:00"
    assert rows[0]["d"] == "2025-01-01"


def test_cast_date_only_string_to_date() -> None:
    """Date-only string still works."""
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.createDataFrame(
        [{"date_str": "2025-01-01"}],
        [("date_str", "string")],
    )
    result = df.with_column("d", rs.col("date_str").cast("date"))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["d"] == "2025-01-01"


def test_try_cast_datetime_string_to_date_invalid_null() -> None:
    """try_cast: invalid string -> null."""
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.createDataFrame(
        [{"s": "2025-01-01 10:30:00"}, {"s": "not-a-date"}],
        [("s", "string")],
    )
    result = df.with_column("d", rs.try_cast(rs.col("s"), "date"))
    rows = result.collect()
    assert len(rows) == 2
    assert rows[0]["d"] == "2025-01-01"
    assert rows[1]["d"] is None

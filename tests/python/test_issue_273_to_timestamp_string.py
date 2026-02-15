"""
Tests for issue #273: to_timestamp() on string column (PySpark parity).

PySpark to_timestamp(col("ts_str")) parses common formats like "2024-01-01 10:00:00".
With format, PySpark parses e.g. "2024-01-01T10:00:00" with "yyyy-MM-dd'T'HH:mm:ss".
Robin previously failed without format or returned None with format.
"""

from __future__ import annotations

import robin_sparkless as rs

F = rs


def test_to_timestamp_string_no_format() -> None:
    """to_timestamp(col('ts_str')) without format parses 'YYYY-MM-DD HH:MM:SS'."""
    spark = F.SparkSession.builder().app_name("test_273").get_or_create()
    df = spark.createDataFrame(
        [{"ts_str": "2024-01-01 10:00:00"}],
        [("ts_str", "str")],
    )
    df = df.with_column("ts", F.to_timestamp(F.col("ts_str")))
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["ts"] is not None
    ts = rows[0]["ts"]
    assert "2024-01-01" in str(ts) and "10:00:00" in str(ts)


def test_to_timestamp_string_with_format() -> None:
    """to_timestamp(col('ts_str'), \"yyyy-MM-dd'T'HH:mm:ss\") parses ISO-like strings."""
    spark = F.SparkSession.builder().app_name("test_273").get_or_create()
    df = spark.createDataFrame(
        [{"ts_str": "2024-01-01T10:00:00"}],
        [("ts_str", "str")],
    )
    df = df.with_column(
        "ts",
        F.to_timestamp(F.col("ts_str"), "yyyy-MM-dd'T'HH:mm:ss"),
    )
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["ts"] is not None
    ts = rows[0]["ts"]
    assert "2024-01-01" in str(ts) and "10:00:00" in str(ts)

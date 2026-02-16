"""
Tests for #403: date functions (hour, minute, etc.) on string timestamp column (PySpark parity).
"""

from __future__ import annotations

import robin_sparkless as rs


def _spark() -> rs.SparkSession:
    return rs.SparkSession.builder().app_name("issue_403").get_or_create()


def test_hour_on_string_timestamp() -> None:
    """hour(col) accepts string timestamp column; parses and returns hour (0-23)."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"ts": "2024-01-15 14:30:00"}],
        schema=[("ts", "string")],
    )
    result = df.select(rs.hour(rs.col("ts")).alias("h")).collect()
    assert len(result) == 1
    assert result[0]["h"] == 14


def test_minute_second_on_string_timestamp() -> None:
    """minute(col) and second(col) accept string timestamp column."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"ts": "2024-06-10 09:45:30"}],
        schema=[("ts", "string")],
    )
    result = df.select(
        rs.minute(rs.col("ts")).alias("m"),
        rs.second(rs.col("ts")).alias("s"),
    ).collect()
    assert len(result) == 1
    assert result[0]["m"] == 45
    assert result[0]["s"] == 30

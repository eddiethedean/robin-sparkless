"""
Tests for #403: date functions (hour, minute, etc.) on string timestamp column (PySpark parity).
"""

from __future__ import annotations

import pytest

from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
F = _imports.F
def test_hour_on_string_timestamp(spark) -> None:
    """hour(col) accepts string timestamp column; parses and returns hour (0-23)."""
    df = spark.createDataFrame(
        [{"ts": "2024-01-15 14:30:00"}],
        schema=["ts"],
    )
    result = df.select(F.hour(F.col("ts")).alias("h")).collect()
    assert len(result) == 1
    assert result[0]["h"] == 14
def test_minute_second_on_string_timestamp(spark) -> None:
    """minute(col) and second(col) accept string timestamp column."""
    df = spark.createDataFrame(
        [{"ts": "2024-06-10 09:45:30"}],
        schema=["ts"],
    )
    result = df.select(
        F.minute(F.col("ts")).alias("m"),
        F.second(F.col("ts")).alias("s"),
    ).collect()
    assert len(result) == 1
    assert result[0]["m"] == 45
    assert result[0]["s"] == 30

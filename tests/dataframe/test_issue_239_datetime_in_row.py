"""
Tests for issue #239: datetime in row not accepted.

With robin-sparkless 0.6.0, _create_dataframe_from_rows rejected Python datetime
values with TypeError (row values must be None, int, float, bool, str, ...).
PySpark accepts datetime in createDataFrame. We now accept datetime.date and
datetime.datetime in row values and map them to date/timestamp columns.
"""

from __future__ import annotations

from datetime import datetime, date


def test_create_dataframe_from_rows_accepts_datetime_and_none(spark) -> None:
    """Row with datetime and None for timestamp column works (PySpark parity)."""
    data = [
        {"id": 1, "ts": datetime(2025, 2, 10, 12, 0, 0)},
        {"id": 2, "ts": None},
    ]
    df = spark.createDataFrame(data, schema=["id", "ts"])
    out = df.orderBy(["id"]).collect()
    assert len(out) == 2
    assert out[0]["id"] == 1 and out[0]["ts"] is not None
    assert out[1]["id"] == 2 and out[1]["ts"] is None


def test_create_dataframe_from_rows_accepts_date(spark) -> None:
    """Row with datetime.date for date column works."""
    data = [
        (1, date(2025, 2, 10)),
        (2, None),
    ]
    df = spark.createDataFrame(data, schema=["id", "d"])
    out = df.orderBy(["id"]).collect()
    assert len(out) == 2
    assert out[0]["id"] == 1 and out[0]["d"] is not None
    assert out[1]["id"] == 2 and out[1]["d"] is None

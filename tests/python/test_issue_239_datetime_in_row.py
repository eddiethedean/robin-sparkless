"""
Tests for issue #239: datetime in row not accepted.

With robin-sparkless 0.6.0, _create_dataframe_from_rows rejected Python datetime
values with TypeError (row values must be None, int, float, bool, str, ...).
PySpark accepts datetime in createDataFrame. We now accept datetime.date and
datetime.datetime in row values and map them to date/timestamp columns.
"""

from __future__ import annotations

from datetime import datetime, date


def test_create_dataframe_from_rows_accepts_datetime_and_none() -> None:
    """Row with datetime and None for timestamp column works (PySpark parity)."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("datetime_row").get_or_create()
    data = [
        {"id": 1, "ts": datetime(2025, 2, 10, 12, 0, 0)},
        {"id": 2, "ts": None},
    ]
    schema = [("id", "int"), ("ts", "timestamp")]
    df = spark._create_dataframe_from_rows(data, schema)
    out = df.order_by(["id"]).collect()
    assert len(out) == 2
    assert out[0]["id"] == 1 and out[0]["ts"] is not None
    assert out[1]["id"] == 2 and out[1]["ts"] is None


def test_create_dataframe_from_rows_accepts_date() -> None:
    """Row with datetime.date for date column works."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("date_row").get_or_create()
    data = [
        {"id": 1, "d": date(2025, 2, 10)},
        {"id": 2, "d": None},
    ]
    schema = [("id", "int"), ("d", "date")]
    df = spark._create_dataframe_from_rows(data, schema)
    out = df.order_by(["id"]).collect()
    assert len(out) == 2
    assert out[0]["id"] == 1 and out[0]["d"] is not None
    assert out[1]["id"] == 2 and out[1]["d"] is None


def test_execute_plan_accepts_datetime_in_row() -> None:
    """execute_plan with datetime in row data works (plan path uses same py_to_json_value)."""
    import json

    import robin_sparkless as rs

    data = [
        {"id": 1, "ts": datetime(2025, 2, 10, 12, 0, 0)},
        {"id": 2, "ts": None},
    ]
    schema = [("id", "int"), ("ts", "timestamp")]
    plan = [
        {
            "op": "filter",
            "payload": {"op": "gt", "left": {"col": "id"}, "right": {"lit": 0}},
        }
    ]
    plan_json = json.dumps(plan)
    df = rs._execute_plan(data, schema, plan_json)
    out = df.collect()
    assert len(out) == 2

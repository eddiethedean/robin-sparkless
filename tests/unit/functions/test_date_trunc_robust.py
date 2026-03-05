"""
Robust tests for the date_trunc function across backends.

These tests are designed to run against both:
  - The real PySpark backend (SPARKLESS_TEST_BACKEND=pyspark)
  - The Sparkless backends (mock / polars)

They focus on positive, parity-style behavior that should be consistent
across implementations rather than backend-specific details like the
exact Python type (date vs datetime). The assertions are written to be
flexible enough to accommodate minor type differences while still
validating the important semantics.
"""

from __future__ import annotations

import datetime as _dt

from tests.fixtures.spark_imports import get_spark_imports

imports = get_spark_imports()
F = imports.F


def _as_datetime(value: object) -> _dt.datetime:
    """Normalize date/datetime-like values to a datetime for comparison."""
    if isinstance(value, _dt.datetime):
        return value
    if isinstance(value, _dt.date):
        return _dt.datetime.combine(value, _dt.time())
    raise AssertionError(
        f"Unexpected value type for datetime comparison: {type(value)!r}"
    )


class TestDateTruncRobust:
    """Backend-agnostic behavioral tests for F.date_trunc."""

    def test_date_trunc_timestamp_core_units(self, spark) -> None:
        """date_trunc should correctly truncate timestamps to year/month/day/hour."""

        df = spark.createDataFrame(
            [
                {"ts": _dt.datetime(2024, 3, 15, 10, 30, 45)},
            ]
        )

        result = df.select(
            F.date_trunc("year", "ts").alias("year_trunc"),
            F.date_trunc("month", "ts").alias("month_trunc"),
            F.date_trunc("day", "ts").alias("day_trunc"),
            F.date_trunc("hour", "ts").alias("hour_trunc"),
        ).collect()

        assert len(result) == 1
        row = result[0]

        year_trunc = _as_datetime(row["year_trunc"])
        month_trunc = _as_datetime(row["month_trunc"])
        day_trunc = _as_datetime(row["day_trunc"])
        hour_trunc = _as_datetime(row["hour_trunc"])

        # year truncates to Jan 1 at midnight
        assert year_trunc.year == 2024
        assert year_trunc.month == 1
        assert year_trunc.day == 1
        assert year_trunc.hour == 0
        assert year_trunc.minute == 0
        assert year_trunc.second == 0

        # month truncates to first of the same month at midnight
        assert month_trunc.year == 2024
        assert month_trunc.month == 3
        assert month_trunc.day == 1
        assert month_trunc.hour == 0
        assert month_trunc.minute == 0
        assert month_trunc.second == 0

        # day truncates to same calendar day at midnight
        assert day_trunc.date() == _dt.date(2024, 3, 15)
        assert day_trunc.hour == 0
        assert day_trunc.minute == 0
        assert day_trunc.second == 0

        # hour truncates to same day/hour with minutes/seconds zeroed
        assert hour_trunc.year == 2024
        assert hour_trunc.month == 3
        assert hour_trunc.day == 15
        assert hour_trunc.hour == 10
        assert hour_trunc.minute == 0
        assert hour_trunc.second == 0

    def test_date_trunc_on_date_column(self, spark) -> None:
        """date_trunc on a date column should truncate the calendar component correctly."""

        df = spark.createDataFrame(
            [
                {"d": _dt.date(2024, 3, 15)},
            ]
        )

        result = df.select(
            F.date_trunc("year", "d").alias("year_trunc"),
            F.date_trunc("month", "d").alias("month_trunc"),
            F.date_trunc("day", "d").alias("day_trunc"),
        ).collect()

        assert len(result) == 1
        row = result[0]

        year_trunc = _as_datetime(row["year_trunc"])
        month_trunc = _as_datetime(row["month_trunc"])
        day_trunc = _as_datetime(row["day_trunc"])

        assert year_trunc.date() == _dt.date(2024, 1, 1)
        assert month_trunc.date() == _dt.date(2024, 3, 1)
        assert day_trunc.date() == _dt.date(2024, 3, 15)

    def test_date_trunc_preserves_nulls(self, spark) -> None:
        """date_trunc should propagate null inputs to null outputs."""

        df = spark.createDataFrame(
            [
                {"ts": None},
                {"ts": _dt.datetime(2024, 3, 15, 10, 30, 45)},
            ]
        )

        result = df.select(
            F.date_trunc("month", "ts").alias("month_trunc"),
        ).collect()

        assert len(result) == 2

        # First row: None input -> None output
        assert result[0]["month_trunc"] is None

        # Second row: valid datetime -> truncated to first of month
        month_trunc = _as_datetime(result[1]["month_trunc"])
        assert month_trunc.date() == _dt.date(2024, 3, 1)

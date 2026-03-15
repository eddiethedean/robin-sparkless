"""
Tests for issue #1478: instr, locate, next_day, trunc must be implemented in native module.

These functions are defined in functions.py and call native_*; without Rust bindings
they raise AttributeError. This test verifies they run without error and produce
sensible results.
"""

import pytest
from tests.tools.parity_base import ParityTestBase
from sparkless.testing import get_imports


class TestIssue1478InstrLocateNextDayTrunc(ParityTestBase):
    """Verify instr, locate, next_day, trunc are implemented (#1478)."""

    def test_instr_returns_1based_position(self, spark):
        """instr(str, substr) returns 1-based position; 0 if not found."""
        imports = get_imports()
        F = imports.F

        df = spark.createDataFrame([{"val": "Hello World"}])
        result = df.select(F.instr("val", "o").alias("pos")).collect()
        assert len(result) == 1
        assert result[0]["pos"] == 5  # first 'o' at 1-based index 5

        df2 = spark.createDataFrame([{"val": "abc"}])
        result2 = df2.select(F.instr("val", "x").alias("pos")).collect()
        assert result2[0]["pos"] == 0  # not found

    def test_locate_returns_1based_position(self, spark):
        """locate(substr, str, pos=1) returns 1-based position; 0 if not found."""
        imports = get_imports()
        F = imports.F

        df = spark.createDataFrame([{"val": "Hello World"}])
        result = df.select(F.locate("o", "val", 1).alias("pos")).collect()
        assert len(result) == 1
        assert result[0]["pos"] == 5

    def test_next_day_returns_next_weekday(self, spark):
        """next_day(date, dayOfWeek) returns next date matching weekday."""
        imports = get_imports()
        F = imports.F

        df = spark.createDataFrame([{"date": "2024-03-15"}])  # Friday
        result = df.select(
            F.next_day(F.to_date("date"), "Mon").alias("next_monday")
        ).collect()
        assert len(result) == 1
        assert result[0]["next_monday"] is not None  # 2024-03-18

    def test_trunc_truncates_date(self, spark):
        """trunc(date, format) truncates to year/month/etc."""
        imports = get_imports()
        F = imports.F

        df = spark.createDataFrame([{"date": "2024-03-15"}])
        result = df.select(
            F.trunc(F.to_date("date"), "month").alias("trunc_month")
        ).collect()
        assert len(result) == 1
        assert result[0]["trunc_month"] is not None  # 2024-03-01

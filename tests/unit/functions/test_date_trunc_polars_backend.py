"""
Tests for date_trunc (PySpark API).

Asserts PySpark behavior: date_trunc('month', date_column) works and returns
truncated date/datetime. Any backend must match this behavior.
"""

from __future__ import annotations

import datetime as _dt

from sparkless.testing import get_imports

imports = get_imports()
SparkSession = imports.SparkSession
F = imports.F


class TestDateTruncPolarsBackend:
    """Tests for F.date_trunc (PySpark API)."""

    def test_date_trunc_month_on_date_column(self, spark) -> None:
        """date_trunc('month', to_date(col)) should materialize without error (PySpark behavior)."""
        df = spark.createDataFrame([("2024-03-15",)], ["d"])
        df = df.withColumn("d", F.to_date("d")).withColumn(
            "month", F.date_trunc("month", F.col("d"))
        )

        rows = df.collect()
        assert len(rows) == 1

        month_value = rows[0]["month"]
        assert isinstance(month_value, (_dt.date, _dt.datetime))
        if isinstance(month_value, _dt.datetime):
            assert month_value.date() == _dt.date(2024, 3, 1)
        else:
            assert month_value == _dt.date(2024, 3, 1)

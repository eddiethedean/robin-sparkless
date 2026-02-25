"""
Tests for date_trunc when using the Polars backend.

These tests focus on the runtime behavior that previously raised
``ValueError: Unsupported function: date_trunc`` in the Polars
expression translator (see GitHub issue #465).
"""

from __future__ import annotations

import datetime as _dt

from tests.fixtures.spark_imports import get_spark_imports

imports = get_spark_imports()
SparkSession = imports.SparkSession
F = imports.F


class TestDateTruncPolarsBackend:
    """Tests for F.date_trunc with backend_type='polars'."""

    def test_date_trunc_month_on_date_column(self) -> None:
        """date_trunc('month', to_date(col)) should materialize without error.

        This mirrors the repro from issue #465:

            df = spark.createDataFrame([('2024-03-15',)], ['d'])
            df = df.withColumn('d', F.to_date('d')).withColumn(
                'month', F.date_trunc('month', F.col('d'))
            )
            df.show()
        """

        spark = SparkSession("test-date-trunc-polars", backend_type="polars")
        df = spark.createDataFrame([("2024-03-15",)], ["d"])

        df_truncated = df.withColumn("d", F.to_date("d")).withColumn(
            "month", F.date_trunc("month", F.col("d"))
        )

        rows = df_truncated.collect()
        assert len(rows) == 1

        month_value = rows[0]["month"]

        # Accept both date and datetime with the correct truncated date.
        assert isinstance(month_value, (_dt.date, _dt.datetime))
        if isinstance(month_value, _dt.datetime):
            assert month_value.date() == _dt.date(2024, 3, 1)
        else:
            assert month_value == _dt.date(2024, 3, 1)

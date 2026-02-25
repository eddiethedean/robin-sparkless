"""Test issue #329: log() function with float constants as base.

This test verifies that F.log() correctly supports float constants as the base
argument, matching PySpark's behavior.
"""

from sparkless.sql import SparkSession
import sparkless.sql.functions as F


class TestIssue329LogFloatConstant:
    """Test log() function with float constants."""

    def _get_unique_app_name(self, test_name: str) -> str:
        """Generate unique app name for parallel test execution."""
        import os
        import threading

        thread_id = threading.current_thread().ident
        process_id = os.getpid()
        return f"{test_name}_{process_id}_{thread_id}"

    def test_log_with_float_base(self):
        """Test log with float constant as base (issue example)."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Value": 100.0},
                    {"Value": 1000.0},
                ]
            )

            # PySpark signature: log(base, column)
            result = df.select(
                "Value",
                F.log(10.0, F.col("Value")).alias("Log10"),
            )

            rows = result.collect()
            assert len(rows) == 2

            # log10(100) = 2.0, log10(1000) = 3.0
            row1 = [r for r in rows if r["Value"] == 100.0][0]
            row2 = [r for r in rows if r["Value"] == 1000.0][0]

            assert abs(row1["Log10"] - 2.0) < 0.0001
            assert abs(row2["Log10"] - 3.0) < 0.0001
        finally:
            spark.stop()

    def test_log_with_int_base(self):
        """Test log with int constant as base."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Value": 8.0},
                ]
            )

            # log base 2
            result = df.select(F.log(2, F.col("Value")).alias("Log2"))

            rows = result.collect()
            assert len(rows) == 1
            # log2(8) = 3.0
            assert abs(rows[0]["Log2"] - 3.0) < 0.0001
        finally:
            spark.stop()

    def test_log_natural_log(self):
        """Test log without base (natural logarithm)."""
        import inspect
        import math

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Value": math.e},
                ]
            )

            # Natural log: log(column)
            result = df.select(F.log(F.col("Value")).alias("Ln"))

            rows = result.collect()
            assert len(rows) == 1
            # ln(e) = 1.0
            assert abs(rows[0]["Ln"] - 1.0) < 0.0001
        finally:
            spark.stop()

    def test_log_with_different_bases(self):
        """Test log with different base values."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Value": 100.0},
                ]
            )

            # Test different bases
            result = df.select(
                F.log(10.0, F.col("Value")).alias("Log10"),
                F.log(2.0, F.col("Value")).alias("Log2"),
                F.log(3.0, F.col("Value")).alias("Log3"),
            )

            rows = result.collect()
            assert len(rows) == 1
            row = rows[0]

            # log10(100) = 2.0
            assert abs(row["Log10"] - 2.0) < 0.0001
            # log2(100) ≈ 6.644
            assert abs(row["Log2"] - 6.644) < 0.01
            # log3(100) ≈ 4.192
            assert abs(row["Log3"] - 4.192) < 0.01
        finally:
            spark.stop()

    def test_log_with_column_base(self):
        """Test log with Column as base (existing behavior)."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Value": 100.0, "Base": 10.0},
                ]
            )

            # log with column base
            result = df.select(F.log(F.col("Base"), F.col("Value")).alias("LogBase"))

            rows = result.collect()
            assert len(rows) == 1
            # log10(100) = 2.0
            assert abs(rows[0]["LogBase"] - 2.0) < 0.0001
        finally:
            spark.stop()

    def test_log_with_null_values(self):
        """Test log with null values."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Value": 100.0},
                    {"Value": None},
                ]
            )

            result = df.select(
                "Value",
                F.log(10.0, F.col("Value")).alias("Log10"),
            )

            rows = result.collect()
            assert len(rows) == 2

            row1 = [r for r in rows if r["Value"] == 100.0][0]
            row2 = [r for r in rows if r["Value"] is None][0]

            assert abs(row1["Log10"] - 2.0) < 0.0001
            assert row2["Log10"] is None
        finally:
            spark.stop()

    def test_log_in_with_column(self):
        """Test log with float base in withColumn context."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Value": 100.0},
                ]
            )

            df = df.withColumn("Log10", F.log(10.0, F.col("Value")))

            rows = df.collect()
            assert len(rows) == 1
            assert abs(rows[0]["Log10"] - 2.0) < 0.0001
        finally:
            spark.stop()

    def test_log_edge_cases(self):
        """Test log with edge case values."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Value": 1.0},
                    {"Value": 0.5},
                    {"Value": 2.0},
                ]
            )

            result = df.select(
                "Value",
                F.log(10.0, F.col("Value")).alias("Log10"),
                F.log(F.col("Value")).alias("Ln"),
            )

            rows = result.collect()
            assert len(rows) == 3

            # log10(1) = 0
            row1 = [r for r in rows if r["Value"] == 1.0][0]
            assert abs(row1["Log10"]) < 0.0001
            assert abs(row1["Ln"]) < 0.0001

            # log10(0.5) ≈ -0.301
            row2 = [r for r in rows if r["Value"] == 0.5][0]
            assert abs(row2["Log10"] - (-0.301)) < 0.01

            # log10(2) ≈ 0.301
            row3 = [r for r in rows if r["Value"] == 2.0][0]
            assert abs(row3["Log10"] - 0.301) < 0.01
        finally:
            spark.stop()

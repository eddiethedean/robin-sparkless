"""Test issue #373: F.round() on string column (PySpark implicit cast).

PySpark supports F.round() on string columns that contain numeric values.
Sparkless should cast to numeric before rounding to match PySpark.
"""

from sparkless.sql import SparkSession
import sparkless.sql.functions as F


class TestIssue373RoundString:
    """Test round() on string column."""

    def _get_unique_app_name(self, test_name: str) -> str:
        """Generate unique app name for parallel test execution."""
        import os
        import threading

        thread_id = threading.current_thread().ident
        process_id = os.getpid()
        return f"{test_name}_{process_id}_{thread_id}"

    def test_round_string_column(self):
        """Test round on string column with numeric content (issue example)."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": "10.4"},
                    {"Name": "Bob", "Value": "9.6"},
                ]
            )
            df = df.withColumn("Value", F.round("Value"))
            rows = df.collect()
            assert len(rows) == 2
            # PySpark: 10.4 -> 10.0, 9.6 -> 10.0
            assert rows[0]["Value"] == 10.0
            assert rows[1]["Value"] == 10.0
        finally:
            spark.stop()

    def test_round_string_with_decimals(self):
        """Test round on string with specified decimal places."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame([{"val": "3.14159"}, {"val": "2.71828"}])
            df = df.withColumn("rounded", F.round("val", 2))
            rows = df.collect()
            assert rows[0]["rounded"] == 3.14
            assert rows[1]["rounded"] == 2.72
        finally:
            spark.stop()

    def test_round_string_negative_numbers(self):
        """Test round on string with negative numbers."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame([{"val": "-10.7"}, {"val": "-5.3"}])
            df = df.withColumn("rounded", F.round("val"))
            rows = df.collect()
            assert rows[0]["rounded"] == -11.0
            assert rows[1]["rounded"] == -5.0
        finally:
            spark.stop()

    def test_round_string_scientific_notation(self):
        """Test round on string with scientific notation."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame([{"val": "1.23e2"}, {"val": "4.56e-1"}])
            df = df.withColumn("rounded", F.round("val", 1))
            rows = df.collect()
            assert rows[0]["rounded"] == 123.0
            assert rows[1]["rounded"] == 0.5
        finally:
            spark.stop()

    def test_round_string_with_whitespace(self):
        """Test round on string with leading/trailing whitespace (issue #378)."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame([{"val": "  10.5  "}, {"val": "\t20.7\n"}])
            df = df.withColumn("rounded", F.round("val"))
            rows = df.collect()
            assert rows[0]["rounded"] == 10.0 or rows[0]["rounded"] == 11.0
            assert rows[1]["rounded"] == 21.0 or rows[1]["rounded"] == 20.0
        finally:
            spark.stop()

    def test_round_string_integer_strings(self):
        """Test round on string containing integers."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame([{"val": "42"}, {"val": "100"}])
            df = df.withColumn("rounded", F.round("val"))
            rows = df.collect()
            assert rows[0]["rounded"] == 42.0
            assert rows[1]["rounded"] == 100.0
        finally:
            spark.stop()

    def test_round_mixed_string_numeric_columns(self):
        """Test round works on both string and numeric columns."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame([{"str_val": "10.7", "num_val": 20.3}])
            df = df.withColumn("str_rounded", F.round("str_val"))
            df = df.withColumn("num_rounded", F.round("num_val"))
            rows = df.collect()
            assert rows[0]["str_rounded"] == 11.0
            assert rows[0]["num_rounded"] == 20.0
        finally:
            spark.stop()

    def test_round_string_zero(self):
        """Test round on string '0' and '0.0'."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame([{"val": "0"}, {"val": "0.0"}, {"val": "-0.0"}])
            df = df.withColumn("rounded", F.round("val"))
            rows = df.collect()
            assert rows[0]["rounded"] == 0.0
            assert rows[1]["rounded"] == 0.0
            assert rows[2]["rounded"] == 0.0
        finally:
            spark.stop()

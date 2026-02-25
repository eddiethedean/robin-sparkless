"""
PySpark parity tests for format_string function.

Tests validate that Sparkless format_string function behaves identically to PySpark.
"""

from tests.fixtures.parity_base import ParityTestBase
from tests.fixtures.spark_imports import get_spark_imports


class TestFormatStringParity(ParityTestBase):
    """Test format_string function parity with PySpark."""

    def test_format_string_basic_parity(self, spark):
        """Test format_string basic functionality matches PySpark (issue #326)."""
        imports = get_spark_imports()
        F = imports.F

        # Exact example from issue #326
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "StringValue": "abc", "IntegerValue": 123},
                {"Name": "Bob", "StringValue": "def", "IntegerValue": 456},
            ]
        )

        result = df.withColumn(
            "NewValue",
            F.format_string("%s-%s", F.col("StringValue"), F.col("IntegerValue")),
        )

        rows = result.collect()
        assert len(rows) == 2

        # Find rows by Name to avoid order dependency
        row_alice = [r for r in rows if r["Name"] == "Alice"][0]
        row_bob = [r for r in rows if r["Name"] == "Bob"][0]

        # Expected results from PySpark
        assert row_alice["NewValue"] == "abc-123"
        assert row_bob["NewValue"] == "def-456"

    def test_format_string_multiple_args_parity(self, spark):
        """Test format_string with multiple arguments matches PySpark."""
        imports = get_spark_imports()
        F = imports.F

        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Age": 25, "City": "NYC"},
                {"Name": "Bob", "Age": 30, "City": "LA"},
            ]
        )

        result = df.withColumn(
            "Info",
            F.format_string(
                "%s is %d years old and lives in %s",
                F.col("Name"),
                F.col("Age"),
                F.col("City"),
            ),
        )

        rows = result.collect()
        assert len(rows) == 2

        row_alice = [r for r in rows if r["Name"] == "Alice"][0]
        row_bob = [r for r in rows if r["Name"] == "Bob"][0]

        assert row_alice["Info"] == "Alice is 25 years old and lives in NYC"
        assert row_bob["Info"] == "Bob is 30 years old and lives in LA"

    def test_format_string_with_null_parity(self, spark):
        """Test format_string with null values matches PySpark."""
        imports = get_spark_imports()
        F = imports.F

        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": "abc", "Number": 123},
                {"Name": "Bob", "Value": None, "Number": 456},
            ]
        )

        result = df.withColumn(
            "NewValue",
            F.format_string("%s-%s", F.col("Value"), F.col("Number")),
        )

        rows = result.collect()
        assert len(rows) == 2

        row_alice = [r for r in rows if r["Name"] == "Alice"][0]
        row_bob = [r for r in rows if r["Name"] == "Bob"][0]

        # PySpark converts None to "null" string in format_string
        assert row_alice["NewValue"] == "abc-123"
        assert row_bob["NewValue"] == "null-456"

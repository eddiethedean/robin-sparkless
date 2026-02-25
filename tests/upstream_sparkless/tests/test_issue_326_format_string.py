"""Test issue #326: format_string function support.

This test verifies that F.format_string() correctly formats strings using
printf-style placeholders with column values.
"""

from sparkless.sql import SparkSession
import sparkless.sql.functions as F


class TestIssue326FormatString:
    """Test format_string function."""

    def _get_unique_app_name(self, test_name: str) -> str:
        """Generate unique app name for parallel test execution."""
        import os
        import threading

        thread_id = threading.current_thread().ident
        process_id = os.getpid()
        return f"{test_name}_{process_id}_{thread_id}"

    def test_format_string_basic(self):
        """Test basic format_string functionality (issue example)."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            # Exact example from issue #326
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "StringValue": "abc", "IntegerValue": 123},
                    {"Name": "Bob", "StringValue": "def", "IntegerValue": 456},
                ]
            )

            df = df.withColumn(
                "NewValue",
                F.format_string("%s-%s", F.col("StringValue"), F.col("IntegerValue")),
            )

            rows = df.collect()
            assert len(rows) == 2

            # Find rows by Name to avoid order dependency
            row_alice = [r for r in rows if r["Name"] == "Alice"][0]
            row_bob = [r for r in rows if r["Name"] == "Bob"][0]

            # Expected: "abc-123" and "def-456"
            assert row_alice["NewValue"] == "abc-123", (
                f"Expected 'abc-123', got {row_alice['NewValue']}"
            )
            assert row_bob["NewValue"] == "def-456", (
                f"Expected 'def-456', got {row_bob['NewValue']}"
            )
        finally:
            spark.stop()

    def test_format_string_multiple_columns(self):
        """Test format_string with multiple columns."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Age": 25, "City": "NYC"},
                    {"Name": "Bob", "Age": 30, "City": "LA"},
                ]
            )

            df = df.withColumn(
                "Info",
                F.format_string(
                    "%s is %d years old and lives in %s",
                    F.col("Name"),
                    F.col("Age"),
                    F.col("City"),
                ),
            )

            rows = df.collect()
            assert len(rows) == 2

            row_alice = [r for r in rows if r["Name"] == "Alice"][0]
            row_bob = [r for r in rows if r["Name"] == "Bob"][0]

            assert row_alice["Info"] == "Alice is 25 years old and lives in NYC"
            assert row_bob["Info"] == "Bob is 30 years old and lives in LA"
        finally:
            spark.stop()

    def test_format_string_with_null_values(self):
        """Test format_string with null values."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": "abc", "Number": 123},
                    {"Name": "Bob", "Value": None, "Number": 456},
                    {"Name": "Charlie", "Value": "def", "Number": None},
                ]
            )

            df = df.withColumn(
                "NewValue",
                F.format_string("%s-%s", F.col("Value"), F.col("Number")),
            )

            rows = df.collect()
            assert len(rows) == 3

            row_alice = [r for r in rows if r["Name"] == "Alice"][0]
            row_bob = [r for r in rows if r["Name"] == "Bob"][0]
            row_charlie = [r for r in rows if r["Name"] == "Charlie"][0]

            # PySpark converts None to "null" string in format_string
            assert row_alice["NewValue"] == "abc-123"
            assert row_bob["NewValue"] == "null-456"  # None -> "null" string
            assert row_charlie["NewValue"] == "def-null"  # None -> "null" string
        finally:
            spark.stop()

    def test_format_string_in_select(self):
        """Test format_string in select context."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "StringValue": "abc", "IntegerValue": 123},
                    {"Name": "Bob", "StringValue": "def", "IntegerValue": 456},
                ]
            )

            df = df.select(
                "Name",
                F.format_string(
                    "%s-%s", F.col("StringValue"), F.col("IntegerValue")
                ).alias("NewValue"),
            )

            rows = df.collect()
            assert len(rows) == 2

            row_alice = [r for r in rows if r["Name"] == "Alice"][0]
            row_bob = [r for r in rows if r["Name"] == "Bob"][0]

            assert row_alice["NewValue"] == "abc-123"
            assert row_bob["NewValue"] == "def-456"
        finally:
            spark.stop()

    def test_format_string_different_format_specifiers(self):
        """Test format_string with different format specifiers."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Age": 25, "Salary": 50000.5},
                    {"Name": "Bob", "Age": 30, "Salary": 75000.75},
                ]
            )

            # Test %s (string), %d (integer), %f (float)
            df = df.withColumn(
                "Info",
                F.format_string(
                    "Name: %s, Age: %d, Salary: %.2f",
                    F.col("Name"),
                    F.col("Age"),
                    F.col("Salary"),
                ),
            )

            rows = df.collect()
            assert len(rows) == 2

            row_alice = [r for r in rows if r["Name"] == "Alice"][0]
            row_bob = [r for r in rows if r["Name"] == "Bob"][0]

            # Note: Python % formatting behavior
            assert "Name: Alice" in row_alice["Info"]
            assert "Age: 25" in row_alice["Info"]
            assert "Name: Bob" in row_bob["Info"]
            assert "Age: 30" in row_bob["Info"]
        finally:
            spark.stop()

    def test_format_string_empty_strings(self):
        """Test format_string with empty strings."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": "", "Number": 123},
                    {"Name": "Bob", "Value": "def", "Number": 456},
                ]
            )

            df = df.withColumn(
                "NewValue",
                F.format_string("%s-%s", F.col("Value"), F.col("Number")),
            )

            rows = df.collect()
            assert len(rows) == 2

            row_alice = [r for r in rows if r["Name"] == "Alice"][0]
            row_bob = [r for r in rows if r["Name"] == "Bob"][0]

            assert (
                row_alice["NewValue"] == "-123"
            )  # Empty string (not None, so not "null")
            assert row_bob["NewValue"] == "def-456"
        finally:
            spark.stop()

    def test_format_string_many_columns(self):
        """Test format_string with many columns (5+)."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {
                        "A": "one",
                        "B": 2,
                        "C": 3.0,
                        "D": "four",
                        "E": 5,
                        "F": "six",
                    },
                ]
            )

            df = df.withColumn(
                "Result",
                F.format_string(
                    "%s-%d-%.1f-%s-%d-%s",
                    F.col("A"),
                    F.col("B"),
                    F.col("C"),
                    F.col("D"),
                    F.col("E"),
                    F.col("F"),
                ),
            )

            rows = df.collect()
            assert len(rows) == 1
            assert rows[0]["Result"] == "one-2-3.0-four-5-six"
        finally:
            spark.stop()

    def test_format_string_numeric_edge_cases(self):
        """Test format_string with numeric edge cases (zero, negative, large numbers)."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {
                        "Int": 0,
                        "Neg": -42,
                        "Large": 999999,
                        "Float": 0.0,
                        "NegFloat": -3.14,
                    },
                ]
            )

            df = df.withColumn(
                "Result",
                F.format_string(
                    "%d|%d|%d|%.2f|%.2f",
                    F.col("Int"),
                    F.col("Neg"),
                    F.col("Large"),
                    F.col("Float"),
                    F.col("NegFloat"),
                ),
            )

            rows = df.collect()
            assert len(rows) == 1
            result = rows[0]["Result"]
            assert "0|" in result
            assert "-42|" in result
            assert "999999|" in result
            assert "0.00|" in result
            assert "-3.14" in result
        finally:
            spark.stop()

    def test_format_string_unicode(self):
        """Test format_string with Unicode characters."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "José", "City": "São Paulo", "Emoji": "🎉"},
                ]
            )

            df = df.withColumn(
                "Result",
                F.format_string(
                    "%s from %s says %s",
                    F.col("Name"),
                    F.col("City"),
                    F.col("Emoji"),
                ),
            )

            rows = df.collect()
            assert len(rows) == 1
            result = rows[0]["Result"]
            assert "José" in result
            assert "São Paulo" in result
            assert "🎉" in result
        finally:
            spark.stop()

    def test_format_string_special_characters_in_format(self):
        """Test format_string with special characters in format string."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"A": "test", "B": 123},
                ]
            )

            df = df.withColumn(
                "Result",
                F.format_string(
                    "Value: %s | Number: %d | End",
                    F.col("A"),
                    F.col("B"),
                ),
            )

            rows = df.collect()
            assert len(rows) == 1
            result = rows[0]["Result"]
            assert result == "Value: test | Number: 123 | End"
        finally:
            spark.stop()

    def test_format_string_all_null(self):
        """Test format_string when all columns are null."""
        import inspect
        from sparkless.sql.types import StructType, StructField, StringType

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            # Provide explicit schema since all values are null
            schema = StructType(
                [
                    StructField("A", StringType(), True),
                    StructField("B", StringType(), True),
                    StructField("C", StringType(), True),
                ]
            )
            df = spark.createDataFrame(
                [
                    {"A": None, "B": None, "C": None},
                ],
                schema=schema,
            )

            df = df.withColumn(
                "Result",
                F.format_string(
                    "%s-%s-%s",
                    F.col("A"),
                    F.col("B"),
                    F.col("C"),
                ),
            )

            rows = df.collect()
            assert len(rows) == 1
            # All nulls should become "null" strings
            assert rows[0]["Result"] == "null-null-null"
        finally:
            spark.stop()

    def test_format_string_mixed_types(self):
        """Test format_string with mixed data types."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {
                        "Str": "hello",
                        "Int": 42,
                        "Float": 3.14159,
                        "Bool": True,
                    },
                ]
            )

            df = df.withColumn(
                "Result",
                F.format_string(
                    "%s-%d-%.2f-%s",
                    F.col("Str"),
                    F.col("Int"),
                    F.col("Float"),
                    F.col("Bool"),
                ),
            )

            rows = df.collect()
            assert len(rows) == 1
            result = rows[0]["Result"]
            assert "hello-42-3.14" in result
            assert "True" in result or "true" in result
        finally:
            spark.stop()

    def test_format_string_format_specifiers(self):
        """Test format_string with various format specifiers (%x, %o, %e, etc.)."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Dec": 255, "Hex": 255, "Oct": 64},
                ]
            )

            # Test hex and octal formatting
            df = df.withColumn(
                "HexResult",
                F.format_string("%x", F.col("Hex")),
            )
            df = df.withColumn(
                "OctResult",
                F.format_string("%o", F.col("Oct")),
            )

            rows = df.collect()
            assert len(rows) == 1
            # Hex: 255 = ff
            assert rows[0]["HexResult"] == "ff" or rows[0]["HexResult"] == "FF"
            # Oct: 64 = 100
            assert rows[0]["OctResult"] == "100"
        finally:
            spark.stop()

    def test_format_string_precision_formatting(self):
        """Test format_string with precision formatting (%.3f, %05d, etc.)."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Num": 42, "Float": 3.14159265},
                ]
            )

            df = df.withColumn(
                "Padded",
                F.format_string("%05d", F.col("Num")),
            )
            df = df.withColumn(
                "Precise",
                F.format_string("%.3f", F.col("Float")),
            )

            rows = df.collect()
            assert len(rows) == 1
            assert rows[0]["Padded"] == "00042"
            assert rows[0]["Precise"] == "3.142"
        finally:
            spark.stop()

    def test_format_string_long_strings(self):
        """Test format_string with very long strings."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            long_str = "x" * 1000
            df = spark.createDataFrame(
                [
                    {"Long": long_str, "Num": 123},
                ]
            )

            df = df.withColumn(
                "Result",
                F.format_string("%s-%d", F.col("Long"), F.col("Num")),
            )

            rows = df.collect()
            assert len(rows) == 1
            result = rows[0]["Result"]
            assert len(result) == 1004  # 1000 + "-" + "123"
            assert result.startswith("x" * 1000)
            assert result.endswith("-123")
        finally:
            spark.stop()

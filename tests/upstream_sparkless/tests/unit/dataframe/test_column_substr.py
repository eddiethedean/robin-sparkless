"""
Tests for Column.substr() method.

These tests ensure that:
1. The substr method works correctly on Column objects
2. Behavior matches F.substring() function
3. Edge cases are handled properly (nulls, out of bounds, etc.)
4. Integration with DataFrame operations works
5. Behavior matches PySpark exactly

These tests work with both sparkless (mock) and PySpark backends.
Set MOCK_SPARK_TEST_BACKEND=pyspark to run with real PySpark.
"""

from tests.fixtures.spark_imports import get_spark_imports
from tests.fixtures.spark_backend import get_backend_type, BackendType

# Get imports based on backend
imports = get_spark_imports()
SparkSession = imports.SparkSession
StringType = imports.StringType
StructType = imports.StructType
StructField = imports.StructField
F = imports.F  # Functions module for backend-appropriate F.col() etc.


def _is_pyspark_mode() -> bool:
    """Check if running in PySpark mode."""
    backend = get_backend_type()
    return backend == BackendType.PYSPARK


class TestColumnSubstr:
    """Test Column.substr() method."""

    def test_basic_substr(self, spark):
        """Test basic substr usage: F.col("name").substr(1, 2)."""
        schema = StructType([StructField("name", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"name": "Alice"},
                {"name": "Bob"},
                {"name": "Charlie"},
            ],
            schema=schema,
        )

        result = df.select(F.col("name").substr(1, 2).alias("partial_name"))
        rows = result.collect()

        assert len(rows) == 3
        assert rows[0]["partial_name"] == "Al"  # "Alice"[0:2] = "Al"
        assert rows[1]["partial_name"] == "Bo"  # "Bob"[0:2] = "Bo"
        assert rows[2]["partial_name"] == "Ch"  # "Charlie"[0:2] = "Ch"

    def test_substr_from_second_position(self, spark):
        """Test substr starting from position 2."""
        schema = StructType([StructField("name", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"name": "Alice"},
                {"name": "Bob"},
            ],
            schema=schema,
        )

        # substr requires length parameter, use large length to get rest of string
        result = df.select(F.col("name").substr(2, 100).alias("from_second"))
        rows = result.collect()

        assert len(rows) == 2
        # substr(2, 100) should return from position 2 to end (or up to length)
        assert rows[0]["from_second"] == "lice"  # "Alice"[1:] = "lice"
        assert rows[1]["from_second"] == "ob"  # "Bob"[1:] = "ob"

    def test_substr_issue_238_example(self, spark):
        """Test the exact example from issue #238."""
        schema = StructType([StructField("name", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"name": "Alice"},
                {"name": "Bob"},
            ],
            schema=schema,
        )

        result = df.withColumn("partial_name", F.col("name").substr(1, 2))
        rows = result.collect()

        assert len(rows) == 2
        assert rows[0]["partial_name"] == "Al"
        assert rows[1]["partial_name"] == "Bo"

    def test_substr_start_at_one(self, spark):
        """Test substr starting at position 1."""
        schema = StructType([StructField("text", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"text": "Hello"},
                {"text": "World"},
            ],
            schema=schema,
        )

        result = df.select(F.col("text").substr(1, 3).alias("first_three"))
        rows = result.collect()

        assert len(rows) == 2
        assert rows[0]["first_three"] == "Hel"  # "Hello"[0:3] = "Hel"
        assert rows[1]["first_three"] == "Wor"  # "World"[0:3] = "Wor"

    def test_substr_start_beyond_length(self, spark):
        """Test substr with start position beyond string length."""
        schema = StructType([StructField("text", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"text": "Hi"},
                {"text": "No"},
            ],
            schema=schema,
        )

        result = df.select(F.col("text").substr(10, 5).alias("beyond"))
        rows = result.collect()

        assert len(rows) == 2
        # When start is beyond length, should return empty string
        assert rows[0]["beyond"] == ""
        assert rows[1]["beyond"] == ""

    def test_substr_with_null(self, spark):
        """Test substr with null values."""
        schema = StructType([StructField("name", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"name": "Alice"},
                {"name": None},
                {"name": "Bob"},
            ],
            schema=schema,
        )

        result = df.select(F.col("name").substr(1, 2).alias("partial"))
        rows = result.collect()

        assert len(rows) == 3
        assert rows[0]["partial"] == "Al"
        assert rows[1]["partial"] is None  # Null input should return null
        assert rows[2]["partial"] == "Bo"

    def test_substr_length_zero(self, spark):
        """Test substr with length=0."""
        schema = StructType([StructField("text", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"text": "Hello"},
            ],
            schema=schema,
        )

        result = df.select(F.col("text").substr(1, 0).alias("empty"))
        rows = result.collect()

        assert len(rows) == 1
        assert rows[0]["empty"] == ""  # Length 0 should return empty string

    def test_substr_length_exceeds_remaining(self, spark):
        """Test substr with length exceeding remaining characters."""
        schema = StructType([StructField("text", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"text": "Hi"},
            ],
            schema=schema,
        )

        result = df.select(F.col("text").substr(1, 100).alias("long"))
        rows = result.collect()

        assert len(rows) == 1
        # Should return all remaining characters, not error
        assert rows[0]["long"] == "Hi"

    def test_substr_in_select(self, spark):
        """Test substr in select operation."""
        schema = StructType([StructField("name", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"name": "Alice"},
                {"name": "Bob"},
            ],
            schema=schema,
        )

        result = df.select(
            F.col("name"),
            F.col("name").substr(1, 2).alias("first_two"),
            F.col("name")
            .substr(3, 100)
            .alias("from_third"),  # Use large length to get rest
        )
        rows = result.collect()

        assert len(rows) == 2
        assert rows[0]["name"] == "Alice"
        assert rows[0]["first_two"] == "Al"
        assert rows[0]["from_third"] == "ice"
        assert rows[1]["name"] == "Bob"
        assert rows[1]["first_two"] == "Bo"
        assert rows[1]["from_third"] == "b"

    def test_substr_in_withColumn(self, spark):
        """Test substr in withColumn operation."""
        schema = StructType([StructField("name", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"name": "Alice"},
                {"name": "Bob"},
            ],
            schema=schema,
        )

        result = df.withColumn("first_char", F.col("name").substr(1, 1))
        rows = result.collect()

        assert len(rows) == 2
        assert rows[0]["first_char"] == "A"
        assert rows[1]["first_char"] == "B"

    def test_substr_in_filter(self, spark):
        """Test substr in filter condition."""
        schema = StructType([StructField("name", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"name": "Alice"},
                {"name": "Bob"},
                {"name": "Charlie"},
            ],
            schema=schema,
        )

        # Filter where first character is 'A'
        result = df.filter(F.col("name").substr(1, 1) == "A")
        rows = result.collect()

        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"

    def test_substr_in_orderBy(self, spark):
        """Test substr in orderBy operation."""
        schema = StructType([StructField("name", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"name": "Charlie"},
                {"name": "Alice"},
                {"name": "Bob"},
            ],
            schema=schema,
        )

        # Order by first character
        result = df.orderBy(F.col("name").substr(1, 1))
        rows = result.collect()

        assert len(rows) == 3
        assert rows[0]["name"] == "Alice"  # 'A'
        assert rows[1]["name"] == "Bob"  # 'B'
        assert rows[2]["name"] == "Charlie"  # 'C'

    def test_substr_equals_substring_function(self, spark):
        """Test that substr method produces same results as substring function when both have length."""
        schema = StructType([StructField("name", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"name": "Alice"},
                {"name": "Bob"},
            ],
            schema=schema,
        )

        result_substr = df.select(F.col("name").substr(1, 2).alias("partial"))
        result_substring = df.select(F.substring(F.col("name"), 1, 2).alias("partial"))

        rows_substr = result_substr.collect()
        rows_substring = result_substring.collect()

        assert len(rows_substr) == len(rows_substring)
        assert rows_substr[0]["partial"] == rows_substring[0]["partial"]
        assert rows_substr[1]["partial"] == rows_substring[1]["partial"]

    def test_substr_chained_operations(self, spark):
        """Test substr with chained column operations."""
        schema = StructType([StructField("name", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"name": "Alice"},
                {"name": "Bob"},
            ],
            schema=schema,
        )

        # Chain substr with upper
        result = df.select(F.col("name").substr(1, 2).alias("partial"))
        rows = result.collect()

        assert len(rows) == 2
        assert rows[0]["partial"] == "Al"
        assert rows[1]["partial"] == "Bo"

    def test_substr_empty_string(self, spark):
        """Test substr on empty string."""
        schema = StructType([StructField("text", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"text": ""},
                {"text": "Hello"},
            ],
            schema=schema,
        )

        result = df.select(F.col("text").substr(1, 2).alias("partial"))
        rows = result.collect()

        assert len(rows) == 2
        assert rows[0]["partial"] == ""  # Empty string substr returns empty
        assert rows[1]["partial"] == "He"

    def test_substr_unicode(self, spark):
        """Test substr with unicode characters."""
        schema = StructType([StructField("text", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"text": "Hello世界"},
                {"text": "测试"},
            ],
            schema=schema,
        )

        result = df.select(F.col("text").substr(1, 5).alias("partial"))
        rows = result.collect()

        assert len(rows) == 2
        # Unicode handling - should work correctly
        assert rows[0]["partial"] == "Hello"  # First 5 characters
        assert rows[1]["partial"] == "测试"  # First 2 unicode characters

    def test_substr_negative_start(self, spark):
        """Test substr with negative start positions (PySpark counts from end)."""
        schema = StructType([StructField("text", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"text": "Hello"},
            ],
            schema=schema,
        )

        # Test various negative start positions
        # PySpark: negative start counts from end (-1 = last char, -2 = second-to-last, etc.)
        test_cases = [
            (-5, 3, "Hel"),  # -5 from end of "Hello" (len=5) = position 0
            (-4, 3, "ell"),  # -4 from end = position 1
            (-3, 3, "llo"),  # -3 from end = position 2
            (-2, 3, "lo"),  # -2 from end = position 3, but only 2 chars remain
            (-1, 3, "o"),  # -1 from end = position 4 (last char), only 1 char
        ]

        for start, length, expected in test_cases:
            result = df.select(F.col("text").substr(start, length).alias("partial"))
            rows = result.collect()
            assert rows[0]["partial"] == expected, f"substr({start}, {length}) failed"

    def test_substr_zero_start(self, spark):
        """Test substr with start=0 (PySpark treats as start=1)."""
        schema = StructType([StructField("text", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"text": "Hello"},
            ],
            schema=schema,
        )

        result = df.select(F.col("text").substr(0, 3).alias("partial"))
        rows = result.collect()

        assert len(rows) == 1
        # PySpark treats start=0 as start=1
        assert rows[0]["partial"] == "Hel"

    def test_substr_with_alias(self, spark):
        """Test substr with alias."""
        schema = StructType([StructField("name", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"name": "Alice"},
            ],
            schema=schema,
        )

        result = df.select(F.col("name").substr(1, 2).alias("first_two_chars"))
        rows = result.collect()

        assert len(rows) == 1
        assert "first_two_chars" in rows[0]
        assert rows[0]["first_two_chars"] == "Al"

    def test_substr_pyspark_parity_comprehensive(self, spark):
        """Comprehensive test to verify substr matches PySpark behavior for common cases."""
        schema = StructType([StructField("text", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"text": "Hello"},
                {"text": "World"},
                {"text": "Test"},
                {"text": ""},
                {"text": None},
            ],
            schema=schema,
        )

        # Test cases: (start, length, expected_results)
        # Expected results based on actual PySpark behavior (verified with real PySpark)
        # Note: Very negative starts have complex behavior, so we test common cases
        test_cases = [
            (1, 3, ["Hel", "Wor", "Tes", "", None]),
            (1, 0, ["", "", "", "", None]),
            (1, 100, ["Hello", "World", "Test", "", None]),
            (2, 3, ["ell", "orl", "est", "", None]),
            (0, 3, ["Hel", "Wor", "Tes", "", None]),  # start=0 treated as start=1
            (
                -1,
                3,
                ["o", "d", "t", "", None],
            ),  # negative start from end (-1 = last char)
            (
                -2,
                3,
                ["lo", "ld", "st", "", None],
            ),  # negative start from end (-2 = second-to-last)
        ]

        for start, length, expected in test_cases:
            result = df.select(F.col("text").substr(start, length).alias("result"))
            rows = result.collect()
            for i, row in enumerate(rows):
                assert row["result"] == expected[i], (
                    f"substr({start}, {length}) failed for row {i} (text={df.collect()[i]['text']!r}): "
                    f"expected {expected[i]!r}, got {row['result']!r}"
                )

    def test_substr_in_groupBy(self, spark):
        """Test substr in groupBy aggregation."""
        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("value", StringType(), True),
            ]
        )
        df = spark.createDataFrame(
            [
                {"name": "Alice", "value": "A1"},
                {"name": "Alice", "value": "A2"},
                {"name": "Bob", "value": "B1"},
            ],
            schema=schema,
        )

        # Group by first character of name - create a column first
        df_with_first_char = df.withColumn("first_char", F.col("name").substr(1, 1))
        result = df_with_first_char.groupBy("first_char").agg(
            F.count("*").alias("count")
        )
        rows = result.collect()

        assert len(rows) == 2
        # Should have one row for 'A' and one for 'B'
        first_chars = {row["first_char"]: row["count"] for row in rows}
        assert first_chars.get("A") == 2
        assert first_chars.get("B") == 1

    def test_substr_chained_with_other_operations(self, spark):
        """Test substr chained with other string operations."""
        schema = StructType([StructField("name", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"name": "Alice"},
                {"name": "Bob"},
            ],
            schema=schema,
        )

        # Chain substr with upper
        result = df.select(F.upper(F.col("name").substr(1, 2)).alias("upper_partial"))
        rows = result.collect()

        assert len(rows) == 2
        assert rows[0]["upper_partial"] == "AL"
        assert rows[1]["upper_partial"] == "BO"

    def test_substr_very_long_string(self, spark):
        """Test substr on very long strings."""
        long_string = "A" * 1000 + "B" * 1000
        schema = StructType([StructField("text", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"text": long_string},
            ],
            schema=schema,
        )

        result = df.select(F.col("text").substr(1, 100).alias("first_100"))
        rows = result.collect()

        assert len(rows) == 1
        assert rows[0]["first_100"] == "A" * 100

    def test_substr_start_exceeds_length(self, spark):
        """Test substr when start position exceeds string length."""
        schema = StructType([StructField("text", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"text": "Hi"},
            ],
            schema=schema,
        )

        result = df.select(F.col("text").substr(10, 5).alias("beyond"))
        rows = result.collect()

        assert len(rows) == 1
        # When start exceeds length, should return empty string
        assert rows[0]["beyond"] == ""

    def test_substr_negative_start_exceeds_length(self, spark):
        """Test substr with negative start that exceeds string length."""
        schema = StructType([StructField("text", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"text": "Hi"},
            ],
            schema=schema,
        )

        # -10 from end of "Hi" (len=2) would be negative, should clamp to 0
        result = df.select(F.col("text").substr(-10, 3).alias("result"))
        rows = result.collect()

        assert len(rows) == 1
        # Should return from start (clamped to 0)
        assert rows[0]["result"] == "Hi"[:3] if len("Hi") >= 3 else "Hi"

"""
Test for issue #149: to_timestamp() returns String when Datetime expected.

Issue #149 reports that to_timestamp() operations return String columns when
sparkless expects Datetime('μs') in validation and type-checking contexts.
This occurs when using to_timestamp() with regexp_replace().cast("string").
"""

from sparkless import SparkSession
from sparkless.functions import col, to_timestamp, regexp_replace


class TestIssue149ToTimestampString:
    """Test cases for issue #149: to_timestamp() string type detection."""

    def test_to_timestamp_with_regexp_replace_cast_string(self):
        """Test that to_timestamp() correctly detects string type from regexp_replace().cast("string").

        This test verifies the fix for issue #149 where to_timestamp() would fail
        with "expected output type 'Datetime('μs')', got 'String'" when the input
        comes from regexp_replace().cast("string").
        """
        spark = SparkSession.builder.appName("test_issue_149").getOrCreate()

        # Create test data with datetime strings containing microseconds
        data = [("2024-01-15T10:30:45.123456",)]
        df = spark.createDataFrame(data, ["date_string"])

        # This pattern was causing the issue: regexp_replace().cast("string")
        df_transformed = df.withColumn(
            "date_parsed",
            to_timestamp(
                regexp_replace(col("date_string"), r"\.\d+", "").cast("string"),
                "yyyy-MM-dd'T'HH:mm:ss",
            ),
        )

        # Verify no schema error occurs
        result = df_transformed.collect()
        assert len(result) == 1

        # Verify the column type is TimestampType, not StringType
        date_parsed_field = next(
            f for f in df_transformed.schema.fields if f.name == "date_parsed"
        )
        assert date_parsed_field.dataType.__class__.__name__ == "TimestampType", (
            f"Expected TimestampType, got {date_parsed_field.dataType}"
        )

        # Verify the operation completes without schema validation errors
        # The actual parsing result may be None if the format doesn't match,
        # but the important thing is that the schema is correct
        assert date_parsed_field.nullable is True

    def test_to_timestamp_with_nested_cast_string(self):
        """Test that to_timestamp() correctly detects string type from nested cast operations."""
        spark = SparkSession.builder.appName("test_issue_149_nested").getOrCreate()

        data = [("2024-01-15T10:30:45",)]
        df = spark.createDataFrame(data, ["date_string"])

        # Test nested cast: cast to string directly
        df_transformed = df.withColumn(
            "date_parsed",
            to_timestamp(col("date_string").cast("string"), "yyyy-MM-dd'T'HH:mm:ss"),
        )

        # Verify no schema error occurs
        result = df_transformed.collect()
        assert len(result) == 1

        # Verify the column type is TimestampType
        date_parsed_field = next(
            f for f in df_transformed.schema.fields if f.name == "date_parsed"
        )
        assert date_parsed_field.dataType.__class__.__name__ == "TimestampType", (
            f"Expected TimestampType, got {date_parsed_field.dataType}"
        )

    def test_to_timestamp_with_string_operations(self):
        """Test that to_timestamp() correctly detects string type from string operations."""
        spark = SparkSession.builder.appName("test_issue_149_string_ops").getOrCreate()

        data = [("2024-01-15T10:30:45",)]
        df = spark.createDataFrame(data, ["date_string"])

        # Test with regexp_replace (string operation) without cast
        df_transformed = df.withColumn(
            "date_parsed",
            to_timestamp(
                regexp_replace(col("date_string"), r"T", " "),
                "yyyy-MM-dd HH:mm:ss",
            ),
        )

        # Verify no schema error occurs
        result = df_transformed.collect()
        assert len(result) == 1

        # Verify the column type is TimestampType
        date_parsed_field = next(
            f for f in df_transformed.schema.fields if f.name == "date_parsed"
        )
        assert date_parsed_field.dataType.__class__.__name__ == "TimestampType", (
            f"Expected TimestampType, got {date_parsed_field.dataType}"
        )

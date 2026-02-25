"""
Test for issue #135: Datetime columns cause SchemaError when filtering.

Issue #135 reports that when creating datetime columns using to_timestamp()
and then attempting to validate or filter on those columns, sparkless raises
a SchemaError: expected String, got datetime[μs].
"""

from sparkless import SparkSession
from sparkless.functions import col, to_timestamp, regexp_replace
from datetime import datetime


class TestIssue135DatetimeFilter:
    """Test cases for issue #135: datetime column filtering."""

    def test_to_timestamp_with_filter_isnotnull(self):
        """Test that filtering on datetime columns created by to_timestamp() works.

        This is the exact scenario from issue #135.
        """
        spark = SparkSession.builder.appName("BugRepro").getOrCreate()
        try:
            data = [("imp1", "2024-01-15T10:30:00")]
            df = spark.createDataFrame(data, ["impression_id", "impression_date"])

            transformed = df.withColumn(
                "impression_date_parsed",
                to_timestamp(
                    regexp_replace(col("impression_date"), r"\.\d+", "").cast("string"),
                    "yyyy-MM-dd'T'HH:mm:ss",
                ),
            )

            # This should not fail with SchemaError
            validation_result = transformed.filter(
                col("impression_date_parsed").isNotNull()
            )
            count = validation_result.count()
            assert count == 1

            # Also test collect() to ensure materialization works
            rows = validation_result.collect()
            assert len(rows) == 1
            assert isinstance(rows[0]["impression_date_parsed"], datetime)
        finally:
            spark.stop()

    def test_to_timestamp_with_filter_isnull(self):
        """Test that filtering for null datetime columns works."""
        spark = SparkSession.builder.appName("BugRepro").getOrCreate()
        try:
            from sparkless.spark_types import StringType, StructType, StructField

            # Use explicit schema to avoid type inference issues with None values
            schema = StructType(
                [
                    StructField("impression_id", StringType(), False),
                    StructField("impression_date", StringType(), True),
                ]
            )
            data = [
                {"impression_id": "imp1", "impression_date": "2024-01-15T10:30:00"},
                {
                    "impression_id": "imp2",
                    "impression_date": None,
                },  # This will result in null after to_timestamp
                {
                    "impression_id": "imp3",
                    "impression_date": "invalid-date",
                },  # This will also result in null
            ]
            df = spark.createDataFrame(data, schema=schema)

            transformed = df.withColumn(
                "impression_date_parsed",
                to_timestamp(
                    regexp_replace(col("impression_date"), r"\.\d+", "").cast("string"),
                    "yyyy-MM-dd'T'HH:mm:ss",
                ),
            )

            # Filter for null values
            null_result = transformed.filter(col("impression_date_parsed").isNull())
            null_count = null_result.count()
            assert null_count == 2  # imp2 and imp3 should have null timestamps

            # Filter for non-null values
            non_null_result = transformed.filter(
                col("impression_date_parsed").isNotNull()
            )
            non_null_count = non_null_result.count()
            assert non_null_count == 1  # Only imp1 should have valid timestamp

        finally:
            spark.stop()

    def test_to_timestamp_with_multiple_filters(self):
        """Test that multiple filters on datetime columns work correctly."""
        spark = SparkSession.builder.appName("BugRepro").getOrCreate()
        try:
            data = [
                ("imp1", "2024-01-15T10:30:00"),
                ("imp2", "2024-01-16T11:00:00"),
                ("imp3", "2024-01-17T12:00:00"),
            ]
            df = spark.createDataFrame(data, ["impression_id", "impression_date"])

            transformed = df.withColumn(
                "impression_date_parsed",
                to_timestamp(
                    regexp_replace(col("impression_date"), r"\.\d+", "").cast("string"),
                    "yyyy-MM-dd'T'HH:mm:ss",
                ),
            )

            # Multiple filters
            result = transformed.filter(
                col("impression_date_parsed").isNotNull()
            ).filter(col("impression_id") == "imp1")

            count = result.count()
            assert count == 1

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["impression_id"] == "imp1"

        finally:
            spark.stop()

    def test_to_timestamp_with_multiple_operations_and_filter(self):
        """Test that filtering works after multiple operations involving to_timestamp."""
        spark = SparkSession.builder.appName("BugRepro").getOrCreate()
        try:
            from sparkless.spark_types import StringType, StructType, StructField

            # Use explicit schema with StringType
            schema = StructType(
                [
                    StructField("impression_id", StringType(), False),
                    StructField("impression_date", StringType(), False),
                ]
            )
            # Simple timestamp string without microseconds
            data = [{"impression_id": "imp1", "impression_date": "2024-01-15T10:30:00"}]
            df = spark.createDataFrame(data, schema=schema)

            # Multiple transformations before filter
            transformed = df.withColumn(
                "timestamp_str", col("impression_date").cast("string")
            ).withColumn(
                "impression_date_parsed",
                to_timestamp(col("timestamp_str"), "yyyy-MM-dd'T'HH:mm:ss"),
            )

            # Filter should work without SchemaError
            result = transformed.filter(col("impression_date_parsed").isNotNull())
            count = result.count()
            assert count == 1

            rows = result.collect()
            assert len(rows) == 1
            assert isinstance(rows[0]["impression_date_parsed"], datetime)

        finally:
            spark.stop()

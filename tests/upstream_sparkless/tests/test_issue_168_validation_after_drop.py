"""
Test for issue #168: Validation fails with 'cannot resolve' when validating after transform that drops column

Issue #168 reports that when validating a DataFrame after a transform that uses a column and then drops it,
sparkless tries to resolve the dropped column during validation, causing a "cannot resolve" error.
"""

from sparkless import SparkSession, functions as F
from datetime import datetime, timedelta


class TestIssue168ValidationAfterDrop:
    """Test cases for issue #168: validation after transform that drops columns."""

    def test_validation_after_drop_columns(self):
        """Test that validation works after a transform that drops columns."""
        spark = SparkSession.builder.appName("test").getOrCreate()

        try:
            # Create test data (150 rows - bug manifests with larger datasets)
            data = []
            for i in range(150):
                data.append(
                    {
                        "impression_id": f"IMP-{i:08d}",
                        "impression_date": (
                            datetime.now() - timedelta(hours=i % 720)
                        ).isoformat(),
                    }
                )

            bronze_df = spark.createDataFrame(
                data, ["impression_id", "impression_date"]
            )

            # Transform that uses impression_date then drops it
            silver_df = (
                bronze_df.withColumn(
                    "impression_date_parsed",
                    F.to_timestamp(
                        F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast(
                            "string"
                        ),
                        "yyyy-MM-dd'T'HH:mm:ss",
                    ),
                ).select(
                    "impression_id", "impression_date_parsed"
                )  # impression_date is DROPPED
            )

            # Validation (THIS SHOULD WORK)
            validation_predicate = (
                F.col("impression_id").isNotNull()
                & F.col("impression_date_parsed").isNotNull()
            )

            valid_df = silver_df.filter(validation_predicate)  # Should not raise error
            count = valid_df.count()
            assert count >= 0  # Should succeed without error
            assert count == 150  # All rows should be valid
        finally:
            spark.stop()

    def test_validation_after_drop_with_nested_operations(self):
        """Test validation after dropping columns used in nested operations."""
        spark = SparkSession.builder.appName("test").getOrCreate()

        try:
            data = []
            for i in range(50):
                data.append(
                    {
                        "event_id": f"EVT-{i:08d}",
                        "event_time": (datetime.now() - timedelta(hours=i)).isoformat(),
                    }
                )

            df = spark.createDataFrame(data, ["event_id", "event_time"])

            # Transform with nested operations
            transformed_df = (
                df.withColumn(
                    "event_time_clean",
                    F.regexp_replace(F.col("event_time"), r"\.\d+", ""),
                )
                .withColumn(
                    "event_timestamp",
                    F.to_timestamp(F.col("event_time_clean"), "yyyy-MM-dd'T'HH:mm:ss"),
                )
                .select(
                    "event_id", "event_timestamp"
                )  # event_time and event_time_clean are DROPPED
            )

            # Validation should work
            validation_predicate = F.col("event_timestamp").isNotNull()
            valid_df = transformed_df.filter(validation_predicate)
            count = valid_df.count()
            assert count == 50
        finally:
            spark.stop()

    def test_validation_after_drop_with_complex_filter(self):
        """Test validation with complex filter expressions after dropping columns."""
        spark = SparkSession.builder.appName("test").getOrCreate()

        try:
            data = []
            for i in range(200):
                data.append(
                    {
                        "record_id": f"REC-{i:08d}",
                        "created_at": (
                            datetime.now() - timedelta(days=i % 30)
                        ).isoformat(),
                        "status": "active" if i % 2 == 0 else "inactive",
                    }
                )

            df = spark.createDataFrame(data, ["record_id", "created_at", "status"])

            # Transform that drops original columns
            transformed_df = (
                df.withColumn(
                    "created_at_parsed",
                    F.to_timestamp(
                        F.regexp_replace(F.col("created_at"), r"\.\d+", "").cast(
                            "string"
                        ),
                        "yyyy-MM-dd'T'HH:mm:ss",
                    ),
                ).select(
                    "record_id", "created_at_parsed", "status"
                )  # created_at is DROPPED
            )

            # Complex validation with multiple conditions
            validation_predicate = (
                F.col("record_id").isNotNull()
                & F.col("created_at_parsed").isNotNull()
                & (F.col("status") == "active")
            )

            valid_df = transformed_df.filter(validation_predicate)
            count = valid_df.count()
            assert count == 100  # Half should be active
        finally:
            spark.stop()

"""
Test for issue #160: cannot resolve error when execution plan references dropped columns.

Issue #160 reports that when a DataFrame operation drops a column via `.select()`, but the
execution plan still contains references to that column from earlier operations, sparkless
fails with a `cannot resolve` error during plan evaluation (materialization, validation, or write operations).

The root cause is that the expression cache stores Polars expressions that reference column
names directly. When columns are dropped, these cached expressions become invalid, but they're
still used during plan evaluation.

These tests verify that the fix works correctly - they ensure that materialization succeeds
even when columns are used in transformations and then dropped via select().
"""

from sparkless import SparkSession, functions as F


class TestIssue160DroppedColumnExecutionPlan:
    """Test cases for issue #160: execution plan references dropped columns."""

    def test_dropped_column_in_execution_plan(self):
        """Test that materialization works when a column is used then dropped."""
        spark = SparkSession.builder.appName("bug_reproduction").getOrCreate()

        # Create test data
        data = [
            (
                "imp_001",
                "2024-01-15T10:30:45.123456",
                "campaign_1",
                "customer_1",
                "web",
                "ad_1",
                "mobile",
                0.05,
            ),
            (
                "imp_002",
                "2024-01-16T14:20:30.789012",
                "campaign_2",
                "customer_2",
                "mobile",
                "ad_2",
                "mobile",
                0.03,
            ),
        ]

        bronze_df = spark.createDataFrame(
            data,
            [
                "impression_id",
                "impression_date",  # This column will be dropped
                "campaign_id",
                "customer_id",
                "channel",
                "ad_id",
                "device_type",
                "cost_per_impression",
            ],
        )

        # Apply transform that uses impression_date then drops it
        silver_df = (
            bronze_df.withColumn(
                "impression_date_parsed",
                F.to_timestamp(
                    F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast(
                        "string"
                    ),
                    "yyyy-MM-dd'T'HH:mm:ss",
                ),
            )
            .withColumn("hour_of_day", F.hour(F.col("impression_date_parsed")))
            .withColumn("day_of_week", F.dayofweek(F.col("impression_date_parsed")))
            .withColumn(
                "is_mobile",
                F.when(F.col("device_type") == "mobile", True).otherwise(False),
            )
            .select(
                "impression_id",
                "campaign_id",
                "customer_id",
                "impression_date_parsed",  # New column
                "hour_of_day",
                "day_of_week",
                "channel",
                "ad_id",
                "cost_per_impression",
                "device_type",
                "is_mobile",
                # impression_date is DROPPED - not in select list
            )
        )

        # Verify column was dropped
        assert "impression_date" not in silver_df.columns
        assert "impression_date_parsed" in silver_df.columns

        # ERROR: Try to materialize/evaluate the DataFrame
        # This should work - the execution plan should not reference dropped columns
        count = silver_df.count()  # This should not raise an error
        assert count == 2

        # Verify we can collect the data
        rows = silver_df.collect()
        assert len(rows) == 2

        spark.stop()

    def test_dropped_column_with_cache(self):
        """Test that materialization works with cached expressions (150+ rows scenario)."""
        spark = SparkSession.builder.appName("cache_test").getOrCreate()

        # Create test data with 150+ rows to trigger cache behavior
        data = [
            (
                f"imp_{i:03d}",
                f"2024-01-15T10:30:45.{i:06d}",
                f"campaign_{i}",
                f"customer_{i}",
                "web",
                f"ad_{i}",
                "mobile",
                0.05,
            )
            for i in range(200)
        ]

        bronze_df = spark.createDataFrame(
            data,
            [
                "impression_id",
                "impression_date",  # This column will be dropped
                "campaign_id",
                "customer_id",
                "channel",
                "ad_id",
                "device_type",
                "cost_per_impression",
            ],
        )

        # Apply transform that uses impression_date then drops it
        silver_df = (
            bronze_df.withColumn(
                "impression_date_parsed",
                F.to_timestamp(
                    F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast(
                        "string"
                    ),
                    "yyyy-MM-dd'T'HH:mm:ss",
                ),
            )
            .withColumn("hour_of_day", F.hour(F.col("impression_date_parsed")))
            .withColumn("day_of_week", F.dayofweek(F.col("impression_date_parsed")))
            .withColumn(
                "is_mobile",
                F.when(F.col("device_type") == "mobile", True).otherwise(False),
            )
            .select(
                "impression_id",
                "campaign_id",
                "customer_id",
                "impression_date_parsed",  # New column
                "hour_of_day",
                "day_of_week",
                "channel",
                "ad_id",
                "cost_per_impression",
                "device_type",
                "is_mobile",
                # impression_date is DROPPED - not in select list
            )
        )

        # Verify column was dropped
        assert "impression_date" not in silver_df.columns
        assert "impression_date_parsed" in silver_df.columns

        # ERROR: Try to materialize/evaluate the DataFrame
        # This should work even with cached expressions
        count = silver_df.count()  # This should not raise an error
        assert count == 200

        # Verify we can collect the data
        rows = silver_df.collect()
        assert len(rows) == 200

        spark.stop()

"""
Test for issue #170: to_date() on TimestampType fails with 'NoneType' object has no attribute 'collect'.

This test reproduces the exact scenario from issue #170 where:
1. A DataFrame is created with timestamp strings
2. to_timestamp() is used to parse the timestamp (creates TimestampType column)
3. to_date() is applied on the TimestampType column
4. The DataFrame is materialized (e.g., groupBy + agg + collect)

The bug occurred because to_date() on TimestampType might set lazy_df = None
and the subsequent operations don't check for df_materialized.
"""

from sparkless import SparkSession, functions as F
from datetime import datetime


class TestIssue170ToDateTimestampType:
    """Test fix for issue #170: to_date() on TimestampType + materialize() error."""

    def test_to_date_on_timestamp_type_basic(self):
        """Test the basic reproduction case from issue #170."""
        spark = SparkSession("test_issue_170")
        try:
            # Create test data
            data = []
            for i in range(100):
                data.append(
                    {
                        "event_id": f"EVT-{i:08d}",
                        "event_timestamp": datetime.now().isoformat(),
                    }
                )

            bronze_df = spark.createDataFrame(data, ["event_id", "event_timestamp"])

            # Parse timestamp (creates TimestampType column)
            silver_df = bronze_df.withColumn(
                "event_timestamp_parsed",
                F.to_timestamp(
                    F.col("event_timestamp").cast("string"), "yyyy-MM-dd'T'HH:mm:ss"
                ),
            ).select("event_id", "event_timestamp_parsed")

            # Apply to_date() on TimestampType (THIS WAS FAILING BEFORE THE FIX)
            gold_df = (
                silver_df.withColumn(
                    "metric_date",
                    F.to_date(F.col("event_timestamp_parsed")),  # Should work now
                )
                .groupBy("metric_date")
                .agg(F.count("*").alias("total_events"))
            )

            # Materialize (THIS WAS FAILING BEFORE THE FIX)
            result = gold_df.collect()
            assert len(result) > 0

            # Verify the data is correct
            for row in result:
                assert "metric_date" in row
                assert "total_events" in row
                assert row["total_events"] > 0
        finally:
            spark.stop()

    def test_to_date_on_timestamp_type_with_drop(self):
        """Test to_date() on TimestampType followed by drop()."""
        spark = SparkSession("test_issue_170_drop")
        try:
            data = [
                {"id": 1, "ts_str": "2024-01-01T10:00:00"},
                {"id": 2, "ts_str": "2024-01-02T11:00:00"},
            ]

            df = spark.createDataFrame(data)

            result = (
                df.withColumn(
                    "ts",
                    F.to_timestamp(
                        F.col("ts_str").cast("string"), "yyyy-MM-dd'T'HH:mm:ss"
                    ),
                )
                .withColumn("date", F.to_date(F.col("ts")))
                .drop("ts_str")
                .select("id", "date")
            )

            # Materialize - should work now
            rows = result.collect()
            assert len(rows) == 2
            for row in rows:
                assert "date" in row
        finally:
            spark.stop()

    def test_to_date_on_timestamp_type_with_select(self):
        """Test to_date() on TimestampType + select() chain."""
        spark = SparkSession("test_issue_170_select")
        try:
            data = [
                {"id": 1, "ts_str": "2024-01-01T10:00:00"},
                {"id": 2, "ts_str": "2024-01-02T11:00:00"},
            ]

            df = spark.createDataFrame(data)

            result = (
                df.withColumn(
                    "ts",
                    F.to_timestamp(
                        F.col("ts_str").cast("string"), "yyyy-MM-dd'T'HH:mm:ss"
                    ),
                )
                .withColumn("date", F.to_date(F.col("ts")))
                .select("id", "date")
            )

            # Materialize - should work
            rows = result.collect()
            assert len(rows) == 2
        finally:
            spark.stop()

    def test_to_date_on_timestamp_type_with_filter(self):
        """Test to_date() on TimestampType + filter() chain."""
        spark = SparkSession("test_issue_170_filter")
        try:
            data = [
                {"id": 1, "ts_str": "2024-01-01T10:00:00"},
                {"id": 2, "ts_str": "2024-01-02T11:00:00"},
            ]

            df = spark.createDataFrame(data)

            result = (
                df.withColumn(
                    "ts",
                    F.to_timestamp(
                        F.col("ts_str").cast("string"), "yyyy-MM-dd'T'HH:mm:ss"
                    ),
                )
                .withColumn("date", F.to_date(F.col("ts")))
                .filter(F.col("id") > 1)
            )

            # Materialize - should work
            count = result.count()
            assert count == 1
        finally:
            spark.stop()

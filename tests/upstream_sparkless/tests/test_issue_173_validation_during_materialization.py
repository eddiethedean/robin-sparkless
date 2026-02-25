"""Tests for issue #173: Validation fails during materialization when replaying operations.

This issue occurs when:
1. A DataFrame has withColumn operations that reference columns
2. Those columns are then dropped via select()
3. Materialization replays the withColumn operations
4. Validation uses the final schema (after select) instead of the schema at queue time
"""

from sparkless import SparkSession, functions as F
from datetime import datetime, timedelta


class TestIssue173ValidationDuringMaterialization:
    """Test cases for issue #173: validation during materialization replay."""

    def test_validation_during_materialization_with_dropped_columns(self):
        """Test that validation works during materialization when columns are dropped.

        This test verifies the fix where:
        - withColumn operations reference columns (timestamp_str, value)
        - select() drops those columns
        - Materialization replays withColumn operations
        - Validation uses schema at queue time (not final schema after select)
        """
        spark = SparkSession.builder.appName("test").getOrCreate()

        try:
            data = []
            for i in range(100):
                data.append(
                    {
                        "id": f"ID-{i:08d}",
                        "timestamp_str": (
                            datetime.now() - timedelta(days=i % 365)
                        ).isoformat(),
                        "value": i * 10,
                    }
                )

            df = spark.createDataFrame(data, ["id", "timestamp_str", "value"])

            # Transform that uses timestamp_str then drops it
            # This should work - columns exist when withColumn is called
            transformed_df = (
                df.withColumn(
                    "timestamp_parsed",
                    F.to_timestamp(F.col("timestamp_str"), "yyyy-MM-dd'T'HH:mm:ss"),
                )
                .withColumn("value_doubled", F.col("value") * 2)
                .select(
                    "id", "timestamp_parsed", "value_doubled"
                )  # timestamp_str and value are DROPPED
            )

            # This triggers materialization and the bug
            # During materialization, withColumn operations are replayed
            # Validation uses the final schema (after select) which doesn't have timestamp_str
            validation_predicate = (
                F.col("id").isNotNull()
                & F.col("timestamp_parsed").isNotNull()
                & F.col("value_doubled").isNotNull()
            )

            valid_df = transformed_df.filter(validation_predicate)
            count = valid_df.count()
            assert count == 100
        finally:
            spark.stop()

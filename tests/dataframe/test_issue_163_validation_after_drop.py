"""
Test for issue #163: Validation fails with 'cannot resolve' when validating after transform that drops columns.

Uses get_spark_imports from fixture only.
"""

from datetime import datetime, timedelta

from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
SparkSession = _imports.SparkSession
F = _imports.F


class TestIssue163ValidationAfterDrop:
    """Test cases for issue #163: validation after transform that drops columns."""

    def test_validation_after_drop_columns(self):
        """Test that validation works after a transform that drops columns."""
        spark = SparkSession.builder.appName("test").getOrCreate()

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

        bronze_df = spark.createDataFrame(data, ["impression_id", "impression_date"])

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

        spark.stop()

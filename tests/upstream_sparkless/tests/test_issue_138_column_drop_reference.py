"""
Test for issue #138: Column reference error after drop().

Issue #138 reports that after transforming a column (e.g., snapshot_date ->
snapshot_date_parsed) and dropping the original column, sparkless may still try
to reference the original column name internally, causing an AttributeError.
"""

from sparkless import SparkSession
from sparkless.functions import col, to_timestamp, regexp_replace
from datetime import datetime


class TestIssue138ColumnDropReference:
    """Test cases for issue #138: column drop reference errors."""

    def test_drop_column_after_transform(self):
        """Test that dropping a column after transformation works correctly.

        This is the exact scenario from issue #138.
        """
        spark = SparkSession.builder.appName("BugRepro").getOrCreate()
        try:
            data = [("inv1", "2024-01-15T10:30:00", 100)]
            df = spark.createDataFrame(
                data, ["inventory_id", "snapshot_date", "quantity_on_hand"]
            )

            transformed = (
                df.withColumn(
                    "snapshot_date_parsed",
                    to_timestamp(
                        regexp_replace(col("snapshot_date"), r"\.\d+", "").cast(
                            "string"
                        ),
                        "yyyy-MM-dd'T'HH:mm:ss",
                    ),
                ).drop("snapshot_date")  # Drop original column
            )

            # This should not fail with AttributeError
            result = transformed.select("inventory_id", "snapshot_date_parsed")
            count = result.count()
            assert count == 1, f"Expected count 1, got {count}"

            # Verify the data is correct
            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["inventory_id"] == "inv1"
            assert isinstance(rows[0]["snapshot_date_parsed"], datetime)

        finally:
            spark.stop()

    def test_drop_multiple_columns_after_transform(self):
        """Test dropping multiple columns after transformation."""
        spark = SparkSession.builder.appName("BugRepro").getOrCreate()
        try:
            data = [("inv1", "2024-01-15T10:30:00", 100, "temp")]
            df = spark.createDataFrame(
                data,
                ["inventory_id", "snapshot_date", "quantity_on_hand", "temp_col"],
            )

            transformed = (
                df.withColumn(
                    "snapshot_date_parsed",
                    to_timestamp(
                        regexp_replace(col("snapshot_date"), r"\.\d+", "").cast(
                            "string"
                        ),
                        "yyyy-MM-dd'T'HH:mm:ss",
                    ),
                )
                .drop("snapshot_date")
                .drop("temp_col")
            )

            # Should work without AttributeError
            result = transformed.select(
                "inventory_id", "snapshot_date_parsed", "quantity_on_hand"
            )
            count = result.count()
            assert count == 1

            rows = result.collect()
            assert len(rows) == 1
            assert "snapshot_date" not in rows[0]
            assert "temp_col" not in rows[0]

        finally:
            spark.stop()

    def test_drop_then_select(self):
        """Test selecting columns after dropping some columns."""
        spark = SparkSession.builder.appName("BugRepro").getOrCreate()
        try:
            data = [("inv1", "2024-01-15T10:30:00", 100)]
            df = spark.createDataFrame(
                data, ["inventory_id", "snapshot_date", "quantity_on_hand"]
            )

            transformed = df.withColumn(
                "snapshot_date_parsed",
                to_timestamp(
                    regexp_replace(col("snapshot_date"), r"\.\d+", "").cast("string"),
                    "yyyy-MM-dd'T'HH:mm:ss",
                ),
            ).drop("snapshot_date")

            # Select should work even after drop
            result = transformed.select(
                "inventory_id", "snapshot_date_parsed", "quantity_on_hand"
            )
            count = result.count()
            assert count == 1

        finally:
            spark.stop()

    def test_drop_then_filter(self):
        """Test filtering after dropping columns."""
        spark = SparkSession.builder.appName("BugRepro").getOrCreate()
        try:
            data = [("inv1", "2024-01-15T10:30:00", 100)]
            df = spark.createDataFrame(
                data, ["inventory_id", "snapshot_date", "quantity_on_hand"]
            )

            transformed = df.withColumn(
                "snapshot_date_parsed",
                to_timestamp(
                    regexp_replace(col("snapshot_date"), r"\.\d+", "").cast("string"),
                    "yyyy-MM-dd'T'HH:mm:ss",
                ),
            ).drop("snapshot_date")

            # Filter should work without referencing dropped column
            result = transformed.filter(col("inventory_id") == "inv1")
            count = result.count()
            assert count == 1

        finally:
            spark.stop()

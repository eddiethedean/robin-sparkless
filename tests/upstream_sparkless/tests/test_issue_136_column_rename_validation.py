"""
Test for issue #136: Column rename/transform not reflected in validation.

Issue #136 reports that when renaming columns and creating new columns with
transformations, sparkless validation sees the old column structure instead of
the transformed one, causing `unable to find column` errors.

FIXED:
- Schema tracking: Added withColumnRenamed handling to SchemaManager
  and materializer schema tracking.
- Materialization fix: Fixed withColumnRenamed to materialize the DataFrame
  before accessing data, ensuring queued operations (like withColumn) are
  applied before renaming columns.
"""

from sparkless import SparkSession
from sparkless.functions import col, to_timestamp, regexp_replace
from datetime import datetime


class TestIssue136ColumnRenameValidation:
    """Test cases for issue #136: column rename validation."""

    def test_column_rename_and_transform_with_filter(self):
        """Test that filtering works after column rename and transformation.

        This is the exact scenario from issue #136.
        """
        spark = SparkSession.builder.appName("BugRepro").getOrCreate()
        try:
            data = [("rec1", "cust1", "2024-01-15T10:30:00", 100.0)]
            df = spark.createDataFrame(data, ["record_id", "cust_id", "date", "value"])

            transformed = (
                df.withColumn(
                    "transaction_date_parsed",
                    to_timestamp(
                        regexp_replace(col("date"), r"\.\d+", "").cast("string"),
                        "yyyy-MM-dd'T'HH:mm:ss",
                    ),
                )
                .withColumnRenamed("record_id", "id")
                .withColumnRenamed("cust_id", "customer_id")
                .withColumnRenamed("value", "amount")
                .select(
                    "id",
                    "customer_id",
                    "transaction_date_parsed",  # This column exists in select
                    "amount",
                )
            )

            # This should not fail - sparkless should see transformed column structure
            validation_result = transformed.filter(
                col("transaction_date_parsed").isNotNull()
            )
            count = validation_result.count()
            assert count == 1

            # Verify the data is correct
            rows = validation_result.collect()
            assert len(rows) == 1
            assert rows[0]["id"] == "rec1"
            assert rows[0]["customer_id"] == "cust1"
            assert isinstance(rows[0]["transaction_date_parsed"], datetime)
            assert rows[0]["amount"] == 100.0

        finally:
            spark.stop()

    def test_multiple_column_renames(self):
        """Test that multiple column renames work correctly."""
        spark = SparkSession.builder.appName("BugRepro").getOrCreate()
        try:
            data = [("a", "b", "c")]
            df = spark.createDataFrame(data, ["col1", "col2", "col3"])

            transformed = (
                df.withColumnRenamed("col1", "new_col1")
                .withColumnRenamed("col2", "new_col2")
                .withColumnRenamed("col3", "new_col3")
                .select("new_col1", "new_col2", "new_col3")
            )

            # Filter on renamed column
            result = transformed.filter(col("new_col1") == "a")
            count = result.count()
            assert count == 1

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["new_col1"] == "a"
            assert rows[0]["new_col2"] == "b"
            assert rows[0]["new_col3"] == "c"

        finally:
            spark.stop()

    def test_rename_then_add_column_then_filter(self):
        """Test renaming, adding a column, then filtering."""
        spark = SparkSession.builder.appName("BugRepro").getOrCreate()
        try:
            data = [("rec1", "cust1")]
            df = spark.createDataFrame(data, ["record_id", "cust_id"])

            transformed = (
                df.withColumnRenamed("record_id", "id")
                .withColumnRenamed("cust_id", "customer_id")
                .withColumn("full_id", col("id") + "_" + col("customer_id"))
                .select("id", "customer_id", "full_id")
            )

            # Filter on the new column
            result = transformed.filter(col("full_id") == "rec1_cust1")
            count = result.count()
            assert count == 1

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["full_id"] == "rec1_cust1"

        finally:
            spark.stop()

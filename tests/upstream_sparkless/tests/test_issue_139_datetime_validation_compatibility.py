"""
Test for issue #139: Validation system incompatible with datetime column operations.

Issue #139 reports that the validation system has compatibility issues when working
with datetime columns, causing validation failures even when data is valid. This
includes:
- Validation rules that reference datetime columns fail
- All rows marked as invalid (0.0% valid rate) when datetime columns are involved
- Validation sees wrong column structure when datetime transformations are applied

This may be related to issues #135, #136, #137 which have already been fixed.
"""

from sparkless import SparkSession
from sparkless.functions import col, to_timestamp, to_date, current_date
from datetime import datetime, date


class TestIssue139DatetimeValidationCompatibility:
    """Test cases for issue #139: datetime validation compatibility."""

    def test_validation_with_datetime_column(self):
        """Test that validation rules work correctly with datetime columns."""
        spark = SparkSession.builder.appName("BugRepro").getOrCreate()
        try:
            data = [
                ("e1", "2024-01-15T10:30:00"),
                ("e2", "2024-01-16T11:00:00"),
                ("e3", "2024-01-17T12:00:00"),
            ]
            df = spark.createDataFrame(data, ["event_id", "event_date_str"])

            # Transform to datetime
            transformed = df.withColumn(
                "event_date",
                to_timestamp(col("event_date_str"), "yyyy-MM-dd'T'HH:mm:ss"),
            )

            # Validation rule: event_date should not be null
            validation_result = transformed.filter(col("event_date").isNotNull())

            count = validation_result.count()
            assert count == 3, f"Expected 3 valid rows, got {count}"

            # All rows should be valid
            rows = validation_result.collect()
            assert len(rows) == 3
            for row in rows:
                assert row["event_date"] is not None
                assert isinstance(row["event_date"], datetime)

        finally:
            spark.stop()

    def test_validation_with_date_column_and_operations(self):
        """Test validation with date columns and date operations."""
        spark = SparkSession.builder.appName("BugRepro").getOrCreate()
        try:
            data = [("p1", "John", "1990-01-15")]
            df = spark.createDataFrame(
                data, ["patient_id", "first_name", "date_of_birth"]
            )

            # Transform to date
            transformed = df.withColumn(
                "birth_date", to_date(col("date_of_birth"), "yyyy-MM-dd")
            )

            # Validation rule: birth_date should not be null and should be in the past
            validation_result = transformed.filter(
                col("birth_date").isNotNull() & (col("birth_date") < current_date())
            )

            count = validation_result.count()
            assert count == 1, f"Expected 1 valid row, got {count}"

            rows = validation_result.collect()
            assert len(rows) == 1
            assert rows[0]["birth_date"] is not None
            assert isinstance(rows[0]["birth_date"], date)

        finally:
            spark.stop()

    def test_validation_with_datetime_comparison(self):
        """Test validation with datetime column comparisons."""
        spark = SparkSession.builder.appName("BugRepro").getOrCreate()
        try:
            data = [
                ("t1", "2024-01-10T10:00:00"),
                ("t2", "2024-01-15T10:00:00"),
                ("t3", "2024-01-20T10:00:00"),
            ]
            df = spark.createDataFrame(data, ["txn_id", "txn_date_str"])

            # Transform to datetime
            transformed = df.withColumn(
                "txn_date", to_timestamp(col("txn_date_str"), "yyyy-MM-dd'T'HH:mm:ss")
            )

            # Validation rule: txn_date should be within a date range
            start_date = datetime(2024, 1, 12, 0, 0, 0)
            end_date = datetime(2024, 1, 18, 23, 59, 59)
            validation_result = transformed.filter(
                (col("txn_date") >= start_date) & (col("txn_date") <= end_date)
            )

            count = validation_result.count()
            assert count == 1, f"Expected 1 valid row, got {count}"

            rows = validation_result.collect()
            assert len(rows) == 1
            assert rows[0]["txn_id"] == "t2"
            assert isinstance(rows[0]["txn_date"], datetime)

        finally:
            spark.stop()

    def test_validation_with_multiple_datetime_columns(self):
        """Test validation with multiple datetime columns."""
        spark = SparkSession.builder.appName("BugRepro").getOrCreate()
        try:
            data = [
                ("r1", "2024-01-10T10:00:00", "2024-01-15T10:00:00"),
                ("r2", "2024-01-12T10:00:00", "2024-01-18T10:00:00"),
            ]
            df = spark.createDataFrame(
                data, ["record_id", "start_date_str", "end_date_str"]
            )

            # Transform to datetime
            transformed = df.withColumn(
                "start_date",
                to_timestamp(col("start_date_str"), "yyyy-MM-dd'T'HH:mm:ss"),
            ).withColumn(
                "end_date", to_timestamp(col("end_date_str"), "yyyy-MM-dd'T'HH:mm:ss")
            )

            # Validation rule: end_date should be after start_date
            validation_result = transformed.filter(col("end_date") > col("start_date"))

            count = validation_result.count()
            assert count == 2, f"Expected 2 valid rows, got {count}"

            rows = validation_result.collect()
            assert len(rows) == 2
            for row in rows:
                assert isinstance(row["start_date"], datetime)
                assert isinstance(row["end_date"], datetime)
                assert row["end_date"] > row["start_date"]

        finally:
            spark.stop()

    def test_validation_with_datetime_after_column_rename(self):
        """Test validation with datetime columns after column rename."""
        spark = SparkSession.builder.appName("BugRepro").getOrCreate()
        try:
            data = [("inv1", "2024-01-15T10:30:00")]
            df = spark.createDataFrame(data, ["inventory_id", "snapshot_date"])

            # Transform and rename
            transformed = (
                df.withColumn(
                    "snapshot_date_parsed",
                    to_timestamp(col("snapshot_date"), "yyyy-MM-dd'T'HH:mm:ss"),
                )
                .withColumnRenamed("inventory_id", "id")
                .drop("snapshot_date")
            )

            # Validation rule: snapshot_date_parsed should not be null
            validation_result = transformed.filter(
                col("snapshot_date_parsed").isNotNull()
            )

            count = validation_result.count()
            assert count == 1, f"Expected 1 valid row, got {count}"

            rows = validation_result.collect()
            assert len(rows) == 1
            assert isinstance(rows[0]["snapshot_date_parsed"], datetime)

        finally:
            spark.stop()

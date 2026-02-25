"""
Test for issue #137: Validation rules fail on datetime columns.

Issue #137 reports that when applying validation rules to dataframes containing
datetime columns created from string dates, all rows are marked as invalid
(0.0% valid rate) even though they should pass validation.
"""

from sparkless import SparkSession
from sparkless.functions import col, to_date, datediff, current_date, floor


class TestIssue137DatetimeValidation:
    """Test cases for issue #137: datetime column validation."""

    def test_datetime_validation_with_age_calculation(self):
        """Test that validation rules work correctly with datetime columns.

        This is the exact scenario from issue #137.
        """
        spark = SparkSession.builder.appName("BugRepro").getOrCreate()
        try:
            data = [("p1", "John", "1990-01-15")]
            df = spark.createDataFrame(
                data, ["patient_id", "first_name", "date_of_birth"]
            )

            transformed = df.withColumn(
                "birth_date", to_date(col("date_of_birth"), "yyyy-MM-dd")
            ).withColumn(
                "age", floor(datediff(current_date(), col("birth_date")) / 365.25)
            )

            # Validation rules that should pass
            validation_result = transformed.filter(
                col("patient_id").isNotNull()
                & col("age").isNotNull()
                & (col("age") >= 0)
            )

            # Should have 1 valid row, not 0
            count = validation_result.count()
            assert count == 1, f"Expected 1 valid row, got {count}"

            # Verify the data is correct
            rows = validation_result.collect()
            assert len(rows) == 1
            assert rows[0]["patient_id"] == "p1"
            assert rows[0]["age"] is not None
            assert rows[0]["age"] >= 0

        finally:
            spark.stop()

    def test_datetime_validation_simple_filter(self):
        """Test simple validation on datetime columns."""
        spark = SparkSession.builder.appName("BugRepro").getOrCreate()
        try:
            data = [("p1", "1990-01-15"), ("p2", "1985-05-20")]
            df = spark.createDataFrame(data, ["patient_id", "date_of_birth"])

            transformed = df.withColumn(
                "birth_date", to_date(col("date_of_birth"), "yyyy-MM-dd")
            )

            # Simple filter on datetime column
            result = transformed.filter(col("birth_date").isNotNull())
            count = result.count()
            assert count == 2, f"Expected 2 valid rows, got {count}"

        finally:
            spark.stop()

    def test_datetime_validation_with_multiple_conditions(self):
        """Test validation with multiple conditions involving datetime columns."""
        spark = SparkSession.builder.appName("BugRepro").getOrCreate()
        try:
            data = [("p1", "John", "1990-01-15"), ("p2", "Jane", "1985-05-20")]
            df = spark.createDataFrame(
                data, ["patient_id", "first_name", "date_of_birth"]
            )

            transformed = df.withColumn(
                "birth_date", to_date(col("date_of_birth"), "yyyy-MM-dd")
            ).withColumn(
                "age", floor(datediff(current_date(), col("birth_date")) / 365.25)
            )

            # Validation with multiple conditions
            result = transformed.filter(
                col("patient_id").isNotNull()
                & col("birth_date").isNotNull()
                & col("age").isNotNull()
                & (col("age") > 0)
            )
            count = result.count()
            assert count == 2, f"Expected 2 valid rows, got {count}"

        finally:
            spark.stop()

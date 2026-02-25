"""
Test for issue #126: F.to_date() fails to parse 'YYYY-MM-DD' format dates.

This test reproduces the exact scenario from issue #126 to ensure it's fixed.
"""

import pytest
from sparkless import SparkSession
from sparkless.spark_types import StructType, StructField, StringType
from sparkless import functions as F


class TestToDateIssue126:
    """Test exact reproduction of issue #126."""

    @pytest.fixture(autouse=True)
    def setup_method(self):
        """Set up test fixtures."""
        self.spark = SparkSession.builder.appName(
            "date_conversion_bug_test"
        ).getOrCreate()
        yield
        self.spark.stop()

    def test_issue_126_reproduction(self):
        """Test the exact scenario from issue #126."""
        # Create schema with a date column (as string input)
        schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("name", StringType(), False),
                StructField("date_of_birth", StringType(), True),  # String input
            ]
        )

        # Create data with dates in "YYYY-MM-DD" format (common ISO format)
        data = [
            ("1", "Alice", "2005-12-23"),
            ("2", "Bob", "2004-12-23"),
            ("3", "Charlie", "1996-12-25"),
        ]

        # Create DataFrame
        df = self.spark.createDataFrame(data, schema)
        assert df.count() == 3, "DataFrame should have 3 rows"

        # Try to convert using to_date with format (as done in real pipelines)
        # This should work (like PySpark does), but failed in sparkless before the fix
        df_with_date = df.withColumn(
            "birth_date", F.to_date(F.col("date_of_birth"), "yyyy-MM-dd")
        )

        # Verify the operation completes without error
        result = df_with_date.collect()
        assert len(result) == 3, "Should have 3 rows after conversion"

        # Verify schema - birth_date should be DateType, not StringType
        schema_dict = {
            field.name: field.dataType for field in df_with_date.schema.fields
        }
        from sparkless.spark_types import DateType

        assert isinstance(schema_dict["birth_date"], DateType), (
            f"birth_date should be DateType, got {type(schema_dict['birth_date'])}"
        )

        # Verify data values - birth_date should be date objects
        for row in result:
            assert "birth_date" in row
            birth_date = row["birth_date"]
            assert birth_date is not None, "birth_date should not be None"
            # Check that it's actually a date object
            assert hasattr(birth_date, "year"), (
                f"birth_date should be a date object, got {type(birth_date)}"
            )
            assert birth_date.year in [2005, 2004, 1996], (
                f"Unexpected year: {birth_date.year}"
            )

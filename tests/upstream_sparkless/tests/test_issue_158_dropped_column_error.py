"""
Test for issue #158: 'cannot resolve' error when referencing dropped columns in select() and filter().

Issue #158 reports that sparkless raises a "cannot resolve" error when code tries to reference
a column that was dropped via `.select()`. While this is technically correct behavior (the column
doesn't exist), the error message and behavior should be consistent with PySpark.

This test verifies that:
1. Error messages are consistent with PySpark format
2. Error messages are consistent across select() and filter() operations
3. Error messages work with both string column names and F.col() expressions
"""

import pytest
from sparkless import SparkSession, functions as F
from sparkless.core.exceptions.operation import SparkColumnNotFoundError


class TestIssue158DroppedColumnError:
    """Test cases for issue #158: dropped column error messages."""

    def test_select_dropped_column_raises_consistent_error(self):
        """Test that selecting a dropped column raises consistent error message."""
        spark = SparkSession.builder.appName("test").getOrCreate()

        # Create DataFrame with column
        data = [("imp_001", "2024-01-15T10:30:45.123456", "campaign_1")]
        df = spark.createDataFrame(
            data, ["impression_id", "impression_date", "campaign_id"]
        )

        # Apply transform that drops the column
        df_transformed = df.withColumn(
            "impression_date_parsed",
            F.to_timestamp(
                F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
                "yyyy-MM-dd'T'HH:mm:ss",
            ),
        ).select(
            "impression_id",
            "campaign_id",
            "impression_date_parsed",  # New column, original 'impression_date' is dropped
        )

        # Verify column is dropped
        assert "impression_date" not in df_transformed.columns
        assert "impression_date_parsed" in df_transformed.columns

        # THE BUG: select() should raise SparkColumnNotFoundError with consistent message
        with pytest.raises(SparkColumnNotFoundError) as exc_info:
            df_transformed.select("impression_date")

        # Verify the error message format matches PySpark
        error_msg = str(exc_info.value)
        assert "cannot resolve" in error_msg.lower()
        assert "impression_date" in error_msg
        assert "impression_id" in error_msg or "campaign_id" in error_msg

    def test_select_dropped_column_with_f_col(self):
        """Test that selecting a dropped column with F.col() raises consistent error."""
        spark = SparkSession.builder.appName("test").getOrCreate()

        # Create DataFrame
        df = spark.createDataFrame([("a", "b")], ["col1", "col2"])

        # Drop column via select
        df_dropped = df.select("col1")

        # Try to select dropped column with F.col() - should raise consistent error
        with pytest.raises(SparkColumnNotFoundError) as exc_info:
            df_dropped.select(F.col("col2"))

        # Verify the error message format
        error_msg = str(exc_info.value)
        assert "cannot resolve" in error_msg.lower()
        assert "col2" in error_msg
        assert "col1" in error_msg

    def test_filter_dropped_column_raises_consistent_error(self):
        """Test that filtering with a dropped column raises consistent error."""
        spark = SparkSession.builder.appName("test").getOrCreate()

        # Create DataFrame
        df = spark.createDataFrame([("a", "b")], ["col1", "col2"])

        # Drop column via select
        df_dropped = df.select("col1")

        # Try to filter with dropped column - should raise consistent error
        # Note: The error should be raised during validation, not during materialization
        with pytest.raises(SparkColumnNotFoundError) as exc_info:
            df_dropped.filter(F.col("col2").isNotNull())

        # Verify the error message format matches PySpark
        error_msg = str(exc_info.value)
        assert "cannot resolve" in error_msg.lower()
        assert "col2" in error_msg
        assert "col1" in error_msg

    def test_minimal_reproduction(self):
        """Minimal reproduction of the bug."""
        spark = SparkSession.builder.appName("minimal_repro").getOrCreate()

        # Create DataFrame
        df = spark.createDataFrame([("a", "b")], ["col1", "col2"])

        # Drop column via select
        df_dropped = df.select("col1")

        # Try to select dropped column - should raise SparkColumnNotFoundError with consistent message
        with pytest.raises(SparkColumnNotFoundError) as exc_info:
            df_dropped.select("col2")

        # Verify the error message format matches PySpark
        error_msg = str(exc_info.value)
        assert "cannot resolve" in error_msg.lower()
        assert "col2" in error_msg
        assert "col1" in error_msg

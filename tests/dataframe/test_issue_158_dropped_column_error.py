"""
Test for issue #158: 'cannot resolve' error when referencing dropped columns in select() and filter().

Verifies that referencing a dropped column raises an error with a consistent message.
Uses get_imports from fixture only.
"""

import pytest

from sparkless.testing import get_imports

_imports = get_imports()
SparkSession = _imports.SparkSession
F = _imports.F


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

        # select() should raise with consistent message
        with pytest.raises(Exception) as exc_info:
            df_transformed.select("impression_date")

        # PySpark error message includes unresolved_column.with_suggestion and
        # the phrase "cannot be resolved".
        error_msg = str(exc_info.value).lower()
        assert "cannot be resolved" in error_msg
        assert "unresolved_column" in error_msg
        assert "impression_date" in error_msg
        assert "impression_id" in error_msg or "campaign_id" in error_msg

    def test_select_dropped_column_with_f_col(self):
        """Test that selecting a dropped column with F.col() raises consistent error."""
        spark = SparkSession.builder.appName("test").getOrCreate()

        # Create DataFrame
        df = spark.createDataFrame([("a", "b")], ["col1", "col2"])

        # Drop column via select
        df_dropped = df.select("col1")

        # Select dropped column with F.col() must raise (at select or at collect)
        with pytest.raises(Exception) as exc_info:
            df_dropped.select(F.col("col2")).collect()

        # PySpark error message includes unresolved_column.with_suggestion and
        # the phrase "cannot be resolved".
        error_msg = str(exc_info.value).lower()
        assert "cannot be resolved" in error_msg
        assert "unresolved_column" in error_msg
        assert "col2" in error_msg
        assert "col1" in error_msg

    def test_filter_dropped_column_behavior_matches_pyspark(self):
        """Test filtering with a dropped column matches PySpark behavior."""
        spark = SparkSession.builder.appName("test").getOrCreate()

        # Create DataFrame
        df = spark.createDataFrame([("a", "b")], ["col1", "col2"])

        # Drop column via select
        df_dropped = df.select("col1")

        # In PySpark, applying a filter that references a previously-dropped
        # column is still allowed: the filter is pushed below the projection
        # and the result retains only the projected columns.
        result = df_dropped.filter(F.col("col2").isNotNull())
        rows = result.collect()

        assert result.columns == ["col1"]
        assert len(rows) == 1
        assert rows[0]["col1"] == "a"

    def test_minimal_reproduction(self):
        """Minimal reproduction of the bug."""
        spark = SparkSession.builder.appName("minimal_repro").getOrCreate()

        # Create DataFrame
        df = spark.createDataFrame([("a", "b")], ["col1", "col2"])

        # Drop column via select
        df_dropped = df.select("col1")

        # Try to select dropped column - should raise with consistent message
        with pytest.raises(Exception) as exc_info:
            df_dropped.select("col2")

        # PySpark error message includes unresolved_column.with_suggestion and
        # the phrase "cannot be resolved".
        error_msg = str(exc_info.value).lower()
        assert "cannot be resolved" in error_msg
        assert "unresolved_column" in error_msg
        assert "col2" in error_msg
        assert "col1" in error_msg

"""
Test for issue #156: select() uses attribute access for dropped columns, causing AttributeError.

This test reproduces the bug where selecting a dropped column raises AttributeError
instead of a proper "column not found" error.
"""

import pytest
from sparkless import SparkSession, functions as F
from sparkless.core.exceptions.operation import SparkColumnNotFoundError


def test_select_dropped_column_raises_proper_error():
    """Test that selecting a dropped column raises SparkColumnNotFoundError, not AttributeError."""
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

    # THE BUG: select() should raise SparkColumnNotFoundError, not AttributeError
    with pytest.raises(SparkColumnNotFoundError) as exc_info:
        df_transformed.select("impression_date")

    # Verify the error message is appropriate
    assert "impression_date" in str(exc_info.value)
    assert "impression_id" in str(exc_info.value) or "campaign_id" in str(
        exc_info.value
    )


def test_select_dropped_column_minimal_repro():
    """Minimal reproduction of the bug."""
    spark = SparkSession.builder.appName("minimal_repro").getOrCreate()

    # Create DataFrame
    df = spark.createDataFrame([("a", "b")], ["col1", "col2"])

    # Drop column via select
    df_dropped = df.select("col1")

    # Try to select dropped column - should raise SparkColumnNotFoundError, not AttributeError
    with pytest.raises(SparkColumnNotFoundError) as exc_info:
        df_dropped.select("col2")

    # Verify the error message
    assert "col2" in str(exc_info.value)
    assert "col1" in str(exc_info.value)

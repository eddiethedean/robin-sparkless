"""
Test to reproduce issue #160 by testing nested operations and lazy frame reuse.

The bug might occur when:
1. Operations create a lazy Polars DataFrame
2. Columns are dropped via select
3. The lazy DataFrame's execution plan still references the dropped columns
4. When operations are chained, the lazy frame is reused and fails
"""

import os
import pytest
from sparkless import SparkSession, functions as F
import polars as pl


@pytest.fixture
def enable_cache():
    """Enable expression translation cache."""
    os.environ["SPARKLESS_FEATURE_ENABLE_EXPRESSION_TRANSLATION_CACHE"] = "1"
    from sparkless import config

    config._load_feature_flag_overrides.cache_clear()
    yield
    if "SPARKLESS_FEATURE_enable_expression_translation_cache" in os.environ:
        del os.environ["SPARKLESS_FEATURE_ENABLE_EXPRESSION_TRANSLATION_CACHE"]
    config._load_feature_flag_overrides.cache_clear()


def test_nested_operations_with_drop(enable_cache):
    """
    Test nested operations where a column is used, then dropped, then operations continue.

    The issue might be that when we have:
    1. withColumn using column A (creates lazy frame with reference to A)
    2. select drops column A (but lazy frame execution plan still has A)
    3. Another operation tries to use the lazy frame
    """
    spark = SparkSession.builder.appName("nested_ops").getOrCreate()

    # Create data with 200 rows to ensure caching
    data = [
        (f"imp_{i:03d}", f"2024-01-15T10:30:45.{i:06d}", f"campaign_{i}")
        for i in range(200)
    ]
    df = spark.createDataFrame(
        data, ["impression_id", "impression_date", "campaign_id"]
    )

    # Chain operations: use impression_date, then drop it, then continue
    df_result = (
        df.withColumn(
            "date_cleaned", F.regexp_replace(F.col("impression_date"), r"\.\d+", "")
        )
        .withColumn(
            "date_parsed",
            F.to_timestamp(
                F.col("date_cleaned").cast("string"), "yyyy-MM-dd'T'HH:mm:ss"
            ),
        )
        .select(
            "impression_id", "campaign_id", "date_parsed"
        )  # Drop impression_date and date_cleaned
        .withColumn(
            "hour", F.hour(F.col("date_parsed"))
        )  # Continue with operations after drop
        .filter(F.col("hour").isNotNull())
    )

    assert "impression_date" not in df_result.columns
    assert "date_cleaned" not in df_result.columns

    # Try to materialize - if lazy frame execution plan references dropped columns, this will fail
    try:
        count = df_result.count()
        assert count == 200

        rows = df_result.collect()
        assert len(rows) == 200
    except Exception as e:
        error_msg = str(e).lower()
        if ("impression_date" in error_msg or "date_cleaned" in error_msg) and (
            "cannot resolve" in error_msg
            or "not found" in error_msg
            or "unable to find" in error_msg
        ):
            pytest.fail(
                f"BUG REPRODUCED! Nested operations with drop failed: {e}\n"
                f"This suggests that lazy frame execution plan preserves column references."
            )
        raise

    spark.stop()


def test_lazy_frame_reuse_after_select(enable_cache):
    """
    Test if a lazy Polars DataFrame created before select still references dropped columns.

    This test manually creates a Polars lazy DataFrame and checks if it preserves
    column references after columns are dropped via select.
    """
    spark = SparkSession.builder.appName("lazy_reuse").getOrCreate()

    data = [
        (f"imp_{i:03d}", f"2024-01-15T10:30:45.{i:06d}", f"campaign_{i}")
        for i in range(200)
    ]
    spark.createDataFrame(data, ["impression_id", "impression_date", "campaign_id"])

    # Create a Polars DataFrame and convert to lazy
    polars_df = pl.DataFrame(
        [
            {
                "impression_id": f"imp_{i:03d}",
                "impression_date": f"2024-01-15T10:30:45.{i:06d}",
                "campaign_id": f"campaign_{i}",
            }
            for i in range(200)
        ]
    )

    # Create a lazy frame with operations that reference impression_date
    lazy_df = polars_df.lazy().with_columns(
        [pl.col("impression_date").str.replace(r"\.\d+", "").alias("date_cleaned")]
    )

    # Now drop impression_date from the original DataFrame
    polars_df_dropped = polars_df.select("impression_id", "campaign_id")

    # Try to use the lazy frame (which still references impression_date) on the dropped DataFrame
    # This should fail because the lazy frame's execution plan references a column that doesn't exist
    try:
        # Convert dropped DataFrame to lazy and try to use the cached lazy frame operations
        # Actually, we can't directly reuse the lazy frame, but we can check if Polars
        # preserves column references in the execution plan
        lazy_df.collect()
        # If we get here, the lazy frame worked with the original columns

        # Now try to create a new lazy frame from the dropped DataFrame and see if it fails
        lazy_df_dropped = polars_df_dropped.lazy()

        # Try to apply similar operations - if cached expressions are reused, this might fail
        try:
            lazy_df_dropped.with_columns(
                [pl.col("impression_id").alias("id")]
            ).collect()
        except pl.exceptions.ColumnNotFoundError as e:
            if "impression_date" in str(e):
                pytest.fail(
                    f"BUG REPRODUCED! Lazy frame execution plan references dropped column: {e}"
                )
            raise
    except pl.exceptions.ColumnNotFoundError as e:
        if "impression_date" in str(e):
            pytest.fail(f"BUG REPRODUCED! Lazy frame references dropped column: {e}")
        raise

    spark.stop()

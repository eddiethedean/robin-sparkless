"""
Test to reproduce issue #160 by testing nested operations and lazy frame reuse.

Uses get_imports from fixture only.
"""

import os
import pytest

from sparkless.testing import get_imports

_imports = get_imports()
SparkSession = _imports.SparkSession
F = _imports.F


@pytest.fixture
def enable_cache():
    """Enable expression translation cache (env var only; no sparkless config)."""
    os.environ["SPARKLESS_FEATURE_ENABLE_EXPRESSION_TRANSLATION_CACHE"] = "1"
    yield
    if "SPARKLESS_FEATURE_enable_expression_translation_cache" in os.environ:
        del os.environ["SPARKLESS_FEATURE_ENABLE_EXPRESSION_TRANSLATION_CACHE"]


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
    Test that after select() drops columns, subsequent operations do not reference dropped columns.

    Uses only sparkless: build a chain that uses a column, drops it via select, then
    continues. Materializing should succeed; if the engine still references the dropped
    column we get a "not found" / "cannot resolve" error.
    """
    spark = SparkSession.builder.appName("lazy_reuse").getOrCreate()

    data = [
        (f"imp_{i:03d}", f"2024-01-15T10:30:45.{i:06d}", f"campaign_{i}")
        for i in range(200)
    ]
    df = spark.createDataFrame(
        data, ["impression_id", "impression_date", "campaign_id"]
    )

    # Use impression_date, then drop it via select, then more operations
    df_result = (
        df.withColumn(
            "date_cleaned", F.regexp_replace(F.col("impression_date"), r"\.\d+", "")
        )
        .select("impression_id", "campaign_id", "date_cleaned")  # drop impression_date
        .withColumn("id_copy", F.col("impression_id"))
        .filter(F.col("campaign_id").isNotNull())
    )

    assert "impression_date" not in df_result.columns

    try:
        count = df_result.count()
        assert count == 200
        rows = df_result.collect()
        assert len(rows) == 200
    except Exception as e:
        error_msg = str(e).lower()
        if ("impression_date" in error_msg) and (
            "cannot resolve" in error_msg
            or "not found" in error_msg
            or "unable to find" in error_msg
        ):
            pytest.fail(
                f"BUG REPRODUCED! Execution plan still references dropped column: {e}"
            )
        raise

    spark.stop()

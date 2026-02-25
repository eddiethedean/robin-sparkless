"""
Test to reproduce issue #160 with expression cache enabled.

The bug occurs when the expression cache is enabled and cached expressions
reference dropped columns. This test enables the cache and tries to reproduce
the bug.
"""

import os
import pytest
from sparkless import SparkSession, functions as F
from sparkless.core.exceptions.operation import SparkColumnNotFoundError


@pytest.fixture
def enable_cache():
    """Enable expression translation cache for this test."""
    os.environ["MOCK_SPARK_FEATURE_ENABLE_EXPRESSION_TRANSLATION_CACHE"] = "1"
    yield
    # Clean up
    if "MOCK_SPARK_FEATURE_enable_expression_translation_cache" in os.environ:
        del os.environ["MOCK_SPARK_FEATURE_ENABLE_EXPRESSION_TRANSLATION_CACHE"]


def test_bug_with_cache_enabled(enable_cache):
    """
    Reproduce the bug with cache enabled.

    The scenario:
    1. Create DataFrame with column 'impression_date'
    2. Use that column in a withColumn operation (expression gets cached)
    3. Drop the column via select()
    4. Try to materialize - the cached expression might reference the dropped column
    """
    spark = SparkSession.builder.appName("bug_cache").getOrCreate()

    # Create test data with 150+ rows to trigger cache behavior
    data = [
        (f"imp_{i:03d}", f"2024-01-15T10:30:45.{i:06d}", f"campaign_{i}")
        for i in range(200)
    ]

    bronze_df = spark.createDataFrame(
        data,
        [
            "impression_id",
            "impression_date",  # This column will be dropped
            "campaign_id",
        ],
    )

    # Apply transform that uses impression_date then drops it
    # The expression F.regexp_replace(F.col("impression_date"), ...) will be cached
    silver_df = bronze_df.withColumn(
        "impression_date_parsed",
        F.to_timestamp(
            F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
            "yyyy-MM-dd'T'HH:mm:ss",
        ),
    ).select(
        "impression_id",
        "campaign_id",
        "impression_date_parsed",
        # impression_date is DROPPED
    )

    # Verify column was dropped
    assert "impression_date" not in silver_df.columns

    # Try to materialize - this is where the bug would occur
    # The cached expression for F.regexp_replace(F.col("impression_date"), ...)
    # might still reference "impression_date" even though it was dropped
    try:
        count = silver_df.count()
        assert count == 200
    except SparkColumnNotFoundError as e:
        error_msg = str(e).lower()
        if "impression_date" in error_msg and "cannot resolve" in error_msg:
            pytest.fail(
                f"Bug reproduced with cache enabled! Got SparkColumnNotFoundError for dropped column 'impression_date': {e}"
            )
        raise

    spark.stop()

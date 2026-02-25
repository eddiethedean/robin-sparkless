"""
Test to reproduce issue #160 by testing lazy Polars expression behavior.

The bug might occur when:
1. Lazy Polars expressions are created that reference columns
2. Those columns are dropped
3. The lazy expressions are evaluated and try to reference the dropped columns

This is different from the cache approach - it's about how Polars lazy expressions
are built up and evaluated.
"""

import pytest
from sparkless import SparkSession, functions as F


def test_lazy_polars_expression_after_column_drop():
    """
    Test if lazy Polars expressions reference dropped columns.

    The issue might be that when we:
    1. Create a DataFrame with operations that build up lazy Polars expressions
    2. Drop a column via select()
    3. The lazy Polars expressions from step 1 still reference the dropped column
    4. When we materialize, Polars tries to evaluate those expressions and fails
    """
    spark = SparkSession.builder.appName("lazy_expr_test").getOrCreate()

    # Create test data
    data = [
        (f"imp_{i:03d}", f"2024-01-15T10:30:45.{i:06d}", f"campaign_{i}")
        for i in range(200)
    ]
    df = spark.createDataFrame(
        data, ["impression_id", "impression_date", "campaign_id"]
    )

    # Build up a chain of operations that will create lazy Polars expressions
    # Each operation might create intermediate lazy expressions
    df_transformed = (
        df.withColumn(
            "date_cleaned", F.regexp_replace(F.col("impression_date"), r"\.\d+", "")
        )
        .withColumn(
            "date_parsed",
            F.to_timestamp(
                F.col("date_cleaned").cast("string"), "yyyy-MM-dd'T'HH:mm:ss"
            ),
        )
        .withColumn("hour", F.hour(F.col("date_parsed")))
    )

    # At this point, we have lazy Polars expressions that reference:
    # - impression_date (in date_cleaned)
    # - date_cleaned (in date_parsed)
    # - date_parsed (in hour)

    # Now drop impression_date and date_cleaned
    df_dropped = df_transformed.select(
        "impression_id",
        "campaign_id",
        "date_parsed",
        "hour",
        # impression_date and date_cleaned are DROPPED
    )

    assert "impression_date" not in df_dropped.columns
    assert "date_cleaned" not in df_dropped.columns

    # Try to materialize - if lazy expressions still reference dropped columns, this will fail
    try:
        count = df_dropped.count()
        assert count == 200

        # Try to collect - this forces full materialization
        rows = df_dropped.collect()
        assert len(rows) == 200
    except Exception as e:
        error_msg = str(e).lower()
        if ("impression_date" in error_msg or "date_cleaned" in error_msg) and (
            "cannot resolve" in error_msg
            or "not found" in error_msg
            or "unable to find" in error_msg
        ):
            pytest.fail(
                f"BUG REPRODUCED! Lazy Polars expression referenced dropped column: {e}\n"
                f"This suggests that lazy expressions built before column drop still reference the dropped columns."
            )
        raise

    spark.stop()


def test_nested_operations_with_drop():
    """
    Test nested operations where inner operations reference columns that get dropped.

    Scenario:
    1. Create nested expression: F.to_timestamp(F.regexp_replace(F.col("impression_date"), ...))
    2. The inner F.regexp_replace creates a Polars expression referencing impression_date
    3. Drop impression_date
    4. Try to evaluate the nested expression - inner expression might still reference dropped column
    """
    spark = SparkSession.builder.appName("nested_ops_test").getOrCreate()

    data = [
        (f"imp_{i:03d}", f"2024-01-15T10:30:45.{i:06d}", f"campaign_{i}")
        for i in range(200)
    ]
    df = spark.createDataFrame(
        data, ["impression_id", "impression_date", "campaign_id"]
    )

    # Create a nested expression that will be translated to nested Polars expressions
    # The inner expression (regexp_replace) references impression_date
    df_with_nested = df.withColumn(
        "date_parsed",
        F.to_timestamp(
            F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
            "yyyy-MM-dd'T'HH:mm:ss",
        ),
    )

    # Materialize to ensure the nested expression is built
    _ = df_with_nested.count()

    # Now drop the column that the nested expression references
    df_dropped = df_with_nested.select(
        "impression_id",
        "campaign_id",
        "date_parsed",
        # impression_date is DROPPED
    )

    assert "impression_date" not in df_dropped.columns

    # Try to materialize - if nested expressions still reference dropped columns, this will fail
    try:
        count = df_dropped.count()
        assert count == 200

        rows = df_dropped.collect()
        assert len(rows) == 200
    except Exception as e:
        error_msg = str(e).lower()
        if "impression_date" in error_msg and (
            "cannot resolve" in error_msg
            or "not found" in error_msg
            or "unable to find" in error_msg
        ):
            pytest.fail(
                f"BUG REPRODUCED! Nested expression referenced dropped column: {e}\n"
                f"This suggests that nested Polars expressions still reference dropped columns."
            )
        raise

    spark.stop()


def test_operations_chain_with_intermediate_drop():
    """
    Test a chain of operations where we drop a column in the middle.

    This tests if operations queued after a drop still try to reference dropped columns.
    """
    spark = SparkSession.builder.appName("chain_drop_test").getOrCreate()

    data = [
        (f"imp_{i:03d}", f"2024-01-15T10:30:45.{i:06d}", f"campaign_{i}")
        for i in range(200)
    ]
    df = spark.createDataFrame(
        data, ["impression_id", "impression_date", "campaign_id"]
    )

    # Create a chain: use column -> drop column -> use another column
    df_chain = (
        df.withColumn(
            "date_parsed",
            F.to_timestamp(
                F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
                "yyyy-MM-dd'T'HH:mm:ss",
            ),
        )
        .select("impression_id", "campaign_id", "date_parsed")  # Drop impression_date
        .withColumn("hour", F.hour(F.col("date_parsed")))  # Use date_parsed after drop
    )

    # Try to materialize - operations after drop should not reference dropped columns
    try:
        count = df_chain.count()
        assert count == 200

        rows = df_chain.collect()
        assert len(rows) == 200
    except Exception as e:
        error_msg = str(e).lower()
        if "impression_date" in error_msg and (
            "cannot resolve" in error_msg
            or "not found" in error_msg
            or "unable to find" in error_msg
        ):
            pytest.fail(
                f"BUG REPRODUCED! Operation after drop referenced dropped column: {e}\n"
                f"This suggests that operations queued after drop still reference dropped columns."
            )
        raise

    spark.stop()


def test_write_operation_after_drop():
    """
    Test write operation after dropping columns.

    The issue mentions the bug occurs during "materialization, validation, or write operations".
    Let's test write operations specifically.
    """
    spark = SparkSession.builder.appName("write_test").getOrCreate()

    data = [
        (f"imp_{i:03d}", f"2024-01-15T10:30:45.{i:06d}", f"campaign_{i}")
        for i in range(200)
    ]
    df = spark.createDataFrame(
        data, ["impression_id", "impression_date", "campaign_id"]
    )

    # Use impression_date then drop it
    df_transformed = df.withColumn(
        "date_parsed",
        F.to_timestamp(
            F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
            "yyyy-MM-dd'T'HH:mm:ss",
        ),
    ).select("impression_id", "campaign_id", "date_parsed")

    assert "impression_date" not in df_transformed.columns

    # Try to write - this might trigger the bug
    try:
        df_transformed.write.mode("overwrite").saveAsTable("test_table_160")

        # Read it back to verify
        result = spark.table("test_table_160")
        count = result.count()
        assert count == 200

        # Clean up
        spark.sql("DROP TABLE IF EXISTS test_table_160")
    except Exception as e:
        error_msg = str(e).lower()
        if "impression_date" in error_msg and (
            "cannot resolve" in error_msg
            or "not found" in error_msg
            or "unable to find" in error_msg
        ):
            pytest.fail(
                f"BUG REPRODUCED during write! Error about dropped column: {e}\n"
                f"This suggests that write operations try to resolve dropped columns."
            )
        raise

    spark.stop()

"""
PySpark parity tests for Issue #336: WindowFunction comparison operators.

Tests that sparkless WindowFunction comparison operations match PySpark behavior.
"""

import os
import pytest
from sparkless.sql import SparkSession
from sparkless import functions as F
from sparkless.window import Window


class TestWindowFunctionComparisonParity:
    """PySpark parity tests for WindowFunction comparison operators."""

    @pytest.mark.skipif(
        os.getenv("MOCK_SPARK_TEST_BACKEND") != "pyspark",
        reason="PySpark parity test - only run with MOCK_SPARK_TEST_BACKEND=pyspark",
    )
    def test_window_function_gt_comparison_parity(self):
        """Test WindowFunction > comparison matches PySpark."""
        spark = SparkSession.builder.appName("issue-336-parity").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A"},
                    {"Name": "Bob", "Type": "B"},
                ]
            )

            w = Window().partitionBy("Type").orderBy("Type")
            result = df.withColumn(
                "GT-Zero",
                F.when(F.row_number().over(w) > 0, F.lit(True)).otherwise(F.lit(False)),
            )
            rows = result.collect()

            assert len(rows) == 2
            assert rows[0]["GT-Zero"] is True
            assert rows[1]["GT-Zero"] is True
        finally:
            spark.stop()

    @pytest.mark.skipif(
        os.getenv("MOCK_SPARK_TEST_BACKEND") != "pyspark",
        reason="PySpark parity test - only run with MOCK_SPARK_TEST_BACKEND=pyspark",
    )
    def test_window_function_eq_comparison_parity(self):
        """Test WindowFunction == comparison matches PySpark."""
        spark = SparkSession.builder.appName("issue-336-parity").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A"},
                    {"Name": "Bob", "Type": "B"},
                ]
            )

            w = Window().partitionBy("Type").orderBy("Type")
            result = df.withColumn(
                "EQ-One",
                F.when(F.row_number().over(w) == 1, F.lit("First")).otherwise(
                    F.lit("Other")
                ),
            )
            rows = result.collect()

            assert len(rows) == 2
            # Both should be "First" since each is rank 1 in their partition
            assert rows[0]["EQ-One"] == "First"
            assert rows[1]["EQ-One"] == "First"
        finally:
            spark.stop()

    @pytest.mark.skipif(
        os.getenv("MOCK_SPARK_TEST_BACKEND") != "pyspark",
        reason="PySpark parity test - only run with MOCK_SPARK_TEST_BACKEND=pyspark",
    )
    def test_window_function_comparison_in_filter_parity(self):
        """Test WindowFunction comparison in filter matches PySpark."""
        spark = SparkSession.builder.appName("issue-336-parity").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Bob", "Type": "A", "Score": 90},
                    {"Name": "Charlie", "Type": "A", "Score": 80},
                ]
            )

            w = Window().partitionBy("Type").orderBy(F.col("Score").desc())
            result = df.filter(F.row_number().over(w) == 1).select(
                "Name", "Type", "Score"
            )
            rows = result.collect()

            assert len(rows) == 1
            assert rows[0]["Name"] == "Alice"
            assert rows[0]["Score"] == 100
        finally:
            spark.stop()

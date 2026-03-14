"""
PySpark parity tests for Issue #336: WindowFunction comparison. Uses get_imports from fixture only.
"""

import os

import pytest

from sparkless.testing import get_imports

_imports = get_imports()
SparkSession = _imports.SparkSession
F = _imports.F
Window = _imports.Window


class TestWindowFunctionComparisonParity:
    """PySpark parity tests for WindowFunction comparison operators."""

    @pytest.mark.skipif(
        os.getenv("SPARKLESS_TEST_MODE") != "pyspark",
        reason="PySpark parity test - only run with SPARKLESS_TEST_MODE=pyspark",
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
        os.getenv("SPARKLESS_TEST_MODE") != "pyspark",
        reason="PySpark parity test - only run with SPARKLESS_TEST_MODE=pyspark",
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
        os.getenv("SPARKLESS_TEST_MODE") != "pyspark",
        reason="PySpark parity test - only run with SPARKLESS_TEST_MODE=pyspark",
    )
    def test_window_function_comparison_in_filter_parity(self):
        """Test WindowFunction comparison in filter raises error (PySpark parity).

        PySpark raises AnalysisException: "It is not allowed to use window
        functions inside WHERE clause." Sparkless must match this behavior.
        """
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
            # PySpark raises AnalysisException for window functions in WHERE clause
            with pytest.raises(Exception) as exc_info:
                df.filter(F.row_number().over(w) == 1).select(
                    "Name", "Type", "Score"
                ).collect()

            msg = str(exc_info.value).lower()
            assert "window" in msg and "where" in msg, (
                f"Expected error about window functions in WHERE clause, got: {exc_info.value}"
            )
        finally:
            spark.stop()

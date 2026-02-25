"""
PySpark parity tests for Issue #339: Column subscript notation for struct access.

Tests that Column subscript notation (e.g., F.col("StructVal")["E1"]) matches PySpark behavior.
"""

import os
import pytest

pytestmark = pytest.mark.skipif(
    os.getenv("MOCK_SPARK_TEST_BACKEND") != "pyspark",
    reason="PySpark parity test - only run with MOCK_SPARK_TEST_BACKEND=pyspark",
)


def get_spark_imports():
    """Get Spark imports based on backend."""
    backend = os.getenv("MOCK_SPARK_TEST_BACKEND", "sparkless")
    if backend == "pyspark":
        from pyspark.sql import SparkSession
        import pyspark.sql.functions as F

        return SparkSession, F
    else:
        from sparkless.sql import SparkSession
        from sparkless import functions as F

        return SparkSession, F


class TestColumnSubscriptParity:
    """PySpark parity tests for Column subscript notation."""

    def test_column_subscript_single_field_parity(self):
        """Test that Column subscript notation matches PySpark for single field."""
        SparkSession, F = get_spark_imports()
        spark = SparkSession.builder.appName("issue-339-parity").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "StructVal": {"E1": 1, "E2": 2}},
                    {"Name": "Bob", "StructVal": {"E1": 3, "E2": 4}},
                ]
            )

            # Test subscript notation
            result = df.withColumn("Extract-E1", F.col("StructVal")["E1"])
            rows = result.collect()

            assert len(rows) == 2
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            bob_row = next(row for row in rows if row["Name"] == "Bob")

            assert alice_row["Extract-E1"] == 1
            assert bob_row["Extract-E1"] == 3
        finally:
            spark.stop()

    def test_column_subscript_multiple_fields_parity(self):
        """Test that Column subscript notation matches PySpark for multiple fields."""
        SparkSession, F = get_spark_imports()
        spark = SparkSession.builder.appName("issue-339-parity").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "StructVal": {"E1": 1, "E2": 2}},
                    {"Name": "Bob", "StructVal": {"E1": 3, "E2": 4}},
                ]
            )

            result = df.withColumn("Extract-E1", F.col("StructVal")["E1"]).withColumn(
                "Extract-E2", F.col("StructVal")["E2"]
            )
            rows = result.collect()

            assert len(rows) == 2
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            bob_row = next(row for row in rows if row["Name"] == "Bob")

            assert alice_row["Extract-E1"] == 1
            assert alice_row["Extract-E2"] == 2
            assert bob_row["Extract-E1"] == 3
            assert bob_row["Extract-E2"] == 4
        finally:
            spark.stop()

    def test_column_subscript_equals_dot_notation_parity(self):
        """Test that subscript notation produces same results as dot notation in PySpark."""
        SparkSession, F = get_spark_imports()
        spark = SparkSession.builder.appName("issue-339-parity").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "StructVal": {"E1": 1, "E2": 2}},
                    {"Name": "Bob", "StructVal": {"E1": 3, "E2": 4}},
                ]
            )

            result_subscript = df.withColumn("Extract-E1", F.col("StructVal")["E1"])
            result_dot = df.withColumn("Extract-E1", F.col("StructVal.E1"))

            rows_subscript = result_subscript.collect()
            rows_dot = result_dot.collect()

            assert len(rows_subscript) == len(rows_dot) == 2

            for sub_row, dot_row in zip(rows_subscript, rows_dot):
                assert sub_row["Extract-E1"] == dot_row["Extract-E1"]
        finally:
            spark.stop()

"""
PySpark parity tests for DataFrame groupBy operations.

Tests validate that Sparkless groupBy operations behave identically to PySpark.
"""

from tests.fixtures.parity_base import ParityTestBase
from tests.fixtures.spark_imports import get_spark_imports


class TestGroupByParity(ParityTestBase):
    """Test DataFrame groupBy operations parity with PySpark."""

    def test_group_by(self, spark):
        """Test groupBy matches PySpark behavior."""
        expected = self.load_expected("dataframe_operations", "group_by")

        df = spark.createDataFrame(expected["input_data"])
        # Use .count() shorthand - expected output was generated from this (column "count")
        result = df.groupBy("department").count()

        self.assert_parity(result, expected)

    def test_aggregation(self, spark):
        """Test aggregation matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("dataframe_operations", "aggregation")

        df = spark.createDataFrame(expected["input_data"])
        result = df.groupBy("department").agg(
            F.avg("salary").alias("avg_salary"), F.count("id").alias("count")
        )

        self.assert_parity(result, expected)

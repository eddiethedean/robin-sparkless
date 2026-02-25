"""
PySpark parity tests for DataFrame transformation operations.

Tests validate that Sparkless transformation operations behave identically to PySpark.
"""

from tests.fixtures.parity_base import ParityTestBase


class TestTransformationsParity(ParityTestBase):
    """Test DataFrame transformation operations parity with PySpark."""

    def test_with_column(self, spark):
        """Test withColumn matches PySpark behavior."""
        expected = self.load_expected("dataframe_operations", "with_column")

        df = spark.createDataFrame(expected["input_data"])
        result = df.withColumn("bonus", df.salary * 0.1)

        self.assert_parity(result, expected)

    def test_drop_column(self, spark):
        """Test drop matches PySpark behavior."""
        expected = self.load_expected("dataframe_operations", "drop_column")

        df = spark.createDataFrame(expected["input_data"])
        result = df.drop("department")

        self.assert_parity(result, expected)

    def test_distinct(self, spark):
        """Test distinct matches PySpark behavior."""
        expected = self.load_expected("dataframe_operations", "distinct")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select("department").distinct()

        self.assert_parity(result, expected)

    def test_order_by(self, spark):
        """Test orderBy matches PySpark behavior."""
        expected = self.load_expected("dataframe_operations", "order_by")

        df = spark.createDataFrame(expected["input_data"])
        result = df.orderBy("salary")

        self.assert_parity(result, expected)

    def test_order_by_desc(self, spark):
        """Test orderBy desc matches PySpark behavior."""
        expected = self.load_expected("dataframe_operations", "order_by_desc")

        df = spark.createDataFrame(expected["input_data"])
        result = df.orderBy(df.salary.desc())

        self.assert_parity(result, expected)

    def test_limit(self, spark):
        """Test limit matches PySpark behavior."""
        expected = self.load_expected("dataframe_operations", "limit")

        df = spark.createDataFrame(expected["input_data"])
        result = df.limit(2)

        self.assert_parity(result, expected)

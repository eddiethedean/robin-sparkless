"""
PySpark parity tests for DataFrame select operations.

Tests validate that Sparkless select operations behave identically to PySpark.
"""

from tests.fixtures.parity_base import ParityTestBase


class TestSelectParity(ParityTestBase):
    """Test DataFrame select operations parity with PySpark."""

    def test_basic_select(self, spark):
        """Test basic select matches PySpark behavior."""
        expected = self.load_expected("dataframe_operations", "basic_select")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select("id", "name", "age")

        self.assert_parity(result, expected)

    def test_select_with_alias(self, spark):
        """Test select with alias matches PySpark behavior."""
        expected = self.load_expected("dataframe_operations", "select_with_alias")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(df.id.alias("user_id"), df.name.alias("full_name"))

        self.assert_parity(result, expected)

    def test_column_access(self, spark):
        """Test column access matches PySpark behavior."""
        expected = self.load_expected("dataframe_operations", "column_access")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(df["id"], df["name"], df["salary"])

        self.assert_parity(result, expected)

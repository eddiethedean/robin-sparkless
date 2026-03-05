"""
PySpark parity tests for DataFrame set operations.

Tests validate that Sparkless set operations behave identically to PySpark.
"""

from tests.fixtures.parity_base import ParityTestBase


class TestSetOperationsParity(ParityTestBase):
    """Test DataFrame set operations parity with PySpark."""

    def test_union(self, spark):
        """Test union matches PySpark behavior."""
        expected = self.load_expected("set_operations", "union")

        # Set operations expected outputs combine input data
        # Split based on known structure: first 3 rows are df1, last 3 are df2
        all_data = expected["input_data"]
        df1_data = (
            all_data[:3] if len(all_data) >= 6 else all_data[: len(all_data) // 2]
        )
        df2_data = (
            all_data[3:6] if len(all_data) >= 6 else all_data[len(all_data) // 2 :]
        )

        df1 = spark.createDataFrame(df1_data)
        df2 = spark.createDataFrame(df2_data)
        result = df1.union(df2)

        self.assert_parity(result, expected)

    def test_union_all(self, spark):
        """Test union (unionAll) matches PySpark behavior."""
        expected = self.load_expected("set_operations", "union_all")

        all_data = expected["input_data"]
        df1_data = (
            all_data[:3] if len(all_data) >= 6 else all_data[: len(all_data) // 2]
        )
        df2_data = (
            all_data[3:6] if len(all_data) >= 6 else all_data[len(all_data) // 2 :]
        )

        df1 = spark.createDataFrame(df1_data)
        df2 = spark.createDataFrame(df2_data)
        result = df1.union(df2)

        self.assert_parity(result, expected)

    def test_intersect(self, spark):
        """Test intersect matches PySpark behavior."""
        expected = self.load_expected("set_operations", "intersect")

        all_data = expected["input_data"]
        df1_data = (
            all_data[:3] if len(all_data) >= 6 else all_data[: len(all_data) // 2]
        )
        df2_data = (
            all_data[3:6] if len(all_data) >= 6 else all_data[len(all_data) // 2 :]
        )

        df1 = spark.createDataFrame(df1_data)
        df2 = spark.createDataFrame(df2_data)
        result = df1.intersect(df2)

        self.assert_parity(result, expected)

    def test_except(self, spark):
        """Test except matches PySpark behavior."""
        expected = self.load_expected("set_operations", "except")

        all_data = expected["input_data"]
        df1_data = (
            all_data[:3] if len(all_data) >= 6 else all_data[: len(all_data) // 2]
        )
        df2_data = (
            all_data[3:6] if len(all_data) >= 6 else all_data[len(all_data) // 2 :]
        )

        df1 = spark.createDataFrame(df1_data)
        df2 = spark.createDataFrame(df2_data)
        result = df1.exceptAll(df2)  # exceptAll is what PySpark's except() does

        self.assert_parity(result, expected)

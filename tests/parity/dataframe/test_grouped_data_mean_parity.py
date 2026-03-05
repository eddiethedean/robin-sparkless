"""
PySpark parity tests for GroupedData.mean() method.

Tests validate that Sparkless GroupedData.mean() behaves identically to PySpark.
"""

from tests.fixtures.parity_base import ParityTestBase


class TestGroupedDataMeanParity(ParityTestBase):
    """Test GroupedData.mean() parity with PySpark."""

    def test_grouped_data_mean_single_column(self, spark):
        """Test GroupedData.mean() with single column matches PySpark behavior."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1},
                {"Name": "Alice", "Value": 10},
                {"Name": "Bob", "Value": 5},
            ]
        )

        result = df.groupBy("Name").mean("Value")
        rows = result.collect()

        assert len(rows) == 2
        # Find Alice and Bob rows
        alice_row = next(row for row in rows if row["Name"] == "Alice")
        bob_row = next(row for row in rows if row["Name"] == "Bob")

        # Alice: (1 + 10) / 2 = 5.5
        assert alice_row["avg(Value)"] == 5.5
        # Bob: 5 / 1 = 5.0
        assert bob_row["avg(Value)"] == 5.0

    def test_grouped_data_mean_multiple_columns(self, spark):
        """Test GroupedData.mean() with multiple columns matches PySpark behavior."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value1": 1, "Value2": 2},
                {"Name": "Alice", "Value1": 10, "Value2": 20},
                {"Name": "Bob", "Value1": 5, "Value2": 6},
            ]
        )

        result = df.groupBy("Name").mean("Value1", "Value2")
        rows = result.collect()

        assert len(rows) == 2
        alice_row = next(row for row in rows if row["Name"] == "Alice")
        bob_row = next(row for row in rows if row["Name"] == "Bob")

        # Alice: Value1 = (1 + 10) / 2 = 5.5, Value2 = (2 + 20) / 2 = 11.0
        assert alice_row["avg(Value1)"] == 5.5
        assert alice_row["avg(Value2)"] == 11.0
        # Bob: Value1 = 5.0, Value2 = 6.0
        assert bob_row["avg(Value1)"] == 5.0
        assert bob_row["avg(Value2)"] == 6.0

    def test_grouped_data_mean_equals_avg(self, spark):
        """Test that mean() produces same results as avg() matches PySpark behavior."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1},
                {"Name": "Alice", "Value": 10},
                {"Name": "Bob", "Value": 5},
            ]
        )

        result_mean = df.groupBy("Name").mean("Value")
        result_avg = df.groupBy("Name").avg("Value")

        rows_mean = result_mean.collect()
        rows_avg = result_avg.collect()

        assert len(rows_mean) == len(rows_avg) == 2

        # Results should be identical
        for mean_row in rows_mean:
            avg_row = next(row for row in rows_avg if row["Name"] == mean_row["Name"])
            assert mean_row["avg(Value)"] == avg_row["avg(Value)"]

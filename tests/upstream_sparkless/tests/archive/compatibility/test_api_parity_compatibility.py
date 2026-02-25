"""
Compatibility tests for API parity using expected outputs.

This module validates that mock-spark API operations produce
the same results as PySpark by comparing against pre-generated expected outputs.
"""

import pytest
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal


@pytest.mark.compatibility
class TestAPIParityCompatibility:
    """Test API parity compatibility against expected PySpark outputs."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        from sparkless import SparkSession

        session = SparkSession("api_parity_test")
        yield session
        session.stop()

    def test_select_operations_parity(self, spark):
        """Test select operations against expected outputs."""
        expected = load_expected_output("dataframe_operations", "basic_select")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select("id", "name", "age")

        assert_dataframes_equal(result, expected)

    def test_filter_operations_parity(self, spark):
        """Test filter operations against expected outputs."""
        expected = load_expected_output("dataframe_operations", "filter_operations")

        df = spark.createDataFrame(expected["input_data"])
        result = df.filter(df.age > 30)

        assert_dataframes_equal(result, expected)

    def test_with_column_operations_parity(self, spark):
        """Test withColumn operations against expected outputs."""
        expected = load_expected_output("dataframe_operations", "with_column")

        df = spark.createDataFrame(expected["input_data"])
        result = df.withColumn("bonus", df.salary * 0.1)

        assert_dataframes_equal(result, expected)

    def test_column_access_patterns_parity(self, spark):
        """Test column access patterns against expected outputs."""
        expected = load_expected_output("dataframe_operations", "column_access")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select("id", "name", "salary")

        assert_dataframes_equal(result, expected)

    def test_alias_operations_parity(self, spark):
        """Test alias operations against expected outputs."""
        expected = load_expected_output("dataframe_operations", "select_with_alias")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(df.id.alias("user_id"), df.name.alias("full_name"))

        assert_dataframes_equal(result, expected)

    def test_drop_operations_parity(self, spark):
        """Test drop operations against expected outputs."""
        expected = load_expected_output("dataframe_operations", "drop_column")

        df = spark.createDataFrame(expected["input_data"])
        result = df.drop("department")

        assert_dataframes_equal(result, expected)

    def test_distinct_operations_parity(self, spark):
        """Test distinct operations against expected outputs."""
        expected = load_expected_output("dataframe_operations", "distinct")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select("department").distinct()

        assert_dataframes_equal(result, expected)

    def test_order_by_operations_parity(self, spark):
        """Test orderBy operations against expected outputs."""
        expected = load_expected_output("dataframe_operations", "order_by")

        df = spark.createDataFrame(expected["input_data"])
        result = df.orderBy("salary")

        assert_dataframes_equal(result, expected)

    def test_group_by_operations_parity(self, spark):
        """Test groupBy operations against expected outputs."""
        expected = load_expected_output("dataframe_operations", "group_by")

        df = spark.createDataFrame(expected["input_data"])
        result = df.groupBy("department").count()

        assert_dataframes_equal(result, expected)

    def test_aggregation_operations_parity(self, spark):
        """Test aggregation operations against expected outputs."""
        expected = load_expected_output("dataframe_operations", "aggregation")

        from sparkless import F

        df = spark.createDataFrame(expected["input_data"])
        result = df.groupBy("department").agg(
            F.avg("salary").alias("avg_salary"), F.count("id").alias("count")
        )

        assert_dataframes_equal(result, expected)

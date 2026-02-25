"""
Compatibility tests for DataFrame operations using expected outputs.

This module validates that mock-spark DataFrame operations produce
the same results as PySpark by comparing against pre-generated expected outputs.
"""

import pytest
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal


class TestDataFrameCompatibility:
    """Test DataFrame operation compatibility against expected PySpark outputs."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        from sparkless import SparkSession

        session = SparkSession("compatibility_test")
        yield session
        session.stop()

    def test_basic_select_operations(self, spark):
        """Test basic select operations against expected outputs."""
        # Load expected output
        expected = load_expected_output("dataframe_operations", "basic_select")

        # Create DataFrame with same input data
        df = spark.createDataFrame(expected["input_data"])

        # Perform the operation
        result = df.select("id", "name", "age")

        # Compare with expected output
        assert_dataframes_equal(result, expected)

    def test_select_with_alias(self, spark):
        """Test select with aliases against expected outputs."""
        expected = load_expected_output("dataframe_operations", "select_with_alias")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(df.id.alias("user_id"), df.name.alias("full_name"))

        assert_dataframes_equal(result, expected)

    def test_filter_operations(self, spark):
        """Test filter operations against expected outputs."""
        expected = load_expected_output("dataframe_operations", "filter_operations")

        df = spark.createDataFrame(expected["input_data"])
        result = df.filter(df.age > 30)

        assert_dataframes_equal(result, expected)

    def test_filter_with_boolean(self, spark):
        """Test boolean filter operations against expected outputs."""
        expected = load_expected_output("dataframe_operations", "filter_with_boolean")

        df = spark.createDataFrame(expected["input_data"])
        result = df.filter(df.salary > 60000)

        assert_dataframes_equal(result, expected)

    def test_with_column(self, spark):
        """Test withColumn operations against expected outputs."""
        expected = load_expected_output("dataframe_operations", "with_column")

        df = spark.createDataFrame(expected["input_data"])
        result = df.withColumn("bonus", df.salary * 0.1)

        assert_dataframes_equal(result, expected)

    def test_drop_column(self, spark):
        """Test drop operations against expected outputs."""
        expected = load_expected_output("dataframe_operations", "drop_column")

        df = spark.createDataFrame(expected["input_data"])
        result = df.drop("department")

        assert_dataframes_equal(result, expected)

    def test_distinct(self, spark):
        """Test distinct operations against expected outputs."""
        expected = load_expected_output("dataframe_operations", "distinct")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select("department").distinct()

        assert_dataframes_equal(result, expected)

    def test_order_by(self, spark):
        """Test orderBy operations against expected outputs."""
        expected = load_expected_output("dataframe_operations", "order_by")

        df = spark.createDataFrame(expected["input_data"])
        result = df.orderBy("salary")

        assert_dataframes_equal(result, expected)

    def test_order_by_desc(self, spark):
        """Test orderBy desc operations against expected outputs."""
        expected = load_expected_output("dataframe_operations", "order_by_desc")

        df = spark.createDataFrame(expected["input_data"])
        result = df.orderBy(df.salary.desc())

        assert_dataframes_equal(result, expected)

    def test_limit(self, spark):
        """Test limit operations against expected outputs."""
        expected = load_expected_output("dataframe_operations", "limit")

        df = spark.createDataFrame(expected["input_data"])
        result = df.limit(2)

        assert_dataframes_equal(result, expected)

    def test_group_by_operations(self, spark):
        """Test groupBy operations against expected outputs."""
        expected = load_expected_output("dataframe_operations", "group_by")

        df = spark.createDataFrame(expected["input_data"])
        result = df.groupBy("department").count()

        assert_dataframes_equal(result, expected)

    def test_aggregation_operations(self, spark):
        """Test aggregation operations against expected outputs."""
        expected = load_expected_output("dataframe_operations", "aggregation")

        from sparkless import F

        df = spark.createDataFrame(expected["input_data"])
        result = df.groupBy("department").agg(
            F.avg("salary").alias("avg_salary"), F.count("id").alias("count")
        )

        assert_dataframes_equal(result, expected)

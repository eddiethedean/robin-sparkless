"""
Compatibility tests for extended window functions.

This module validates extended window functions against pre-generated PySpark outputs.
"""

import pytest
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal
from sparkless import F


class TestWindowFunctionsExtendedCompatibility:
    """Test extended window functions against expected PySpark outputs."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        from sparkless import SparkSession

        session = SparkSession("window_functions_test")
        yield session
        session.stop()

    def test_cume_dist(self, spark):
        """Test cume_dist window function."""
        expected = load_expected_output("windows", "cume_dist")
        df = spark.createDataFrame(expected["input_data"])
        from sparkless.window import Window

        window_spec = Window.partitionBy("dept").orderBy("salary")
        result = df.withColumn("cume_dist", F.cume_dist().over(window_spec))
        assert_dataframes_equal(result, expected)

    def test_dense_rank(self, spark):
        """Test dense_rank window function."""
        expected = load_expected_output("windows", "dense_rank")
        df = spark.createDataFrame(expected["input_data"])
        from sparkless.window import Window

        window_spec = Window.partitionBy("dept").orderBy("salary")
        result = df.withColumn("dense_rank", F.dense_rank().over(window_spec))
        assert_dataframes_equal(result, expected)

    def test_first_value(self, spark):
        """Test first_value window function."""
        expected = load_expected_output("windows", "first_value")
        df = spark.createDataFrame(expected["input_data"])
        from sparkless.window import Window

        window_spec = Window.partitionBy("dept").orderBy("salary")
        result = df.withColumn("first_salary", F.first("salary").over(window_spec))
        assert_dataframes_equal(result, expected)

    def test_lag(self, spark):
        """Test lag window function."""
        expected = load_expected_output("windows", "lag")
        df = spark.createDataFrame(expected["input_data"])
        from sparkless.window import Window

        window_spec = Window.partitionBy("dept").orderBy("salary")
        result = df.withColumn("lag_salary", F.lag("salary", 1).over(window_spec))
        assert_dataframes_equal(result, expected)

    def test_last_value(self, spark):
        """Test last_value window function."""
        expected = load_expected_output("windows", "last_value")
        df = spark.createDataFrame(expected["input_data"])
        from sparkless.window import Window

        window_spec = Window.partitionBy("dept").orderBy("salary")
        result = df.withColumn("last_salary", F.last("salary").over(window_spec))
        assert_dataframes_equal(result, expected)

    def test_lead(self, spark):
        """Test lead window function."""
        expected = load_expected_output("windows", "lead")
        df = spark.createDataFrame(expected["input_data"])
        from sparkless.window import Window

        window_spec = Window.partitionBy("dept").orderBy("salary")
        result = df.withColumn("lead_salary", F.lead("salary", 1).over(window_spec))
        assert_dataframes_equal(result, expected)

    def test_nth_value(self, spark):
        """Test nth_value window function."""
        expected = load_expected_output("windows", "nth_value")
        df = spark.createDataFrame(expected["input_data"])
        from sparkless.window import Window

        window_spec = Window.partitionBy("dept").orderBy("salary")
        result = df.withColumn("nth", F.nth_value("salary", 2).over(window_spec))
        assert_dataframes_equal(result, expected)

    def test_ntile(self, spark):
        """Test ntile window function."""
        expected = load_expected_output("windows", "ntile")
        df = spark.createDataFrame(expected["input_data"])
        from sparkless.window import Window

        window_spec = Window.partitionBy("dept").orderBy("salary")
        result = df.withColumn("ntile", F.ntile(2).over(window_spec))
        assert_dataframes_equal(result, expected)

    def test_percent_rank(self, spark):
        """Test percent_rank window function."""
        expected = load_expected_output("windows", "percent_rank")
        df = spark.createDataFrame(expected["input_data"])
        from sparkless.window import Window

        window_spec = Window.partitionBy("dept").orderBy("salary")
        result = df.withColumn("percent_rank", F.percent_rank().over(window_spec))
        assert_dataframes_equal(result, expected)

    def test_rank(self, spark):
        """Test rank window function."""
        expected = load_expected_output("windows", "rank")
        df = spark.createDataFrame(expected["input_data"])
        from sparkless.window import Window

        window_spec = Window.partitionBy("dept").orderBy("salary")
        result = df.withColumn("rank", F.rank().over(window_spec))
        assert_dataframes_equal(result, expected)

    def test_row_number(self, spark):
        """Test row_number window function."""
        expected = load_expected_output("windows", "row_number")
        df = spark.createDataFrame(expected["input_data"])
        from sparkless.window import Window

        window_spec = Window.partitionBy("dept").orderBy("salary")
        result = df.withColumn("row_num", F.row_number().over(window_spec))
        assert_dataframes_equal(result, expected)

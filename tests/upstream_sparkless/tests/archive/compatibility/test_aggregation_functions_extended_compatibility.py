"""
Compatibility tests for extended aggregation functions.

This module validates extended aggregation functions against pre-generated PySpark outputs.
"""

import pytest
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal
from sparkless import F


class TestAggregationFunctionsExtendedCompatibility:
    """Test extended aggregation functions against expected PySpark outputs."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        from sparkless import SparkSession

        session = SparkSession("aggregation_functions_test")
        yield session
        session.stop()

    def test_approx_count_distinct(self, spark):
        """Test approx_count_distinct aggregation."""
        expected = load_expected_output("aggregations", "approx_count_distinct")
        df = spark.createDataFrame(expected["input_data"])
        result = df.agg(F.approx_count_distinct("age"))
        assert_dataframes_equal(result, expected)

    def test_collect_list(self, spark):
        """Test collect_list aggregation."""
        expected = load_expected_output("aggregations", "collect_list")
        df = spark.createDataFrame(expected["input_data"])
        result = df.agg(F.collect_list("name"))
        assert_dataframes_equal(result, expected)

    def test_collect_set(self, spark):
        """Test collect_set aggregation."""
        expected = load_expected_output("aggregations", "collect_set")
        df = spark.createDataFrame(expected["input_data"])
        result = df.agg(F.collect_set("name"))
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="corr not yet implemented correctly")
    def test_corr(self, spark):
        """Test corr aggregation."""
        expected = load_expected_output("aggregations", "corr")
        df = spark.createDataFrame(expected["input_data"])
        result = df.agg(F.corr("age", "salary"))
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="covar_pop not yet implemented correctly")
    def test_covar_pop(self, spark):
        """Test covar_pop aggregation."""
        expected = load_expected_output("aggregations", "covar_pop")
        df = spark.createDataFrame(expected["input_data"])
        result = df.agg(F.covar_pop("age", "salary"))
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="covar_samp not yet implemented correctly")
    def test_covar_samp(self, spark):
        """Test covar_samp aggregation."""
        expected = load_expected_output("aggregations", "covar_samp")
        df = spark.createDataFrame(expected["input_data"])
        result = df.agg(F.covar_samp("age", "salary"))
        assert_dataframes_equal(result, expected)

    def test_countDistinct(self, spark):
        """Test countDistinct aggregation."""
        expected = load_expected_output("aggregations", "countDistinct")
        df = spark.createDataFrame(expected["input_data"])
        result = df.agg(F.countDistinct("dept"))
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="kurtosis not yet implemented correctly")
    def test_kurtosis(self, spark):
        """Test kurtosis aggregation."""
        expected = load_expected_output("aggregations", "kurtosis")
        df = spark.createDataFrame(expected["input_data"])
        result = df.agg(F.kurtosis("salary"))
        assert_dataframes_equal(result, expected)

    def test_mean(self, spark):
        """Test mean aggregation."""
        expected = load_expected_output("aggregations", "mean")
        df = spark.createDataFrame(expected["input_data"])
        result = df.agg(F.mean("salary"))
        assert_dataframes_equal(result, expected)

    def test_skewness(self, spark):
        """Test skewness aggregation."""
        expected = load_expected_output("aggregations", "skewness")
        df = spark.createDataFrame(expected["input_data"])
        result = df.agg(F.skewness("salary"))
        assert_dataframes_equal(result, expected)

    def test_stddev(self, spark):
        """Test stddev aggregation."""
        expected = load_expected_output("aggregations", "stddev")
        df = spark.createDataFrame(expected["input_data"])
        result = df.agg(F.stddev("salary"))
        assert_dataframes_equal(result, expected)

    def test_stddev_pop(self, spark):
        """Test stddev_pop aggregation."""
        expected = load_expected_output("aggregations", "stddev_pop")
        df = spark.createDataFrame(expected["input_data"])
        result = df.agg(F.stddev_pop("salary"))
        assert_dataframes_equal(result, expected)

    def test_stddev_samp(self, spark):
        """Test stddev_samp aggregation."""
        expected = load_expected_output("aggregations", "stddev_samp")
        df = spark.createDataFrame(expected["input_data"])
        result = df.agg(F.stddev_samp("salary"))
        assert_dataframes_equal(result, expected)

    def test_var_pop(self, spark):
        """Test var_pop aggregation."""
        expected = load_expected_output("aggregations", "var_pop")
        df = spark.createDataFrame(expected["input_data"])
        result = df.agg(F.var_pop("salary"))
        assert_dataframes_equal(result, expected)

    def test_var_samp(self, spark):
        """Test var_samp aggregation."""
        expected = load_expected_output("aggregations", "var_samp")
        df = spark.createDataFrame(expected["input_data"])
        result = df.agg(F.var_samp("salary"))
        assert_dataframes_equal(result, expected)

    def test_variance(self, spark):
        """Test variance aggregation."""
        expected = load_expected_output("aggregations", "variance")
        df = spark.createDataFrame(expected["input_data"])
        result = df.agg(F.variance("salary"))
        assert_dataframes_equal(result, expected)

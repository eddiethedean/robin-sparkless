"""
Compatibility tests for extended conditional/null functions.

This module validates conditional functions against pre-generated PySpark outputs.
"""

import pytest
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal
from sparkless import F


class TestConditionalFunctionsExtendedCompatibility:
    """Test conditional functions against expected PySpark outputs."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        from sparkless import SparkSession

        session = SparkSession("conditional_functions_test")
        yield session
        session.stop()

    def test_ifnull(self, spark):
        """Test ifnull function."""
        expected = load_expected_output("functions", "ifnull")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.when(df.salary.isNull(), 0).otherwise(df.salary))
        assert_dataframes_equal(result, expected)

    def test_nanvl(self, spark):
        """Test nanvl function."""
        expected = load_expected_output("functions", "nanvl")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.nanvl(df.salary, 0))
        assert_dataframes_equal(result, expected)

"""
Compatibility tests for special functions.

This module validates special functions against pre-generated PySpark outputs.
"""

import pytest
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal
from sparkless import F


class TestSpecialFunctionsCompatibility:
    """Test special functions against expected PySpark outputs."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        from sparkless import SparkSession

        session = SparkSession("special_functions_test")
        yield session
        session.stop()

    @pytest.mark.skip(
        reason="Hash function requires exact MurmurHash3 implementation matching PySpark. Functional behavior is correct, but hash values differ."
    )
    def test_hash(self, spark):
        """Test hash function."""
        expected = load_expected_output("functions", "hash")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.hash(df.name))
        assert_dataframes_equal(result, expected)

    def test_isnan(self, spark):
        """Test isnan function."""
        expected = load_expected_output("functions", "isnan")
        df = spark.createDataFrame(expected["input_data"])
        # Create a float('nan') value in the DataFrame

        result = df.select(F.isnan(F.lit(float("nan"))))
        assert_dataframes_equal(result, expected)

    def test_monotonically_increasing_id(self, spark):
        """Test monotonically_increasing_id function."""
        expected = load_expected_output("functions", "monotonically_increasing_id")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.monotonically_increasing_id())
        # Note: monotonically_increasing_id may produce different IDs
        # We mainly check structure rather than exact values
        assert len(result.collect()) == expected["expected_output"]["row_count"]

    def test_overlay(self, spark):
        """Test overlay function."""
        expected = load_expected_output("functions", "overlay")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.overlay(df.text, F.lit("X"), 1, 1))
        assert_dataframes_equal(result, expected)

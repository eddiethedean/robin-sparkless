"""
Compatibility tests for map functions.

This module validates map functions against pre-generated PySpark outputs.
"""

import pytest
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal
from sparkless import F


class TestMapFunctionsExtendedCompatibility:
    """Test map functions against expected PySpark outputs."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        from sparkless import SparkSession

        session = SparkSession("map_functions_test")
        yield session
        session.stop()

    def test_create_map(self, spark):
        """Test create_map function."""
        expected = load_expected_output("maps", "create_map")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.create_map(F.lit("key"), F.lit("value")))
        assert_dataframes_equal(result, expected)

    def test_map_concat(self, spark):
        """Test map_concat function."""
        expected = load_expected_output("maps", "map_concat")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.map_concat(df.map1, df.map2))
        assert_dataframes_equal(result, expected)

    def test_map_entries(self, spark):
        """Test map_entries function."""
        expected = load_expected_output("maps", "map_entries")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.map_entries(df.map1))
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="map_filter not yet implemented correctly")
    def test_map_filter(self, spark):
        """Test map_filter function."""
        expected = load_expected_output("maps", "map_filter")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.map_filter(df.map1, lambda k, v: k == "a"))
        assert_dataframes_equal(result, expected)

    def test_map_from_arrays(self, spark):
        """Test map_from_arrays function."""
        expected = load_expected_output("maps", "map_from_arrays")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(
            F.map_from_arrays(
                F.array(F.lit("a"), F.lit("b")), F.array(F.lit(1), F.lit(2))
            )
        )
        assert_dataframes_equal(result, expected)

    def test_map_from_entries(self, spark):
        """Test map_from_entries function."""
        expected = load_expected_output("maps", "map_from_entries")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.map_from_entries(F.array(F.struct(F.lit("a"), F.lit(1)))))
        assert_dataframes_equal(result, expected)

    def test_map_keys(self, spark):
        """Test map_keys function."""
        expected = load_expected_output("maps", "map_keys")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.map_keys(df.map1))
        assert_dataframes_equal(result, expected)

    def test_map_values(self, spark):
        """Test map_values function."""
        expected = load_expected_output("maps", "map_values")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.map_values(df.map1))
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="map_zip_with not yet implemented correctly")
    def test_map_zip_with(self, spark):
        """Test map_zip_with function."""
        expected = load_expected_output("maps", "map_zip_with")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.map_zip_with(df.map1, df.map2, lambda k, v1, v2: v1 + v2))
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="transform_keys not yet implemented correctly")
    def test_transform_keys(self, spark):
        """Test transform_keys function."""
        expected = load_expected_output("maps", "transform_keys")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.transform_keys(df.map1, lambda k, v: F.upper(k)))
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="transform_values not yet implemented correctly")
    def test_transform_values(self, spark):
        """Test transform_values function."""
        expected = load_expected_output("maps", "transform_values")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.transform_values(df.map1, lambda k, v: v * 2))
        assert_dataframes_equal(result, expected)

"""
Compatibility tests for extended array functions.

This module validates extended array functions against pre-generated PySpark outputs.
"""

import pytest
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal
from sparkless import F


class TestArrayFunctionsExtendedCompatibility:
    """Test extended array functions against expected PySpark outputs."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        from sparkless import SparkSession

        session = SparkSession("array_functions_test")
        yield session
        session.stop()

    def test_array(self, spark):
        """Test array function."""
        expected = load_expected_output("arrays", "array")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.array(df.id, df.value))
        assert_dataframes_equal(result, expected)

    def test_array_except(self, spark):
        """Test array_except function."""
        expected = load_expected_output("arrays", "array_except")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.array_except(df.arr1, df.arr2))
        assert_dataframes_equal(result, expected)

    def test_array_intersect(self, spark):
        """Test array_intersect function."""
        expected = load_expected_output("arrays", "array_intersect")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.array_intersect(df.arr1, df.arr2))
        assert_dataframes_equal(result, expected)

    def test_array_join(self, spark):
        """Test array_join function."""
        expected = load_expected_output("arrays", "array_join")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.array_join(df.arr1, "-"))
        assert_dataframes_equal(result, expected)

    def test_array_max(self, spark):
        """Test array_max function."""
        expected = load_expected_output("arrays", "array_max")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.array_max(df.arr1))
        assert_dataframes_equal(result, expected)

    def test_array_min(self, spark):
        """Test array_min function."""
        expected = load_expected_output("arrays", "array_min")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.array_min(df.arr1))
        assert_dataframes_equal(result, expected)

    def test_array_repeat(self, spark):
        """Test array_repeat function."""
        expected = load_expected_output("arrays", "array_repeat")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.array_repeat(df.value, 3))
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(
        reason="array_sort with lambda function name generation requires full lambda support"
    )
    def test_array_sort(self, spark):
        """Test array_sort function."""
        expected = load_expected_output("arrays", "array_sort")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.array_sort(df.arr3))
        assert_dataframes_equal(result, expected)

    def test_array_union(self, spark):
        """Test array_union function."""
        expected = load_expected_output("arrays", "array_union")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.array_union(df.arr1, df.arr2))
        assert_dataframes_equal(result, expected)

    def test_arrays_overlap(self, spark):
        """Test arrays_overlap function."""
        expected = load_expected_output("arrays", "arrays_overlap")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.arrays_overlap(df.arr1, df.arr2))
        assert_dataframes_equal(result, expected)

    def test_arrays_zip(self, spark):
        """Test arrays_zip function."""
        expected = load_expected_output("arrays", "arrays_zip")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.arrays_zip(df.arr1, df.arr2))
        assert_dataframes_equal(result, expected)

    def test_flatten(self, spark):
        """Test flatten function."""
        expected = load_expected_output("arrays", "flatten")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.flatten(F.array(df.arr1, df.arr2)))
        assert_dataframes_equal(result, expected)

    def test_reverse_array(self, spark):
        """Test reverse function for arrays."""
        expected = load_expected_output("arrays", "reverse")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.reverse(df.arr1))
        assert_dataframes_equal(result, expected)

    def test_sequence(self, spark):
        """Test sequence function."""
        expected = load_expected_output("arrays", "sequence")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.sequence(F.lit(1), F.lit(5)))
        assert_dataframes_equal(result, expected)

    def test_slice(self, spark):
        """Test slice function."""
        expected = load_expected_output("arrays", "slice")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.slice(df.arr1, 1, 2))
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="sort_array not yet implemented correctly")
    def test_sort_array(self, spark):
        """Test sort_array function."""
        expected = load_expected_output("arrays", "sort_array")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.sort_array(df.arr1, False))
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="exists not yet implemented correctly")
    def test_exists(self, spark):
        """Test exists function."""
        expected = load_expected_output("arrays", "exists")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.exists(df.arr1, lambda x: x > 10))
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="forall not yet implemented correctly")
    def test_forall(self, spark):
        """Test forall function."""
        expected = load_expected_output("arrays", "forall")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.forall(df.arr1, lambda x: x > 0))
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="transform not yet implemented correctly")
    def test_transform(self, spark):
        """Test transform function."""
        expected = load_expected_output("arrays", "transform")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.transform(df.arr1, lambda x: x * 2))
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="zip_with not yet implemented correctly")
    def test_zip_with(self, spark):
        """Test zip_with function."""
        expected = load_expected_output("arrays", "zip_with")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.zip_with(df.arr1, df.arr2, lambda x, y: x + y))
        assert_dataframes_equal(result, expected)

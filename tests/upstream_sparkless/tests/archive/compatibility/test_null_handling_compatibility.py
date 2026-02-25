"""
Compatibility tests for null handling operations using expected outputs.

This module tests MockSpark's null handling functions against PySpark-generated expected outputs
to ensure compatibility across different null operations and edge cases.
"""

import pytest
from sparkless import F
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal


@pytest.mark.compatibility
class TestNullHandlingCompatibility:
    """Tests for null handling operations compatibility using expected outputs."""

    def test_isnull_function(self, mock_spark_session):
        """Test isnull function against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "age": 25, "salary": 50000.0},
            {"id": 2, "name": None, "age": 30, "salary": None},
            {"id": 3, "name": "Charlie", "age": None, "salary": 70000.0},
            {"id": 4, "name": "David", "age": 40, "salary": 80000.0},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.select(F.isnull(df.name))

        expected = load_expected_output("null_handling", "isnull")
        assert_dataframes_equal(result, expected)

    def test_isnotnull_function(self, mock_spark_session):
        """Test isnotnull function against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "age": 25, "salary": 50000.0},
            {"id": 2, "name": None, "age": 30, "salary": None},
            {"id": 3, "name": "Charlie", "age": None, "salary": 70000.0},
            {"id": 4, "name": "David", "age": 40, "salary": 80000.0},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.select(F.isnotnull(df.name))

        expected = load_expected_output("null_handling", "isnotnull")
        assert_dataframes_equal(result, expected)

    def test_coalesce_function(self, mock_spark_session):
        """Test coalesce function against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "age": 25, "salary": 50000.0},
            {"id": 2, "name": None, "age": 30, "salary": None},
            {"id": 3, "name": "Charlie", "age": None, "salary": 70000.0},
            {"id": 4, "name": "David", "age": 40, "salary": 80000.0},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.select(F.coalesce(df.salary, F.lit(0)))

        expected = load_expected_output("null_handling", "coalesce")
        assert_dataframes_equal(result, expected)

    def test_when_otherwise_function(self, mock_spark_session):
        """Test when/otherwise function against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "age": 25, "salary": 50000.0},
            {"id": 2, "name": None, "age": 30, "salary": None},
            {"id": 3, "name": "Charlie", "age": None, "salary": 70000.0},
            {"id": 4, "name": "David", "age": 40, "salary": 80000.0},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.select(F.when(df.salary.isNull(), 0).otherwise(df.salary))

        expected = load_expected_output("null_handling", "when_otherwise")
        assert_dataframes_equal(result, expected)

    def test_nvl_function(self, mock_spark_session):
        """Test nvl function against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "age": 25, "salary": 50000.0},
            {"id": 2, "name": None, "age": 30, "salary": None},
            {"id": 3, "name": "Charlie", "age": None, "salary": 70000.0},
            {"id": 4, "name": "David", "age": 40, "salary": 80000.0},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.select(F.nvl(df.salary, 0))

        expected = load_expected_output("null_handling", "nvl")
        assert_dataframes_equal(result, expected)

    def test_nvl2_function(self, mock_spark_session):
        """Test nvl2 function against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "age": 25, "salary": 50000.0},
            {"id": 2, "name": None, "age": 30, "salary": None},
            {"id": 3, "name": "Charlie", "age": None, "salary": 70000.0},
            {"id": 4, "name": "David", "age": 40, "salary": 80000.0},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.select(F.nvl2(df.salary, df.salary * 1.1, 0))

        expected = load_expected_output("null_handling", "nvl2")
        assert_dataframes_equal(result, expected)

    def test_nullif_function(self, mock_spark_session):
        """Test nullif function against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "age": 25, "salary": 50000.0},
            {"id": 2, "name": None, "age": 30, "salary": None},
            {"id": 3, "name": "Charlie", "age": None, "salary": 70000.0},
            {"id": 4, "name": "David", "age": 40, "salary": 80000.0},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.select(F.nullif(df.age, 30))

        expected = load_expected_output("null_handling", "nullif")
        assert_dataframes_equal(result, expected)

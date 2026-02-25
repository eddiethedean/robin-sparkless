"""
Compatibility tests for set operations using expected outputs.

This module tests MockSpark's set operations against PySpark-generated expected outputs
to ensure compatibility across different set operation types and edge cases.
"""

import pytest
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal


@pytest.mark.compatibility
class TestSetOperationsCompatibility:
    """Tests for set operations compatibility using expected outputs."""

    def test_union_operation(self, mock_spark_session):
        """Test union operation against expected output."""
        df1_data = [
            {"id": 1, "name": "Alice", "age": 25},
            {"id": 2, "name": "Bob", "age": 30},
            {"id": 3, "name": "Charlie", "age": 35},
        ]

        df2_data = [
            {"id": 2, "name": "Bob", "age": 30},
            {"id": 3, "name": "Charlie", "age": 35},
            {"id": 4, "name": "David", "age": 40},
        ]

        df1 = mock_spark_session.createDataFrame(df1_data)
        df2 = mock_spark_session.createDataFrame(df2_data)
        result = df1.union(df2)

        expected = load_expected_output("set_operations", "union")
        assert_dataframes_equal(result, expected)

    def test_union_all_operation(self, mock_spark_session):
        """Test union (unionAll) operation against expected output."""
        df1_data = [
            {"id": 1, "name": "Alice", "age": 25},
            {"id": 2, "name": "Bob", "age": 30},
            {"id": 3, "name": "Charlie", "age": 35},
        ]

        df2_data = [
            {"id": 2, "name": "Bob", "age": 30},
            {"id": 3, "name": "Charlie", "age": 35},
            {"id": 4, "name": "David", "age": 40},
        ]

        df1 = mock_spark_session.createDataFrame(df1_data)
        df2 = mock_spark_session.createDataFrame(df2_data)
        result = df1.union(df2)

        expected = load_expected_output("set_operations", "union_all")
        assert_dataframes_equal(result, expected)

    def test_intersect_operation(self, mock_spark_session):
        """Test intersect operation against expected output."""
        df1_data = [
            {"id": 1, "name": "Alice", "age": 25},
            {"id": 2, "name": "Bob", "age": 30},
            {"id": 3, "name": "Charlie", "age": 35},
        ]

        df2_data = [
            {"id": 2, "name": "Bob", "age": 30},
            {"id": 3, "name": "Charlie", "age": 35},
            {"id": 4, "name": "David", "age": 40},
        ]

        df1 = mock_spark_session.createDataFrame(df1_data)
        df2 = mock_spark_session.createDataFrame(df2_data)
        result = df1.intersect(df2)

        expected = load_expected_output("set_operations", "intersect")
        assert_dataframes_equal(result, expected)

    def test_except_operation(self, mock_spark_session):
        """Test except operation against expected output."""
        df1_data = [
            {"id": 1, "name": "Alice", "age": 25},
            {"id": 2, "name": "Bob", "age": 30},
            {"id": 3, "name": "Charlie", "age": 35},
        ]

        df2_data = [
            {"id": 2, "name": "Bob", "age": 30},
            {"id": 3, "name": "Charlie", "age": 35},
            {"id": 4, "name": "David", "age": 40},
        ]

        df1 = mock_spark_session.createDataFrame(df1_data)
        df2 = mock_spark_session.createDataFrame(df2_data)
        result = df1.exceptAll(df2)

        expected = load_expected_output("set_operations", "except")
        assert_dataframes_equal(result, expected)

    def test_subtract_operation(self, mock_spark_session):
        """Test subtract operation against expected output."""
        df1_data = [
            {"id": 1, "name": "Alice", "age": 25},
            {"id": 2, "name": "Bob", "age": 30},
            {"id": 3, "name": "Charlie", "age": 35},
        ]

        df2_data = [
            {"id": 2, "name": "Bob", "age": 30},
            {"id": 3, "name": "Charlie", "age": 35},
            {"id": 4, "name": "David", "age": 40},
        ]

        df1 = mock_spark_session.createDataFrame(df1_data)
        df2 = mock_spark_session.createDataFrame(df2_data)
        result = df1.subtract(df2)

        expected = load_expected_output("set_operations", "subtract")
        assert_dataframes_equal(result, expected)

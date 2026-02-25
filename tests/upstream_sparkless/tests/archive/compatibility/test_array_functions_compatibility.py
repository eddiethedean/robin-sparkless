"""
Compatibility tests for array functions using expected outputs.

This module tests MockSpark's array functions against PySpark-generated expected outputs
to ensure compatibility across different array operations and data types.
"""

import pytest
from sparkless import F
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal


@pytest.mark.compatibility
class TestArrayFunctionsCompatibility:
    """Tests for array functions compatibility using expected outputs."""

    def test_array_contains(self, mock_spark_session):
        """Test array_contains function against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "scores": [85, 90, 78]},
            {"id": 2, "name": "Bob", "scores": [92, 88, 95]},
            {"id": 3, "name": "Charlie", "scores": [76, 82, 89]},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.select(F.array_contains(df.scores, 90))

        expected = load_expected_output("arrays", "array_contains")
        assert_dataframes_equal(result, expected)

    def test_array_position(self, mock_spark_session):
        """Test array_position function against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "scores": [85, 90, 78]},
            {"id": 2, "name": "Bob", "scores": [92, 88, 95]},
            {"id": 3, "name": "Charlie", "scores": [76, 82, 89]},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.select(F.array_position(df.scores, 90))

        expected = load_expected_output("arrays", "array_position")
        assert_dataframes_equal(result, expected)

    def test_size_function(self, mock_spark_session):
        """Test size function against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "scores": [85, 90, 78]},
            {"id": 2, "name": "Bob", "scores": [92, 88, 95]},
            {"id": 3, "name": "Charlie", "scores": [76, 82, 89]},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.select(F.size(df.scores))

        expected = load_expected_output("arrays", "size")
        assert_dataframes_equal(result, expected)

    def test_element_at(self, mock_spark_session):
        """Test element_at function against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "scores": [85, 90, 78]},
            {"id": 2, "name": "Bob", "scores": [92, 88, 95]},
            {"id": 3, "name": "Charlie", "scores": [76, 82, 89]},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.select(F.element_at(df.scores, 2))

        expected = load_expected_output("arrays", "element_at")
        assert_dataframes_equal(result, expected)

    def test_array_append(self, mock_spark_session):
        """Test array_append function against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "scores": [85, 90, 78]},
            {"id": 2, "name": "Bob", "scores": [92, 88, 95]},
            {"id": 3, "name": "Charlie", "scores": [76, 82, 89]},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.select(F.array_append(df.scores, 100))

        expected = load_expected_output("arrays", "array_append")
        assert_dataframes_equal(result, expected)

    def test_array_remove(self, mock_spark_session):
        """Test array_remove function against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "scores": [85, 90, 78]},
            {"id": 2, "name": "Bob", "scores": [92, 88, 95]},
            {"id": 3, "name": "Charlie", "scores": [76, 82, 89]},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.select(F.array_remove(df.scores, 90))

        expected = load_expected_output("arrays", "array_remove")
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="array_distinct feature removed")
    def test_array_distinct(self, mock_spark_session):
        """Test array_distinct function against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "tags": ["python", "data", "python"]},
            {"id": 2, "name": "Bob", "tags": ["java", "backend", "java"]},
            {"id": 3, "name": "Charlie", "tags": ["python", "ml", "python"]},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        # Maintain deterministic order by including id temporarily for sorting
        result = (
            df.select(df.id, F.array_distinct(df.tags))
            .orderBy("id")
            .select("array_distinct(tags)")
        )

        expected = load_expected_output("arrays", "array_distinct")
        assert_dataframes_equal(result, expected)

    def test_explode(self, mock_spark_session):
        """Test explode function against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "scores": [85, 90, 78]},
            {"id": 2, "name": "Bob", "scores": [92, 88, 95]},
            {"id": 3, "name": "Charlie", "scores": [76, 82, 89]},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        # Explode and sort by name, then score for deterministic order
        result = (
            df.select(df.id, df.name, F.explode(df.scores).alias("score"))
            .orderBy("id", "score")
            .select("name", "score")
        )

        expected = load_expected_output("arrays", "explode")
        assert_dataframes_equal(result, expected)

    def test_explode_outer(self, mock_spark_session):
        """Test explode_outer function against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "scores": [85, 90, 78]},
            {"id": 2, "name": "Bob", "scores": [92, 88, 95]},
            {"id": 3, "name": "Charlie", "scores": [76, 82, 89]},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        # Explode and sort by name, then score for deterministic order
        result = (
            df.select(df.id, df.name, F.explode_outer(df.scores).alias("score"))
            .orderBy("id", "score")
            .select("name", "score")
        )

        expected = load_expected_output("arrays", "explode_outer")
        assert_dataframes_equal(result, expected)

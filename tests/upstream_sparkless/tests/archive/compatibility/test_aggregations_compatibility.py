"""
Compatibility tests for aggregation operations using expected outputs.

This module tests MockSpark's aggregation functions against PySpark-generated expected outputs
to ensure compatibility across different aggregation types and grouping scenarios.
"""

import pytest
from sparkless import F
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal


@pytest.mark.compatibility
class TestAggregationsCompatibility:
    """Tests for aggregation operations compatibility using expected outputs."""

    def test_sum_aggregation(self, mock_spark_session):
        """Test sum aggregation against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "department": "IT", "salary": 50000},
            {"id": 2, "name": "Bob", "department": "HR", "salary": 60000},
            {"id": 3, "name": "Charlie", "department": "IT", "salary": 70000},
            {"id": 4, "name": "David", "department": "Finance", "salary": 80000},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.groupBy("department").agg(F.sum("salary").alias("total_salary"))

        expected = load_expected_output("aggregations", "sum_aggregation")
        assert_dataframes_equal(result, expected)

    def test_avg_aggregation(self, mock_spark_session):
        """Test average aggregation against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "department": "IT", "salary": 50000},
            {"id": 2, "name": "Bob", "department": "HR", "salary": 60000},
            {"id": 3, "name": "Charlie", "department": "IT", "salary": 70000},
            {"id": 4, "name": "David", "department": "Finance", "salary": 80000},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.groupBy("department").agg(F.avg("salary").alias("avg_salary"))

        expected = load_expected_output("aggregations", "avg_aggregation")
        assert_dataframes_equal(result, expected)

    def test_count_aggregation(self, mock_spark_session):
        """Test count aggregation against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "department": "IT", "salary": 50000},
            {"id": 2, "name": "Bob", "department": "HR", "salary": 60000},
            {"id": 3, "name": "Charlie", "department": "IT", "salary": 70000},
            {"id": 4, "name": "David", "department": "Finance", "salary": 80000},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.groupBy("department").agg(F.count("id").alias("employee_count"))

        expected = load_expected_output("aggregations", "count_aggregation")
        assert_dataframes_equal(result, expected)

    def test_max_aggregation(self, mock_spark_session):
        """Test max aggregation against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "department": "IT", "salary": 50000},
            {"id": 2, "name": "Bob", "department": "HR", "salary": 60000},
            {"id": 3, "name": "Charlie", "department": "IT", "salary": 70000},
            {"id": 4, "name": "David", "department": "Finance", "salary": 80000},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.groupBy("department").agg(F.max("salary").alias("max_salary"))

        expected = load_expected_output("aggregations", "max_aggregation")
        assert_dataframes_equal(result, expected)

    def test_min_aggregation(self, mock_spark_session):
        """Test min aggregation against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "department": "IT", "salary": 50000},
            {"id": 2, "name": "Bob", "department": "HR", "salary": 60000},
            {"id": 3, "name": "Charlie", "department": "IT", "salary": 70000},
            {"id": 4, "name": "David", "department": "Finance", "salary": 80000},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.groupBy("department").agg(F.min("salary").alias("min_salary"))

        expected = load_expected_output("aggregations", "min_aggregation")
        assert_dataframes_equal(result, expected)

    def test_multiple_aggregations(self, mock_spark_session):
        """Test multiple aggregations in single groupBy against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "department": "IT", "salary": 50000, "age": 25},
            {"id": 2, "name": "Bob", "department": "HR", "salary": 60000, "age": 30},
            {
                "id": 3,
                "name": "Charlie",
                "department": "IT",
                "salary": 70000,
                "age": 35,
            },
            {
                "id": 4,
                "name": "David",
                "department": "Finance",
                "salary": 80000,
                "age": 40,
            },
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.groupBy("department").agg(
            F.sum("salary").alias("total_salary"),
            F.avg("salary").alias("avg_salary"),
            F.count("id").alias("employee_count"),
            F.max("age").alias("max_age"),
        )

        expected = load_expected_output("aggregations", "multiple_aggregations")
        assert_dataframes_equal(result, expected)

    def test_groupby_multiple_columns(self, mock_spark_session):
        """Test groupBy with multiple columns against expected output."""
        test_data = [
            {
                "id": 1,
                "name": "Alice",
                "department": "IT",
                "level": "Senior",
                "salary": 50000,
            },
            {
                "id": 2,
                "name": "Bob",
                "department": "HR",
                "level": "Junior",
                "salary": 60000,
            },
            {
                "id": 3,
                "name": "Charlie",
                "department": "IT",
                "level": "Senior",
                "salary": 70000,
            },
            {
                "id": 4,
                "name": "David",
                "department": "IT",
                "level": "Junior",
                "salary": 45000,
            },
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.groupBy("department", "level").agg(
            F.avg("salary").alias("avg_salary")
        )

        expected = load_expected_output("aggregations", "groupby_multiple_columns")
        assert_dataframes_equal(result, expected)

    def test_global_aggregation(self, mock_spark_session):
        """Test global aggregation (no groupBy) against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "salary": 50000},
            {"id": 2, "name": "Bob", "salary": 60000},
            {"id": 3, "name": "Charlie", "salary": 70000},
            {"id": 4, "name": "David", "salary": 80000},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.agg(
            F.sum("salary").alias("total_salary"),
            F.avg("salary").alias("avg_salary"),
            F.count("id").alias("total_employees"),
        )

        expected = load_expected_output("aggregations", "global_aggregation")
        assert_dataframes_equal(result, expected)

    def test_aggregation_with_nulls(self, mock_spark_session):
        """Test aggregation with null values against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "department": "IT", "salary": 50000},
            {"id": 2, "name": "Bob", "department": "HR", "salary": None},
            {"id": 3, "name": "Charlie", "department": "IT", "salary": 70000},
            {"id": 4, "name": "David", "department": None, "salary": 80000},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.groupBy("department").agg(F.avg("salary").alias("avg_salary"))

        expected = load_expected_output("aggregations", "aggregation_with_nulls")
        assert_dataframes_equal(result, expected)

"""
Compatibility tests for window functions using expected outputs.

This module tests MockSpark's window functions against PySpark-generated expected outputs
to ensure compatibility across different window specifications and function types.
"""

import pytest
from sparkless import F
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal


@pytest.mark.compatibility
class TestWindowFunctionsCompatibility:
    """Tests for window functions compatibility using expected outputs."""

    def test_row_number_window(self, mock_spark_session):
        """Test row_number window function against expected output."""
        test_data = [
            {
                "id": 1,
                "name": "Alice",
                "dept": "IT",
                "salary": 50000,
                "hire_date": "2020-01-15",
            },
            {
                "id": 2,
                "name": "Bob",
                "dept": "HR",
                "salary": 60000,
                "hire_date": "2019-03-10",
            },
            {
                "id": 3,
                "name": "Charlie",
                "dept": "IT",
                "salary": 70000,
                "hire_date": "2021-07-22",
            },
            {
                "id": 4,
                "name": "David",
                "dept": "IT",
                "salary": 55000,
                "hire_date": "2020-11-05",
            },
        ]

        df = mock_spark_session.createDataFrame(test_data)

        from sparkless.window import Window

        window_spec = Window.partitionBy("dept").orderBy("salary")
        result = df.withColumn("row_num", F.row_number().over(window_spec))

        expected = load_expected_output("windows", "row_number")
        assert_dataframes_equal(result, expected)

    def test_rank_window(self, mock_spark_session):
        """Test rank window function against expected output."""
        test_data = [
            {
                "id": 1,
                "name": "Alice",
                "dept": "IT",
                "salary": 50000,
                "hire_date": "2020-01-15",
            },
            {
                "id": 2,
                "name": "Bob",
                "dept": "HR",
                "salary": 60000,
                "hire_date": "2019-03-10",
            },
            {
                "id": 3,
                "name": "Charlie",
                "dept": "IT",
                "salary": 70000,
                "hire_date": "2021-07-22",
            },
            {
                "id": 4,
                "name": "David",
                "dept": "IT",
                "salary": 55000,
                "hire_date": "2020-11-05",
            },
        ]

        df = mock_spark_session.createDataFrame(test_data)

        from sparkless.window import Window

        window_spec = Window.partitionBy("dept").orderBy("salary")
        result = df.withColumn("rank", F.rank().over(window_spec))

        expected = load_expected_output("windows", "rank")
        assert_dataframes_equal(result, expected)

    def test_dense_rank_window(self, mock_spark_session):
        """Test dense_rank window function against expected output."""
        test_data = [
            {
                "id": 1,
                "name": "Alice",
                "dept": "IT",
                "salary": 50000,
                "hire_date": "2020-01-15",
            },
            {
                "id": 2,
                "name": "Bob",
                "dept": "HR",
                "salary": 60000,
                "hire_date": "2019-03-10",
            },
            {
                "id": 3,
                "name": "Charlie",
                "dept": "IT",
                "salary": 70000,
                "hire_date": "2021-07-22",
            },
            {
                "id": 4,
                "name": "David",
                "dept": "IT",
                "salary": 55000,
                "hire_date": "2020-11-05",
            },
        ]

        df = mock_spark_session.createDataFrame(test_data)

        from sparkless.window import Window

        window_spec = Window.partitionBy("dept").orderBy("salary")
        result = df.withColumn("dense_rank", F.dense_rank().over(window_spec))

        expected = load_expected_output("windows", "dense_rank")
        assert_dataframes_equal(result, expected)

    def test_lag_window(self, mock_spark_session):
        """Test lag window function against expected output."""
        test_data = [
            {
                "id": 1,
                "name": "Alice",
                "dept": "IT",
                "salary": 50000,
                "hire_date": "2020-01-15",
            },
            {
                "id": 2,
                "name": "Bob",
                "dept": "HR",
                "salary": 60000,
                "hire_date": "2019-03-10",
            },
            {
                "id": 3,
                "name": "Charlie",
                "dept": "IT",
                "salary": 70000,
                "hire_date": "2021-07-22",
            },
            {
                "id": 4,
                "name": "David",
                "dept": "IT",
                "salary": 55000,
                "hire_date": "2020-11-05",
            },
        ]

        df = mock_spark_session.createDataFrame(test_data)

        from sparkless.window import Window

        window_spec = Window.partitionBy("dept").orderBy("salary")
        result = df.withColumn("lag_salary", F.lag("salary", 1).over(window_spec))

        expected = load_expected_output("windows", "lag")
        assert_dataframes_equal(result, expected)

    def test_lead_window(self, mock_spark_session):
        """Test lead window function against expected output."""
        test_data = [
            {
                "id": 1,
                "name": "Alice",
                "dept": "IT",
                "salary": 50000,
                "hire_date": "2020-01-15",
            },
            {
                "id": 2,
                "name": "Bob",
                "dept": "HR",
                "salary": 60000,
                "hire_date": "2019-03-10",
            },
            {
                "id": 3,
                "name": "Charlie",
                "dept": "IT",
                "salary": 70000,
                "hire_date": "2021-07-22",
            },
            {
                "id": 4,
                "name": "David",
                "dept": "IT",
                "salary": 55000,
                "hire_date": "2020-11-05",
            },
        ]

        df = mock_spark_session.createDataFrame(test_data)

        from sparkless.window import Window

        window_spec = Window.partitionBy("dept").orderBy("salary")
        result = df.withColumn("lead_salary", F.lead("salary", 1).over(window_spec))

        expected = load_expected_output("windows", "lead")
        assert_dataframes_equal(result, expected)

    def test_sum_over_window(self, mock_spark_session):
        """Test sum aggregation over window against expected output."""
        test_data = [
            {
                "id": 1,
                "name": "Alice",
                "department": "IT",
                "salary": 50000,
                "hire_date": "2020-01-15",
            },
            {
                "id": 2,
                "name": "Bob",
                "department": "HR",
                "salary": 60000,
                "hire_date": "2019-03-10",
            },
            {
                "id": 3,
                "name": "Charlie",
                "department": "IT",
                "salary": 70000,
                "hire_date": "2021-07-22",
            },
            {
                "id": 4,
                "name": "David",
                "department": "IT",
                "salary": 55000,
                "hire_date": "2020-11-05",
            },
        ]

        df = mock_spark_session.createDataFrame(test_data)

        from sparkless.window import Window

        window_spec = Window.partitionBy("department")
        result = df.withColumn("dept_total", F.sum("salary").over(window_spec))

        expected = load_expected_output("windows", "sum_over_window")
        assert_dataframes_equal(result, expected)

    def test_avg_over_window(self, mock_spark_session):
        """Test average aggregation over window against expected output."""
        test_data = [
            {
                "id": 1,
                "name": "Alice",
                "department": "IT",
                "salary": 50000,
                "hire_date": "2020-01-15",
            },
            {
                "id": 2,
                "name": "Bob",
                "department": "HR",
                "salary": 60000,
                "hire_date": "2019-03-10",
            },
            {
                "id": 3,
                "name": "Charlie",
                "department": "IT",
                "salary": 70000,
                "hire_date": "2021-07-22",
            },
            {
                "id": 4,
                "name": "David",
                "department": "IT",
                "salary": 55000,
                "hire_date": "2020-11-05",
            },
        ]

        df = mock_spark_session.createDataFrame(test_data)

        from sparkless.window import Window

        window_spec = Window.partitionBy("department")
        result = df.withColumn("dept_avg", F.avg("salary").over(window_spec))

        expected = load_expected_output("windows", "avg_over_window")
        assert_dataframes_equal(result, expected)

    def test_count_over_window(self, mock_spark_session):
        """Test count aggregation over window against expected output."""
        test_data = [
            {
                "id": 1,
                "name": "Alice",
                "department": "IT",
                "salary": 50000,
                "hire_date": "2020-01-15",
            },
            {
                "id": 2,
                "name": "Bob",
                "department": "HR",
                "salary": 60000,
                "hire_date": "2019-03-10",
            },
            {
                "id": 3,
                "name": "Charlie",
                "department": "IT",
                "salary": 70000,
                "hire_date": "2021-07-22",
            },
            {
                "id": 4,
                "name": "David",
                "department": "IT",
                "salary": 55000,
                "hire_date": "2020-11-05",
            },
        ]

        df = mock_spark_session.createDataFrame(test_data)

        from sparkless.window import Window

        window_spec = Window.partitionBy("department")
        result = df.withColumn("dept_count", F.count("salary").over(window_spec))

        expected = load_expected_output("windows", "count_over_window")
        assert_dataframes_equal(result, expected)

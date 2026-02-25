"""
Compatibility tests for complex scenarios using expected outputs.

This module tests MockSpark's complex chained operations against PySpark-generated expected outputs
to ensure compatibility across real-world ETL scenarios and complex expressions.
"""

import pytest
from sparkless import F
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal


@pytest.mark.compatibility
class TestComplexScenariosCompatibility:
    """Tests for complex scenarios compatibility using expected outputs."""

    def test_filter_select_groupby_agg_chain(self, mock_spark_session):
        """Test complex filter -> select -> groupBy -> agg chain against expected output."""
        test_data = [
            {
                "id": 1,
                "name": "Alice",
                "age": 25,
                "salary": 50000.0,
                "department": "IT",
                "hire_date": "2020-01-15",
            },
            {
                "id": 2,
                "name": "Bob",
                "age": 30,
                "salary": 60000.0,
                "department": "HR",
                "hire_date": "2019-03-10",
            },
            {
                "id": 3,
                "name": "Charlie",
                "age": 35,
                "salary": 70000.0,
                "department": "IT",
                "hire_date": "2021-07-22",
            },
            {
                "id": 4,
                "name": "David",
                "age": 40,
                "salary": 80000.0,
                "department": "Finance",
                "hire_date": "2018-11-05",
            },
            {
                "id": 5,
                "name": "Eve",
                "age": 28,
                "salary": 55000.0,
                "department": "IT",
                "hire_date": "2022-02-14",
            },
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = (
            df.filter(df.age > 30)
            .select("name", "salary", "department")
            .groupBy("department")
            .agg(F.avg("salary").alias("avg_salary"))
        )

        expected = load_expected_output(
            "chained_operations", "filter_select_groupby_agg"
        )
        assert_dataframes_equal(result, expected)

    def test_withcolumn_filter_orderby_chain(self, mock_spark_session):
        """Test withColumn -> filter -> orderBy chain against expected output."""
        test_data = [
            {
                "id": 1,
                "name": "Alice",
                "age": 25,
                "salary": 50000.0,
                "department": "IT",
                "hire_date": "2020-01-15",
            },
            {
                "id": 2,
                "name": "Bob",
                "age": 30,
                "salary": 60000.0,
                "department": "HR",
                "hire_date": "2019-03-10",
            },
            {
                "id": 3,
                "name": "Charlie",
                "age": 35,
                "salary": 70000.0,
                "department": "IT",
                "hire_date": "2021-07-22",
            },
            {
                "id": 4,
                "name": "David",
                "age": 40,
                "salary": 80000.0,
                "department": "Finance",
                "hire_date": "2018-11-05",
            },
            {
                "id": 5,
                "name": "Eve",
                "age": 28,
                "salary": 55000.0,
                "department": "IT",
                "hire_date": "2022-02-14",
            },
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = (
            df.withColumn("bonus", df.salary * 0.1)
            .filter(df.salary * 0.1 > 5000)
            .orderBy((df.salary * 0.1).desc())
        )

        expected = load_expected_output(
            "chained_operations", "withcolumn_filter_orderby"
        )
        assert_dataframes_equal(result, expected)

    def test_select_expr_groupby_agg_orderby_chain(self, mock_spark_session):
        """Test selectExpr -> groupBy -> agg -> orderBy chain against expected output."""
        test_data = [
            {
                "id": 1,
                "name": "Alice",
                "age": 25,
                "salary": 50000.0,
                "department": "IT",
                "hire_date": "2020-01-15",
            },
            {
                "id": 2,
                "name": "Bob",
                "age": 30,
                "salary": 60000.0,
                "department": "HR",
                "hire_date": "2019-03-10",
            },
            {
                "id": 3,
                "name": "Charlie",
                "age": 35,
                "salary": 70000.0,
                "department": "IT",
                "hire_date": "2021-07-22",
            },
            {
                "id": 4,
                "name": "David",
                "age": 40,
                "salary": 80000.0,
                "department": "Finance",
                "hire_date": "2018-11-05",
            },
            {
                "id": 5,
                "name": "Eve",
                "age": 28,
                "salary": 55000.0,
                "department": "IT",
                "hire_date": "2022-02-14",
            },
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = (
            df.selectExpr(
                "name",
                "salary",
                "department",
                "CASE WHEN age > 30 THEN 'Senior' ELSE 'Junior' END as level",
            )
            .groupBy("level")
            .agg(F.count("name").alias("count"), F.avg("salary").alias("avg_salary"))
            .orderBy("avg_salary")
        )

        expected = load_expected_output(
            "chained_operations", "select_expr_groupby_agg_orderby"
        )
        assert_dataframes_equal(result, expected)

    def test_complex_string_operations_chain(self, mock_spark_session):
        """Test complex string operations chain against expected output."""
        test_data = [
            {
                "id": 1,
                "name": "Alice",
                "age": 25,
                "salary": 50000.0,
                "department": "IT",
                "hire_date": "2020-01-15",
            },
            {
                "id": 2,
                "name": "Bob",
                "age": 30,
                "salary": 60000.0,
                "department": "HR",
                "hire_date": "2019-03-10",
            },
            {
                "id": 3,
                "name": "Charlie",
                "age": 35,
                "salary": 70000.0,
                "department": "IT",
                "hire_date": "2021-07-22",
            },
            {
                "id": 4,
                "name": "David",
                "age": 40,
                "salary": 80000.0,
                "department": "Finance",
                "hire_date": "2018-11-05",
            },
            {
                "id": 5,
                "name": "Eve",
                "age": 28,
                "salary": 55000.0,
                "department": "IT",
                "hire_date": "2022-02-14",
            },
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.select(
            F.upper(df.name).alias("name_upper"),
            F.concat(df.name, F.lit(" ("), df.department, F.lit(")")).alias(
                "name_dept"
            ),
            F.length(df.name).alias("name_length"),
        ).filter(F.length(df.name) > 4)

        expected = load_expected_output(
            "chained_operations", "complex_string_operations"
        )
        assert_dataframes_equal(result, expected)

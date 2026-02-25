"""
Compatibility tests for datetime functions using expected outputs.

This module tests MockSpark's datetime functions against PySpark-generated expected outputs
to ensure compatibility across different date/time operations and formats.
"""

import pytest
from sparkless import F
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal


@pytest.mark.compatibility
class TestDatetimeFunctionsCompatibility:
    """Tests for datetime functions compatibility using expected outputs."""

    def test_year_function(self, mock_spark_session):
        """Test year function against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "hire_date": "2020-01-15"},
            {"id": 2, "name": "Bob", "hire_date": "2019-03-10"},
            {"id": 3, "name": "Charlie", "hire_date": "2021-07-22"},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.select(F.year(df.hire_date))

        expected = load_expected_output("datetime", "year")
        assert_dataframes_equal(result, expected)

    def test_month_function(self, mock_spark_session):
        """Test month function against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "hire_date": "2020-01-15"},
            {"id": 2, "name": "Bob", "hire_date": "2019-03-10"},
            {"id": 3, "name": "Charlie", "hire_date": "2021-07-22"},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.select(F.month(df.hire_date))

        expected = load_expected_output("datetime", "month")
        assert_dataframes_equal(result, expected)

    def test_day_function(self, mock_spark_session):
        """Test day function against expected output."""
        expected = load_expected_output("datetime", "day")

        df = mock_spark_session.createDataFrame(expected["input_data"])
        # Expected output uses dayofmonth(date), so use dayofmonth function
        # or use day() but the column name will be different
        # Check what column name is expected
        result = df.select(F.dayofmonth(df.date))

        assert_dataframes_equal(result, expected)

    def test_dayofweek_function(self, mock_spark_session):
        """Test dayofweek function against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "hire_date": "2020-01-15"},
            {"id": 2, "name": "Bob", "hire_date": "2019-03-10"},
            {"id": 3, "name": "Charlie", "hire_date": "2021-07-22"},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.select(F.dayofweek(df.hire_date))

        expected = load_expected_output("datetime", "dayofweek")
        assert_dataframes_equal(result, expected)

    def test_dayofyear_function(self, mock_spark_session):
        """Test dayofyear function against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "hire_date": "2020-01-15"},
            {"id": 2, "name": "Bob", "hire_date": "2019-03-10"},
            {"id": 3, "name": "Charlie", "hire_date": "2021-07-22"},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.select(F.dayofyear(df.hire_date))

        expected = load_expected_output("datetime", "dayofyear")
        assert_dataframes_equal(result, expected)

    def test_weekofyear_function(self, mock_spark_session):
        """Test weekofyear function against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "hire_date": "2020-01-15"},
            {"id": 2, "name": "Bob", "hire_date": "2019-03-10"},
            {"id": 3, "name": "Charlie", "hire_date": "2021-07-22"},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.select(F.weekofyear(df.hire_date))

        expected = load_expected_output("datetime", "weekofyear")
        assert_dataframes_equal(result, expected)

    def test_quarter_function(self, mock_spark_session):
        """Test quarter function against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "hire_date": "2020-01-15"},
            {"id": 2, "name": "Bob", "hire_date": "2019-03-10"},
            {"id": 3, "name": "Charlie", "hire_date": "2021-07-22"},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.select(F.quarter(df.hire_date))

        expected = load_expected_output("datetime", "quarter")
        assert_dataframes_equal(result, expected)

    def test_date_add_function(self, mock_spark_session):
        """Test date_add function against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "hire_date": "2020-01-15"},
            {"id": 2, "name": "Bob", "hire_date": "2019-03-10"},
            {"id": 3, "name": "Charlie", "hire_date": "2021-07-22"},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.select(F.date_add(df.hire_date, 30))

        expected = load_expected_output("datetime", "date_add")
        assert_dataframes_equal(result, expected)

    def test_date_sub_function(self, mock_spark_session):
        """Test date_sub function against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "hire_date": "2020-01-15"},
            {"id": 2, "name": "Bob", "hire_date": "2019-03-10"},
            {"id": 3, "name": "Charlie", "hire_date": "2021-07-22"},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.select(F.date_sub(df.hire_date, 30))

        expected = load_expected_output("datetime", "date_sub")
        assert_dataframes_equal(result, expected)

    def test_months_between_function(self, mock_spark_session):
        """Test months_between function against expected output."""
        expected = load_expected_output("datetime", "months_between")
        df = mock_spark_session.createDataFrame(expected["input_data"])
        result = df.select(F.months_between(df.date, F.lit("2019-01-01")))
        assert_dataframes_equal(result, expected)

    def test_date_format_function(self, mock_spark_session):
        """Test date_format function against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "hire_date": "2020-01-15"},
            {"id": 2, "name": "Bob", "hire_date": "2019-03-10"},
            {"id": 3, "name": "Charlie", "hire_date": "2021-07-22"},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.select(F.date_format(df.hire_date, "yyyy-MM"))

        expected = load_expected_output("datetime", "date_format")
        assert_dataframes_equal(result, expected)

    def test_to_date_function(self, mock_spark_session):
        """Test to_date function against expected output."""
        test_data = [
            {"id": 1, "name": "Alice", "hire_date": "2020-01-15"},
            {"id": 2, "name": "Bob", "hire_date": "2019-03-10"},
            {"id": 3, "name": "Charlie", "hire_date": "2021-07-22"},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.select(F.to_date(df.hire_date))

        expected = load_expected_output("datetime", "to_date")
        assert_dataframes_equal(result, expected)

    def test_current_date_function(self, mock_spark_session):
        """Test current_date function against expected output."""
        test_data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.select(F.current_date())

        expected = load_expected_output("datetime", "current_date")
        assert_dataframes_equal(result, expected)

    def test_current_timestamp_function(self, mock_spark_session):
        """Test current_timestamp function against expected output."""
        test_data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ]

        df = mock_spark_session.createDataFrame(test_data)
        result = df.select(F.current_timestamp())

        expected = load_expected_output("datetime", "current_timestamp")
        assert_dataframes_equal(result, expected)

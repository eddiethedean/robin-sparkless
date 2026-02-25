"""
Compatibility tests for extended datetime functions.

This module validates extended datetime functions against pre-generated PySpark outputs.
"""

import pytest
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal
from sparkless import F


class TestDatetimeFunctionsExtendedCompatibility:
    """Test extended datetime functions against expected PySpark outputs."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        from sparkless import SparkSession

        session = SparkSession("datetime_functions_test")
        yield session
        session.stop()

    def test_add_months(self, spark):
        """Test add_months function."""
        expected = load_expected_output("datetime", "add_months")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.add_months(df.date, 3))
        assert_dataframes_equal(result, expected)

    def test_datediff(self, spark):
        """Test datediff function."""
        expected = load_expected_output("datetime", "datediff")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.datediff(df.date, F.lit("2020-01-01")))
        assert_dataframes_equal(result, expected)

    def test_dayofmonth_extended(self, spark):
        """Test dayofmonth function."""
        expected = load_expected_output("datetime", "dayofmonth")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.dayofmonth(df.date))
        assert_dataframes_equal(result, expected)

    def test_from_unixtime(self, spark):
        """Test from_unixtime function."""
        expected = load_expected_output("datetime", "from_unixtime")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.from_unixtime(F.lit(1577836800)))
        assert_dataframes_equal(result, expected)

    def test_from_utc_timestamp(self, spark):
        """Test from_utc_timestamp function."""
        expected = load_expected_output("datetime", "from_utc_timestamp")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(
            F.from_utc_timestamp(df.timestamp, F.lit("America/New_York"))
        )
        assert_dataframes_equal(result, expected)

    def test_hour(self, spark):
        """Test hour function."""
        expected = load_expected_output("datetime", "hour")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.hour(df.timestamp))
        assert_dataframes_equal(result, expected)

    def test_minute(self, spark):
        """Test minute function."""
        expected = load_expected_output("datetime", "minute")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.minute(df.timestamp))
        assert_dataframes_equal(result, expected)

    def test_second(self, spark):
        """Test second function."""
        expected = load_expected_output("datetime", "second")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.second(df.timestamp))
        assert_dataframes_equal(result, expected)

    def test_last_day(self, spark):
        """Test last_day function."""
        expected = load_expected_output("datetime", "last_day")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.last_day(df.date))
        assert_dataframes_equal(result, expected)

    def test_months_between_extended(self, spark):
        """Test months_between function."""
        expected = load_expected_output("datetime", "months_between")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.months_between(df.date, F.lit("2019-01-01")))
        assert_dataframes_equal(result, expected)

    def test_timestamp_seconds(self, spark):
        """Test timestamp_seconds function."""
        expected = load_expected_output("datetime", "timestamp_seconds")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.timestamp_seconds(F.lit(1577836800)))
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="not yet implemented")
    def test_to_timestamp_extended(self, spark):
        """Test to_timestamp function."""
        expected = load_expected_output("datetime", "to_timestamp")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.to_timestamp(df.timestamp))
        assert_dataframes_equal(result, expected)

    def test_to_utc_timestamp(self, spark):
        """Test to_utc_timestamp function."""
        expected = load_expected_output("datetime", "to_utc_timestamp")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.to_utc_timestamp(df.timestamp, F.lit("America/New_York")))
        assert_dataframes_equal(result, expected)

    def test_unix_timestamp_extended(self, spark):
        """Test unix_timestamp function."""
        expected = load_expected_output("datetime", "unix_timestamp")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.unix_timestamp(df.timestamp))
        assert_dataframes_equal(result, expected)

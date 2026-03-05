"""
Test for Issue #213: createDataFrame().toDF() with single DataType schema. Uses get_spark_imports from fixture only.
"""

import datetime

import pytest

from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
SparkSession = _imports.SparkSession


@pytest.fixture
def spark():
    """Create a SparkSession for testing."""
    return SparkSession.builder.appName("Example").getOrCreate()


def test_createDataFrame_with_single_date_type(spark):
    """Test that createDataFrame with single DateType and toDF() works correctly."""
    date_list = [
        datetime.date(2024, 7, 31),
        datetime.date(2024, 8, 31),
        datetime.date(2024, 9, 30),
    ]

    # This should work without PySparkValueError
    df_dates = spark.createDataFrame(date_list, _imports.DateType()).toDF("dates")
    df_dates.show(999)

    # Verify the DataFrame was created correctly
    rows = df_dates.collect()
    assert len(rows) == 3

    # Verify the column name
    assert "dates" in df_dates.columns
    assert len(df_dates.columns) == 1

    # Verify the data
    assert rows[0]["dates"] == datetime.date(2024, 7, 31)
    assert rows[1]["dates"] == datetime.date(2024, 8, 31)
    assert rows[2]["dates"] == datetime.date(2024, 9, 30)

    # Verify schema
    assert df_dates.dtypes[0][0] == "dates"
    assert df_dates.dtypes[0][1] == "date"


def test_createDataFrame_with_single_string_type(spark):
    """Test that createDataFrame with single StringType and toDF() works."""
    string_list = ["value1", "value2", "value3"]

    df_strings = spark.createDataFrame(string_list, _imports.StringType()).toDF("names")

    rows = df_strings.collect()
    assert len(rows) == 3
    assert "names" in df_strings.columns
    assert rows[0]["names"] == "value1"
    assert rows[1]["names"] == "value2"
    assert rows[2]["names"] == "value3"


def test_createDataFrame_with_single_integer_type(spark):
    """Test that createDataFrame with single IntegerType and toDF() works."""
    int_list = [1, 2, 3]

    df_ints = spark.createDataFrame(int_list, _imports.IntegerType()).toDF("numbers")

    rows = df_ints.collect()
    assert len(rows) == 3
    assert "numbers" in df_ints.columns
    assert rows[0]["numbers"] == 1
    assert rows[1]["numbers"] == 2
    assert rows[2]["numbers"] == 3

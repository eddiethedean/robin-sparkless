"""
Test for Issue #215: Row kwargs-style initialization.

This test verifies that Row(Column1="Value1", Column2=2) works correctly.
"""

import pytest
from sparkless.sql import SparkSession
from sparkless.sql import Row
from datetime import date


@pytest.fixture
def spark():
    """Create a SparkSession for testing."""
    return SparkSession.builder.appName("Example").getOrCreate()


def test_row_kwargs_initialization(spark):
    """Test that Row supports kwargs-style initialization."""
    # This should work without TypeError
    row = Row(Column1="Value1", Column2=2, Column3=3.0, Column4=date(2026, 1, 1))

    # Verify the row can be accessed by attribute
    assert row.Column1 == "Value1"
    assert row.Column2 == 2
    assert row.Column3 == 3.0
    assert row.Column4 == date(2026, 1, 1)

    # Verify the row can be accessed by key
    assert row["Column1"] == "Value1"
    assert row["Column2"] == 2
    assert row["Column3"] == 3.0
    assert row["Column4"] == date(2026, 1, 1)

    # Verify asDict() works
    row_dict = row.asDict()
    assert row_dict["Column1"] == "Value1"
    assert row_dict["Column2"] == 2
    assert row_dict["Column3"] == 3.0
    assert row_dict["Column4"] == date(2026, 1, 1)


def test_row_kwargs_with_createDataFrame(spark):
    """Test that Row with kwargs-style initialization works with createDataFrame."""
    # Create DataFrame using Row with kwargs-style initialization
    df = spark.createDataFrame(
        [Row(Column1="Value1", Column2=2, Column3=3.0, Column4=date(2026, 1, 1))]
    )

    # Verify the DataFrame was created correctly
    rows = df.collect()
    assert len(rows) == 1

    row = rows[0]
    assert row["Column1"] == "Value1"
    assert row["Column2"] == 2
    assert row["Column3"] == 3.0
    assert row["Column4"] == date(2026, 1, 1)

    # Verify schema
    assert "Column1" in df.columns
    assert "Column2" in df.columns
    assert "Column3" in df.columns
    assert "Column4" in df.columns

    # Verify data types (string, long, double, date)
    dtypes = df.dtypes
    type_dict = dict(dtypes)
    assert type_dict["Column1"] == "string"
    assert type_dict["Column2"] == "long"
    assert type_dict["Column3"] == "double"
    assert type_dict["Column4"] == "date"


def test_row_dict_initialization_still_works(spark):
    """Test that Row with dict initialization still works (backward compatibility)."""
    # This should still work
    row = Row({"name": "Alice", "age": 25})

    assert row["name"] == "Alice"
    assert row["age"] == 25
    assert row.name == "Alice"
    assert row.age == 25


def test_row_empty_kwargs(spark):
    """Test that Row with empty kwargs still works."""
    # If data is None and kwargs is empty, should handle gracefully
    # This might raise an error, which is fine
    with pytest.raises((ValueError, TypeError)):
        Row()


def test_row_explicit_none_data_with_kwargs(spark):
    """Test that Row(data=None, **kwargs) uses kwargs."""
    # Explicit None with kwargs should use kwargs
    row = Row(data=None, Column1="Value1", Column2=2)

    assert row["Column1"] == "Value1"
    assert row["Column2"] == 2

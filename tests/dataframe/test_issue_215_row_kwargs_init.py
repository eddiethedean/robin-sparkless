"""
Test for Issue #215: Row kwargs-style initialization. Uses get_spark_imports from fixture only.
"""

import pytest
from datetime import date

from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
SparkSession = _imports.SparkSession
Row = _imports.Row


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


@pytest.mark.skip(reason="Issue #1117: unskip when fixing createDataFrame Row kwargs")
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

    # Verify data types (string, bigint/long, double, date)
    dtypes = df.dtypes
    type_dict = dict(dtypes)
    assert type_dict["Column1"] == "string"
    # PySpark reports integral type here as 'bigint' (its long alias).
    assert type_dict["Column2"] in ("long", "bigint")
    assert type_dict["Column3"] == "double"
    assert type_dict["Column4"] == "date"


@pytest.mark.skip(reason="Issue #1136: unskip when fixing Row() empty kwargs and Row(**dict)")
def test_row_dict_initialization_still_works(spark):
    """Row(dict) behavior in PySpark (non-indexable sentinel Row)."""
    row = Row({"name": "Alice", "age": 25})

    # In PySpark, this produces a sentinel Row which does not expose the keys
    # as fields; accessing by key or attribute raises AttributeError("__fields__").
    import pytest as _pytest

    with _pytest.raises(AttributeError):
        _ = row["name"]
    with _pytest.raises(AttributeError):
        _ = row["age"]
    with _pytest.raises(AttributeError):
        _ = row.name
    with _pytest.raises(AttributeError):
        _ = row.age


@pytest.mark.skip(reason="Issue #1136: unskip when fixing Row() empty kwargs and Row(**dict)")
def test_row_empty_kwargs(spark):
    """Test that Row with empty kwargs still works."""
    # In PySpark, Row() constructs an empty Row without raising.
    row = Row()
    assert isinstance(row, Row)


def test_row_explicit_none_data_with_kwargs(spark):
    """Test that Row(data=None, **kwargs) uses kwargs."""
    # Explicit None with kwargs should use kwargs
    row = Row(data=None, Column1="Value1", Column2=2)

    assert row["Column1"] == "Value1"
    assert row["Column2"] == 2

"""
Test for Issue #214: DataFrame.sort() with list parameter.

This test verifies that df.sort(["dept", "name"]) and df.sort(df.columns) work correctly.
"""

import pytest
from sparkless.sql import SparkSession


@pytest.fixture
def spark():
    """Create a SparkSession for testing."""
    return SparkSession.builder.appName("Example").getOrCreate()


def test_sort_with_list_of_column_names(spark):
    """Test that sort() works with a list of column names."""
    df = spark.createDataFrame(
        [
            {"name": "Alice", "dept": "IT", "salary": 50000},
            {"name": "Bob", "dept": "HR", "salary": 60000},
            {"name": "Charlie", "dept": "IT", "salary": 70000},
        ]
    )

    # This should work without ColumnNotFoundError
    result = df.sort(["dept", "name", "salary"])
    result.show(999)

    rows = result.collect()

    assert len(rows) == 3

    # Verify sorted order: HR (Bob), IT (Alice), IT (Charlie)
    assert rows[0]["dept"] == "HR"
    assert rows[0]["name"] == "Bob"
    assert rows[1]["dept"] == "IT"
    assert rows[1]["name"] == "Alice"
    assert rows[2]["dept"] == "IT"
    assert rows[2]["name"] == "Charlie"


def test_sort_with_df_columns(spark):
    """Test that sort() works with df.columns (a list attribute)."""
    df = spark.createDataFrame(
        [
            {"name": "Alice", "dept": "IT", "salary": 50000},
            {"name": "Bob", "dept": "HR", "salary": 60000},
            {"name": "Charlie", "dept": "IT", "salary": 70000},
        ]
    )

    # This should work - df.columns returns a list
    result = df.sort(df.columns)
    result.show(999)

    rows = result.collect()

    assert len(rows) == 3
    # Should be sorted by dept, name, salary (alphabetical order of columns)
    # Since dept comes first alphabetically, rows should be sorted by dept first
    assert rows[0]["dept"] == "HR"  # HR comes before IT alphabetically
    assert rows[1]["dept"] == "IT"
    assert rows[2]["dept"] == "IT"


def test_sort_with_explicit_column_list(spark):
    """Test that sort() works with an explicit list of column names."""
    df = spark.createDataFrame(
        [
            {"name": "Alice", "dept": "IT", "salary": 50000},
            {"name": "Bob", "dept": "HR", "salary": 60000},
            {"name": "Charlie", "dept": "IT", "salary": 70000},
        ]
    )

    # Explicit list of column names
    columns_to_sort = ["dept", "name", "salary"]
    result = df.sort(columns_to_sort)

    rows = result.collect()
    assert len(rows) == 3


def test_sort_with_tuple(spark):
    """Test that sort() works with a tuple of column names."""
    df = spark.createDataFrame(
        [
            {"name": "Alice", "dept": "IT", "salary": 50000},
            {"name": "Bob", "dept": "HR", "salary": 60000},
            {"name": "Charlie", "dept": "IT", "salary": 70000},
        ]
    )

    # Tuple should also work
    columns_to_sort = ("dept", "name")
    result = df.sort(columns_to_sort)

    rows = result.collect()
    assert len(rows) == 3

"""
Test for Issue #212: DataFrame.select() with list of Column objects.

This test verifies that df.select([psf.col("name"), psf.col("dept")]) works correctly.
"""

import pytest
from sparkless.sql import SparkSession
import sparkless.sql.functions as psf


@pytest.fixture
def spark():
    """Create a SparkSession for testing."""
    return SparkSession.builder.appName("Example").getOrCreate()


def test_select_with_list_of_column_objects(spark):
    """Test that select() works with a list of Column objects."""
    df = spark.createDataFrame(
        [
            {"name": "Alice", "dept": "IT", "salary": 50000},
            {"name": "Bob", "dept": "HR", "salary": 60000},
            {"name": "Charlie", "dept": "IT", "salary": 70000},
        ]
    )

    # This should work without TypeError
    columns_to_select = [psf.col("name"), psf.col("dept")]
    result = df.select(columns_to_select)

    # Verify the result
    result.show(999)
    rows = result.collect()

    assert len(rows) == 3
    assert "name" in result.columns
    assert "dept" in result.columns
    assert "salary" not in result.columns

    # Verify the data
    assert rows[0]["name"] == "Alice"
    assert rows[0]["dept"] == "IT"
    assert rows[1]["name"] == "Bob"
    assert rows[1]["dept"] == "HR"
    assert rows[2]["name"] == "Charlie"
    assert rows[2]["dept"] == "IT"


def test_select_with_list_of_strings(spark):
    """Test that select() still works with a list of strings (existing functionality)."""
    df = spark.createDataFrame(
        [
            {"name": "Alice", "dept": "IT", "salary": 50000},
            {"name": "Bob", "dept": "HR", "salary": 60000},
        ]
    )

    # This should still work
    columns_to_select = ["name", "dept"]
    result = df.select(columns_to_select)

    assert len(result.collect()) == 2
    assert "name" in result.columns
    assert "dept" in result.columns


def test_select_with_mixed_list(spark):
    """Test that select() works with a mixed list of strings and Column objects."""
    df = spark.createDataFrame(
        [
            {"name": "Alice", "dept": "IT", "salary": 50000},
            {"name": "Bob", "dept": "HR", "salary": 60000},
        ]
    )

    # Mixed list of strings and Column objects
    columns_to_select = ["name", psf.col("dept")]
    result = df.select(columns_to_select)

    assert len(result.collect()) == 2
    assert "name" in result.columns
    assert "dept" in result.columns

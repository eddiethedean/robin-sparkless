"""
Test for list unpacking issues in groupBy, rollup, and cube methods.

This test verifies that df.groupBy(["col1", "col2"]), df.rollup(["col1", "col2"]),
and df.cube(["col1", "col2"]) work correctly, similar to fixes for issues #212 and #214.
"""

import pytest
from sparkless.sql import SparkSession


@pytest.fixture
def spark():
    """Create a SparkSession for testing."""
    return SparkSession.builder.appName("test").getOrCreate()


@pytest.fixture
def sample_df(spark):
    """Create a sample DataFrame for testing."""
    data = [
        {"dept": "IT", "year": 2023, "sales": 100},
        {"dept": "IT", "year": 2024, "sales": 200},
        {"dept": "HR", "year": 2023, "sales": 150},
        {"dept": "HR", "year": 2024, "sales": 250},
    ]
    return spark.createDataFrame(data)


def test_groupBy_with_list(sample_df):
    """Test that groupBy() works with a list of column names."""
    # This should work without SparkColumnNotFoundError
    result = sample_df.groupBy(["dept", "year"]).count()

    collected = result.collect()
    assert (
        len(collected) == 4
    )  # 4 combinations: (IT,2023), (IT,2024), (HR,2023), (HR,2024)
    assert "dept" in result.columns
    assert "year" in result.columns
    assert "count" in result.columns

    # Verify counts are correct
    counts = [row.asDict() for row in collected]
    assert {"dept": "IT", "year": 2023, "count": 1} in counts
    assert {"dept": "IT", "year": 2024, "count": 1} in counts
    assert {"dept": "HR", "year": 2023, "count": 1} in counts
    assert {"dept": "HR", "year": 2024, "count": 1} in counts


def test_groupBy_with_tuple(sample_df):
    """Test that groupBy() works with a tuple of column names."""
    result = sample_df.groupBy(("dept", "year")).count()

    assert len(result.collect()) == 4
    assert "dept" in result.columns
    assert "year" in result.columns
    assert "count" in result.columns


def test_groupBy_with_df_columns(sample_df):
    """Test that groupBy() works with df.columns (a list attribute)."""
    # This is a common PySpark pattern
    # Note: sample_df.columns order is ['dept', 'sales', 'year'], so [:2] gives ['dept', 'sales']
    result = sample_df.groupBy(
        sample_df.columns[:2]
    ).count()  # First 2 columns: dept and sales

    collected = result.collect()
    assert len(collected) == 4  # Should have 4 combinations
    assert "dept" in result.columns
    assert "sales" in result.columns  # First 2 columns are dept and sales
    assert "count" in result.columns


def test_groupBy_backward_compatibility(sample_df):
    """Test that groupBy() still works with individual column arguments."""
    result = sample_df.groupBy("dept", "year").count()

    assert len(result.collect()) == 4
    assert "dept" in result.columns
    assert "year" in result.columns
    assert "count" in result.columns


def test_rollup_with_list(sample_df):
    """Test that rollup() works with a list of column names."""
    # This should work without SparkColumnNotFoundError
    result = sample_df.rollup(["dept", "year"]).count()

    collected = result.collect()
    assert len(collected) > 0  # Rollup creates hierarchical groupings
    assert "dept" in result.columns
    assert "year" in result.columns
    assert "count" in result.columns


def test_rollup_with_tuple(sample_df):
    """Test that rollup() works with a tuple of column names."""
    result = sample_df.rollup(("dept", "year")).count()

    assert len(result.collect()) > 0
    assert "dept" in result.columns
    assert "year" in result.columns
    assert "count" in result.columns


def test_rollup_backward_compatibility(sample_df):
    """Test that rollup() still works with individual column arguments."""
    result = sample_df.rollup("dept", "year").count()

    assert len(result.collect()) > 0
    assert "dept" in result.columns
    assert "year" in result.columns
    assert "count" in result.columns


def test_cube_with_list(sample_df):
    """Test that cube() works with a list of column names."""
    # This should work without SparkColumnNotFoundError
    result = sample_df.cube(["dept", "year"]).count()

    collected = result.collect()
    assert len(collected) > 0  # Cube creates multi-dimensional groupings
    assert "dept" in result.columns
    assert "year" in result.columns
    assert "count" in result.columns


def test_cube_with_tuple(sample_df):
    """Test that cube() works with a tuple of column names."""
    result = sample_df.cube(("dept", "year")).count()

    assert len(result.collect()) > 0
    assert "dept" in result.columns
    assert "year" in result.columns
    assert "count" in result.columns


def test_cube_backward_compatibility(sample_df):
    """Test that cube() still works with individual column arguments."""
    result = sample_df.cube("dept", "year").count()

    assert len(result.collect()) > 0
    assert "dept" in result.columns
    assert "year" in result.columns
    assert "count" in result.columns

"""
Tests for DataFrame.fillna() with subset parameter support.

These tests ensure that:
1. fillna() supports subset parameter (string, list, tuple)
2. Only specified columns are filled when subset is provided
3. Other columns remain unchanged
4. Dict value ignores subset parameter (PySpark behavior)
5. Error handling for non-existent columns
6. Edge cases are handled correctly

These tests work with both sparkless (mock) and PySpark backends.
Set MOCK_SPARK_TEST_BACKEND=pyspark to run with real PySpark.
"""

import pytest

from tests.fixtures.spark_imports import get_spark_imports
from tests.fixtures.spark_backend import get_backend_type, BackendType

# Get imports based on backend
imports = get_spark_imports()
SparkSession = imports.SparkSession
StringType = imports.StringType
IntegerType = imports.IntegerType
StructType = imports.StructType
StructField = imports.StructField
F = imports.F  # Functions module for backend-appropriate F.col() etc.


def _is_pyspark_mode() -> bool:
    """Check if running in PySpark mode."""
    backend = get_backend_type()
    return backend == BackendType.PYSPARK


# Import exception class based on backend
if _is_pyspark_mode():
    try:
        from pyspark.sql.utils import AnalysisException as ColumnNotFoundException
    except ImportError:
        from sparkless.core.exceptions.analysis import ColumnNotFoundException
else:
    from sparkless.core.exceptions.analysis import ColumnNotFoundException


class TestFillnaSubset:
    """Test fillna() with subset parameter.

    These tests work with both sparkless (mock) and PySpark backends.
    Use the unified 'spark' fixture from conftest.py which automatically
    selects the backend based on MOCK_SPARK_TEST_BACKEND environment variable.
    """

    @pytest.fixture
    def sample_df(self, spark):
        """Create a sample DataFrame with null values.

        Uses string values to ensure compatibility with PySpark
        (PySpark ignores type mismatches when filling).
        """
        data = [
            {"key": "A", "value": "1"},
            {"key": None, "value": "2"},
            {"key": "C", "value": None},
        ]
        return spark.createDataFrame(data)

    def test_fillna_subset_string_single_column(self, sample_df):
        """Test fillna with subset as string (single column)."""
        result = sample_df.fillna("", subset="value")

        # Only "value" column should be filled
        rows = result.collect()
        assert rows[0]["key"] == "A"
        assert rows[0]["value"] == "1"  # Not null, unchanged
        assert rows[1]["key"] is None  # Not in subset, unchanged
        assert rows[1]["value"] == "2"  # Not null, unchanged
        assert rows[2]["key"] == "C"
        assert rows[2]["value"] == ""  # Was null, now filled

    def test_fillna_subset_list_multiple_columns(self, sample_df):
        """Test fillna with subset as list (multiple columns)."""
        result = sample_df.fillna("", subset=["key", "value"])

        # Both columns should be filled
        rows = result.collect()
        assert rows[0]["key"] == "A"  # Not null, unchanged
        assert rows[0]["value"] == "1"  # Not null, unchanged
        assert rows[1]["key"] == ""  # Was null, now filled
        assert rows[1]["value"] == "2"  # Not null, unchanged
        assert rows[2]["key"] == "C"  # Not null, unchanged
        assert rows[2]["value"] == ""  # Was null, now filled

    def test_fillna_subset_tuple_multiple_columns(self, sample_df):
        """Test fillna with subset as tuple (multiple columns)."""
        result = sample_df.fillna("", subset=("key", "value"))

        # Both columns should be filled
        rows = result.collect()
        assert rows[0]["key"] == "A"
        assert rows[0]["value"] == "1"
        assert rows[1]["key"] == ""
        assert rows[1]["value"] == "2"
        assert rows[2]["key"] == "C"
        assert rows[2]["value"] == ""

    def test_fillna_subset_only_specified_columns_filled(self, spark):
        """Test that only columns in subset are filled, others remain unchanged."""
        data = [
            {
                "col1": None,
                "col2": "B",
                "col3": None,
            },  # col1 and col3 are null, col2 has value
            {
                "col1": "A",
                "col2": None,
                "col3": "C",
            },  # col1 and col3 have values, col2 is null
        ]
        df = spark.createDataFrame(data)
        result = df.fillna("FILLED", subset=["col1", "col3"])

        rows = result.collect()
        # col1 and col3 should be filled where null
        assert rows[0]["col1"] == "FILLED"  # Was null, now filled
        assert rows[0]["col2"] == "B"  # Not in subset, unchanged
        assert rows[0]["col3"] == "FILLED"  # Was null, now filled
        # col2 should remain None (not in subset)
        assert rows[1]["col1"] == "A"  # Not null, unchanged
        assert rows[1]["col2"] is None  # Not in subset, unchanged
        assert rows[1]["col3"] == "C"  # Not null, unchanged

    def test_fillna_subset_other_columns_unchanged(self, sample_df):
        """Test that columns not in subset remain unchanged."""
        result = sample_df.fillna("FILLED", subset="value")

        rows = result.collect()
        # "key" column should remain unchanged (including nulls)
        assert rows[0]["key"] == "A"
        assert rows[1]["key"] is None  # Not in subset, null remains
        assert rows[2]["key"] == "C"

    def test_fillna_dict_value_ignores_subset(self, sample_df):
        """Test that when value is a dict, subset parameter is ignored (PySpark behavior)."""
        result = sample_df.fillna(
            {"key": "DEFAULT_KEY", "value": "DEFAULT_VALUE"}, subset="value"
        )

        rows = result.collect()
        # Dict value should fill both columns, ignoring subset
        assert rows[0]["key"] == "A"  # Not null, unchanged
        assert rows[0]["value"] == "1"  # Not null, unchanged
        assert rows[1]["key"] == "DEFAULT_KEY"  # Was null, filled by dict
        assert rows[1]["value"] == "2"  # Not null, unchanged
        assert rows[2]["key"] == "C"  # Not null, unchanged
        assert rows[2]["value"] == "DEFAULT_VALUE"  # Was null, filled by dict

    def test_fillna_subset_nonexistent_column_raises_error(self, sample_df):
        """Test that non-existent column in subset raises ColumnNotFoundException."""
        with pytest.raises(ColumnNotFoundException, match="nonexistent"):
            sample_df.fillna("", subset="nonexistent")

    def test_fillna_subset_multiple_nonexistent_columns_raises_error(self, sample_df):
        """Test that any non-existent column in subset raises error."""
        with pytest.raises(ColumnNotFoundException):
            sample_df.fillna("", subset=["key", "nonexistent"])

    def test_fillna_subset_empty_list(self, sample_df):
        """Test that empty subset list results in no columns being filled."""
        result = sample_df.fillna("FILLED", subset=[])

        rows = result.collect()
        # No columns should be filled
        assert rows[0]["key"] == "A"
        assert rows[0]["value"] == "1"
        assert rows[1]["key"] is None  # Not filled
        assert rows[1]["value"] == "2"
        assert rows[2]["key"] == "C"
        assert rows[2]["value"] is None  # Not filled

    def test_fillna_subset_all_columns(self, sample_df):
        """Test that subset with all columns works correctly."""
        result = sample_df.fillna("FILLED", subset=["key", "value"])

        rows = result.collect()
        # All columns should be filled where null
        assert rows[0]["key"] == "A"  # Not null, unchanged
        assert rows[0]["value"] == "1"  # Not null, unchanged
        assert rows[1]["key"] == "FILLED"  # Was null, now filled
        assert rows[1]["value"] == "2"  # Not null, unchanged
        assert rows[2]["key"] == "C"  # Not null, unchanged
        assert rows[2]["value"] == "FILLED"  # Was null, now filled

    def test_fillna_no_subset_backward_compatibility(self, sample_df):
        """Test that fillna without subset maintains backward compatibility."""
        result = sample_df.fillna("FILLED")

        rows = result.collect()
        # All null values should be filled
        assert rows[0]["key"] == "A"  # Not null, unchanged
        assert rows[0]["value"] == "1"  # Not null, unchanged
        assert rows[1]["key"] == "FILLED"  # Was null, now filled
        assert rows[1]["value"] == "2"  # Not null, unchanged
        assert rows[2]["key"] == "C"  # Not null, unchanged
        assert rows[2]["value"] == "FILLED"  # Was null, now filled

    def test_fillna_subset_issue_234_example(self, spark):
        """Test the exact example from issue #234.

        Note: PySpark ignores type mismatches - filling integer columns with strings
        doesn't work. This test uses string values to match PySpark behavior.
        """
        # Use string values to match PySpark behavior (PySpark ignores type mismatches)
        df = spark.createDataFrame(
            [
                {"key": "A", "value": "1"},
                {"key": None, "value": "2"},
                {"key": "C", "value": None},
            ]
        )

        result = df.fillna("", subset=["value"])

        rows = result.collect()
        # Only "value" column should be filled
        assert rows[0]["key"] == "A"
        assert rows[0]["value"] == "1"
        assert rows[1]["key"] is None  # Not in subset, unchanged
        assert rows[1]["value"] == "2"
        assert rows[2]["key"] == "C"
        assert rows[2]["value"] == ""  # Was null, now filled

    def test_fillna_subset_issue_234_string_variant(self, spark):
        """Test issue #234 example with string subset parameter.

        Note: Uses string values to match PySpark behavior.
        """
        # Use string values to match PySpark behavior
        df = spark.createDataFrame(
            [
                {"key": "A", "value": "1"},
                {"key": None, "value": "2"},
                {"key": "C", "value": None},
            ]
        )

        result = df.fillna("", subset="value")

        rows = result.collect()
        # Only "value" column should be filled
        assert rows[0]["key"] == "A"
        assert rows[0]["value"] == "1"
        assert rows[1]["key"] is None  # Not in subset, unchanged
        assert rows[1]["value"] == "2"
        assert rows[2]["key"] == "C"
        assert rows[2]["value"] == ""  # Was null, now filled

    def test_fillna_subset_numeric_value(self, spark):
        """Test fillna with subset using numeric fill value."""
        data = [
            {"col1": None, "col2": 10, "col3": None},
            {"col1": 5, "col2": None, "col3": 20},
        ]
        df = spark.createDataFrame(data)
        result = df.fillna(0, subset=["col1", "col3"])

        rows = result.collect()
        # Only col1 and col3 should be filled with 0
        assert rows[0]["col1"] == 0  # Was null, now filled
        assert rows[0]["col2"] == 10  # Not in subset, unchanged
        assert rows[0]["col3"] == 0  # Was null, now filled
        assert rows[1]["col1"] == 5  # Not null, unchanged
        assert rows[1]["col2"] is None  # Not in subset, unchanged
        assert rows[1]["col3"] == 20  # Not null, unchanged

    def test_fillna_subset_boolean_values(self, spark):
        """Test fillna with subset using boolean fill value."""
        data = [
            {"name": "Alice", "active": None, "verified": True},
            {"name": "Bob", "active": False, "verified": None},
            {"name": "Charlie", "active": None, "verified": None},
        ]
        df = spark.createDataFrame(data)
        result = df.fillna(False, subset=["active"])

        rows = result.collect()
        # Only "active" column should be filled
        assert rows[0]["name"] == "Alice"
        assert rows[0]["active"] is False  # Was null, now filled
        assert rows[0]["verified"] is True  # Not in subset, unchanged
        assert rows[1]["name"] == "Bob"
        assert rows[1]["active"] is False  # Not null, unchanged
        assert rows[1]["verified"] is None  # Not in subset, unchanged
        assert rows[2]["name"] == "Charlie"
        assert rows[2]["active"] is False  # Was null, now filled
        assert rows[2]["verified"] is None  # Not in subset, unchanged

    def test_fillna_subset_float_values(self, spark):
        """Test fillna with subset using float fill value."""
        data = [
            {"id": 1, "price": None, "discount": 0.1},
            {"id": 2, "price": 99.99, "discount": None},
            {"id": 3, "price": None, "discount": None},
        ]
        df = spark.createDataFrame(data)
        result = df.fillna(0.0, subset=["price"])

        rows = result.collect()
        # Only "price" column should be filled
        assert rows[0]["id"] == 1
        assert rows[0]["price"] == 0.0  # Was null, now filled
        assert rows[0]["discount"] == 0.1  # Not in subset, unchanged
        assert rows[1]["id"] == 2
        assert rows[1]["price"] == 99.99  # Not null, unchanged
        assert rows[1]["discount"] is None  # Not in subset, unchanged
        assert rows[2]["id"] == 3
        assert rows[2]["price"] == 0.0  # Was null, now filled
        assert rows[2]["discount"] is None  # Not in subset, unchanged

    def test_fillna_subset_multiple_nulls_same_column(self, spark):
        """Test fillna with subset when multiple rows have nulls in the same column."""
        data = [
            {"col1": "A", "col2": None},
            {"col1": "B", "col2": None},
            {"col1": "C", "col2": None},
            {"col1": "D", "col2": "X"},
        ]
        df = spark.createDataFrame(data)
        result = df.fillna("FILLED", subset=["col2"])

        rows = result.collect()
        # All nulls in col2 should be filled
        assert rows[0]["col1"] == "A"
        assert rows[0]["col2"] == "FILLED"  # Was null, now filled
        assert rows[1]["col1"] == "B"
        assert rows[1]["col2"] == "FILLED"  # Was null, now filled
        assert rows[2]["col1"] == "C"
        assert rows[2]["col2"] == "FILLED"  # Was null, now filled
        assert rows[3]["col1"] == "D"
        assert rows[3]["col2"] == "X"  # Not null, unchanged

    def test_fillna_subset_all_nulls_in_column(self, spark):
        """Test fillna with subset when all values in a column are null."""
        # Create schema using backend-appropriate types (already imported at module level)
        schema = StructType(
            [
                StructField("col1", StringType()),
                StructField("col2", StringType()),
            ]
        )
        data = [
            {"col1": "A", "col2": None},
            {"col1": "B", "col2": None},
            {"col1": "C", "col2": None},
        ]
        df = spark.createDataFrame(data, schema)
        result = df.fillna("ALL_NULL", subset=["col2"])

        rows = result.collect()
        # All nulls should be filled
        assert rows[0]["col1"] == "A"
        assert rows[0]["col2"] == "ALL_NULL"
        assert rows[1]["col1"] == "B"
        assert rows[1]["col2"] == "ALL_NULL"
        assert rows[2]["col1"] == "C"
        assert rows[2]["col2"] == "ALL_NULL"

    def test_fillna_subset_no_nulls_in_subset_columns(self, spark):
        """Test fillna with subset when subset columns have no nulls."""
        schema = StructType(
            [
                StructField("col1", StringType()),
                StructField("col2", StringType()),
                StructField("col3", StringType()),
            ]
        )
        data = [
            {"col1": "A", "col2": "X", "col3": None},
            {"col1": "B", "col2": "Y", "col3": None},
        ]
        df = spark.createDataFrame(data, schema)
        result = df.fillna("FILLED", subset=["col1", "col2"])

        rows = result.collect()
        # col1 and col2 have no nulls, so nothing should change
        assert rows[0]["col1"] == "A"  # No null, unchanged
        assert rows[0]["col2"] == "X"  # No null, unchanged
        assert rows[0]["col3"] is None  # Not in subset, unchanged
        assert rows[1]["col1"] == "B"  # No null, unchanged
        assert rows[1]["col2"] == "Y"  # No null, unchanged
        assert rows[1]["col3"] is None  # Not in subset, unchanged

    def test_fillna_subset_mixed_data_types(self, spark):
        """Test fillna with subset containing columns of different data types."""
        data = [
            {"name": None, "age": 25, "score": None, "active": True},
            {"name": "Bob", "age": None, "score": 85.5, "active": None},
        ]
        df = spark.createDataFrame(data)
        result = df.fillna("UNKNOWN", subset=["name"])

        rows = result.collect()
        # Only "name" (string) should be filled, other types unchanged
        assert rows[0]["name"] == "UNKNOWN"  # Was null, now filled
        assert rows[0]["age"] == 25  # Not in subset, unchanged
        assert rows[0]["score"] is None  # Not in subset, unchanged
        assert rows[0]["active"] is True  # Not in subset, unchanged
        assert rows[1]["name"] == "Bob"  # Not null, unchanged
        assert rows[1]["age"] is None  # Not in subset, unchanged
        assert rows[1]["score"] == 85.5  # Not in subset, unchanged
        assert rows[1]["active"] is None  # Not in subset, unchanged

    def test_fillna_subset_empty_dataframe(self, spark):
        """Test fillna with subset on empty DataFrame."""
        schema = StructType(
            [
                StructField("name", StringType()),
                StructField("age", IntegerType()),
            ]
        )
        df = spark.createDataFrame([], schema)
        result = df.fillna("UNKNOWN", subset=["name"])

        rows = result.collect()
        assert len(rows) == 0  # Still empty

    def test_fillna_subset_single_row(self, spark):
        """Test fillna with subset on DataFrame with single row."""
        schema = StructType(
            [
                StructField("col1", StringType()),
                StructField("col2", StringType()),
                StructField("col3", StringType()),
            ]
        )
        data = [{"col1": None, "col2": "X", "col3": None}]
        df = spark.createDataFrame(data, schema)
        result = df.fillna("FILLED", subset=["col1", "col3"])

        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["col1"] == "FILLED"  # Was null, now filled
        assert rows[0]["col2"] == "X"  # Not in subset, unchanged
        assert rows[0]["col3"] == "FILLED"  # Was null, now filled

    def test_fillna_subset_chained_operations(self, spark):
        """Test fillna with subset in chained DataFrame operations."""
        data = [
            {"name": None, "age": 25, "city": None},
            {"name": "Bob", "age": None, "city": "NYC"},
        ]
        df = spark.createDataFrame(data)
        result = df.fillna("UNKNOWN", subset=["name"]).fillna("N/A", subset=["city"])

        rows = result.collect()
        # First fillna should fill name, second should fill city
        assert rows[0]["name"] == "UNKNOWN"  # Filled by first fillna
        assert rows[0]["age"] == 25  # Not filled
        assert rows[0]["city"] == "N/A"  # Filled by second fillna
        assert rows[1]["name"] == "Bob"  # Not null, unchanged
        assert rows[1]["age"] is None  # Not filled
        assert rows[1]["city"] == "NYC"  # Not null, unchanged

    def test_fillna_subset_unicode_and_special_characters(self, spark):
        """Test fillna with subset using unicode and special characters."""
        schema = StructType(
            [
                StructField("name", StringType()),
                StructField("comment", StringType()),
            ]
        )
        data = [
            {"name": "Alice", "comment": None},
            {"name": "Bob", "comment": None},
        ]
        df = spark.createDataFrame(data, schema)
        result = df.fillna("🚀 Unicode: 测试 🎉", subset=["comment"])

        rows = result.collect()
        assert rows[0]["name"] == "Alice"
        assert rows[0]["comment"] == "🚀 Unicode: 测试 🎉"  # Was null, now filled
        assert rows[1]["name"] == "Bob"
        assert rows[1]["comment"] == "🚀 Unicode: 测试 🎉"  # Was null, now filled

    def test_fillna_subset_large_dataset(self, spark):
        """Test fillna with subset on larger dataset."""
        data = [{"id": i, "value": None if i % 2 == 0 else i} for i in range(100)]
        df = spark.createDataFrame(data)
        result = df.fillna(-1, subset=["value"])

        rows = result.collect()
        assert len(rows) == 100
        for i, row in enumerate(rows):
            assert row["id"] == i
            if i % 2 == 0:
                assert row["value"] == -1  # Was null, now filled
            else:
                assert row["value"] == i  # Not null, unchanged

    def test_fillna_subset_single_column_all_rows(self, spark):
        """Test fillna with subset on single column affecting all rows."""
        schema = StructType(
            [
                StructField("col1", StringType()),
                StructField("col2", StringType()),
            ]
        )
        data = [
            {"col1": None, "col2": "A"},
            {"col1": None, "col2": "B"},
            {"col1": None, "col2": "C"},
        ]
        df = spark.createDataFrame(data, schema)
        result = df.fillna("FILLED", subset=["col1"])

        rows = result.collect()
        for row in rows:
            assert row["col1"] == "FILLED"  # All were null, all filled
            # col2 should remain unchanged
        assert rows[0]["col2"] == "A"
        assert rows[1]["col2"] == "B"
        assert rows[2]["col2"] == "C"

    def test_fillna_subset_zero_value(self, spark):
        """Test fillna with subset using zero as fill value."""
        data = [
            {"id": 1, "count": None, "total": 100},
            {"id": 2, "count": 5, "total": None},
        ]
        df = spark.createDataFrame(data)
        result = df.fillna(0, subset=["count"])

        rows = result.collect()
        assert rows[0]["id"] == 1
        assert rows[0]["count"] == 0  # Was null, now filled with 0
        assert rows[0]["total"] == 100  # Not in subset, unchanged
        assert rows[1]["id"] == 2
        assert rows[1]["count"] == 5  # Not null, unchanged
        assert rows[1]["total"] is None  # Not in subset, unchanged

    def test_fillna_subset_negative_value(self, spark):
        """Test fillna with subset using negative number as fill value."""
        data = [
            {"id": 1, "balance": None, "debt": 100},
            {"id": 2, "balance": 50, "debt": None},
        ]
        df = spark.createDataFrame(data)
        result = df.fillna(-999, subset=["balance"])

        rows = result.collect()
        assert rows[0]["id"] == 1
        assert rows[0]["balance"] == -999  # Was null, now filled
        assert rows[0]["debt"] == 100  # Not in subset, unchanged
        assert rows[1]["id"] == 2
        assert rows[1]["balance"] == 50  # Not null, unchanged
        assert rows[1]["debt"] is None  # Not in subset, unchanged

    def test_fillna_subset_empty_string(self, spark):
        """Test fillna with subset using empty string as fill value."""
        schema = StructType(
            [
                StructField("name", StringType()),
                StructField("email", StringType()),
            ]
        )
        data = [
            {"name": "Alice", "email": None},
            {"name": "Bob", "email": None},
        ]
        df = spark.createDataFrame(data, schema)
        result = df.fillna("", subset=["email"])

        rows = result.collect()
        assert rows[0]["name"] == "Alice"
        assert rows[0]["email"] == ""  # Was null, now filled with empty string
        assert rows[1]["name"] == "Bob"
        assert rows[1]["email"] == ""  # Was null, now filled with empty string

    def test_fillna_subset_whitespace_string(self, spark):
        """Test fillna with subset using whitespace string as fill value."""
        schema = StructType(
            [
                StructField("name", StringType()),
                StructField("notes", StringType()),
            ]
        )
        data = [
            {"name": "Alice", "notes": None},
            {"name": "Bob", "notes": None},
        ]
        df = spark.createDataFrame(data, schema)
        result = df.fillna("   ", subset=["notes"])

        rows = result.collect()
        assert rows[0]["name"] == "Alice"
        assert rows[0]["notes"] == "   "  # Was null, now filled with whitespace
        assert rows[1]["name"] == "Bob"
        assert rows[1]["notes"] == "   "  # Was null, now filled with whitespace

    def test_fillna_subset_very_long_string(self, spark):
        """Test fillna with subset using very long string as fill value."""
        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("description", StringType()),
            ]
        )
        long_string = "X" * 1000
        data = [
            {"id": 1, "description": None},
            {"id": 2, "description": None},
        ]
        df = spark.createDataFrame(data, schema)
        result = df.fillna(long_string, subset=["description"])

        rows = result.collect()
        assert rows[0]["id"] == 1
        assert rows[0]["description"] == long_string
        assert rows[1]["id"] == 2
        assert rows[1]["description"] == long_string

    def test_fillna_subset_partial_column_fill(self, spark):
        """Test fillna with subset when only some rows in column are null."""
        data = [
            {"id": 1, "status": "active", "priority": None},
            {"id": 2, "status": None, "priority": "high"},
            {"id": 3, "status": "inactive", "priority": None},
            {"id": 4, "status": None, "priority": "low"},
        ]
        df = spark.createDataFrame(data)
        result = df.fillna("default", subset=["status"])

        rows = result.collect()
        # Only status column nulls should be filled
        assert rows[0]["status"] == "active"  # Not null, unchanged
        assert rows[0]["priority"] is None  # Not in subset, unchanged
        assert rows[1]["status"] == "default"  # Was null, now filled
        assert rows[1]["priority"] == "high"  # Not in subset, unchanged
        assert rows[2]["status"] == "inactive"  # Not null, unchanged
        assert rows[2]["priority"] is None  # Not in subset, unchanged
        assert rows[3]["status"] == "default"  # Was null, now filled
        assert rows[3]["priority"] == "low"  # Not in subset, unchanged

    def test_fillna_subset_with_filter_operation(self, spark):
        """Test fillna with subset combined with filter operation."""
        # Use backend-appropriate F (already imported at module level)
        data = [
            {"id": 1, "name": None, "score": 85},
            {"id": 2, "name": "Bob", "score": 90},
            {"id": 3, "name": None, "score": 75},
        ]
        df = spark.createDataFrame(data)
        # First fillna, then filter
        result = df.fillna("UNKNOWN", subset=["name"]).filter(F.col("score") > 80)

        rows = result.collect()
        # Only rows with score > 80, name column already filled
        assert len(rows) == 2
        assert rows[0]["id"] == 1
        assert rows[0]["name"] == "UNKNOWN"  # Was null, now filled
        assert rows[0]["score"] == 85
        assert rows[1]["id"] == 2
        assert rows[1]["name"] == "Bob"  # Not null, unchanged
        assert rows[1]["score"] == 90

    def test_fillna_subset_with_select_operation(self, spark):
        """Test fillna with subset combined with select operation."""
        schema = StructType(
            [
                StructField("col1", StringType()),
                StructField("col2", StringType()),
                StructField("col3", StringType()),
            ]
        )
        data = [
            {"col1": None, "col2": "A", "col3": None},
            {"col1": "B", "col2": "C", "col3": None},
        ]
        df = spark.createDataFrame(data, schema)
        result = df.select("col1", "col2").fillna("FILLED", subset=["col1"])

        rows = result.collect()
        # Only selected columns should be present, col1 filled
        assert rows[0]["col1"] == "FILLED"  # Was null, now filled
        assert rows[0]["col2"] == "A"
        assert rows[1]["col1"] == "B"  # Not null, unchanged
        assert rows[1]["col2"] == "C"
        # col3 should not be in result (not selected)

    def test_fillna_subset_preserves_data_types(self, spark):
        """Test that fillna with subset preserves data types of non-filled columns."""
        data = [
            {"id": 1, "name": None, "age": 25, "active": True},
            {"id": 2, "name": "Bob", "age": None, "active": False},
        ]
        df = spark.createDataFrame(data)
        result = df.fillna("UNKNOWN", subset=["name"])

        rows = result.collect()
        # Data types should be preserved
        assert isinstance(rows[0]["id"], (int, type(None)))
        assert isinstance(rows[0]["name"], str)  # Filled with string
        assert isinstance(rows[0]["age"], (int, type(None)))
        assert isinstance(rows[0]["active"], bool)
        assert isinstance(rows[1]["id"], (int, type(None)))
        assert isinstance(rows[1]["name"], str)  # Already string
        assert isinstance(rows[1]["age"], (int, type(None)))
        assert isinstance(rows[1]["active"], bool)

    def test_fillna_subset_type_mismatch_int_column_string_fill(self, spark):
        """Test that type mismatches are silently ignored (PySpark behavior).

        When filling an integer column with a string, PySpark silently ignores
        the fill and leaves nulls unchanged. This test verifies sparkless matches
        this behavior.
        """
        data = [
            {"id": 1, "value": None},
            {"id": 2, "value": 5},
            {"id": 3, "value": None},
        ]
        df = spark.createDataFrame(data)
        result = df.fillna("", subset=["value"])

        rows = result.collect()
        # Type mismatch should be silently ignored - nulls remain None
        assert rows[0]["value"] is None  # Not filled due to type mismatch
        assert rows[1]["value"] == 5  # Not null, unchanged
        assert rows[2]["value"] is None  # Not filled due to type mismatch

    def test_fillna_subset_type_mismatch_string_column_int_fill(self, spark):
        """Test that type mismatches are silently ignored (PySpark behavior).

        When filling a string column with an integer, PySpark silently ignores
        the fill and leaves nulls unchanged.
        """
        data = [
            {"id": 1, "name": None},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": None},
        ]
        df = spark.createDataFrame(data)
        result = df.fillna(999, subset=["name"])

        rows = result.collect()
        # Type mismatch should be silently ignored - nulls remain None
        assert rows[0]["name"] is None  # Not filled due to type mismatch
        assert rows[1]["name"] == "Bob"  # Not null, unchanged
        assert rows[2]["name"] is None  # Not filled due to type mismatch

    def test_fillna_subset_type_mismatch_float_column_string_fill(self, spark):
        """Test that type mismatches are silently ignored for float columns."""
        data = [
            {"id": 1, "price": None},
            {"id": 2, "price": 99.99},
            {"id": 3, "price": None},
        ]
        df = spark.createDataFrame(data)
        result = df.fillna("FREE", subset=["price"])

        rows = result.collect()
        # Type mismatch should be silently ignored - nulls remain None
        assert rows[0]["price"] is None  # Not filled due to type mismatch
        assert rows[1]["price"] == 99.99  # Not null, unchanged
        assert rows[2]["price"] is None  # Not filled due to type mismatch

    def test_fillna_subset_type_mismatch_boolean_column_string_fill(self, spark):
        """Test that type mismatches are silently ignored for boolean columns."""
        data = [
            {"id": 1, "active": None},
            {"id": 2, "active": True},
            {"id": 3, "active": None},
        ]
        df = spark.createDataFrame(data)
        result = df.fillna("YES", subset=["active"])

        rows = result.collect()
        # Type mismatch should be silently ignored - nulls remain None
        assert rows[0]["active"] is None  # Not filled due to type mismatch
        assert rows[1]["active"] is True  # Not null, unchanged
        assert rows[2]["active"] is None  # Not filled due to type mismatch

    def test_fillna_subset_type_compatible_string_column_string_fill(self, spark):
        """Test that compatible types still work correctly."""
        data = [
            {"id": 1, "name": None},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": None},
        ]
        df = spark.createDataFrame(data)
        result = df.fillna("UNKNOWN", subset=["name"])

        rows = result.collect()
        # Compatible types should work - nulls are filled
        assert rows[0]["name"] == "UNKNOWN"  # Filled with compatible type
        assert rows[1]["name"] == "Bob"  # Not null, unchanged
        assert rows[2]["name"] == "UNKNOWN"  # Filled with compatible type

    def test_fillna_subset_type_compatible_int_column_int_fill(self, spark):
        """Test that compatible types still work correctly for integer columns."""
        data = [
            {"id": 1, "value": None},
            {"id": 2, "value": 5},
            {"id": 3, "value": None},
        ]
        df = spark.createDataFrame(data)
        result = df.fillna(0, subset=["value"])

        rows = result.collect()
        # Compatible types should work - nulls are filled
        assert rows[0]["value"] == 0  # Filled with compatible type
        assert rows[1]["value"] == 5  # Not null, unchanged
        assert rows[2]["value"] == 0  # Filled with compatible type

    def test_fillna_subset_type_compatible_float_column_float_fill(self, spark):
        """Test that compatible types still work correctly for float columns."""
        data = [
            {"id": 1, "price": None},
            {"id": 2, "price": 99.99},
            {"id": 3, "price": None},
        ]
        df = spark.createDataFrame(data)
        result = df.fillna(0.0, subset=["price"])

        rows = result.collect()
        # Compatible types should work - nulls are filled
        assert rows[0]["price"] == 0.0  # Filled with compatible type
        assert rows[1]["price"] == 99.99  # Not null, unchanged
        assert rows[2]["price"] == 0.0  # Filled with compatible type

    def test_fillna_subset_type_compatible_float_column_int_fill(self, spark):
        """Test that int can fill float columns (numeric compatibility)."""
        data = [
            {"id": 1, "price": None},
            {"id": 2, "price": 99.99},
            {"id": 3, "price": None},
        ]
        df = spark.createDataFrame(data)
        result = df.fillna(0, subset=["price"])  # int filling float

        rows = result.collect()
        # Int can fill float (numeric compatibility)
        assert rows[0]["price"] == 0  # Filled with compatible numeric type
        assert rows[1]["price"] == 99.99  # Not null, unchanged
        assert rows[2]["price"] == 0  # Filled with compatible numeric type

    def test_fillna_subset_type_mismatch_dict_value(self, spark):
        """Test that type mismatches in dict values are also silently ignored."""
        data = [
            {"id": 1, "name": None, "value": None},
            {"id": 2, "name": "Bob", "value": 5},
            {"id": 3, "name": None, "value": None},
        ]
        df = spark.createDataFrame(data)
        # Dict with type mismatch for 'value' column
        result = df.fillna({"name": "UNKNOWN", "value": "INVALID"}, subset=["name"])

        rows = result.collect()
        # 'name' should be filled (compatible type)
        assert rows[0]["name"] == "UNKNOWN"  # Filled with compatible type
        assert rows[1]["name"] == "Bob"  # Not null, unchanged
        assert rows[2]["name"] == "UNKNOWN"  # Filled with compatible type
        # 'value' should NOT be filled (type mismatch - dict ignores subset)
        # But since it's a dict, it should try to fill, but type mismatch prevents it
        assert rows[0]["value"] is None  # Type mismatch prevents fill
        assert rows[1]["value"] == 5  # Not null, unchanged
        assert rows[2]["value"] is None  # Type mismatch prevents fill

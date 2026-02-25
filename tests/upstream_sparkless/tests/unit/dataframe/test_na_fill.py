"""
Tests for DataFrame.na.fill() syntax support.

This module tests that sparkless correctly supports the .na.fill() syntax
for filling null values in DataFrames, matching PySpark behavior.

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
F = imports.F


def _is_pyspark_mode() -> bool:
    """Check if running in PySpark mode."""
    backend = get_backend_type()
    return bool(backend == BackendType.PYSPARK)


# Import exception class based on backend
if _is_pyspark_mode():
    try:
        from pyspark.sql.utils import AnalysisException as ColumnNotFoundException
    except ImportError:
        from sparkless.core.exceptions.analysis import ColumnNotFoundException
else:
    from sparkless.core.exceptions.analysis import ColumnNotFoundException


class TestNaFill:
    """Test .na.fill() syntax for DataFrame."""

    def test_na_fill_scalar(self, spark):
        """Test .na.fill() with scalar value fills all nulls."""
        # Use strings to avoid type compatibility issues
        df = spark.createDataFrame(
            [
                {"key": "A", "value": "1"},
                {"key": None, "value": "2"},
                {"key": "C", "value": None},
            ]
        )

        result = df.na.fill("0")

        rows = result.collect()
        assert len(rows) == 3
        assert rows[0]["key"] == "A"
        assert rows[0]["value"] == "1"
        assert rows[1]["key"] == "0"  # Was null, now filled
        assert rows[1]["value"] == "2"
        assert rows[2]["key"] == "C"
        assert rows[2]["value"] == "0"  # Was null, now filled

    def test_na_fill_dict(self, spark):
        """Test .na.fill() with dict mapping columns to values."""
        df = spark.createDataFrame(
            [
                {"col1": None, "col2": "X", "col3": None},
                {"col1": "A", "col2": None, "col3": "Y"},
            ]
        )

        result = df.na.fill({"col1": "DEFAULT1", "col3": "DEFAULT3"})

        rows = result.collect()
        assert len(rows) == 2
        assert rows[0]["col1"] == "DEFAULT1"  # Was null, now filled
        assert rows[0]["col2"] == "X"  # Not in dict, unchanged
        assert rows[0]["col3"] == "DEFAULT3"  # Was null, now filled
        assert rows[1]["col1"] == "A"  # Not null, unchanged
        assert rows[1]["col2"] is None  # Not in dict, unchanged
        assert rows[1]["col3"] == "Y"  # Not null, unchanged

    def test_na_fill_subset(self, spark):
        """Test .na.fill() with subset parameter."""
        # Provide explicit schema since some columns are all null
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        StructType = imports.StructType
        StructField = imports.StructField
        StringType = imports.StringType

        schema = StructType(
            [
                StructField("key", StringType(), nullable=True),
                StructField("value", StringType(), nullable=True),
                StructField("other", StringType(), nullable=True),
            ]
        )
        df = spark.createDataFrame(
            [
                {"key": None, "value": None, "other": "X"},
                {"key": "A", "value": None, "other": None},
            ],
            schema=schema,
        )

        result = df.na.fill("FILLED", subset=["key", "value"])

        rows = result.collect()
        assert len(rows) == 2
        assert rows[0]["key"] == "FILLED"  # Was null, now filled
        assert rows[0]["value"] == "FILLED"  # Was null, now filled
        assert rows[0]["other"] == "X"  # Not in subset, unchanged (was "X")
        assert rows[1]["key"] == "A"  # Not null, unchanged
        assert rows[1]["value"] == "FILLED"  # Was null, now filled
        assert rows[1]["other"] is None  # Not in subset, unchanged

    def test_na_fill_after_join(self, spark):
        """Test .na.fill() after join operation (exact scenario from issue #245)."""
        # Use integers for the exact scenario from issue #245
        # Note: PySpark accepts integers for filling LongType columns
        df_left = spark.createDataFrame(
            [
                {"key": "123", "value_left": 1},
                {
                    "key": "456",
                    "value_left": None,
                },  # <- null value that should be filled
            ]
        )

        df_right = spark.createDataFrame(
            [
                {
                    "key": "123",
                    "value_right": None,
                },  # <- null value that should be filled
                {"key": "456", "value_right": 2},
            ]
        )

        # This is the exact code from issue #245
        df = df_left.join(df_right, on="key", how="inner").na.fill(0)

        rows = df.collect()
        assert len(rows) == 2

        # Row 1: key="123", value_left=1, value_right should be filled
        row1 = next((r for r in rows if r["key"] == "123"), None)
        assert row1 is not None
        assert row1["value_left"] == 1
        # value_right was null, should be filled with 0 (or may remain None if type incompatible)
        # PySpark allows int to fill LongType, so expect 0
        assert row1["value_right"] == 0  # Was null, now filled

        # Row 2: key="456", value_left should be filled, value_right=2
        row2 = next((r for r in rows if r["key"] == "456"), None)
        assert row2 is not None
        # value_left was null, should be filled with 0
        assert row2["value_left"] == 0  # Was null, now filled
        assert row2["value_right"] == 2

    def test_na_fill_nonexistent_column(self, spark):
        """Test .na.fill() error handling for non-existent columns."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        StructType = imports.StructType
        StructField = imports.StructField
        StringType = imports.StringType

        schema = StructType(
            [
                StructField("col1", StringType(), nullable=True),
                StructField("col2", StringType()),
            ]
        )
        df = spark.createDataFrame([{"col1": None, "col2": "X"}], schema=schema)

        # Test with subset containing non-existent column
        with pytest.raises(ColumnNotFoundException):
            df.na.fill("FILLED", subset=["col1", "nonexistent"])

        # Test with dict containing non-existent column
        with pytest.raises(ColumnNotFoundException):
            df.na.fill({"col1": "FILLED", "nonexistent": "VALUE"})

    def test_na_fill_different_types(self, spark):
        """Test .na.fill() with different data types."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        StructType = imports.StructType
        StructField = imports.StructField
        StringType = imports.StringType
        IntegerType = imports.IntegerType
        DoubleType = imports.DoubleType
        BooleanType = imports.BooleanType

        # Test with integers
        schema_int = StructType(
            [
                StructField("key", IntegerType()),
                StructField("value", IntegerType(), nullable=True),
            ]
        )
        df_int = spark.createDataFrame([{"key": 1, "value": None}], schema=schema_int)
        result_int = df_int.na.fill(0)
        rows_int = result_int.collect()
        assert rows_int[0]["value"] == 0

        # Test with strings
        schema_str = StructType(
            [
                StructField("key", StringType()),
                StructField("value", StringType(), nullable=True),
            ]
        )
        df_str = spark.createDataFrame([{"key": "A", "value": None}], schema=schema_str)
        result_str = df_str.na.fill("DEFAULT")
        rows_str = result_str.collect()
        assert rows_str[0]["value"] == "DEFAULT"

        # Test with floats
        schema_float = StructType(
            [
                StructField("key", DoubleType()),
                StructField("value", DoubleType(), nullable=True),
            ]
        )
        df_float = spark.createDataFrame(
            [{"key": 1.5, "value": None}], schema=schema_float
        )
        result_float = df_float.na.fill(0.0)
        rows_float = result_float.collect()
        assert rows_float[0]["value"] == 0.0

        # Test with booleans
        schema_bool = StructType(
            [
                StructField("key", BooleanType()),
                StructField("value", BooleanType(), nullable=True),
            ]
        )
        df_bool = spark.createDataFrame(
            [{"key": True, "value": None}], schema=schema_bool
        )
        result_bool = df_bool.na.fill(False)
        rows_bool = result_bool.collect()
        assert rows_bool[0]["value"] is False

    def test_na_fill_chained_operations(self, spark):
        """Test chaining .na.fill() with other operations."""
        df = spark.createDataFrame(
            [
                {"name": None, "age": 25, "city": None},
                {"name": "Bob", "age": None, "city": "NYC"},
            ]
        )

        result = df.na.fill("UNKNOWN", subset=["name"]).na.fill("N/A", subset=["city"])

        rows = result.collect()
        assert len(rows) == 2
        # First fillna should fill name, second should fill city
        assert rows[0]["name"] == "UNKNOWN"  # Filled by first na.fill
        assert rows[0]["age"] == 25  # Not filled
        assert rows[0]["city"] == "N/A"  # Filled by second na.fill
        assert rows[1]["name"] == "Bob"  # Not null, unchanged
        assert rows[1]["age"] is None  # Not filled
        assert rows[1]["city"] == "NYC"  # Not null, unchanged

    def test_na_fill_pyspark_parity(self, spark):
        """Test .na.fill() matches PySpark behavior exactly."""
        df = spark.createDataFrame(
            [
                {"key": "123", "value_left": 1},
                {"key": "456", "value_left": None},
            ]
        )

        # This should work exactly like PySpark
        result = df.na.fill(0)

        rows = result.collect()
        assert len(rows) == 2
        assert rows[0]["key"] == "123"
        assert rows[0]["value_left"] == 1
        assert rows[1]["key"] == "456"
        assert rows[1]["value_left"] == 0  # Was null, now filled

        if _is_pyspark_mode():
            # Verify schema matches PySpark
            schema = result.schema
            value_left_field = next(
                (f for f in schema.fields if f.name == "value_left"), None
            )
            assert value_left_field is not None

    def test_na_fill_empty_dataframe(self, spark):
        """Test .na.fill() on empty DataFrame."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        StructType = imports.StructType
        StructField = imports.StructField
        StringType = imports.StringType

        schema = StructType(
            [
                StructField("col1", StringType()),
                StructField("col2", StringType()),
            ]
        )
        df = spark.createDataFrame([], schema=schema)

        result = df.na.fill("DEFAULT")

        rows = result.collect()
        assert len(rows) == 0
        # Schema should be preserved
        assert len(result.schema.fields) == 2

    def test_na_fill_no_nulls(self, spark):
        """Test .na.fill() when DataFrame has no nulls."""
        df = spark.createDataFrame(
            [
                {"key": "A", "value": 1},
                {"key": "B", "value": 2},
            ]
        )

        result = df.na.fill(0)

        rows = result.collect()
        assert len(rows) == 2
        # All values should remain unchanged
        assert rows[0]["key"] == "A"
        assert rows[0]["value"] == 1
        assert rows[1]["key"] == "B"
        assert rows[1]["value"] == 2

    def test_na_fill_equivalence_with_fillna(self, spark):
        """Test that .na.fill() produces identical results to .fillna()."""
        df = spark.createDataFrame(
            [
                {"col1": None, "col2": "X", "col3": None},
                {"col1": "A", "col2": None, "col3": "Y"},
            ]
        )

        result_na = df.na.fill("FILLED")
        result_fillna = df.fillna("FILLED")

        rows_na = result_na.collect()
        rows_fillna = result_fillna.collect()

        assert len(rows_na) == len(rows_fillna)
        for i, row_na in enumerate(rows_na):
            row_fillna = rows_fillna[i]
            assert row_na["col1"] == row_fillna["col1"]
            assert row_na["col2"] == row_fillna["col2"]
            assert row_na["col3"] == row_fillna["col3"]

    def test_na_fill_subset_string(self, spark):
        """Test .na.fill() with subset as string (single column)."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        StructType = imports.StructType
        StructField = imports.StructField
        StringType = imports.StringType

        schema = StructType(
            [
                StructField("key", StringType(), nullable=True),
                StructField("value", StringType(), nullable=True),
            ]
        )
        df = spark.createDataFrame(
            [
                {"key": None, "value": None},
                {"key": "A", "value": None},
            ],
            schema=schema,
        )

        result = df.na.fill("FILLED", subset="key")

        rows = result.collect()
        assert rows[0]["key"] == "FILLED"  # Was null, now filled
        assert rows[0]["value"] is None  # Not in subset, unchanged
        assert rows[1]["key"] == "A"  # Not null, unchanged
        assert rows[1]["value"] is None  # Not in subset, unchanged

    def test_na_fill_subset_tuple(self, spark):
        """Test .na.fill() with subset as tuple."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        StructType = imports.StructType
        StructField = imports.StructField
        StringType = imports.StringType

        schema = StructType(
            [
                StructField("col1", StringType(), nullable=True),
                StructField("col2", StringType(), nullable=True),
                StructField("col3", StringType()),
            ]
        )
        df = spark.createDataFrame(
            [
                {"col1": None, "col2": None, "col3": "X"},
            ],
            schema=schema,
        )

        result = df.na.fill("FILLED", subset=("col1", "col2"))

        rows = result.collect()
        assert rows[0]["col1"] == "FILLED"  # Was null, now filled
        assert rows[0]["col2"] == "FILLED"  # Was null, now filled
        assert rows[0]["col3"] == "X"  # Not in subset, unchanged

    def test_na_fill_all_nulls(self, spark):
        """Test .na.fill() when all values are null."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        StructType = imports.StructType
        StructField = imports.StructField
        StringType = imports.StringType

        schema = StructType(
            [
                StructField("col1", StringType()),
                StructField("col2", StringType()),
            ]
        )
        df = spark.createDataFrame(
            [
                {"col1": None, "col2": None},
                {"col1": None, "col2": None},
            ],
            schema=schema,
        )

        result = df.na.fill("ALL_FILLED")

        rows = result.collect()
        assert len(rows) == 2
        assert rows[0]["col1"] == "ALL_FILLED"
        assert rows[0]["col2"] == "ALL_FILLED"
        assert rows[1]["col1"] == "ALL_FILLED"
        assert rows[1]["col2"] == "ALL_FILLED"

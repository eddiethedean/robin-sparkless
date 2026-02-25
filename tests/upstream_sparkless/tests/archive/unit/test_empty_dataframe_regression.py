"""
Tests for Regression 2: Empty DataFrame Column Detection

Tests that empty DataFrames created with explicit schemas preserve column information.
"""

from sparkless import SparkSession
from sparkless.spark_types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)


class TestEmptyDataFrameRegression:
    """Test empty DataFrame column preservation."""

    def test_empty_dataframe_with_explicit_schema_preserves_columns(self):
        """Test that empty DataFrame with explicit schema preserves column information."""
        spark = SparkSession("test")

        schema = StructType(
            [
                StructField("col1", StringType(), True),
            ]
        )

        empty_df = spark.createDataFrame([], schema)

        # This was failing in 2.15.0 - should return 1, not 0
        assert len(empty_df.columns) == 1
        assert "col1" in empty_df.columns

    def test_empty_dataframe_with_multiple_columns(self):
        """Test that empty DataFrame with multiple columns preserves all columns."""
        spark = SparkSession("test")

        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
                StructField("score", DoubleType()),
            ]
        )

        empty_df = spark.createDataFrame([], schema)

        assert len(empty_df.columns) == 3
        assert "id" in empty_df.columns
        assert "name" in empty_df.columns
        assert "score" in empty_df.columns

    def test_empty_dataframe_schema_access(self):
        """Test that empty DataFrame schema is accessible and correct."""
        spark = SparkSession("test")

        schema = StructType(
            [
                StructField("col1", StringType(), True),
            ]
        )

        empty_df = spark.createDataFrame([], schema)

        # Schema should be accessible
        assert empty_df.schema is not None
        assert len(empty_df.schema.fields) == 1
        assert empty_df.schema.fields[0].name == "col1"
        assert isinstance(empty_df.schema.fields[0].dataType, StringType)

    def test_empty_dataframe_with_nullable_fields(self):
        """Test that empty DataFrame preserves nullable field information."""
        spark = SparkSession("test")

        schema = StructType(
            [
                StructField("required", StringType(), False),
                StructField("optional", StringType(), True),
            ]
        )

        empty_df = spark.createDataFrame([], schema)

        assert len(empty_df.columns) == 2
        assert "required" in empty_df.columns
        assert "optional" in empty_df.columns

        # Check nullable flags are preserved
        required_field = next(f for f in empty_df.schema.fields if f.name == "required")
        optional_field = next(f for f in empty_df.schema.fields if f.name == "optional")

        assert required_field.nullable is False
        assert optional_field.nullable is True

    def test_empty_dataframe_get_dataframe_info(self):
        """Test that get_dataframe_info returns correct column count for empty DataFrame.

        This is the specific use case mentioned in the regression report.
        """
        spark = SparkSession("test")

        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
            ]
        )

        empty_df = spark.createDataFrame([], schema)

        # Simulate get_dataframe_info() behavior
        column_count = len(empty_df.columns)
        column_names = empty_df.columns

        assert column_count == 2
        assert "id" in column_names
        assert "name" in column_names

    def test_empty_dataframe_validate_dataframe_schema(self):
        """Test that validation functions work with empty DataFrames with schemas.

        This is the specific use case mentioned in the regression report.
        """
        spark = SparkSession("test")

        schema = StructType(
            [
                StructField("id", IntegerType()),
            ]
        )

        empty_df = spark.createDataFrame([], schema)

        # Validation should work - columns exist even if data is empty
        assert "id" in empty_df.columns
        assert len(empty_df.columns) == 1

        # Should be able to reference the column
        result_df = empty_df.select("id")
        assert len(result_df.columns) == 1
        assert "id" in result_df.columns

    def test_empty_dataframe_with_complex_types(self):
        """Test that empty DataFrame preserves complex type information."""
        spark = SparkSession("test")

        from sparkless.spark_types import ArrayType, MapType

        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("tags", ArrayType(StringType())),
                StructField("metadata", MapType(StringType(), StringType())),
            ]
        )

        empty_df = spark.createDataFrame([], schema)

        assert len(empty_df.columns) == 3
        assert "id" in empty_df.columns
        assert "tags" in empty_df.columns
        assert "metadata" in empty_df.columns

    def test_empty_dataframe_print_schema(self):
        """Test that printSchema works correctly for empty DataFrames."""
        spark = SparkSession("test")

        schema = StructType(
            [
                StructField("col1", StringType()),
                StructField("col2", IntegerType()),
            ]
        )

        empty_df = spark.createDataFrame([], schema)

        # Should not raise error
        empty_df.printSchema()

        # Verify schema is correct
        assert len(empty_df.schema.fields) == 2

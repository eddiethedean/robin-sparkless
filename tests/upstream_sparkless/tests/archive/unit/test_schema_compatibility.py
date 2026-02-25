"""
Unit tests for schema compatibility improvements.

Tests PySpark-compatible schema validation for empty DataFrame creation
and union operations.
"""

import pytest
from sparkless import SparkSession
from sparkless.core.exceptions.analysis import AnalysisException
from sparkless.spark_types import (
    ArrayType,
    MapType,
    StructField,
    StructType,
    StringType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
)


class TestEmptyDataFrameSchemaValidation:
    """Test empty DataFrame schema validation (PySpark compatibility)."""

    def test_empty_dataframe_with_column_names_raises_error(self):
        """Test that empty DataFrame creation with column names raises ValueError."""
        spark = SparkSession("test")

        # Should raise ValueError matching PySpark's exact error message
        with pytest.raises(ValueError, match="can not infer schema from empty dataset"):
            spark.createDataFrame([], ["col1", "col2"])

    def test_empty_dataframe_with_structtype_succeeds(self):
        """Test that empty DataFrame creation with StructType succeeds."""
        spark = SparkSession("test")
        schema = StructType(
            [
                StructField("col1", StringType(), True),
                StructField("col2", IntegerType(), True),
            ]
        )

        df = spark.createDataFrame([], schema)
        assert df.columns == ["col1", "col2"]
        assert df.count() == 0

    def test_empty_dataframe_with_none_schema_raises_error(self):
        """Test that empty DataFrame creation with None schema raises ValueError."""
        spark = SparkSession("test")

        with pytest.raises(ValueError, match="can not infer schema from empty dataset"):
            spark.createDataFrame([], None)

    def test_empty_dataframe_with_explicit_schema_preserves_schema(self):
        """Test that empty DataFrame with explicit schema preserves schema information."""
        spark = SparkSession("test")
        schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), True),
            ]
        )

        df = spark.createDataFrame([], schema)

        # Verify schema is preserved
        assert len(df.columns) == 2
        assert "id" in df.columns
        assert "name" in df.columns
        assert len(df.schema.fields) == 2

        # Verify field types and nullable flags
        id_field = next(f for f in df.schema.fields if f.name == "id")
        name_field = next(f for f in df.schema.fields if f.name == "name")

        assert isinstance(id_field.dataType, IntegerType)
        assert id_field.nullable is False
        assert isinstance(name_field.dataType, StringType)
        assert name_field.nullable is True

    def test_empty_dataframe_with_ddl_string_schema_succeeds(self):
        """Test that empty DataFrame with DDL string schema succeeds."""
        spark = SparkSession("test")

        # DDL string should be parsed and work
        df = spark.createDataFrame([], "col1 STRING, col2 INT")
        assert df.columns == ["col1", "col2"]
        assert df.count() == 0

    def test_empty_dataframe_with_empty_structtype_succeeds(self):
        """Test that empty DataFrame with empty StructType succeeds."""
        spark = SparkSession("test")

        df = spark.createDataFrame([], StructType([]))
        assert df.count() == 0
        assert len(df.columns) == 0
        assert len(df.schema.fields) == 0

    def test_empty_dataframe_with_complex_types_preserves_schema(self):
        """Test that empty DataFrame with complex types preserves schema."""
        spark = SparkSession("test")
        schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("tags", ArrayType(StringType()), True),
                StructField("metadata", MapType(StringType(), StringType()), True),
                StructField(
                    "nested", StructType([StructField("value", StringType())]), True
                ),
            ]
        )

        df = spark.createDataFrame([], schema)
        assert len(df.columns) == 4
        assert "id" in df.columns
        assert "tags" in df.columns
        assert "metadata" in df.columns
        assert "nested" in df.columns

        # Verify complex types are preserved
        tags_field = next(f for f in df.schema.fields if f.name == "tags")
        assert isinstance(tags_field.dataType, ArrayType)
        assert isinstance(tags_field.dataType.element_type, StringType)

        metadata_field = next(f for f in df.schema.fields if f.name == "metadata")
        assert isinstance(metadata_field.dataType, MapType)

    def test_empty_dataframe_error_message_matches_pyspark(self):
        """Test that error messages match PySpark exactly."""
        spark = SparkSession("test")

        # Test None schema error
        with pytest.raises(ValueError) as exc_info:
            spark.createDataFrame([], None)
        assert "can not infer schema from empty dataset" in str(exc_info.value)

        # Test column list error
        with pytest.raises(ValueError) as exc_info:
            spark.createDataFrame([], ["col1", "col2"])
        error_msg = str(exc_info.value)
        assert "can not infer schema from empty dataset" in error_msg
        assert "Please provide a StructType schema" in error_msg

    def test_empty_dataframe_with_invalid_schema_type_raises_error(self):
        """Test that empty DataFrame with invalid schema type raises TypeError."""
        spark = SparkSession("test")

        with pytest.raises(TypeError, match="schema must be StructType"):
            spark.createDataFrame([], 123)  # Invalid type

        with pytest.raises(TypeError, match="schema must be StructType"):
            spark.createDataFrame([], {"col1": "string"})  # Invalid type


class TestUnionSchemaCompatibility:
    """Test union schema compatibility validation (PySpark compatibility)."""

    def test_union_with_different_column_counts_raises_error(self):
        """Test that union with different column counts raises AnalysisException."""
        spark = SparkSession("test")

        df1 = spark.createDataFrame([("a", 1, "x")], ["col1", "col2", "col3"])
        df2 = spark.createDataFrame([("b",)], ["col1"])

        with pytest.raises(AnalysisException, match="same number of columns"):
            df1.union(df2)

    def test_union_with_different_column_names_raises_error(self):
        """Test that union with different column names raises AnalysisException."""
        spark = SparkSession("test")

        df1 = spark.createDataFrame([("a", 1)], ["col1", "col2"])
        df2 = spark.createDataFrame([("b", 2)], ["col1", "col3"])

        with pytest.raises(AnalysisException, match="compatible column names"):
            df1.union(df2)

    def test_union_with_compatible_schemas_succeeds(self):
        """Test that union with compatible schemas succeeds."""
        spark = SparkSession("test")

        df1 = spark.createDataFrame([("a", 1)], ["col1", "col2"])
        df2 = spark.createDataFrame([("b", 2)], ["col1", "col2"])

        result = df1.union(df2)
        assert result.count() == 2
        assert result.columns == ["col1", "col2"]

    def test_union_with_compatible_numeric_types_succeeds(self):
        """Test that union with compatible numeric types succeeds."""
        spark = SparkSession("test")

        schema1 = StructType([StructField("value", IntegerType(), True)])
        schema2 = StructType([StructField("value", LongType(), True)])

        df1 = spark.createDataFrame([(1,)], schema1)
        df2 = spark.createDataFrame([(2,)], schema2)

        # Should succeed (numeric type promotion allowed)
        result = df1.union(df2)
        assert result.count() == 2

    def test_union_with_string_types_succeeds(self):
        """Test that union with StringType succeeds."""
        spark = SparkSession("test")

        df1 = spark.createDataFrame([("a",)], ["col1"])
        df2 = spark.createDataFrame([("b",)], ["col1"])

        result = df1.union(df2)
        assert result.count() == 2
        assert result.columns == ["col1"]

    def test_union_with_incompatible_types_raises_error(self):
        """Test that union with incompatible types raises AnalysisException."""
        spark = SparkSession("test")

        schema1 = StructType([StructField("value", StringType(), True)])
        schema2 = StructType([StructField("value", IntegerType(), True)])

        df1 = spark.createDataFrame([("a",)], schema1)
        df2 = spark.createDataFrame([(1,)], schema2)

        with pytest.raises(AnalysisException, match="compatible column types"):
            df1.union(df2)

    def test_union_with_float_and_double_types_succeeds(self):
        """Test that union with FloatType and DoubleType succeeds (numeric promotion)."""
        spark = SparkSession("test")

        schema1 = StructType([StructField("value", FloatType(), True)])
        schema2 = StructType([StructField("value", DoubleType(), True)])

        df1 = spark.createDataFrame([(1.0,)], schema1)
        df2 = spark.createDataFrame([(2.0,)], schema2)

        # Should succeed (numeric type promotion allowed)
        result = df1.union(df2)
        assert result.count() == 2


class TestUnionByNameSchemaCompatibility:
    """Test unionByName schema compatibility validation."""

    def test_unionByName_with_type_mismatch_raises_error(self):
        """Test that unionByName with type mismatch raises AnalysisException."""
        spark = SparkSession("test")

        schema1 = StructType([StructField("value", StringType(), True)])
        schema2 = StructType([StructField("value", IntegerType(), True)])

        df1 = spark.createDataFrame([("a",)], schema1)
        df2 = spark.createDataFrame([(1,)], schema2)

        with pytest.raises(AnalysisException, match="compatible column types"):
            df1.unionByName(df2)

    def test_unionByName_with_compatible_types_succeeds(self):
        """Test that unionByName with compatible types succeeds."""
        spark = SparkSession("test")

        schema1 = StructType([StructField("value", IntegerType(), True)])
        schema2 = StructType([StructField("value", LongType(), True)])

        df1 = spark.createDataFrame([(1,)], schema1)
        df2 = spark.createDataFrame([(2,)], schema2)

        # Should succeed (numeric type promotion allowed)
        result = df1.unionByName(df2)
        assert result.count() == 2

    def test_unionByName_with_column_reordering_succeeds(self):
        """Test that unionByName allows column reordering."""
        spark = SparkSession("test")

        schema1 = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        )
        schema2 = StructType(
            [
                StructField("name", StringType(), True),
                StructField("id", IntegerType(), True),
            ]
        )

        # Use dict format to ensure proper type handling
        df1 = spark.createDataFrame([{"id": 1, "name": "Alice"}], schema1)
        df2 = spark.createDataFrame([{"name": "Bob", "id": 2}], schema2)

        # Should succeed - unionByName matches by name, not position
        result = df1.unionByName(df2)
        assert result.count() == 2
        # Column order should match the first DataFrame's schema
        assert set(result.columns) == {"id", "name"}

    def test_unionByName_with_missing_columns_and_allowMissingColumns(self):
        """Test unionByName with allowMissingColumns=True."""
        spark = SparkSession("test")

        schema1 = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )
        schema2 = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        )

        df1 = spark.createDataFrame([(1, "Alice", 25)], schema1)
        df2 = spark.createDataFrame([(2, "Bob")], schema2)

        # Should succeed with allowMissingColumns=True
        result = df1.unionByName(df2, allowMissingColumns=True)
        assert result.count() == 2
        assert "age" in result.columns

    def test_unionByName_with_missing_columns_raises_error(self):
        """Test that unionByName raises error when columns are missing."""
        spark = SparkSession("test")

        schema1 = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )
        schema2 = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        )

        df1 = spark.createDataFrame([(1, "Alice", 25)], schema1)
        df2 = spark.createDataFrame([(2, "Bob")], schema2)

        # Should raise error without allowMissingColumns
        with pytest.raises(AnalysisException, match="missing columns"):
            df1.unionByName(df2)

    def test_unionByName_with_type_mismatch_in_common_columns(self):
        """Test that unionByName validates types for common columns."""
        spark = SparkSession("test")

        schema1 = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True),
            ]
        )
        schema2 = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("value", IntegerType(), True),  # Type mismatch
            ]
        )

        df1 = spark.createDataFrame([(1, "a")], schema1)
        df2 = spark.createDataFrame([(2, 100)], schema2)

        with pytest.raises(AnalysisException, match="compatible column types"):
            df1.unionByName(df2)

    def test_unionByName_nullable_flag_handling(self):
        """Test that unionByName handles nullable flags correctly."""
        spark = SparkSession("test")

        # Result should be nullable if either input is nullable
        schema1 = StructType([StructField("value", IntegerType(), False)])
        schema2 = StructType([StructField("value", IntegerType(), True)])

        df1 = spark.createDataFrame([(1,)], schema1)
        df2 = spark.createDataFrame([(2,)], schema2)

        result = df1.unionByName(df2)
        assert result.count() == 2

        # Check that result field is nullable (True if either is True)
        result_field = result.schema.fields[0]
        assert (
            result_field.nullable is True
        )  # Should be True since schema2 has nullable=True


class TestRealWorldScenarios:
    """Test real-world pipeline scenarios mentioned in the plan."""

    def test_validation_column_filtering_union_scenario(self):
        """Test the validation column filtering scenario from the plan."""
        spark = SparkSession("test")

        # Silver step 1: Rules for ['user_id', 'value', 'action'] -> 3 columns
        schema1 = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("value", IntegerType(), True),
                StructField("action", StringType(), True),
            ]
        )

        # Silver step 2: Rules for ['user_id'] -> 1 column
        schema2 = StructType([StructField("user_id", StringType(), True)])

        # Use dict format to ensure proper type handling
        df1 = spark.createDataFrame(
            [{"user_id": "user1", "value": 100, "action": "click"}], schema1
        )
        df2 = spark.createDataFrame([{"user_id": "user2"}], schema2)

        # Gold step: Union of both -> Should FAIL (3 columns vs 1 column)
        with pytest.raises(AnalysisException, match="same number of columns"):
            df1.union(df2)

    def test_pipeline_with_compatible_schemas_succeeds(self):
        """Test pipeline scenario where schemas are compatible."""
        spark = SparkSession("test")

        # Both silver steps produce compatible schemas
        schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("value", IntegerType(), True),
            ]
        )

        # Use dict format to ensure proper type handling with Polars backend
        df1 = spark.createDataFrame([{"user_id": "user1", "value": 100}], schema)
        df2 = spark.createDataFrame([{"user_id": "user2", "value": 200}], schema)

        # Gold step: Union should succeed
        result = df1.union(df2)
        assert result.count() == 2
        assert result.columns == ["user_id", "value"]

    def test_empty_dataframe_pipeline_scenario(self):
        """Test pipeline scenario with empty DataFrames."""
        spark = SparkSession("test")

        # Create empty DataFrame with explicit schema (correct way)
        schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("value", IntegerType(), True),
            ]
        )

        empty_df = spark.createDataFrame([], schema)

        # Should be able to use in pipeline operations
        assert empty_df.columns == ["user_id", "value"]
        assert empty_df.count() == 0

        # Should be able to union with compatible schema
        # Use dict format to ensure proper type handling with Polars backend
        df_with_data = spark.createDataFrame(
            [{"user_id": "user1", "value": 100}], schema
        )
        result = empty_df.union(df_with_data)
        assert result.count() == 1
        assert result.columns == ["user_id", "value"]

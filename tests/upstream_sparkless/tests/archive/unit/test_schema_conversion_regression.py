"""
Tests for Regression 1: Schema Conversion Failure

Tests that table creation with Spark StructType properly validates and converts schemas.
"""

import pytest

try:
    from sqlalchemy import MetaData
except ImportError:
    MetaData = None

from sparkless import SparkSession
from sparkless.spark_types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
)

try:
    from sparkless.storage.sqlalchemy_helpers import create_table_from_mock_schema
except ImportError:
    create_table_from_mock_schema = None


@pytest.mark.skipif(
    MetaData is None or create_table_from_mock_schema is None,
    reason="sqlalchemy not available",
)
class TestSchemaConversionRegression:
    """Test schema conversion for table creation."""

    def test_create_table_with_valid_schema(self):
        """Test that table creation works with a valid schema."""
        spark = SparkSession("test")

        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
                StructField("score", DoubleType()),
            ]
        )

        df = spark.createDataFrame([{"id": 1, "name": "test", "score": 95.5}], schema)

        # Should be able to write to table
        df.write.mode("overwrite").saveAsTable("test_schema.test_table")

        # Verify table can be read back
        result = spark.table("test_schema.test_table")
        assert result.count() == 1
        assert len(result.columns) == 3
        assert "id" in result.columns
        assert "name" in result.columns
        assert "score" in result.columns

    def test_create_table_with_empty_schema_raises_error(self):
        """Test that creating a table with empty schema raises meaningful error."""
        metadata = MetaData()

        empty_schema = StructType([])

        with pytest.raises(ValueError) as exc_info:
            create_table_from_mock_schema("test_table", empty_schema, metadata)

        assert "empty schema" in str(exc_info.value).lower()
        assert "at least one column" in str(exc_info.value).lower()

    def test_create_table_with_none_schema_raises_error(self):
        """Test that creating a table with None schema raises meaningful error."""
        metadata = MetaData()

        with pytest.raises(ValueError) as exc_info:
            create_table_from_mock_schema("test_table", None, metadata)

        assert (
            "none schema" in str(exc_info.value).lower()
            or "schema is none" in str(exc_info.value).lower()
        )

    def test_create_table_with_missing_fields_attribute_raises_error(self):
        """Test that schema without fields attribute raises error."""
        metadata = MetaData()

        # Create a mock object without fields attribute
        class FakeSchema:
            pass

        fake_schema = FakeSchema()

        with pytest.raises(ValueError) as exc_info:
            create_table_from_mock_schema("test_table", fake_schema, metadata)

        assert "fields" in str(exc_info.value).lower()

    def test_create_table_with_none_fields_raises_error(self):
        """Test that schema with None fields raises error."""
        metadata = MetaData()

        # Create a schema-like object with None fields
        class SchemaWithNoneFields:
            def __init__(self):
                self.fields = None

        schema = SchemaWithNoneFields()

        with pytest.raises(ValueError) as exc_info:
            create_table_from_mock_schema("test_table", schema, metadata)

        assert "fields is none" in str(exc_info.value).lower()

    def test_create_table_with_single_field_schema(self):
        """Test that table creation works with single field schema."""
        spark = SparkSession("test")

        schema = StructType(
            [
                StructField("id", IntegerType()),
            ]
        )

        df = spark.createDataFrame([{"id": 1}], schema)
        df.write.mode("overwrite").saveAsTable("test_schema.single_col_table")

        result = spark.table("test_schema.single_col_table")
        assert result.count() == 1
        assert len(result.columns) == 1
        assert "id" in result.columns

    def test_create_table_with_complex_schema(self):
        """Test that table creation works with complex schema types."""
        spark = SparkSession("test")

        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
                StructField("active", BooleanType()),
                StructField("score", DoubleType()),
            ]
        )

        df = spark.createDataFrame(
            [
                {"id": 1, "name": "Alice", "active": True, "score": 95.5},
                {"id": 2, "name": "Bob", "active": False, "score": 87.0},
            ],
            schema,
        )

        df.write.mode("overwrite").saveAsTable("test_schema.complex_table")

        result = spark.table("test_schema.complex_table")
        assert result.count() == 2
        assert len(result.columns) == 4
        assert all(col in result.columns for col in ["id", "name", "active", "score"])

    def test_write_execution_result_with_schema(self):
        """Test that LogWriter.write_execution_result works with StructType schema.

        This is the specific use case mentioned in the regression report.
        """
        spark = SparkSession("test")

        # Simulate the schema that LogWriter would use
        schema = StructType(
            [
                StructField("execution_id", StringType()),
                StructField("status", StringType()),
                StructField("duration_ms", IntegerType()),
                StructField("metadata", StringType(), nullable=True),
            ]
        )

        # Create DataFrame with the schema (even with None values)
        data = [
            {
                "execution_id": "exec1",
                "status": "completed",
                "duration_ms": 100,
                "metadata": None,
            },
            {
                "execution_id": "exec2",
                "status": "completed",
                "duration_ms": 200,
                "metadata": "test",
            },
        ]

        df = spark.createDataFrame(data, schema)

        # Should be able to write to table
        df.write.mode("overwrite").saveAsTable("test_schema.execution_log")

        result = spark.table("test_schema.execution_log")
        assert result.count() == 2
        assert len(result.columns) == 4
        assert "execution_id" in result.columns
        assert "status" in result.columns
        assert "duration_ms" in result.columns
        assert "metadata" in result.columns

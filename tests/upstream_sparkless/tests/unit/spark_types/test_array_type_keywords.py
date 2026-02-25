"""
Unit tests for ArrayType elementType keyword argument support.

Tests validate PySpark compatibility for ArrayType initialization with elementType
keyword argument, while maintaining backward compatibility with element_type.
"""

import pytest
from sparkless.spark_types import (
    ArrayType,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
)


@pytest.mark.unit
class TestArrayTypeKeywords:
    """Test ArrayType keyword argument support."""

    def test_array_type_with_elementtype_keyword(self):
        """Test ArrayType with elementType keyword (PySpark convention)."""
        at = ArrayType(elementType=StringType())
        assert isinstance(at.element_type, StringType)

    def test_array_type_with_element_type_keyword(self):
        """Test ArrayType with element_type keyword (backward compatibility)."""
        at = ArrayType(element_type=StringType())
        assert isinstance(at.element_type, StringType)

    def test_array_type_positional(self):
        """Test ArrayType with positional argument (existing behavior)."""
        at = ArrayType(StringType())
        assert isinstance(at.element_type, StringType)

    def test_array_type_both_keywords_error(self):
        """Test that specifying both elementType and element_type raises TypeError."""
        with pytest.raises(TypeError, match="Cannot specify both"):
            ArrayType(elementType=StringType(), element_type=LongType())

    def test_array_type_missing_argument(self):
        """Test that missing both arguments raises TypeError."""
        with pytest.raises(TypeError, match="elementType or element_type is required"):
            ArrayType()

    def test_array_type_elementtype_with_nullable(self):
        """Test ArrayType with elementType keyword and nullable parameter."""
        at = ArrayType(elementType=StringType(), nullable=False)
        assert isinstance(at.element_type, StringType)
        assert not at.nullable

    def test_array_type_elementtype_with_different_types(self):
        """Test ArrayType with elementType keyword using different element types."""
        # Test with IntegerType
        at_int = ArrayType(elementType=IntegerType())
        assert isinstance(at_int.element_type, IntegerType)

        # Test with DoubleType
        at_double = ArrayType(elementType=DoubleType())
        assert isinstance(at_double.element_type, DoubleType)

        # Test with LongType
        at_long = ArrayType(elementType=LongType())
        assert isinstance(at_long.element_type, LongType)

    def test_array_type_nested_with_elementtype(self):
        """Test nested ArrayType with elementType keyword."""
        inner = ArrayType(elementType=StringType())
        outer = ArrayType(elementType=inner)
        assert isinstance(outer.element_type, ArrayType)
        assert isinstance(outer.element_type.element_type, StringType)

    def test_array_type_in_schema_with_elementtype(self):
        """Test ArrayType with elementType keyword in schema definition."""
        from sparkless.sql import SparkSession
        from sparkless.sql.types import StructType, StructField, StringType

        spark = SparkSession.builder.appName("Test").getOrCreate()

        schema = StructType(
            [
                StructField("arr", ArrayType(elementType=StringType()), True),
            ]
        )

        df = spark.createDataFrame([{"arr": ["a", "b"]}], schema=schema)
        assert df.schema.fields[0].dataType.element_type == StringType()

    def test_array_type_issue_247_example(self):
        """Test the exact example from issue #247."""
        from sparkless.sql import SparkSession
        from sparkless.sql.types import (
            StructType,
            StructField,
            ArrayType,
            StringType,
            DoubleType,
        )

        spark = SparkSession.builder.appName("Example").getOrCreate()

        # Define a schema which contains array types using elementType
        schema = StructType(
            [
                StructField(
                    "Value_ArrayType_StringType",
                    ArrayType(elementType=StringType()),
                    True,
                ),
                StructField(
                    "Value_ArrayType_DoubleType",
                    ArrayType(elementType=DoubleType()),
                    True,
                ),
            ]
        )

        # Create a dataframe with the arrays populated
        df = spark.createDataFrame(
            [
                {
                    "Value_ArrayType_StringType": ["A", "B"],
                    "Value_ArrayType_DoubleType": [1.0, 2.0],
                },
                {
                    "Value_ArrayType_StringType": ["C", "D"],
                    "Value_ArrayType_DoubleType": [3.0, 4.0],
                },
            ],
            schema=schema,
        )

        # Verify schema
        assert len(df.schema.fields) == 2
        assert df.schema.fields[0].name == "Value_ArrayType_StringType"
        assert isinstance(df.schema.fields[0].dataType, ArrayType)
        assert df.schema.fields[0].dataType.element_type == StringType()

        assert df.schema.fields[1].name == "Value_ArrayType_DoubleType"
        assert isinstance(df.schema.fields[1].dataType, ArrayType)
        assert df.schema.fields[1].dataType.element_type == DoubleType()

        # Verify data
        rows = df.collect()
        assert len(rows) == 2
        assert rows[0]["Value_ArrayType_StringType"] == ["A", "B"]
        assert rows[0]["Value_ArrayType_DoubleType"] == [1.0, 2.0]
        assert rows[1]["Value_ArrayType_StringType"] == ["C", "D"]
        assert rows[1]["Value_ArrayType_DoubleType"] == [3.0, 4.0]

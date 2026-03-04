"""
Unit tests for ArrayType elementType keyword argument support.

Uses get_spark_imports() so types match the backend (PySpark or Robin).
Tests use only public API (elementType) so they run in both backends.
"""

import pytest

from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
ArrayType = _imports.ArrayType
StringType = _imports.StringType
IntegerType = _imports.IntegerType
LongType = _imports.LongType
DoubleType = _imports.DoubleType


@pytest.mark.unit
class TestArrayTypeKeywords:
    """Test ArrayType keyword argument support."""

    def test_array_type_with_elementtype_keyword(self):
        """Test ArrayType with elementType keyword (PySpark convention)."""
        at = ArrayType(elementType=StringType())
        assert isinstance(at.elementType, StringType)

    def test_array_type_with_element_type_keyword(self):
        """Test ArrayType with elementType keyword (public API, works in PySpark and Robin)."""
        at = ArrayType(elementType=StringType())
        assert isinstance(at.elementType, StringType)

    def test_array_type_positional(self):
        """Test ArrayType with positional argument (existing behavior)."""
        at = ArrayType(StringType())
        assert isinstance(at.elementType, StringType)

    def test_array_type_both_keywords_error(self):
        """Test that specifying both elementType and _element_type raises TypeError (both backends)."""
        with pytest.raises(TypeError):
            ArrayType(elementType=StringType(), _element_type=LongType())

    def test_array_type_missing_argument(self):
        """Test that missing required argument raises TypeError."""
        with pytest.raises(TypeError):
            ArrayType()

    def test_array_type_elementtype_with_nullable(self):
        """Test ArrayType with elementType and containsNull (positional for both backends)."""
        at = ArrayType(StringType(), False)
        assert isinstance(at.elementType, StringType)
        # PySpark uses containsNull, Sparkless uses nullable
        assert getattr(at, "nullable", getattr(at, "containsNull", True)) is False

    def test_array_type_elementtype_with_different_types(self):
        """Test ArrayType with elementType keyword using different element types."""
        # Test with IntegerType
        at_int = ArrayType(elementType=IntegerType())
        assert isinstance(at_int.elementType, IntegerType)

        # Test with DoubleType
        at_double = ArrayType(elementType=DoubleType())
        assert isinstance(at_double.elementType, DoubleType)

        # Test with LongType
        at_long = ArrayType(elementType=LongType())
        assert isinstance(at_long.elementType, LongType)

    def test_array_type_nested_with_elementtype(self):
        """Test nested ArrayType with elementType keyword."""
        inner = ArrayType(elementType=StringType())
        outer = ArrayType(elementType=inner)
        assert isinstance(outer.elementType, ArrayType)
        assert isinstance(outer.elementType.elementType, StringType)

    def test_array_type_in_schema_with_elementtype(self, spark):
        """Test ArrayType with elementType keyword in schema definition."""
        StructType = _imports.StructType
        StructField = _imports.StructField

        schema = StructType(
            [
                StructField("arr", ArrayType(elementType=StringType()), True),
            ]
        )

        df = spark.createDataFrame([{"arr": ["a", "b"]}], schema=schema)
        assert df.schema.fields[0].dataType.elementType == StringType()

    def test_array_type_issue_247_example(self, spark):
        """Test the exact example from issue #247."""
        StructType = _imports.StructType
        StructField = _imports.StructField

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
        assert df.schema.fields[0].dataType.elementType == StringType()

        assert df.schema.fields[1].name == "Value_ArrayType_DoubleType"
        assert isinstance(df.schema.fields[1].dataType, ArrayType)
        assert df.schema.fields[1].dataType.elementType == DoubleType()

        # Verify data
        rows = df.collect()
        assert len(rows) == 2
        assert rows[0]["Value_ArrayType_StringType"] == ["A", "B"]
        assert rows[0]["Value_ArrayType_DoubleType"] == [1.0, 2.0]
        assert rows[1]["Value_ArrayType_StringType"] == ["C", "D"]
        assert rows[1]["Value_ArrayType_DoubleType"] == [3.0, 4.0]

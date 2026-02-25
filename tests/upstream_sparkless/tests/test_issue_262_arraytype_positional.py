"""
Test for Issue #262: ArrayType positional arguments support.

This test verifies that ArrayType can be initialized with positional arguments
like ArrayType(DoubleType(), True) without raising a TypeError about specifying
both 'elementType' and 'element_type'.
"""

import pytest

from tests.fixtures.spark_imports import get_spark_imports
from tests.fixtures.spark_backend import get_backend_type, BackendType


def _is_pyspark_mode() -> bool:
    """Check if running in PySpark mode."""
    backend = get_backend_type()
    return bool(backend == BackendType.PYSPARK)


# Get imports based on backend
imports = get_spark_imports()
SparkSession = imports.SparkSession
StringType = imports.StringType
DoubleType = imports.DoubleType
ArrayType = imports.ArrayType
StructType = imports.StructType
StructField = imports.StructField


class TestIssue262ArrayTypePositional:
    """Test ArrayType with positional arguments (Issue #262)."""

    def test_arraytype_positional_arguments(self, spark):
        """Test ArrayType with positional arguments matching the issue example."""
        # This is the exact example from issue #262
        schema = StructType(
            [
                StructField(
                    "Value_ArrayType_StringType",
                    ArrayType(elementType=StringType()),
                    True,  # OK - using keyword argument
                ),
                StructField(
                    "Value_ArrayType_DoubleType",
                    ArrayType(DoubleType(), True),  # Positional args - should work now
                    True,
                ),
            ]
        )

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

        rows = df.collect()
        assert len(rows) == 2
        assert rows[0]["Value_ArrayType_StringType"] == ["A", "B"]
        assert rows[0]["Value_ArrayType_DoubleType"] == [1.0, 2.0]
        assert rows[1]["Value_ArrayType_StringType"] == ["C", "D"]
        assert rows[1]["Value_ArrayType_DoubleType"] == [3.0, 4.0]

    def test_arraytype_all_initialization_patterns(self, spark):
        """Test all ArrayType initialization patterns to ensure backward compatibility."""
        # Pattern 1: Positional element_type only
        at1 = ArrayType(StringType())
        assert at1.element_type == StringType()
        assert at1.nullable is True  # default

        # Pattern 2: Positional element_type with nullable
        at2 = ArrayType(StringType(), nullable=False)
        assert at2.element_type == StringType()
        assert at2.nullable is False

        # Pattern 3: Keyword element_type
        at3 = ArrayType(element_type=StringType())
        assert at3.element_type == StringType()
        assert at3.nullable is True

        # Pattern 4: Keyword elementType (PySpark convention)
        at4 = ArrayType(elementType=StringType())
        assert at4.element_type == StringType()
        assert at4.nullable is True

        # Pattern 5: Keyword elementType with nullable
        at5 = ArrayType(elementType=StringType(), nullable=False)
        assert at5.element_type == StringType()
        assert at5.nullable is False

        # Pattern 6: Positional element_type with keyword nullable
        at6 = ArrayType(StringType(), nullable=False)
        assert at6.element_type == StringType()
        assert at6.nullable is False

    def test_arraytype_positional_with_dataframe(self, spark):
        """Test ArrayType positional arguments work in DataFrame creation."""
        schema = StructType(
            [
                StructField("str_array", ArrayType(StringType()), True),
                StructField("double_array", ArrayType(DoubleType(), True), True),
                StructField(
                    "double_array_not_null", ArrayType(DoubleType(), False), True
                ),
            ]
        )

        df = spark.createDataFrame(
            [
                {
                    "str_array": ["a", "b"],
                    "double_array": [1.0, 2.0],
                    "double_array_not_null": [3.0, 4.0],
                }
            ],
            schema=schema,
        )

        rows = df.collect()
        assert len(rows) == 1
        assert rows[0]["str_array"] == ["a", "b"]
        assert rows[0]["double_array"] == [1.0, 2.0]
        assert rows[0]["double_array_not_null"] == [3.0, 4.0]

    @pytest.mark.skipif(
        not _is_pyspark_mode(),
        reason="PySpark parity test - only run with PySpark backend",
    )
    def test_arraytype_positional_pyspark_parity(self, spark):
        """Parity test: verify positional arguments behavior matches PySpark."""
        # Test the exact pattern from issue #262
        schema = StructType(
            [
                StructField(
                    "Value_ArrayType_StringType",
                    ArrayType(elementType=StringType()),
                    True,
                ),
                StructField(
                    "Value_ArrayType_DoubleType",
                    ArrayType(DoubleType(), True),  # Positional args
                    True,
                ),
            ]
        )

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

        # Verify the DataFrame can be created and data is correct
        rows = df.collect()
        assert len(rows) == 2
        assert rows[0]["Value_ArrayType_StringType"] == ["A", "B"]
        assert rows[0]["Value_ArrayType_DoubleType"] == [1.0, 2.0]
        assert rows[1]["Value_ArrayType_StringType"] == ["C", "D"]
        assert rows[1]["Value_ArrayType_DoubleType"] == [3.0, 4.0]

        # Verify schema matches
        schema_fields = df.schema.fields
        assert len(schema_fields) == 2
        assert schema_fields[0].name == "Value_ArrayType_StringType"
        assert schema_fields[1].name == "Value_ArrayType_DoubleType"

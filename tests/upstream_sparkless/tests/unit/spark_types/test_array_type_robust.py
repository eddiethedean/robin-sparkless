"""
Robust tests for ArrayType elementType keyword argument matching PySpark behavior.

This module contains comprehensive tests that verify edge cases and specific
PySpark behaviors for ArrayType initialization with elementType keyword.

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
LongType = imports.LongType
DoubleType = imports.DoubleType
BooleanType = imports.BooleanType
DateType = imports.DateType
TimestampType = imports.TimestampType
DecimalType = imports.DecimalType
ArrayType = imports.ArrayType
MapType = imports.MapType
StructType = imports.StructType
StructField = imports.StructField
F = imports.F


def _is_pyspark_mode() -> bool:
    """Check if running in PySpark mode."""
    backend = get_backend_type()
    return bool(backend == BackendType.PYSPARK)


@pytest.mark.unit
class TestArrayTypeRobust:
    """Robust tests for ArrayType elementType matching PySpark behavior."""

    def test_array_type_elementtype_with_all_primitive_types(self, spark):
        """Test ArrayType with elementType using all primitive types."""
        schema = StructType(
            [
                StructField("str_array", ArrayType(elementType=StringType()), True),
                StructField("int_array", ArrayType(elementType=IntegerType()), True),
                StructField("long_array", ArrayType(elementType=LongType()), True),
                StructField("double_array", ArrayType(elementType=DoubleType()), True),
                StructField("bool_array", ArrayType(elementType=BooleanType()), True),
            ]
        )

        df = spark.createDataFrame(
            [
                {
                    "str_array": ["a", "b", "c"],
                    "int_array": [1, 2, 3],
                    "long_array": [1000, 2000, 3000],
                    "double_array": [1.1, 2.2, 3.3],
                    "bool_array": [True, False, True],
                }
            ],
            schema=schema,
        )

        rows = df.collect()
        assert rows[0]["str_array"] == ["a", "b", "c"]
        assert rows[0]["int_array"] == [1, 2, 3]
        assert rows[0]["long_array"] == [1000, 2000, 3000]
        assert rows[0]["double_array"] == [1.1, 2.2, 3.3]
        assert rows[0]["bool_array"] == [True, False, True]

    def test_array_type_elementtype_nested_arrays(self, spark):
        """Test nested ArrayType with elementType (array of arrays)."""
        schema = StructType(
            [
                StructField(
                    "nested",
                    ArrayType(elementType=ArrayType(elementType=StringType())),
                    True,
                )
            ]
        )

        df = spark.createDataFrame(
            [{"nested": [["a", "b"], ["c", "d"], ["e"]]}], schema=schema
        )

        rows = df.collect()
        assert rows[0]["nested"] == [["a", "b"], ["c", "d"], ["e"]]

    def test_array_type_elementtype_triple_nested(self, spark):
        """Test triple-nested ArrayType with elementType."""
        schema = StructType(
            [
                StructField(
                    "triple",
                    ArrayType(
                        elementType=ArrayType(
                            elementType=ArrayType(elementType=IntegerType())
                        )
                    ),
                    True,
                )
            ]
        )

        df = spark.createDataFrame(
            [{"triple": [[[1, 2], [3, 4]], [[5, 6], [7, 8]]]}], schema=schema
        )

        rows = df.collect()
        assert rows[0]["triple"] == [[[1, 2], [3, 4]], [[5, 6], [7, 8]]]

    def test_array_type_elementtype_with_map_type(self, spark):
        """Test ArrayType with elementType as MapType."""
        schema = StructType(
            [
                StructField(
                    "map_array",
                    ArrayType(elementType=MapType(StringType(), IntegerType())),
                    True,
                )
            ]
        )

        df = spark.createDataFrame(
            [{"map_array": [{"key1": 1, "key2": 2}, {"key3": 3, "key4": 4}]}],
            schema=schema,
        )

        rows = df.collect()
        assert len(rows[0]["map_array"]) == 2
        assert rows[0]["map_array"][0]["key1"] == 1
        assert rows[0]["map_array"][1]["key3"] == 3

    def test_array_type_elementtype_with_struct_type(self, spark):
        """Test ArrayType with elementType as StructType."""
        element_struct = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )
        schema = StructType(
            [StructField("struct_array", ArrayType(elementType=element_struct), True)]
        )

        df = spark.createDataFrame(
            [
                {
                    "struct_array": [
                        {"name": "Alice", "age": 25},
                        {"name": "Bob", "age": 30},
                    ]
                }
            ],
            schema=schema,
        )

        rows = df.collect()
        assert len(rows[0]["struct_array"]) == 2
        assert rows[0]["struct_array"][0]["name"] == "Alice"
        assert rows[0]["struct_array"][0]["age"] == 25
        assert rows[0]["struct_array"][1]["name"] == "Bob"
        assert rows[0]["struct_array"][1]["age"] == 30

    def test_array_type_elementtype_with_nullable_element(self, spark):
        """Test ArrayType with nullable elements in array."""
        # Note: In PySpark, element nullable is controlled by ArrayType, not elementType
        # Arrays can contain null elements regardless of elementType nullable flag
        schema = StructType(
            [
                StructField(
                    "arr",
                    ArrayType(elementType=StringType()),
                    True,
                )
            ]
        )

        df = spark.createDataFrame(
            [{"arr": ["a", None, "b", None, "c"]}], schema=schema
        )

        rows = df.collect()
        assert rows[0]["arr"] == ["a", None, "b", None, "c"]

    def test_array_type_elementtype_with_non_nullable_array(self, spark):
        """Test ArrayType with elementType and nullable=False."""
        schema = StructType(
            [
                StructField(
                    "arr",
                    ArrayType(elementType=StringType(), nullable=False),
                    True,
                )
            ]
        )

        df = spark.createDataFrame([{"arr": ["a", "b", "c"]}], schema=schema)

        arr_type = df.schema.fields[0].dataType
        assert isinstance(arr_type, ArrayType)
        assert not arr_type.nullable

    def test_array_type_elementtype_in_complex_schema(self, spark):
        """Test ArrayType with elementType in a complex schema with multiple fields."""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("tags", ArrayType(elementType=StringType()), True),
                StructField("scores", ArrayType(elementType=DoubleType()), True),
                StructField("active", BooleanType(), True),
                StructField(
                    "metadata",
                    MapType(StringType(), StringType()),
                    True,
                ),
            ]
        )

        df = spark.createDataFrame(
            [
                {
                    "id": 1,
                    "name": "Alice",
                    "tags": ["python", "spark", "data"],
                    "scores": [95.5, 87.3, 92.1],
                    "active": True,
                    "metadata": {"role": "engineer", "team": "data"},
                },
                {
                    "id": 2,
                    "name": "Bob",
                    "tags": ["java", "kafka"],
                    "scores": [88.0, 91.5],
                    "active": False,
                    "metadata": {"role": "developer", "team": "backend"},
                },
            ],
            schema=schema,
        )

        rows = df.collect()
        assert len(rows) == 2
        assert rows[0]["tags"] == ["python", "spark", "data"]
        assert rows[0]["scores"] == [95.5, 87.3, 92.1]
        assert rows[1]["tags"] == ["java", "kafka"]
        assert rows[1]["scores"] == [88.0, 91.5]

    def test_array_type_elementtype_with_select_operation(self, spark):
        """Test ArrayType with elementType in select operations."""
        schema = StructType(
            [
                StructField("values", ArrayType(elementType=IntegerType()), True),
            ]
        )

        df = spark.createDataFrame([{"values": [1, 2, 3, 4, 5]}], schema=schema)

        # Select array column
        result = df.select("values")
        rows = result.collect()
        assert rows[0]["values"] == [1, 2, 3, 4, 5]

        # Select with array function
        result2 = df.select(F.size("values").alias("size"))
        rows2 = result2.collect()
        assert rows2[0]["size"] == 5

    def test_array_type_elementtype_with_filter_operation(self, spark):
        """Test ArrayType with elementType in filter operations."""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("tags", ArrayType(elementType=StringType()), True),
            ]
        )

        df = spark.createDataFrame(
            [
                {"id": 1, "tags": ["python", "spark"]},
                {"id": 2, "tags": ["java", "kafka"]},
                {"id": 3, "tags": ["python", "pandas"]},
            ],
            schema=schema,
        )

        # Filter using array_contains
        result = df.filter(F.array_contains("tags", "python"))
        rows = result.collect()
        assert len(rows) == 2
        assert rows[0]["id"] == 1
        assert rows[1]["id"] == 3

    def test_array_type_elementtype_with_explode_operation(self, spark):
        """Test ArrayType with elementType in explode operations."""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("values", ArrayType(elementType=IntegerType()), True),
            ]
        )

        df = spark.createDataFrame(
            [
                {"id": 1, "values": [10, 20, 30]},
                {"id": 2, "values": [40, 50]},
            ],
            schema=schema,
        )

        # Explode array
        result = df.select("id", F.explode("values").alias("value"))
        rows = result.collect()
        assert len(rows) == 5
        assert rows[0]["value"] == 10
        assert rows[1]["value"] == 20
        assert rows[2]["value"] == 30
        assert rows[3]["value"] == 40
        assert rows[4]["value"] == 50

    def test_array_type_elementtype_with_withcolumn_operation(self, spark):
        """Test ArrayType with elementType in withColumn operations."""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("values", ArrayType(elementType=IntegerType()), True),
            ]
        )

        df = spark.createDataFrame([{"id": 1, "values": [1, 2, 3]}], schema=schema)

        # Add computed column
        result = df.withColumn("sum", F.size("values"))
        rows = result.collect()
        assert rows[0]["sum"] == 3

    def test_array_type_elementtype_with_groupby_operation(self, spark):
        """Test ArrayType with elementType in groupBy operations."""
        schema = StructType(
            [
                StructField("category", StringType(), True),
                StructField("values", ArrayType(elementType=IntegerType()), True),
            ]
        )

        df = spark.createDataFrame(
            [
                {"category": "A", "values": [1, 2, 3]},
                {"category": "A", "values": [4, 5]},
                {"category": "B", "values": [6, 7, 8]},
            ],
            schema=schema,
        )

        # Group by and aggregate
        result = df.groupBy("category").agg(F.count("values").alias("count"))
        rows = result.collect()
        assert len(rows) == 2
        counts = {row["category"]: row["count"] for row in rows}
        assert counts["A"] == 2
        assert counts["B"] == 1

    def test_array_type_elementtype_with_union_operation(self, spark):
        """Test ArrayType with elementType in union operations."""
        schema = StructType(
            [
                StructField("values", ArrayType(elementType=IntegerType()), True),
            ]
        )

        df1 = spark.createDataFrame([{"values": [1, 2, 3]}], schema=schema)
        df2 = spark.createDataFrame([{"values": [4, 5, 6]}], schema=schema)

        result = df1.union(df2)
        rows = result.collect()
        assert len(rows) == 2
        assert rows[0]["values"] == [1, 2, 3]
        assert rows[1]["values"] == [4, 5, 6]

    def test_array_type_elementtype_empty_array(self, spark):
        """Test ArrayType with elementType handling empty arrays."""
        schema = StructType(
            [
                StructField("values", ArrayType(elementType=StringType()), True),
            ]
        )

        df = spark.createDataFrame([{"values": []}], schema=schema)

        rows = df.collect()
        assert rows[0]["values"] == []

    def test_array_type_elementtype_array_with_nulls(self, spark):
        """Test ArrayType with elementType handling arrays containing null elements."""
        schema = StructType(
            [
                StructField("values", ArrayType(elementType=StringType()), True),
            ]
        )

        df = spark.createDataFrame(
            [{"values": ["a", None, "b", None, "c"]}], schema=schema
        )

        rows = df.collect()
        assert rows[0]["values"] == ["a", None, "b", None, "c"]

    def test_array_type_elementtype_nullable_array_field(self, spark):
        """Test ArrayType with elementType where the array field itself is nullable."""
        schema = StructType(
            [
                StructField("values", ArrayType(elementType=IntegerType()), True),
            ]
        )

        df = spark.createDataFrame(
            [
                {"values": [1, 2, 3]},
                {"values": None},  # Null array
                {"values": [4, 5]},
            ],
            schema=schema,
        )

        rows = df.collect()
        assert rows[0]["values"] == [1, 2, 3]
        assert rows[1]["values"] is None
        assert rows[2]["values"] == [4, 5]

    def test_array_type_elementtype_pyspark_parity_simple(self, spark):
        """Test basic elementType usage matches PySpark behavior."""
        if not _is_pyspark_mode():
            pytest.skip(
                "PySpark parity test - run with MOCK_SPARK_TEST_BACKEND=pyspark"
            )

        from pyspark.sql import SparkSession as PySparkSession
        from pyspark.sql.types import (
            StructType as PySparkStructType,
            StructField as PySparkStructField,
            ArrayType as PySparkArrayType,
            StringType as PySparkStringType,
            IntegerType as PySparkIntegerType,
        )

        pyspark_session = PySparkSession.builder.appName("Parity").getOrCreate()

        # Create schema with elementType in PySpark
        pyspark_schema = PySparkStructType(
            [
                PySparkStructField(
                    "arr", PySparkArrayType(elementType=PySparkStringType()), True
                ),
                PySparkStructField(
                    "nums", PySparkArrayType(elementType=PySparkIntegerType()), True
                ),
            ]
        )

        pyspark_df = pyspark_session.createDataFrame(
            [{"arr": ["a", "b", "c"], "nums": [1, 2, 3]}], schema=pyspark_schema
        )

        # Create same schema with elementType in sparkless
        sparkless_schema = StructType(
            [
                StructField("arr", ArrayType(elementType=StringType()), True),
                StructField("nums", ArrayType(elementType=IntegerType()), True),
            ]
        )

        sparkless_df = spark.createDataFrame(
            [{"arr": ["a", "b", "c"], "nums": [1, 2, 3]}], schema=sparkless_schema
        )

        # Verify schemas match
        pyspark_fields = pyspark_df.schema.fields
        sparkless_fields = sparkless_df.schema.fields

        assert len(pyspark_fields) == len(sparkless_fields)
        assert pyspark_fields[0].name == sparkless_fields[0].name
        assert pyspark_fields[1].name == sparkless_fields[1].name

        # Verify data matches
        pyspark_rows = pyspark_df.collect()
        sparkless_rows = sparkless_df.collect()

        assert pyspark_rows[0]["arr"] == sparkless_rows[0]["arr"]
        assert pyspark_rows[0]["nums"] == sparkless_rows[0]["nums"]

        pyspark_session.stop()

    def test_array_type_elementtype_pyspark_parity_complex(self, spark):
        """Test complex nested elementType usage matches PySpark behavior."""
        if not _is_pyspark_mode():
            pytest.skip(
                "PySpark parity test - run with MOCK_SPARK_TEST_BACKEND=pyspark"
            )

        from pyspark.sql import SparkSession as PySparkSession
        from pyspark.sql.types import (
            StructType as PySparkStructType,
            StructField as PySparkStructField,
            ArrayType as PySparkArrayType,
            StringType as PySparkStringType,
        )

        pyspark_session = PySparkSession.builder.appName("Parity").getOrCreate()

        # Nested array with elementType in PySpark
        pyspark_schema = PySparkStructType(
            [
                PySparkStructField(
                    "nested",
                    PySparkArrayType(
                        elementType=PySparkArrayType(elementType=PySparkStringType())
                    ),
                    True,
                )
            ]
        )

        pyspark_df = pyspark_session.createDataFrame(
            [{"nested": [["a", "b"], ["c", "d"]]}], schema=pyspark_schema
        )

        # Same in sparkless
        sparkless_schema = StructType(
            [
                StructField(
                    "nested",
                    ArrayType(elementType=ArrayType(elementType=StringType())),
                    True,
                )
            ]
        )

        sparkless_df = spark.createDataFrame(
            [{"nested": [["a", "b"], ["c", "d"]]}], schema=sparkless_schema
        )

        # Verify data matches
        pyspark_rows = pyspark_df.collect()
        sparkless_rows = sparkless_df.collect()

        assert pyspark_rows[0]["nested"] == sparkless_rows[0]["nested"]

        pyspark_session.stop()

    def test_array_type_elementtype_with_date_type(self, spark):
        """Test ArrayType with elementType as DateType."""
        schema = StructType(
            [
                StructField("dates", ArrayType(elementType=DateType()), True),
            ]
        )

        from datetime import date

        df = spark.createDataFrame(
            [
                {
                    "dates": [
                        date(2024, 1, 1),
                        date(2024, 1, 2),
                        date(2024, 1, 3),
                    ]
                }
            ],
            schema=schema,
        )

        rows = df.collect()
        assert len(rows[0]["dates"]) == 3

    def test_array_type_elementtype_with_timestamp_type(self, spark):
        """Test ArrayType with elementType as TimestampType."""
        schema = StructType(
            [
                StructField("timestamps", ArrayType(elementType=TimestampType()), True),
            ]
        )

        from datetime import datetime

        df = spark.createDataFrame(
            [
                {
                    "timestamps": [
                        datetime(2024, 1, 1, 12, 0, 0),
                        datetime(2024, 1, 2, 12, 0, 0),
                    ]
                }
            ],
            schema=schema,
        )

        rows = df.collect()
        assert len(rows[0]["timestamps"]) == 2

    def test_array_type_elementtype_with_decimal_type(self, spark):
        """Test ArrayType with elementType as DecimalType."""
        schema = StructType(
            [
                StructField(
                    "decimals",
                    ArrayType(elementType=DecimalType(10, 2)),
                    True,
                )
            ]
        )

        from decimal import Decimal

        df = spark.createDataFrame(
            [
                {
                    "decimals": [
                        Decimal("10.50"),
                        Decimal("20.75"),
                        Decimal("30.25"),
                    ]
                }
            ],
            schema=schema,
        )

        rows = df.collect()
        assert len(rows[0]["decimals"]) == 3

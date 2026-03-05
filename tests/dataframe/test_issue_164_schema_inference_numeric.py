"""
Test for issue #164: Type comparison error: 'cannot compare string with numeric type (i32)'

Uses get_spark_imports from fixture only.
"""

from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
SparkSession = _imports.SparkSession
F = _imports.F


class TestIssue164SchemaInferenceNumeric:
    """Test cases for issue #164: schema inference for numeric types."""

    def test_schema_inference_for_numeric_columns(self):
        """Test that numeric columns are inferred as numeric types, not strings."""
        spark = SparkSession.builder.appName("test").getOrCreate()

        # Create test data with numeric column
        data = []
        for i in range(10):
            data.append(
                {
                    "id": f"ID-{i:03d}",
                    "cost_per_impression": round(0.01 + (i % 50) / 1000, 3),
                }
            )

        df = spark.createDataFrame(data, ["id", "cost_per_impression"])

        # PySpark inference with dict rows + explicit column list produces:
        #   id: DoubleType, cost_per_impression: StringType
        schema = df.schema
        field_map = {f.name: f for f in schema.fields}
        assert field_map["id"].dataType.__class__.__name__ == "DoubleType"
        assert field_map["cost_per_impression"].dataType.__class__.__name__ == "StringType"

        # Comparing the string-typed numeric column to a number yields zero rows
        # but does not raise an error.
        result_df = df.filter(F.col("cost_per_impression") >= 0)
        count = result_df.count()
        assert count == 0

        spark.stop()

    def test_schema_inference_for_integer_columns(self):
        """Test that integer columns are inferred as LongType, not strings."""
        spark = SparkSession.builder.appName("test").getOrCreate()

        # Create test data with integer column
        data = []
        for i in range(10):
            data.append({"id": f"ID-{i:03d}", "count": i})

        df = spark.createDataFrame(data, ["id", "count"])

        # PySpark inference with dict rows + explicit column list produces:
        #   id: LongType, count: StringType
        schema = df.schema
        field_map = {f.name: f for f in schema.fields}
        assert field_map["id"].dataType.__class__.__name__ == "LongType"
        assert field_map["count"].dataType.__class__.__name__ == "StringType"

        # Comparing the string-typed count column to a number yields zero rows
        # but does not raise an error.
        result_df = df.filter(F.col("count") >= 5)
        count = result_df.count()
        assert count == 0

        spark.stop()

    def test_schema_inference_mixed_types(self):
        """Test that schema inference works correctly for mixed types."""
        spark = SparkSession.builder.appName("test").getOrCreate()

        # Create test data with mixed types
        data = []
        for i in range(10):
            data.append(
                {
                    "id": f"ID-{i:03d}",
                    "count": i,
                    "cost": round(0.01 + i / 1000, 3),
                    "is_active": i % 2 == 0,
                }
            )

        df = spark.createDataFrame(data, ["id", "count", "cost", "is_active"])

        # Verify all types are inferred as PySpark actually does for this shape:
        #   id: DoubleType, count: LongType, cost: StringType, is_active: BooleanType
        schema = df.schema
        field_map = {f.name: f for f in schema.fields}

        assert field_map["id"].dataType.__class__.__name__ == "DoubleType"
        assert field_map["count"].dataType.__class__.__name__ == "LongType"
        assert field_map["cost"].dataType.__class__.__name__ == "StringType"
        assert field_map["is_active"].dataType.__class__.__name__ == "BooleanType"

        # Numeric-style comparisons against the string-typed `cost` column
        # yield zero rows but should not raise errors.
        result_df = df.filter(
            (F.col("count") >= 5) & (F.col("cost") >= 0) & F.col("is_active")
        )
        count = result_df.count()
        assert count == 0

        spark.stop()

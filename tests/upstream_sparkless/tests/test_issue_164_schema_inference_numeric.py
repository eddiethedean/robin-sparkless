"""
Test for issue #164: Type comparison error: 'cannot compare string with numeric type (i32)'

Issue #164 reports that sparkless treats numeric columns as strings, causing type comparison
errors when comparing numeric columns with numbers. The root cause is that schema inference
defaults all columns to StringType when column names are provided, even when the values are numeric.
"""

from sparkless import SparkSession, functions as F


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

        # Verify schema shows numeric type, not StringType
        schema = df.schema
        cost_field = next(f for f in schema.fields if f.name == "cost_per_impression")
        assert cost_field.dataType.__class__.__name__ in [
            "DoubleType",
            "DecimalType",
        ], f"Expected numeric type, got {cost_field.dataType.__class__.__name__}"

        # Verify we can compare numeric column with number (THIS SHOULD WORK)
        result_df = df.filter(F.col("cost_per_impression") >= 0)
        count = result_df.count()
        assert count == 10

        spark.stop()

    def test_schema_inference_for_integer_columns(self):
        """Test that integer columns are inferred as LongType, not strings."""
        spark = SparkSession.builder.appName("test").getOrCreate()

        # Create test data with integer column
        data = []
        for i in range(10):
            data.append({"id": f"ID-{i:03d}", "count": i})

        df = spark.createDataFrame(data, ["id", "count"])

        # Verify schema shows LongType, not StringType
        schema = df.schema
        count_field = next(f for f in schema.fields if f.name == "count")
        assert count_field.dataType.__class__.__name__ == "LongType", (
            f"Expected LongType, got {count_field.dataType.__class__.__name__}"
        )

        # Verify we can compare integer column with number
        result_df = df.filter(F.col("count") >= 5)
        count = result_df.count()
        assert count == 5

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

        # Verify all types are inferred correctly
        schema = df.schema
        field_map = {f.name: f for f in schema.fields}

        assert field_map["id"].dataType.__class__.__name__ == "StringType"
        assert field_map["count"].dataType.__class__.__name__ == "LongType"
        assert field_map["cost"].dataType.__class__.__name__ in [
            "DoubleType",
            "DecimalType",
        ]
        assert field_map["is_active"].dataType.__class__.__name__ == "BooleanType"

        # Verify numeric operations work
        result_df = df.filter(
            (F.col("count") >= 5) & (F.col("cost") >= 0) & F.col("is_active")
        )
        count = result_df.count()
        assert count >= 0  # Should not raise an error

        spark.stop()

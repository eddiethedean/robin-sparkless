"""Tests for union type coercion (unioning DataFrames with different types).

This module tests that sparkless correctly handles unioning DataFrames with
different column types (e.g., numeric vs string), matching PySpark behavior.

These tests work with both sparkless (mock) and PySpark backends.
Set MOCK_SPARK_TEST_BACKEND=pyspark to run with real PySpark.
"""

from tests.fixtures.spark_imports import get_spark_imports

# Get imports based on backend
imports = get_spark_imports()
StringType = imports.StringType
IntegerType = imports.IntegerType
LongType = imports.LongType
DoubleType = imports.DoubleType
FloatType = imports.FloatType
StructType = imports.StructType
StructField = imports.StructField


class TestUnionTypeCoercion:
    """Test union operations with different column types."""

    def test_union_int64_with_string(self, spark):
        """Test unioning int64 with string (issue #242).

        PySpark allows unioning DataFrames with different types, normalizing
        numeric to string when mixing numeric and string types.
        """
        # Create left DataFrame with int64 key
        left_data = [{"key": 1, "value": "A"}, {"key": 2, "value": "B"}]

        # Create right DataFrame with string key
        right_data = [{"key": "3", "value": "X"}, {"key": "4", "value": "Y"}]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        # Union the dataframes
        result = df_left.union(df_right)

        rows = result.collect()

        # Verify the union worked correctly
        assert len(rows) == 4

        # Verify the key column is string type (PySpark normalizes to string)
        for row in rows:
            key_val = (
                row["key"] if hasattr(row, "__getitem__") else getattr(row, "key", None)
            )
            assert isinstance(key_val, str), (
                f"Key should be string, got {type(key_val)}: {key_val}"
            )

        # Verify all values are present
        key_values = {
            row["key"] if hasattr(row, "__getitem__") else getattr(row, "key", None)
            for row in rows
        }
        assert "1" in key_values or 1 in key_values  # May be coerced to string
        assert "2" in key_values or 2 in key_values
        assert "3" in key_values
        assert "4" in key_values

    def test_union_string_with_int64(self, spark):
        """Test unioning string with int64 (opposite direction)."""
        left_data = [{"key": "1", "value": "A"}, {"key": "2", "value": "B"}]
        right_data = [{"key": 3, "value": "X"}, {"key": 4, "value": "Y"}]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        result = df_left.union(df_right)
        rows = result.collect()

        assert len(rows) == 4
        # Key should be string (PySpark normalizes to string)
        for row in rows:
            key_val = (
                row["key"] if hasattr(row, "__getitem__") else getattr(row, "key", None)
            )
            assert isinstance(key_val, str), (
                f"Key should be string, got {type(key_val)}"
            )

    def test_union_int32_with_string(self, spark):
        """Test unioning int32 with string."""
        schema_left = StructType(
            [
                StructField("key", IntegerType(), True),
                StructField("value", StringType(), True),
            ]
        )
        schema_right = StructType(
            [
                StructField("key", StringType(), True),
                StructField("value", StringType(), True),
            ]
        )

        left_data = [{"key": 100, "value": "A"}, {"key": 200, "value": "B"}]
        right_data = [{"key": "300", "value": "X"}, {"key": "400", "value": "Y"}]

        df_left = spark.createDataFrame(left_data, schema=schema_left)
        df_right = spark.createDataFrame(right_data, schema=schema_right)

        result = df_left.union(df_right)
        rows = result.collect()

        assert len(rows) == 4
        # Key should be string after union
        for row in rows:
            key_val = (
                row["key"] if hasattr(row, "__getitem__") else getattr(row, "key", None)
            )
            assert isinstance(key_val, str), (
                f"Key should be string, got {type(key_val)}"
            )

    def test_union_float_with_string(self, spark):
        """Test unioning float with string."""
        schema_left = StructType(
            [
                StructField("key", FloatType(), True),
                StructField("value", StringType(), True),
            ]
        )
        schema_right = StructType(
            [
                StructField("key", StringType(), True),
                StructField("value", StringType(), True),
            ]
        )

        left_data = [{"key": 1.5, "value": "A"}, {"key": 2.5, "value": "B"}]
        right_data = [{"key": "3.5", "value": "X"}, {"key": "4.5", "value": "Y"}]

        df_left = spark.createDataFrame(left_data, schema=schema_left)
        df_right = spark.createDataFrame(right_data, schema=schema_right)

        result = df_left.union(df_right)
        rows = result.collect()

        assert len(rows) == 4
        # Key should be string after union
        for row in rows:
            key_val = (
                row["key"] if hasattr(row, "__getitem__") else getattr(row, "key", None)
            )
            assert isinstance(key_val, str), (
                f"Key should be string, got {type(key_val)}"
            )

    def test_union_int32_with_int64(self, spark):
        """Test unioning int32 with int64 (both numeric - promote to int64)."""
        schema_left = StructType(
            [
                StructField("key", IntegerType(), True),
                StructField("value", StringType(), True),
            ]
        )
        schema_right = StructType(
            [
                StructField("key", LongType(), True),
                StructField("value", StringType(), True),
            ]
        )

        left_data = [{"key": 1234, "value": "A"}, {"key": 4567, "value": "B"}]
        right_data = [{"key": 1234, "value": "X"}, {"key": 4567, "value": "Y"}]

        df_left = spark.createDataFrame(left_data, schema=schema_left)
        df_right = spark.createDataFrame(right_data, schema=schema_right)

        result = df_left.union(df_right)
        rows = result.collect()

        assert len(rows) == 4
        # Key should be int64 (promoted from int32)
        for row in rows:
            key_val = (
                row["key"] if hasattr(row, "__getitem__") else getattr(row, "key", None)
            )
            assert isinstance(key_val, int), f"Key should be int, got {type(key_val)}"

    def test_union_int_with_float(self, spark):
        """Test unioning int with float (promote to float)."""
        schema_left = StructType(
            [
                StructField("key", IntegerType(), True),
                StructField("value", StringType(), True),
            ]
        )
        schema_right = StructType(
            [
                StructField("key", FloatType(), True),
                StructField("value", StringType(), True),
            ]
        )

        left_data = [{"key": 100, "value": "A"}, {"key": 200, "value": "B"}]
        right_data = [{"key": 100.0, "value": "X"}, {"key": 200.0, "value": "Y"}]

        df_left = spark.createDataFrame(left_data, schema=schema_left)
        df_right = spark.createDataFrame(right_data, schema=schema_right)

        result = df_left.union(df_right)
        rows = result.collect()

        assert len(rows) == 4
        # Key should be float (promoted from int)
        for row in rows:
            key_val = (
                row["key"] if hasattr(row, "__getitem__") else getattr(row, "key", None)
            )
            assert isinstance(key_val, float), (
                f"Key should be float, got {type(key_val)}"
            )

    def test_union_issue_242_exact_scenario(self, spark):
        """Test the exact scenario from issue #242."""
        df_left = spark.createDataFrame(
            [{"key": 1, "value": "A"}, {"key": 2, "value": "B"}]
        )
        df_right = spark.createDataFrame(
            [{"key": "3", "value": "X"}, {"key": "4", "value": "Y"}]
        )

        result = df_left.union(df_right)

        rows = result.collect()
        assert len(rows) == 4

        # Verify key column is string type (PySpark normalizes to string)
        schema = result.schema
        key_field = next((f for f in schema.fields if f.name == "key"), None)
        assert key_field is not None
        assert isinstance(key_field.dataType, StringType), (
            "Key should be StringType after union"
        )

        # Verify all values are present
        key_values = {
            row["key"] if hasattr(row, "__getitem__") else getattr(row, "key", None)
            for row in rows
        }
        # Values may be strings or ints depending on coercion
        assert any(k in ("1", 1) for k in key_values)
        assert any(k in ("2", 2) for k in key_values)
        assert any(k in ("3", "3") for k in key_values)
        assert any(k in ("4", "4") for k in key_values)


class TestUnionTypeCoercionParity:
    """Test union type coercion with direct PySpark parity comparison."""

    def test_pyspark_parity_int64_string(self, spark):
        """Test that union with int64/string keys matches PySpark exactly (issue #242)."""
        left_data = [{"key": 1, "value": "A"}, {"key": 2, "value": "B"}]
        right_data = [{"key": "3", "value": "X"}, {"key": "4", "value": "Y"}]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        result = df_left.union(df_right)
        rows = result.collect()

        assert len(rows) == 4

        # PySpark normalizes to string when mixing numeric and string
        # Verify schema shows string type
        schema = result.schema
        key_field = next((f for f in schema.fields if f.name == "key"), None)
        assert key_field is not None
        assert isinstance(key_field.dataType, StringType), (
            "Key should be StringType after union"
        )

        # Verify all values are present and are strings
        row_data = []
        for row in rows:
            key_val = (
                row["key"] if hasattr(row, "__getitem__") else getattr(row, "key", None)
            )
            value_val = (
                row["value"]
                if hasattr(row, "__getitem__")
                else getattr(row, "value", None)
            )
            row_data.append({"key": str(key_val), "value": value_val})

        # Sort by key for comparison
        row_data.sort(key=lambda x: x["key"])

        expected_rows = [
            {"key": "1", "value": "A"},
            {"key": "2", "value": "B"},
            {"key": "3", "value": "X"},
            {"key": "4", "value": "Y"},
        ]

        assert len(row_data) == len(expected_rows)
        for i, row in enumerate(row_data):
            assert row["key"] == expected_rows[i]["key"]
            assert row["value"] == expected_rows[i]["value"]

    def test_pyspark_parity_string_int64(self, spark):
        """Test that union with string/int64 keys matches PySpark exactly."""
        left_data = [{"key": "1", "value": "A"}, {"key": "2", "value": "B"}]
        right_data = [{"key": 3, "value": "X"}, {"key": 4, "value": "Y"}]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        result = df_left.union(df_right)
        rows = result.collect()

        assert len(rows) == 4
        # PySpark normalizes to string
        schema = result.schema
        key_field = next((f for f in schema.fields if f.name == "key"), None)
        assert key_field is not None
        assert isinstance(key_field.dataType, StringType), (
            "Key should be StringType after union"
        )

    def test_pyspark_parity_multiple_numeric_string_columns(self, spark):
        """Test union with multiple columns having type mismatches."""
        left_data = [{"key": 1, "value": 10, "other": "A"}]
        right_data = [{"key": "2", "value": "20", "other": "B"}]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        result = df_left.union(df_right)
        rows = result.collect()

        assert len(rows) == 2
        # All numeric columns should be normalized to string
        schema = result.schema
        for field in schema.fields:
            if field.name in ("key", "value"):
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType after union"
                )

    def test_pyspark_parity_null_values(self, spark):
        """Test union type coercion with null values."""
        left_data = [{"key": 1, "value": "A"}, {"key": None, "value": "B"}]
        right_data = [{"key": "3", "value": "X"}, {"key": None, "value": "Y"}]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        result = df_left.union(df_right)
        rows = result.collect()

        assert len(rows) == 4
        # Null values should be preserved
        null_keys = [
            row["key"] if hasattr(row, "__getitem__") else getattr(row, "key", None)
            for row in rows
            if (
                row["key"] if hasattr(row, "__getitem__") else getattr(row, "key", None)
            )
            is None
        ]
        assert len(null_keys) == 2  # Two None values

    def test_pyspark_parity_union_by_name(self, spark):
        """Test unionByName with type coercion."""
        left_data = [{"key": 1, "value": "A"}]
        right_data = [{"key": "2", "value": "B"}]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        result = df_left.unionByName(df_right)
        rows = result.collect()

        assert len(rows) == 2
        # Key should be string after union
        schema = result.schema
        key_field = next((f for f in schema.fields if f.name == "key"), None)
        assert key_field is not None
        assert isinstance(key_field.dataType, StringType), (
            "Key should be StringType after union"
        )


class TestUnionTypeCoercionEdgeCases:
    """Test edge cases and complex scenarios for union type coercion."""

    def test_union_double_with_string(self, spark):
        """Test unioning double with string."""
        schema_left = StructType(
            [
                StructField("key", DoubleType(), True),
                StructField("value", StringType(), True),
            ]
        )
        schema_right = StructType(
            [
                StructField("key", StringType(), True),
                StructField("value", StringType(), True),
            ]
        )

        left_data = [{"key": 123.456, "value": "A"}, {"key": 789.012, "value": "B"}]
        right_data = [
            {"key": "123.456", "value": "X"},
            {"key": "789.012", "value": "Y"},
        ]

        df_left = spark.createDataFrame(left_data, schema=schema_left)
        df_right = spark.createDataFrame(right_data, schema=schema_right)

        result = df_left.union(df_right)
        rows = result.collect()

        assert len(rows) == 4
        # Key should be string after union
        schema = result.schema
        key_field = next((f for f in schema.fields if f.name == "key"), None)
        assert key_field is not None
        assert isinstance(key_field.dataType, StringType), (
            "Key should be StringType after union"
        )

    def test_union_float_with_double(self, spark):
        """Test unioning float with double (promote to double)."""
        schema_left = StructType(
            [
                StructField("key", FloatType(), True),
                StructField("value", StringType(), True),
            ]
        )
        schema_right = StructType(
            [
                StructField("key", DoubleType(), True),
                StructField("value", StringType(), True),
            ]
        )

        left_data = [{"key": 1.5, "value": "A"}, {"key": 2.5, "value": "B"}]
        right_data = [{"key": 3.5, "value": "X"}, {"key": 4.5, "value": "Y"}]

        df_left = spark.createDataFrame(left_data, schema=schema_left)
        df_right = spark.createDataFrame(right_data, schema=schema_right)

        result = df_left.union(df_right)
        rows = result.collect()

        assert len(rows) == 4
        # Key should be double (promoted from float)
        schema = result.schema
        key_field = next((f for f in schema.fields if f.name == "key"), None)
        assert key_field is not None
        assert isinstance(key_field.dataType, (FloatType, DoubleType)), (
            "Key should be Float or Double after union"
        )

    def test_union_negative_numbers(self, spark):
        """Test unioning with negative numeric values."""
        left_data = [{"key": -1, "value": "A"}, {"key": -2, "value": "B"}]
        right_data = [{"key": "-3", "value": "X"}, {"key": "-4", "value": "Y"}]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        result = df_left.union(df_right)
        rows = result.collect()

        assert len(rows) == 4
        # Key should be string after union
        schema = result.schema
        key_field = next((f for f in schema.fields if f.name == "key"), None)
        assert key_field is not None
        assert isinstance(key_field.dataType, StringType), (
            "Key should be StringType after union"
        )

        # Verify negative values are preserved
        key_values = {
            row["key"] if hasattr(row, "__getitem__") else getattr(row, "key", None)
            for row in rows
        }
        assert any(k in ("-1", -1) for k in key_values)
        assert any(k in ("-2", -2) for k in key_values)
        assert any(k in ("-3", "-3") for k in key_values)
        assert any(k in ("-4", "-4") for k in key_values)

    def test_union_zero_values(self, spark):
        """Test unioning with zero values."""
        # Use consistent types - all numeric in left, all string in right
        left_data = [{"key": 0, "value": "A"}, {"key": 0, "value": "B"}]
        right_data = [{"key": "0", "value": "X"}, {"key": "0.0", "value": "Y"}]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        result = df_left.union(df_right)
        rows = result.collect()

        assert len(rows) == 4
        # Key should be string after union
        schema = result.schema
        key_field = next((f for f in schema.fields if f.name == "key"), None)
        assert key_field is not None
        assert isinstance(key_field.dataType, StringType), (
            "Key should be StringType after union"
        )

    def test_union_large_numbers(self, spark):
        """Test unioning with large numeric values."""
        left_data = [
            {"key": 2147483647, "value": "A"},
            {"key": 9223372036854775807, "value": "B"},
        ]
        right_data = [
            {"key": "2147483647", "value": "X"},
            {"key": "9223372036854775807", "value": "Y"},
        ]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        result = df_left.union(df_right)
        rows = result.collect()

        assert len(rows) == 4
        # Key should be string after union
        schema = result.schema
        key_field = next((f for f in schema.fields if f.name == "key"), None)
        assert key_field is not None
        assert isinstance(key_field.dataType, StringType), (
            "Key should be StringType after union"
        )

    def test_union_decimal_precision(self, spark):
        """Test unioning with high decimal precision."""
        left_data = [
            {"key": 1.123456789, "value": "A"},
            {"key": 2.987654321, "value": "B"},
        ]
        right_data = [
            {"key": "1.123456789", "value": "X"},
            {"key": "2.987654321", "value": "Y"},
        ]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        result = df_left.union(df_right)
        rows = result.collect()

        assert len(rows) == 4
        # Key should be string after union
        schema = result.schema
        key_field = next((f for f in schema.fields if f.name == "key"), None)
        assert key_field is not None
        assert isinstance(key_field.dataType, StringType), (
            "Key should be StringType after union"
        )

    def test_union_scientific_notation_strings(self, spark):
        """Test unioning with scientific notation strings."""
        left_data = [{"key": 1.23e2, "value": "A"}]  # 123.0
        right_data = [{"key": "1.23e2", "value": "X"}]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        result = df_left.union(df_right)
        rows = result.collect()

        assert len(rows) == 2
        # Key should be string after union
        schema = result.schema
        key_field = next((f for f in schema.fields if f.name == "key"), None)
        assert key_field is not None
        assert isinstance(key_field.dataType, StringType), (
            "Key should be StringType after union"
        )

    def test_union_multiple_numeric_types(self, spark):
        """Test unioning with multiple numeric types in sequence."""
        # Int32 + String
        df1 = spark.createDataFrame(
            [{"key": 1, "value": "A"}],
            schema=StructType(
                [
                    StructField("key", IntegerType(), True),
                    StructField("value", StringType(), True),
                ]
            ),
        )
        df2 = spark.createDataFrame([{"key": "2", "value": "B"}])

        result1 = df1.union(df2)
        # Result should have StringType for key

        # Now union with Int64
        df3 = spark.createDataFrame(
            [{"key": 3, "value": "C"}],
            schema=StructType(
                [
                    StructField("key", LongType(), True),
                    StructField("value", StringType(), True),
                ]
            ),
        )

        result2 = result1.union(df3)
        rows = result2.collect()

        assert len(rows) == 3
        # Key should still be string (numeric+string -> string)
        schema = result2.schema
        key_field = next((f for f in schema.fields if f.name == "key"), None)
        assert key_field is not None
        assert isinstance(key_field.dataType, StringType), (
            "Key should be StringType after union"
        )

    def test_union_chained_unions(self, spark):
        """Test chained union operations with type coercion."""
        df1 = spark.createDataFrame([{"key": 1, "value": "A"}])
        df2 = spark.createDataFrame([{"key": "2", "value": "B"}])
        df3 = spark.createDataFrame([{"key": 3, "value": "C"}])

        result = df1.union(df2).union(df3)
        rows = result.collect()

        assert len(rows) == 3
        # All keys should be string after multiple unions
        schema = result.schema
        key_field = next((f for f in schema.fields if f.name == "key"), None)
        assert key_field is not None
        assert isinstance(key_field.dataType, StringType), (
            "Key should be StringType after chained unions"
        )

    def test_union_empty_dataframe(self, spark):
        """Test unioning with empty DataFrame."""
        df_left = spark.createDataFrame([{"key": 1, "value": "A"}])
        df_right = spark.createDataFrame(
            [],
            schema=StructType(
                [
                    StructField("key", StringType(), True),
                    StructField("value", StringType(), True),
                ]
            ),
        )

        result = df_left.union(df_right)
        rows = result.collect()

        assert len(rows) == 1
        # Schema should reflect coerced type (string)
        schema = result.schema
        key_field = next((f for f in schema.fields if f.name == "key"), None)
        assert key_field is not None
        assert isinstance(key_field.dataType, StringType), (
            "Key should be StringType after union with empty DataFrame"
        )

    def test_union_all_null_columns(self, spark):
        """Test unioning with columns that are all null."""
        left_data = [{"key": None, "value": "A"}]
        right_data = [{"key": None, "value": "B"}]

        df_left = spark.createDataFrame(
            left_data,
            schema=StructType(
                [
                    StructField("key", IntegerType(), True),
                    StructField("value", StringType(), True),
                ]
            ),
        )
        df_right = spark.createDataFrame(
            right_data,
            schema=StructType(
                [
                    StructField("key", StringType(), True),
                    StructField("value", StringType(), True),
                ]
            ),
        )

        result = df_left.union(df_right)
        rows = result.collect()

        assert len(rows) == 2
        # Both rows should have None for key
        for row in rows:
            key_val = (
                row["key"] if hasattr(row, "__getitem__") else getattr(row, "key", None)
            )
            assert key_val is None

    def test_union_mixed_nulls_and_values(self, spark):
        """Test unioning with mixed null and non-null values."""
        left_data = [{"key": 1, "value": "A"}, {"key": None, "value": "B"}]
        right_data = [{"key": "3", "value": "X"}, {"key": None, "value": "Y"}]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        result = df_left.union(df_right)
        rows = result.collect()

        assert len(rows) == 4
        # Key should be string type
        schema = result.schema
        key_field = next((f for f in schema.fields if f.name == "key"), None)
        assert key_field is not None
        assert isinstance(key_field.dataType, StringType), (
            "Key should be StringType after union"
        )

        # Count nulls - should be 2
        null_count = sum(
            1
            for row in rows
            if (
                row["key"] if hasattr(row, "__getitem__") else getattr(row, "key", None)
            )
            is None
        )
        assert null_count == 2

    def test_union_three_dataframes(self, spark):
        """Test unioning three DataFrames with different types."""
        df1 = spark.createDataFrame([{"key": 1, "value": "A"}])
        df2 = spark.createDataFrame([{"key": "2", "value": "B"}])
        df3 = spark.createDataFrame([{"key": 3.0, "value": "C"}])

        # Chain unions
        result = df1.union(df2).union(df3)
        rows = result.collect()

        assert len(rows) == 3
        # Key should be string (numeric+string -> string)
        schema = result.schema
        key_field = next((f for f in schema.fields if f.name == "key"), None)
        assert key_field is not None
        assert isinstance(key_field.dataType, StringType), (
            "Key should be StringType after multiple unions"
        )


class TestUnionTypeCoercionParityRobust:
    """More robust PySpark parity tests for union type coercion."""

    def test_pyspark_parity_exact_output_match(self, spark):
        """Test that union output matches PySpark exactly (issue #242)."""
        # Exact scenario from issue #242
        df_left = spark.createDataFrame(
            [{"key": 1, "value": "A"}, {"key": 2, "value": "B"}]
        )
        df_right = spark.createDataFrame(
            [{"key": "3", "value": "X"}, {"key": "4", "value": "Y"}]
        )

        result = df_left.union(df_right)
        rows = result.collect()

        assert len(rows) == 4

        # PySpark output format:
        # key:string
        # value:string
        # +---+-----+
        # |key|value|
        # +---+-----+
        # |  1|    A|
        # |  2|    B|
        # |  3|    X|
        # |  4|    Y|
        # +---+-----+

        # Verify schema
        schema = result.schema
        assert len(schema.fields) == 2
        key_field = next((f for f in schema.fields if f.name == "key"), None)
        value_field = next((f for f in schema.fields if f.name == "value"), None)

        assert key_field is not None
        assert value_field is not None
        assert isinstance(key_field.dataType, StringType), (
            f"Key should be StringType, got {key_field.dataType}"
        )
        assert isinstance(value_field.dataType, StringType), (
            "Value should be StringType"
        )

        # Collect and normalize row data
        row_data = []
        for row in rows:
            key_val = (
                row["key"] if hasattr(row, "__getitem__") else getattr(row, "key", None)
            )
            value_val = (
                row["value"]
                if hasattr(row, "__getitem__")
                else getattr(row, "value", None)
            )
            # Normalize key to string for comparison
            row_data.append(
                {
                    "key": str(key_val) if key_val is not None else None,
                    "value": value_val,
                }
            )

        # Sort by key for stable comparison
        row_data.sort(key=lambda x: (x["key"] is None, x["key"]))

        expected_rows = [
            {"key": "1", "value": "A"},
            {"key": "2", "value": "B"},
            {"key": "3", "value": "X"},
            {"key": "4", "value": "Y"},
        ]

        assert len(row_data) == len(expected_rows)
        for i, row in enumerate(row_data):
            assert row["key"] == expected_rows[i]["key"], (
                f"Row {i}: expected key '{expected_rows[i]['key']}', got '{row['key']}'"
            )
            assert row["value"] == expected_rows[i]["value"], (
                f"Row {i}: expected value '{expected_rows[i]['value']}', got '{row['value']}'"
            )

    def test_pyspark_parity_float_string_decimal_values(self, spark):
        """Test that float+string union matches PySpark with decimal values."""
        left_data = [{"key": 1.5, "value": "A"}, {"key": 2.7, "value": "B"}]
        right_data = [{"key": "3.9", "value": "X"}, {"key": "4.1", "value": "Y"}]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        result = df_left.union(df_right)
        rows = result.collect()

        assert len(rows) == 4
        # Key should be string
        schema = result.schema
        key_field = next((f for f in schema.fields if f.name == "key"), None)
        assert key_field is not None
        assert isinstance(key_field.dataType, StringType), (
            "Key should be StringType after union"
        )

        # Verify all values are present
        row_data = []
        for row in rows:
            key_val = (
                row["key"] if hasattr(row, "__getitem__") else getattr(row, "key", None)
            )
            value_val = (
                row["value"]
                if hasattr(row, "__getitem__")
                else getattr(row, "value", None)
            )
            row_data.append(
                {
                    "key": str(key_val) if key_val is not None else None,
                    "value": value_val,
                }
            )

        # Check that decimal values are preserved
        key_values = {row["key"] for row in row_data}
        assert "1.5" in key_values or "1.5" in key_values
        assert "2.7" in key_values or "2.7" in key_values
        assert "3.9" in key_values
        assert "4.1" in key_values

    def test_pyspark_parity_schema_verification(self, spark):
        """Test that result schema matches PySpark exactly."""
        df_left = spark.createDataFrame(
            [{"key": 1, "value": "A"}],
            schema=StructType(
                [
                    StructField("key", LongType(), True),
                    StructField("value", StringType(), True),
                ]
            ),
        )
        df_right = spark.createDataFrame([{"key": "2", "value": "B"}])

        result = df_left.union(df_right)

        # Verify schema
        schema = result.schema
        assert len(schema.fields) == 2

        # Both columns should be StringType (PySpark normalizes numeric+string to string)
        for field in schema.fields:
            assert isinstance(field.dataType, StringType), (
                f"Column '{field.name}' should be StringType, got {field.dataType}"
            )
            assert field.nullable is True  # PySpark makes unioned columns nullable

    def test_pyspark_parity_multiple_columns_comprehensive(self, spark):
        """Test union with multiple columns having various type combinations."""
        left_data = [{"col1": 1, "col2": 10.5, "col3": "A", "col4": 100}]
        right_data = [{"col1": "2", "col2": "20.5", "col3": "B", "col4": "200"}]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        result = df_left.union(df_right)
        rows = result.collect()

        assert len(rows) == 2

        # Verify schema - numeric columns should be string, string columns stay string
        schema = result.schema
        col1_field = next((f for f in schema.fields if f.name == "col1"), None)
        col2_field = next((f for f in schema.fields if f.name == "col2"), None)
        col3_field = next((f for f in schema.fields if f.name == "col3"), None)
        col4_field = next((f for f in schema.fields if f.name == "col4"), None)

        assert col1_field is not None
        assert col2_field is not None
        assert col3_field is not None
        assert col4_field is not None

        # All numeric columns (col1, col2, col4) should be StringType after union
        assert isinstance(col1_field.dataType, StringType), "col1 should be StringType"
        assert isinstance(col2_field.dataType, StringType), "col2 should be StringType"
        assert isinstance(col4_field.dataType, StringType), "col4 should be StringType"
        # String column (col3) stays StringType
        assert isinstance(col3_field.dataType, StringType), "col3 should be StringType"

    def test_pyspark_parity_order_preservation(self, spark):
        """Test that union preserves row order (PySpark behavior)."""
        left_data = [{"key": 1, "value": "A"}, {"key": 2, "value": "B"}]
        right_data = [{"key": "3", "value": "X"}, {"key": "4", "value": "Y"}]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        result = df_left.union(df_right)
        rows = result.collect()

        assert len(rows) == 4

        # PySpark preserves order: first all rows from left, then all rows from right
        # Verify first two rows are from left
        row1_key = (
            rows[0]["key"]
            if hasattr(rows[0], "__getitem__")
            else getattr(rows[0], "key", None)
        )
        row1_value = (
            rows[0]["value"]
            if hasattr(rows[0], "__getitem__")
            else getattr(rows[0], "value", None)
        )
        row2_key = (
            rows[1]["key"]
            if hasattr(rows[1], "__getitem__")
            else getattr(rows[1], "key", None)
        )
        row2_value = (
            rows[1]["value"]
            if hasattr(rows[1], "__getitem__")
            else getattr(rows[1], "value", None)
        )

        # Normalize keys to strings for comparison
        assert str(row1_key) == "1" and row1_value == "A", (
            f"First row should be (1, A), got ({row1_key}, {row1_value})"
        )
        assert str(row2_key) == "2" and row2_value == "B", (
            f"Second row should be (2, B), got ({row2_key}, {row2_value})"
        )

        # Verify last two rows are from right
        row3_key = (
            rows[2]["key"]
            if hasattr(rows[2], "__getitem__")
            else getattr(rows[2], "key", None)
        )
        row3_value = (
            rows[2]["value"]
            if hasattr(rows[2], "__getitem__")
            else getattr(rows[2], "value", None)
        )
        row4_key = (
            rows[3]["key"]
            if hasattr(rows[3], "__getitem__")
            else getattr(rows[3], "key", None)
        )
        row4_value = (
            rows[3]["value"]
            if hasattr(rows[3], "__getitem__")
            else getattr(rows[3], "value", None)
        )

        assert str(row3_key) == "3" and row3_value == "X", (
            f"Third row should be (3, X), got ({row3_key}, {row3_value})"
        )
        assert str(row4_key) == "4" and row4_value == "Y", (
            f"Fourth row should be (4, Y), got ({row4_key}, {row4_value})"
        )

    def test_pyspark_parity_union_by_name_with_type_mismatch(self, spark):
        """Test unionByName with type coercion and different column orders."""
        left_data = [{"id": 1, "name": "A", "age": 25}]
        right_data = [{"id": "2", "name": "B", "age": "30"}]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        result = df_left.unionByName(df_right)
        rows = result.collect()

        assert len(rows) == 2

        # Verify schema - id and age should be string (numeric+string -> string)
        schema = result.schema
        id_field = next((f for f in schema.fields if f.name == "id"), None)
        age_field = next((f for f in schema.fields if f.name == "age"), None)

        assert id_field is not None
        assert age_field is not None
        assert isinstance(id_field.dataType, StringType), (
            "id should be StringType after union"
        )
        assert isinstance(age_field.dataType, StringType), (
            "age should be StringType after union"
        )

    def test_pyspark_parity_large_dataset(self, spark):
        """Test union type coercion with larger datasets."""
        # Create larger datasets
        left_data = [{"key": i, "value": f"A{i}"} for i in range(10)]
        right_data = [{"key": str(i + 10), "value": f"X{i + 10}"} for i in range(10)]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        result = df_left.union(df_right)
        rows = result.collect()

        assert len(rows) == 20

        # Verify schema
        schema = result.schema
        key_field = next((f for f in schema.fields if f.name == "key"), None)
        assert key_field is not None
        assert isinstance(key_field.dataType, StringType), (
            "Key should be StringType after union"
        )

        # Verify all keys are present
        key_values = {
            row["key"] if hasattr(row, "__getitem__") else getattr(row, "key", None)
            for row in rows
        }
        for i in range(20):
            assert str(i) in key_values or i in key_values, (
                f"Key {i} should be present in result"
            )

    def test_pyspark_parity_numeric_promotion_order(self, spark):
        """Test that numeric type promotion happens in correct order."""
        # Test Int32 + Int64 -> Int64
        df1 = spark.createDataFrame(
            [{"key": 1}], schema=StructType([StructField("key", IntegerType(), True)])
        )
        df2 = spark.createDataFrame(
            [{"key": 2}], schema=StructType([StructField("key", LongType(), True)])
        )

        result1 = df1.union(df2)
        schema1 = result1.schema
        key_field1 = next((f for f in schema1.fields if f.name == "key"), None)
        assert key_field1 is not None
        assert isinstance(key_field1.dataType, LongType), (
            "Int32 + Int64 should promote to Int64"
        )

        # Test Int + Float -> Float
        df3 = spark.createDataFrame(
            [{"key": 1}], schema=StructType([StructField("key", IntegerType(), True)])
        )
        df4 = spark.createDataFrame(
            [{"key": 2.0}], schema=StructType([StructField("key", FloatType(), True)])
        )

        result2 = df3.union(df4)
        schema2 = result2.schema
        key_field2 = next((f for f in schema2.fields if f.name == "key"), None)
        assert key_field2 is not None
        assert isinstance(key_field2.dataType, (FloatType, DoubleType)), (
            "Int + Float should promote to Float or Double"
        )

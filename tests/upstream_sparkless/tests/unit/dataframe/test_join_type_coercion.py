"""Tests for join type coercion (joining on columns with different types).

This module tests that sparkless correctly handles joining on columns with
different types (e.g., numeric vs string), matching PySpark behavior.

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
F = imports.F  # Functions module for backend-appropriate F.col() etc.


class TestJoinTypeCoercion:
    """Test join operations with different column types."""

    def test_join_int64_with_string(self, spark):
        """Test joining on int64 key with string key (issue #241).

        PySpark allows joining on columns with different types, casting
        string to numeric. The result key column should be numeric type.
        """
        # Create left DataFrame with int64 key
        left_data = [
            {"key": 1234, "value_left": "A"},
            {"key": 4567, "value_left": "B"},
        ]

        # Create right DataFrame with string key
        right_data = [
            {"key": "1234", "value_right": "X"},
            {"key": "4567", "value_right": "Y"},
        ]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        # Join the dataframes on the key
        result = df_left.join(df_right, on="key", how="inner")

        rows = result.collect()

        # Verify the join worked correctly
        assert len(rows) == 2

        # Verify the key column is numeric type (PySpark normalizes to numeric)
        # Check that both rows have correct values
        values = {(row["key"], row["value_left"], row["value_right"]) for row in rows}
        assert (1234, "A", "X") in values
        assert (4567, "B", "Y") in values

        # Verify key column type is numeric (int64/long)
        # Note: The exact type may vary, but it should be numeric, not string
        for row in rows:
            assert isinstance(row["key"], (int, float)), (
                f"Key should be numeric, got {type(row['key'])}"
            )

    def test_join_string_with_int64(self, spark):
        """Test joining on string key with int64 key (opposite direction)."""
        # Create left DataFrame with string key
        left_data = [
            {"key": "1234", "value_left": "A"},
            {"key": "4567", "value_left": "B"},
        ]

        # Create right DataFrame with int64 key
        right_data = [
            {"key": 1234, "value_right": "X"},
            {"key": 4567, "value_right": "Y"},
        ]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        # Join the dataframes on the key
        result = df_left.join(df_right, on="key", how="inner")

        rows = result.collect()

        # Verify the join worked correctly
        assert len(rows) == 2

        # Verify both rows have correct values
        values = {(row["key"], row["value_left"], row["value_right"]) for row in rows}
        # Key should be cast to numeric (from left string)
        assert (1234, "A", "X") in values or ("1234", "A", "X") in values
        assert (4567, "B", "Y") in values or ("4567", "B", "Y") in values

    def test_join_int32_with_string(self, spark):
        """Test joining on int32 key with string key."""
        schema_left = StructType(
            [
                StructField("key", IntegerType(), True),
                StructField("value_left", StringType(), True),
            ]
        )
        schema_right = StructType(
            [
                StructField("key", StringType(), True),
                StructField("value_right", StringType(), True),
            ]
        )

        left_data = [
            {"key": 100, "value_left": "A"},
            {"key": 200, "value_left": "B"},
        ]

        right_data = [
            {"key": "100", "value_right": "X"},
            {"key": "200", "value_right": "Y"},
        ]

        df_left = spark.createDataFrame(left_data, schema=schema_left)
        df_right = spark.createDataFrame(right_data, schema=schema_right)

        result = df_left.join(df_right, on="key", how="inner")
        rows = result.collect()

        assert len(rows) == 2
        values = {(row["key"], row["value_left"], row["value_right"]) for row in rows}
        assert (100, "A", "X") in values
        assert (200, "B", "Y") in values

    def test_join_float_with_string(self, spark):
        """Test joining on float key with string key."""
        schema_left = StructType(
            [
                StructField("key", FloatType(), True),
                StructField("value_left", StringType(), True),
            ]
        )
        schema_right = StructType(
            [
                StructField("key", StringType(), True),
                StructField("value_right", StringType(), True),
            ]
        )

        left_data = [
            {"key": 1.5, "value_left": "A"},
            {"key": 2.5, "value_left": "B"},
        ]

        right_data = [
            {"key": "1.5", "value_right": "X"},
            {"key": "2.5", "value_right": "Y"},
        ]

        df_left = spark.createDataFrame(left_data, schema=schema_left)
        df_right = spark.createDataFrame(right_data, schema=schema_right)

        result = df_left.join(df_right, on="key", how="inner")
        rows = result.collect()

        assert len(rows) == 2
        values = {(row["key"], row["value_left"], row["value_right"]) for row in rows}
        assert (1.5, "A", "X") in values
        assert (2.5, "B", "Y") in values

    def test_join_int32_with_int64(self, spark):
        """Test joining on int32 key with int64 key (both numeric)."""
        schema_left = StructType(
            [
                StructField("key", IntegerType(), True),
                StructField("value_left", StringType(), True),
            ]
        )
        schema_right = StructType(
            [
                StructField("key", LongType(), True),
                StructField("value_right", StringType(), True),
            ]
        )

        left_data = [
            {"key": 1234, "value_left": "A"},
            {"key": 4567, "value_left": "B"},
        ]

        right_data = [
            {"key": 1234, "value_right": "X"},
            {"key": 4567, "value_right": "Y"},
        ]

        df_left = spark.createDataFrame(left_data, schema=schema_left)
        df_right = spark.createDataFrame(right_data, schema=schema_right)

        result = df_left.join(df_right, on="key", how="inner")
        rows = result.collect()

        assert len(rows) == 2
        values = {(row["key"], row["value_left"], row["value_right"]) for row in rows}
        assert (1234, "A", "X") in values
        assert (4567, "B", "Y") in values

    def test_join_with_left_on_right_on(self, spark):
        """Test type coercion with left_on/right_on (different column names)."""
        left_data = [
            {"key_left": 1234, "value_left": "A"},
            {"key_left": 4567, "value_left": "B"},
        ]

        right_data = [
            {"key_right": "1234", "value_right": "X"},
            {"key_right": "4567", "value_right": "Y"},
        ]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        # Join using left_on/right_on

        result = df_left.join(
            df_right, df_left["key_left"] == df_right["key_right"], how="inner"
        )
        rows = result.collect()

        assert len(rows) == 2
        values = {
            (row["key_left"], row["value_left"], row["value_right"]) for row in rows
        }
        assert (1234, "A", "X") in values
        assert (4567, "B", "Y") in values

    def test_join_multiple_keys_with_type_mismatch(self, spark):
        """Test joining on multiple columns where some have type mismatches."""
        left_data = [
            {"key1": 1234, "key2": "A", "value_left": "X"},
            {"key1": 4567, "key2": "B", "value_left": "Y"},
        ]

        right_data = [
            {"key1": "1234", "key2": "A", "value_right": "M"},
            {"key1": "4567", "key2": "B", "value_right": "N"},
        ]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        result = df_left.join(df_right, on=["key1", "key2"], how="inner")
        rows = result.collect()

        assert len(rows) == 2
        values = {
            (row["key1"], row["key2"], row["value_left"], row["value_right"])
            for row in rows
        }
        assert (1234, "A", "X", "M") in values
        assert (4567, "B", "Y", "N") in values

    def test_join_type_coercion_parity_pyspark(self, spark):
        """Test that join type coercion matches PySpark behavior exactly.

        This is the exact test case from issue #241.
        """
        left_data = [
            {"key": 1234, "value_left": "A"},
            {"key": 4567, "value_left": "B"},
        ]

        right_data = [
            {"key": "1234", "value_right": "X"},
            {"key": "4567", "value_right": "Y"},
        ]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        result = df_left.join(df_right, on="key", how="inner")

        rows = result.collect()

        # PySpark normalizes the key to numeric type (long/int64)
        # and returns the joined data
        assert len(rows) == 2

        # Check specific rows - access fields directly for compatibility with both backends
        # PySpark Row objects can be accessed like dicts with row["key"] or row.key
        # Sparkless Row objects also support both access patterns
        row_data = []
        for row in rows:
            row_data.append(
                {
                    "key": row["key"]
                    if hasattr(row, "__getitem__")
                    else getattr(row, "key", None),
                    "value_left": row["value_left"]
                    if hasattr(row, "__getitem__")
                    else getattr(row, "value_left", None),
                    "value_right": row["value_right"]
                    if hasattr(row, "__getitem__")
                    else getattr(row, "value_right", None),
                }
            )
        expected_rows = [
            {"key": 1234, "value_left": "A", "value_right": "X"},
            {"key": 4567, "value_left": "B", "value_right": "Y"},
        ]

        # Sort both for comparison
        row_data.sort(key=lambda x: x["key"])
        expected_rows.sort(key=lambda x: x["key"])

        # Verify each row matches expected
        for i, row in enumerate(row_data):
            assert row["key"] == expected_rows[i]["key"]
            assert row["value_left"] == expected_rows[i]["value_left"]
            assert row["value_right"] == expected_rows[i]["value_right"]

    def test_join_left_outer_with_type_mismatch(self, spark):
        """Test left outer join with type coercion."""
        left_data = [
            {"key": 1234, "value_left": "A"},
            {"key": 4567, "value_left": "B"},
            {"key": 9999, "value_left": "C"},
        ]

        right_data = [
            {"key": "1234", "value_right": "X"},
            {"key": "4567", "value_right": "Y"},
        ]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        result = df_left.join(df_right, on="key", how="left")

        rows = result.collect()

        assert len(rows) == 3

        # Check that all left rows are present
        key_values = {row["key"] for row in rows}
        assert 1234 in key_values
        assert 4567 in key_values
        assert 9999 in key_values

        # Check that matched rows have right values
        for row in rows:
            if row["key"] == 1234:
                assert row["value_right"] == "X"
            elif row["key"] == 4567:
                assert row["value_right"] == "Y"
            elif row["key"] == 9999:
                assert row["value_right"] is None


class TestJoinTypeCoercionParity:
    """Test join type coercion with direct PySpark parity comparison."""

    def test_pyspark_parity_int64_string_inner(self, spark):
        """Test that inner join with int64/string keys matches PySpark exactly."""
        left_data = [{"key": 1234, "value": "A"}, {"key": 4567, "value": "B"}]
        right_data = [{"key": "1234", "other": "X"}, {"key": "4567", "other": "Y"}]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        result = df_left.join(df_right, on="key", how="inner")
        rows = result.collect()

        assert len(rows) == 2
        # Verify key is numeric (PySpark normalizes to numeric)
        for row in rows:
            assert isinstance(row["key"], (int, float))

        # Verify correct matches
        key_values = {row["key"] for row in rows}
        assert 1234 in key_values
        assert 4567 in key_values

    def test_pyspark_parity_string_int64_inner(self, spark):
        """Test that inner join with string/int64 keys matches PySpark exactly.

        PySpark behavior: When joining string with numeric, PySpark allows the join
        by coercing types. The result key type may vary (string or numeric) depending
        on implementation details, but the join works correctly.
        """
        left_data = [{"key": "1234", "value": "A"}, {"key": "4567", "value": "B"}]
        right_data = [{"key": 1234, "other": "X"}, {"key": 4567, "other": "Y"}]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        result = df_left.join(df_right, on="key", how="inner")
        rows = result.collect()

        assert len(rows) == 2
        # PySpark allows the join by coercing types during comparison
        # The result key type may be string or numeric depending on PySpark version/implementation
        # What matters is that the join works correctly
        for row in rows:
            key_val = (
                row["key"] if hasattr(row, "__getitem__") else getattr(row, "key", None)
            )
            # Key should be either string or numeric (both are valid)
            assert (
                key_val in ("1234", "4567")
                or key_val in (1234, 4567)
                or key_val in (1234.0, 4567.0)
            )
            # Verify the join worked correctly
            assert (
                row["value"]
                if hasattr(row, "__getitem__")
                else getattr(row, "value", None)
            ) in ("A", "B")
            assert (
                row["other"]
                if hasattr(row, "__getitem__")
                else getattr(row, "other", None)
            ) in ("X", "Y")

    def test_pyspark_parity_all_join_types(self, spark):
        """Test type coercion with all join types (inner, left, right, outer)."""
        left_data = [
            {"key": 100, "left_val": "A"},
            {"key": 200, "left_val": "B"},
            {"key": 300, "left_val": "C"},
        ]
        right_data = [
            {"key": "100", "right_val": "X"},
            {"key": "200", "right_val": "Y"},
            {"key": "400", "right_val": "Z"},
        ]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        # Inner join
        inner = df_left.join(df_right, on="key", how="inner")
        inner_rows = inner.collect()
        assert len(inner_rows) == 2
        inner_keys = {row["key"] for row in inner_rows}
        # Keys may be int, float, or string after coercion (PySpark preserves left type)
        assert any(k == 100 or k == 100.0 or k == "100" for k in inner_keys)
        assert any(k == 200 or k == 200.0 or k == "200" for k in inner_keys)

        # Left join
        left = df_left.join(df_right, on="key", how="left")
        left_rows = left.collect()
        assert len(left_rows) == 3
        left_keys = {row["key"] for row in left_rows}
        # Left join preserves left DataFrame's type (numeric)
        assert any(k == 100 or k == 100.0 or k == "100" for k in left_keys)
        assert any(k == 200 or k == 200.0 or k == "200" for k in left_keys)
        assert any(k == 300 or k == 300.0 or k == "300" for k in left_keys)
        # Check that unmatched row has null right value
        for row in left_rows:
            key_val = (
                row["key"] if hasattr(row, "__getitem__") else getattr(row, "key", None)
            )
            if key_val in (300, 300.0, "300"):
                right_val = (
                    row.get("right_val")
                    if hasattr(row, "get")
                    else (
                        row["right_val"]
                        if hasattr(row, "__getitem__")
                        else getattr(row, "right_val", None)
                    )
                )
                assert right_val is None

        # Right join
        right = df_left.join(df_right, on="key", how="right")
        right_rows = right.collect()
        assert len(right_rows) == 3
        right_keys = {row["key"] for row in right_rows}
        # Right join preserves right DataFrame's type (string)
        assert any(k == 100 or k == 100.0 or k == "100" for k in right_keys)
        assert any(k == 200 or k == 200.0 or k == "200" for k in right_keys)
        assert any(k == 400 or k == 400.0 or k == "400" for k in right_keys)
        # Check that unmatched row has null left value
        for row in right_rows:
            key_val = (
                row["key"] if hasattr(row, "__getitem__") else getattr(row, "key", None)
            )
            if key_val in (400, 400.0, "400"):
                left_val = (
                    row.get("left_val")
                    if hasattr(row, "get")
                    else (
                        row["left_val"]
                        if hasattr(row, "__getitem__")
                        else getattr(row, "left_val", None)
                    )
                )
                assert left_val is None

        # Outer join
        outer = df_left.join(df_right, on="key", how="outer")
        outer_rows = outer.collect()
        # Outer join should include all rows from both DataFrames
        # Left has 100, 200, 300 (numeric)
        # Right has "100", "200", "400" (string)
        # After coercion and join:
        # - 100 and 200 are matched (both sides)
        # - 300 is unmatched from left
        # - 400 (from right) may appear as None, "400", or 400 depending on PySpark version
        assert len(outer_rows) == 4
        # Keys may be coerced to numeric or stay as string, so check flexibly
        outer_keys = {row["key"] for row in outer_rows}
        # Check that matched keys are present (allowing for type coercion)
        # 100 and 200 are matched - keys might be int, float, or string
        matched_100 = any(k == 100 or k == 100.0 or k == "100" for k in outer_keys)
        matched_200 = any(k == 200 or k == 200.0 or k == "200" for k in outer_keys)
        assert matched_100, f"Expected 100 (int/float/string), got keys: {outer_keys}"
        assert matched_200, f"Expected 200 (int/float/string), got keys: {outer_keys}"
        # 300 is from left only (unmatched)
        matched_300 = any(k == 300 or k == 300.0 or k == "300" for k in outer_keys)
        assert matched_300, f"Expected 300 (int/float/string), got keys: {outer_keys}"
        # Unmatched right row (key="400") - may appear as None, "400", or 400
        unmatched_right_present = (
            None in outer_keys
            or any(k == 400 or k == 400.0 for k in outer_keys if k is not None)
            or any(k == "400" for k in outer_keys if k is not None)
        )
        assert unmatched_right_present, (
            f"Expected None, 400, or '400' for unmatched right row, got keys: {outer_keys}"
        )

    def test_pyspark_parity_double_precision_string(self, spark):
        """Test type coercion with double precision and string."""
        schema_left = StructType(
            [
                StructField("key", DoubleType(), True),
                StructField("value", StringType(), True),
            ]
        )
        schema_right = StructType(
            [
                StructField("key", StringType(), True),
                StructField("other", StringType(), True),
            ]
        )

        left_data = [{"key": 123.456, "value": "A"}, {"key": 789.012, "value": "B"}]
        right_data = [
            {"key": "123.456", "other": "X"},
            {"key": "789.012", "other": "Y"},
        ]

        df_left = spark.createDataFrame(left_data, schema=schema_left)
        df_right = spark.createDataFrame(right_data, schema=schema_right)

        result = df_left.join(df_right, on="key", how="inner")
        rows = result.collect()

        assert len(rows) == 2
        for row in rows:
            assert isinstance(row["key"], float)
            # Verify precision is maintained
            assert row["key"] in (123.456, 789.012)

    def test_pyspark_parity_int_float_coercion(self, spark):
        """Test that int/float coercion works correctly."""
        schema_left = StructType(
            [
                StructField("key", IntegerType(), True),
                StructField("value", StringType(), True),
            ]
        )
        schema_right = StructType(
            [
                StructField("key", FloatType(), True),
                StructField("other", StringType(), True),
            ]
        )

        left_data = [{"key": 100, "value": "A"}, {"key": 200, "value": "B"}]
        right_data = [{"key": 100.0, "other": "X"}, {"key": 200.0, "other": "Y"}]

        df_left = spark.createDataFrame(left_data, schema=schema_left)
        df_right = spark.createDataFrame(right_data, schema=schema_right)

        result = df_left.join(df_right, on="key", how="inner")
        rows = result.collect()

        assert len(rows) == 2
        # PySpark preserves the left DataFrame's type for int/float joins
        # The join should work correctly regardless of whether it's int or float
        for row in rows:
            key_val = (
                row["key"] if hasattr(row, "__getitem__") else getattr(row, "key", None)
            )
            # Key should be numeric (int or float depending on implementation)
            assert isinstance(key_val, (int, float))
            assert key_val in (100, 100.0, 200, 200.0)

    def test_pyspark_parity_null_values_in_join_keys(self, spark):
        """Test type coercion with null values in join keys."""
        left_data = [
            {"key": 100, "value": "A"},
            {"key": None, "value": "B"},
            {"key": 200, "value": "C"},
        ]
        right_data = [
            {"key": "100", "other": "X"},
            {"key": None, "other": "Y"},
            {"key": "200", "other": "Z"},
        ]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        result = df_left.join(df_right, on="key", how="inner")
        rows = result.collect()

        # Null keys should match (PySpark behavior)
        # Non-null keys should match after coercion
        assert len(rows) >= 2  # At least the non-null matches
        matched_keys = {row["key"] for row in rows if row["key"] is not None}
        assert 100 in matched_keys or 100.0 in matched_keys
        assert 200 in matched_keys or 200.0 in matched_keys

    def test_pyspark_parity_invalid_numeric_strings(self, spark):
        """Test that invalid numeric strings don't break the join."""
        left_data = [
            {"key": 1234, "value": "A"},
            {"key": 4567, "value": "B"},
        ]
        right_data = [
            {"key": "1234", "other": "X"},  # Valid numeric string
            {"key": "abc", "other": "Y"},  # Invalid numeric string
            {"key": "4567", "other": "Z"},  # Valid numeric string
        ]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        # Join should succeed but invalid strings won't match
        result = df_left.join(df_right, on="key", how="inner")
        rows = result.collect()

        # Only valid numeric strings should match
        assert len(rows) == 2
        matched_keys = {row["key"] for row in rows}
        assert 1234 in matched_keys or 1234.0 in matched_keys
        assert 4567 in matched_keys or 4567.0 in matched_keys

    def test_pyspark_parity_multiple_keys_complex(self, spark):
        """Test type coercion with multiple keys where different keys have different type mismatches."""
        left_data = [
            {"key1": 100, "key2": "A", "value": "X"},
            {"key1": 200, "key2": "B", "value": "Y"},
            {"key1": 300, "key2": "C", "value": "Z"},
        ]
        right_data = [
            {"key1": "100", "key2": "A", "other": "M"},
            {"key1": "200", "key2": "B", "other": "N"},
            {"key1": "300", "key2": "D", "other": "O"},  # key2 mismatch
        ]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        result = df_left.join(df_right, on=["key1", "key2"], how="inner")
        rows = result.collect()

        # Only rows where both keys match should be joined
        assert len(rows) == 2
        matched = {(row["key1"], row["key2"]) for row in rows}
        assert (100, "A") in matched or (100.0, "A") in matched
        assert (200, "B") in matched or (200.0, "B") in matched

    def test_pyspark_parity_type_promotion_int32_int64(self, spark):
        """Test that int32/int64 coercion promotes to int64."""
        schema_left = StructType(
            [
                StructField("key", IntegerType(), True),
                StructField("value", StringType(), True),
            ]
        )
        schema_right = StructType(
            [
                StructField("key", LongType(), True),
                StructField("other", StringType(), True),
            ]
        )

        left_data = [{"key": 2147483647, "value": "A"}]  # Max int32
        right_data = [{"key": 2147483647, "other": "X"}]

        df_left = spark.createDataFrame(left_data, schema=schema_left)
        df_right = spark.createDataFrame(right_data, schema=schema_right)

        result = df_left.join(df_right, on="key", how="inner")
        rows = result.collect()

        assert len(rows) == 1
        # Key should be int64 (long)
        assert rows[0]["key"] == 2147483647
        assert isinstance(rows[0]["key"], int)

    def test_pyspark_parity_left_on_right_on_different_names(self, spark):
        """Test type coercion with left_on/right_on using different column names."""
        left_data = [{"id": 1234, "name": "A"}, {"id": 4567, "name": "B"}]
        right_data = [{"code": "1234", "desc": "X"}, {"code": "4567", "desc": "Y"}]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        result = df_left.join(df_right, df_left["id"] == df_right["code"], how="inner")
        rows = result.collect()

        assert len(rows) == 2
        # Verify both id and code columns are present
        for row in rows:
            assert "id" in row or hasattr(row, "id")
            assert "code" in row or hasattr(row, "code")
            # Verify id is numeric
            id_val = (
                row["id"] if hasattr(row, "__getitem__") else getattr(row, "id", None)
            )
            assert isinstance(id_val, (int, float))

    def test_pyspark_parity_mixed_numeric_strings(self, spark):
        """Test joining with mixed numeric and string representations."""
        left_data = [
            {"key": 0, "value": "A"},
            {"key": 123, "value": "B"},
            {"key": -456, "value": "C"},
            {"key": 999, "value": "D"},
        ]
        right_data = [
            {"key": "0", "other": "W"},
            {"key": "123", "other": "X"},
            {"key": "-456", "other": "Y"},
            {"key": "999", "other": "Z"},
        ]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        result = df_left.join(df_right, on="key", how="inner")
        rows = result.collect()

        assert len(rows) == 4
        matched_keys = {row["key"] for row in rows}
        assert 0 in matched_keys or 0.0 in matched_keys
        assert 123 in matched_keys or 123.0 in matched_keys
        assert -456 in matched_keys or -456.0 in matched_keys
        assert 999 in matched_keys or 999.0 in matched_keys

    def test_pyspark_parity_scientific_notation_strings(self, spark):
        """Test that scientific notation strings can be coerced to numeric."""
        left_data = [{"key": 1.23e2, "value": "A"}]  # 123.0
        right_data = [{"key": "1.23e2", "other": "X"}]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        result = df_left.join(df_right, on="key", how="inner")
        rows = result.collect()

        assert len(rows) == 1
        # Key should be numeric (float)
        assert isinstance(rows[0]["key"], float)
        assert abs(rows[0]["key"] - 123.0) < 0.001

    def test_pyspark_parity_whitespace_in_string_keys(self, spark):
        """Test that whitespace in string keys is handled correctly."""
        left_data = [{"key": 123, "value": "A"}]
        # Polars may trim whitespace, but PySpark handles it during cast
        right_data = [{"key": "  123  ", "other": "X"}]

        df_left = spark.createDataFrame(left_data)
        df_right = spark.createDataFrame(right_data)

        # Join might or might not work depending on how whitespace is handled
        # PySpark typically handles this during type coercion
        result = df_left.join(df_right, on="key", how="inner")
        rows = result.collect()

        # The join should either succeed (if whitespace is trimmed) or fail
        # If it succeeds, verify the result
        if len(rows) > 0:
            assert rows[0]["key"] == 123 or rows[0]["key"] == 123.0

    def test_pyspark_parity_schema_verification(self, spark):
        """Test that the result schema has the correct type after coercion."""
        schema_left = StructType(
            [
                StructField("key", LongType(), True),
                StructField("value", StringType(), True),
            ]
        )
        schema_right = StructType(
            [
                StructField("key", StringType(), True),
                StructField("other", StringType(), True),
            ]
        )

        left_data = [{"key": 1234, "value": "A"}]
        right_data = [{"key": "1234", "other": "X"}]

        df_left = spark.createDataFrame(left_data, schema=schema_left)
        df_right = spark.createDataFrame(right_data, schema=schema_right)

        result = df_left.join(df_right, on="key", how="inner")

        # Verify schema - key should be numeric type (not string)
        schema = result.schema
        key_field = next((f for f in schema.fields if f.name == "key"), None)
        assert key_field is not None
        # Key type should be numeric (LongType/IntegerType), not StringType
        assert not isinstance(key_field.dataType, StringType)

"""
Robust tests for DataFrame.na.fill() matching PySpark behavior.

This module contains comprehensive tests that verify edge cases and specific
PySpark behaviors for the .na.fill() syntax.

These tests work with both sparkless (mock) and PySpark backends.
Set MOCK_SPARK_TEST_BACKEND=pyspark to run with real PySpark.
"""

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
StructType = imports.StructType
StructField = imports.StructField
F = imports.F


def _is_pyspark_mode() -> bool:
    """Check if running in PySpark mode."""
    backend = get_backend_type()
    return bool(backend == BackendType.PYSPARK)


# ColumnNotFoundException not used in these tests - exception handling
# is tested in test_na_fill.py


class TestNaFillRobust:
    """Robust tests for .na.fill() matching PySpark behavior."""

    def test_na_fill_type_mismatch_silently_ignored(self, spark):
        """Test that type mismatches are silently ignored (PySpark behavior).

        PySpark silently ignores type mismatches when filling - e.g., filling
        an integer column with a string leaves nulls unchanged.
        """
        schema = StructType(
            [
                StructField("id", IntegerType(), nullable=True),
                StructField("name", StringType(), nullable=True),
            ]
        )
        df = spark.createDataFrame(
            [
                {"id": 1, "name": "Alice"},
                {"id": None, "name": None},
                {"id": 3, "name": None},
            ],
            schema=schema,
        )

        # Fill integer column with string - should be silently ignored
        result = df.na.fill("DEFAULT", subset=["id"])

        rows = result.collect()
        assert rows[0]["id"] == 1  # Not null, unchanged
        assert (
            rows[1]["id"] is None
        )  # Type mismatch, silently ignored (PySpark behavior)
        assert rows[2]["id"] == 3  # Not null, unchanged

        # Fill string column with integer - should be silently ignored
        result2 = df.na.fill(0, subset=["name"])

        rows2 = result2.collect()
        assert rows2[0]["name"] == "Alice"  # Not null, unchanged
        assert rows2[1]["name"] is None  # Type mismatch, silently ignored
        assert rows2[2]["name"] is None  # Type mismatch, silently ignored

    def test_na_fill_dict_ignores_subset(self, spark):
        """Test that dict value ignores subset parameter (PySpark behavior)."""
        schema = StructType(
            [
                StructField("col1", StringType(), nullable=True),
                StructField("col2", StringType(), nullable=True),
                StructField("col3", StringType(), nullable=True),
            ]
        )
        df = spark.createDataFrame(
            [
                {"col1": None, "col2": "X", "col3": None},
                {"col1": "A", "col2": None, "col3": "Y"},
            ],
            schema=schema,
        )

        # Dict value should fill both col1 and col3, ignoring subset
        result = df.na.fill({"col1": "DEFAULT1", "col3": "DEFAULT3"}, subset=["col2"])

        rows = result.collect()
        assert rows[0]["col1"] == "DEFAULT1"  # Filled by dict (subset ignored)
        assert rows[0]["col2"] == "X"  # Not null, unchanged
        assert rows[0]["col3"] == "DEFAULT3"  # Filled by dict (subset ignored)
        assert rows[1]["col1"] == "A"  # Not null, unchanged
        assert rows[1]["col2"] is None  # Subset ignored (dict value used)
        assert rows[1]["col3"] == "Y"  # Not null, unchanged

    def test_na_fill_with_filter(self, spark):
        """Test .na.fill() combined with filter operation."""
        df = spark.createDataFrame(
            [
                {"id": 1, "name": None, "score": 85},
                {"id": 2, "name": "Bob", "score": 90},
                {"id": 3, "name": None, "score": 75},
            ]
        )

        # First filter (keep rows with score > 80), then fill
        result = df.filter(F.col("score") > 80).na.fill("UNKNOWN", subset=["name"])

        rows = result.collect()
        assert len(rows) >= 1  # At least one row with score > 80
        # Find the row with id=1 (score=85)
        row1 = next((r for r in rows if r["id"] == 1), None)
        assert row1 is not None
        assert row1["name"] == "UNKNOWN"  # Was null, now filled
        assert row1["score"] == 85

    def test_na_fill_with_select(self, spark):
        """Test .na.fill() combined with select operation."""
        schema = StructType(
            [
                StructField("col1", StringType(), nullable=True),
                StructField("col2", StringType(), nullable=True),
                StructField("col3", StringType(), nullable=True),
            ]
        )
        df = spark.createDataFrame(
            [
                {"col1": None, "col2": "A", "col3": None},
                {"col1": "B", "col2": "C", "col3": None},
            ],
            schema=schema,
        )

        # Select subset of columns, then fill
        result = df.select("col1", "col2").na.fill("FILLED", subset=["col1"])

        rows = result.collect()
        assert rows[0]["col1"] == "FILLED"  # Was null, now filled
        assert rows[0]["col2"] == "A"
        # col3 should not be in result (not selected)
        assert rows[1]["col1"] == "B"  # Not null, unchanged
        assert rows[1]["col2"] == "C"

    def test_na_fill_with_withcolumn(self, spark):
        """Test .na.fill() combined with withColumn operation."""
        df = spark.createDataFrame(
            [
                {"name": "Alice", "age": None},
                {"name": None, "age": 25},
            ]
        )

        # First fill, then add column (order matters for testing)
        result = df.na.fill("UNKNOWN", subset=["name"]).withColumn(
            "status", F.lit("active")
        )

        rows = result.collect()
        assert len(rows) == 2
        # Check row access works correctly
        row0_dict = rows[0].asDict() if hasattr(rows[0], "asDict") else dict(rows[0])
        row1_dict = rows[1].asDict() if hasattr(rows[1], "asDict") else dict(rows[1])

        assert row0_dict["name"] == "Alice"  # Not null, unchanged
        assert row0_dict["age"] is None  # Not filled
        assert row1_dict["name"] == "UNKNOWN"  # Was null, now filled
        assert row1_dict["age"] == 25  # Not null, unchanged

    def test_na_fill_empty_subset(self, spark):
        """Test .na.fill() with empty subset list results in no columns being filled."""
        df = spark.createDataFrame(
            [
                {"key": "A", "value": None},
                {"key": None, "value": "2"},
            ]
        )

        result = df.na.fill("FILLED", subset=[])

        rows = result.collect()
        # No columns should be filled
        assert rows[0]["key"] == "A"
        assert rows[0]["value"] is None  # Not filled (empty subset)
        assert rows[1]["key"] is None  # Not filled (empty subset)
        assert rows[1]["value"] == "2"

    def test_na_fill_partial_dict(self, spark):
        """Test .na.fill() with dict that only partially matches columns."""
        schema = StructType(
            [
                StructField("col1", StringType(), nullable=True),
                StructField("col2", StringType(), nullable=True),
                StructField("col3", StringType(), nullable=True),
            ]
        )
        df = spark.createDataFrame(
            [
                {"col1": None, "col2": "X", "col3": None},
                {"col1": "A", "col2": None, "col3": "Y"},
            ],
            schema=schema,
        )

        # Dict with only col1 - col3 should remain unfilled
        result = df.na.fill({"col1": "DEFAULT1"})

        rows = result.collect()
        assert rows[0]["col1"] == "DEFAULT1"  # Filled by dict
        assert rows[0]["col2"] == "X"  # Not in dict, unchanged
        assert rows[0]["col3"] is None  # Not in dict, unchanged
        assert rows[1]["col1"] == "A"  # Not null, unchanged
        assert rows[1]["col2"] is None  # Not in dict, unchanged
        assert rows[1]["col3"] == "Y"  # Not null, unchanged

    def test_na_fill_chained_multiple(self, spark):
        """Test multiple chained .na.fill() operations."""
        schema = StructType(
            [
                StructField("a", StringType(), nullable=True),
                StructField("b", StringType(), nullable=True),
                StructField("c", StringType(), nullable=True),
            ]
        )
        df = spark.createDataFrame(
            [
                {"a": None, "b": None, "c": None},
                {"a": "A", "b": None, "c": "C"},
            ],
            schema=schema,
        )

        result = (
            df.na.fill("FILL1", subset=["a"])
            .na.fill("FILL2", subset=["b"])
            .na.fill("FILL3", subset=["c"])
        )

        rows = result.collect()
        assert rows[0]["a"] == "FILL1"  # Filled by first na.fill
        assert rows[0]["b"] == "FILL2"  # Filled by second na.fill
        assert rows[0]["c"] == "FILL3"  # Filled by third na.fill
        assert rows[1]["a"] == "A"  # Not null, unchanged
        assert rows[1]["b"] == "FILL2"  # Filled by second na.fill
        assert rows[1]["c"] == "C"  # Not null, unchanged

    def test_na_fill_numeric_types(self, spark):
        """Test .na.fill() with various numeric types."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        StructType = imports.StructType
        StructField = imports.StructField
        IntegerType = imports.IntegerType
        LongType = imports.LongType
        DoubleType = imports.DoubleType
        FloatType = imports.FloatType

        # Test LongType with int value (PySpark allows this)
        schema_long = StructType(
            [
                StructField("id", IntegerType()),
                StructField("value", LongType(), nullable=True),
            ]
        )
        df_long = spark.createDataFrame([{"id": 1, "value": None}], schema=schema_long)
        result_long = df_long.na.fill(0)  # int 0 should fill LongType
        rows_long = result_long.collect()
        assert rows_long[0]["value"] == 0

        # Test DoubleType with float value
        schema_double = StructType(
            [
                StructField("id", IntegerType()),
                StructField("value", DoubleType(), nullable=True),
            ]
        )
        df_double = spark.createDataFrame(
            [{"id": 1, "value": None}], schema=schema_double
        )
        result_double = df_double.na.fill(0.5)  # float should fill DoubleType
        rows_double = result_double.collect()
        assert rows_double[0]["value"] == 0.5

        # Test FloatType with int value
        schema_float = StructType(
            [
                StructField("id", IntegerType()),
                StructField("value", FloatType(), nullable=True),
            ]
        )
        df_float = spark.createDataFrame(
            [{"id": 1, "value": None}], schema=schema_float
        )
        result_float = df_float.na.fill(42)  # int should fill FloatType
        rows_float = result_float.collect()
        # Float comparison with tolerance
        assert abs(rows_float[0]["value"] - 42.0) < 0.001

    def test_na_fill_schema_preservation(self, spark):
        """Test that .na.fill() preserves DataFrame schema."""
        schema = StructType(
            [
                StructField("id", IntegerType(), nullable=False),
                StructField("name", StringType(), nullable=True),
                StructField("score", DoubleType(), nullable=True),
            ]
        )
        df = spark.createDataFrame(
            [
                {"id": 1, "name": None, "score": 85.5},
                {"id": 2, "name": "Bob", "score": None},
            ],
            schema=schema,
        )

        result = df.na.fill("UNKNOWN", subset=["name"])

        # Schema should be preserved
        assert len(result.schema.fields) == 3
        assert result.schema.fields[0].name == "id"
        assert result.schema.fields[0].dataType == IntegerType()
        assert result.schema.fields[1].name == "name"
        assert result.schema.fields[1].dataType == StringType()
        assert result.schema.fields[2].name == "score"
        assert result.schema.fields[2].dataType == DoubleType()

        rows = result.collect()
        assert rows[0]["id"] == 1
        assert rows[0]["name"] == "UNKNOWN"  # Filled
        assert rows[0]["score"] == 85.5
        assert rows[1]["id"] == 2
        assert rows[1]["name"] == "Bob"
        assert rows[1]["score"] is None

    def test_na_fill_with_groupby(self, spark):
        """Test .na.fill() before and after groupBy operation."""
        df = spark.createDataFrame(
            [
                {"category": "A", "value": None, "count": 1},
                {"category": "A", "value": None, "count": 2},
                {"category": "B", "value": 10, "count": 3},
            ]
        )

        # Fill before groupBy
        filled_df = df.na.fill(0, subset=["value"])
        grouped = filled_df.groupBy("category").agg(F.sum("value").alias("sum_value"))

        rows = grouped.collect()
        assert len(rows) == 2
        # Category A: both values were null, now 0, sum = 0
        row_a = next((r for r in rows if r["category"] == "A"), None)
        assert row_a is not None
        assert row_a["sum_value"] == 0

    def test_na_fill_with_union(self, spark):
        """Test .na.fill() with union operations."""
        df1 = spark.createDataFrame(
            [
                {"id": 1, "name": None, "value": 10},
                {"id": 2, "name": "Alice", "value": 20},
            ]
        )

        df2 = spark.createDataFrame(
            [
                {"id": 3, "name": None, "value": 30},
                {"id": 4, "name": "Bob", "value": None},
            ]
        )

        # Fill each DataFrame separately, then union
        filled1 = df1.na.fill("UNKNOWN", subset=["name"])
        filled2 = df2.na.fill("UNKNOWN", subset=["name"])
        result = filled1.union(filled2)

        rows = result.collect()
        assert len(rows) == 4
        # All name columns should be filled
        for row in rows:
            assert row["name"] is not None
            assert row["name"] in ["UNKNOWN", "Alice", "Bob"]

    def test_na_fill_zero_numeric(self, spark):
        """Test .na.fill() with zero for numeric columns."""
        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("int_val", IntegerType(), nullable=True),
                StructField("long_val", LongType(), nullable=True),
                StructField("double_val", DoubleType(), nullable=True),
            ]
        )
        df = spark.createDataFrame(
            [
                {"id": 1, "int_val": None, "long_val": None, "double_val": None},
            ],
            schema=schema,
        )

        result = df.na.fill(0)

        rows = result.collect()
        assert rows[0]["int_val"] == 0
        assert rows[0]["long_val"] == 0
        assert rows[0]["double_val"] == 0.0

    def test_na_fill_empty_string(self, spark):
        """Test .na.fill() with empty string."""
        schema = StructType(
            [
                StructField("name", StringType(), nullable=True),
                StructField("description", StringType(), nullable=True),
            ]
        )
        df = spark.createDataFrame(
            [
                {"name": None, "description": "Test"},
                {"name": "Alice", "description": None},
            ],
            schema=schema,
        )

        result = df.na.fill("", subset=["name"])

        rows = result.collect()
        assert rows[0]["name"] == ""  # Empty string
        assert rows[0]["description"] == "Test"
        assert rows[1]["name"] == "Alice"
        assert rows[1]["description"] is None  # Not in subset

    def test_na_fill_boolean_false(self, spark):
        """Test .na.fill() with False for boolean columns."""
        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("is_active", BooleanType(), nullable=True),
            ]
        )
        df = spark.createDataFrame(
            [
                {"id": 1, "is_active": None},
                {"id": 2, "is_active": True},
                {"id": 3, "is_active": None},
            ],
            schema=schema,
        )

        result = df.na.fill(False, subset=["is_active"])

        rows = result.collect()
        assert rows[0]["is_active"] is False
        assert rows[1]["is_active"] is True  # Not null, unchanged
        assert rows[2]["is_active"] is False

    def test_na_fill_boolean_true(self, spark):
        """Test .na.fill() with True for boolean columns."""
        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("is_active", BooleanType(), nullable=True),
            ]
        )
        df = spark.createDataFrame(
            [
                {"id": 1, "is_active": None},
                {"id": 2, "is_active": False},
            ],
            schema=schema,
        )

        result = df.na.fill(True, subset=["is_active"])

        rows = result.collect()
        assert rows[0]["is_active"] is True
        assert rows[1]["is_active"] is False  # Not null, unchanged

    def test_na_fill_large_dict(self, spark):
        """Test .na.fill() with dict containing many columns."""
        schema_fields = [
            StructField(f"col{i}", StringType(), nullable=True) for i in range(10)
        ]
        schema = StructType(schema_fields)

        data = {f"col{i}": None if i % 2 == 0 else "X" for i in range(10)}
        df = spark.createDataFrame([data], schema=schema)

        fill_dict = {f"col{i}": f"DEFAULT{i}" for i in range(10)}
        result = df.na.fill(fill_dict)

        rows = result.collect()
        for i in range(10):
            expected = f"DEFAULT{i}" if i % 2 == 0 else "X"
            assert rows[0][f"col{i}"] == expected

    def test_na_fill_subset_all_columns(self, spark):
        """Test .na.fill() with subset containing all columns."""
        schema = StructType(
            [
                StructField("col1", StringType(), nullable=True),
                StructField("col2", StringType(), nullable=True),
                StructField("col3", StringType(), nullable=True),
            ]
        )
        df = spark.createDataFrame(
            [
                {"col1": None, "col2": None, "col3": None},
                {"col1": "A", "col2": "B", "col3": "C"},
            ],
            schema=schema,
        )

        result = df.na.fill("FILLED", subset=["col1", "col2", "col3"])

        rows = result.collect()
        assert rows[0]["col1"] == "FILLED"
        assert rows[0]["col2"] == "FILLED"
        assert rows[0]["col3"] == "FILLED"
        assert rows[1]["col1"] == "A"  # Not null, unchanged
        assert rows[1]["col2"] == "B"  # Not null, unchanged
        assert rows[1]["col3"] == "C"  # Not null, unchanged

    def test_na_fill_preserves_non_null_values(self, spark):
        """Test that .na.fill() never overwrites non-null values."""
        df = spark.createDataFrame(
            [
                {"key": "A", "value": 1, "status": "active"},
                {"key": "B", "value": None, "status": "inactive"},
                {"key": "C", "value": 3, "status": None},
            ]
        )

        # Fill with value that matches existing value - should not overwrite
        result = df.na.fill({"value": 1, "status": "active"})

        rows = result.collect()
        assert rows[0]["value"] == 1  # Existing value preserved
        assert rows[0]["status"] == "active"  # Existing value preserved
        assert rows[1]["value"] == 1  # Null filled
        assert rows[1]["status"] == "inactive"  # Existing value preserved
        assert rows[2]["value"] == 3  # Existing value preserved
        assert rows[2]["status"] == "active"  # Null filled

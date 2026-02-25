"""
Tests for CaseWhen.cast() and WindowFunction.cast() methods.

This module tests that sparkless correctly supports casting CaseWhen and
WindowFunction expressions, matching PySpark behavior.

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
FloatType = imports.FloatType
StructType = imports.StructType
StructField = imports.StructField
F = imports.F
Window = imports.Window


def _is_pyspark_mode() -> bool:
    """Check if running in PySpark mode."""
    backend = get_backend_type()
    return backend == BackendType.PYSPARK


class TestCaseWhenCast:
    """Test CaseWhen.cast() method."""

    def test_casewhen_cast_to_long_issue_243(self, spark):
        """Test CaseWhen.cast() to long (exact scenario from issue #243)."""
        df = spark.createDataFrame(
            [
                {"value": "A"},
                {"value": "B"},
            ]
        )

        # This is the exact code from issue #243
        result = df.withColumn(
            "when_result",
            F.when(F.col("value") == "A", F.lit(100))
            .otherwise(F.lit(200))
            .cast("long"),
        )

        rows = result.collect()

        assert len(rows) == 2
        # Verify the cast worked - values should be long/integer
        assert rows[0]["when_result"] == 100
        assert rows[1]["when_result"] == 200

        # Verify schema shows long type
        schema = result.schema
        # Use PySpark-compatible field access
        when_result_field = next(
            (f for f in schema.fields if f.name == "when_result"), None
        )
        assert when_result_field is not None
        assert isinstance(when_result_field.dataType, LongType), (
            f"Expected LongType, got {type(when_result_field.dataType)}"
        )

    def test_casewhen_cast_to_string(self, spark):
        """Test CaseWhen.cast() to string."""
        df = spark.createDataFrame(
            [
                {"value": "A"},
                {"value": "B"},
            ]
        )

        result = df.withColumn(
            "when_result",
            F.when(F.col("value") == "A", F.lit(100))
            .otherwise(F.lit(200))
            .cast("string"),
        )

        rows = result.collect()

        assert len(rows) == 2
        # Values should be strings
        assert rows[0]["when_result"] == "100"
        assert rows[1]["when_result"] == "200"

        # Verify schema shows string type
        schema = result.schema
        # Use PySpark-compatible field access
        when_result_field = next(
            (f for f in schema.fields if f.name == "when_result"), None
        )
        assert when_result_field is not None
        assert isinstance(when_result_field.dataType, StringType)

    def test_casewhen_cast_to_int(self, spark):
        """Test CaseWhen.cast() to int."""
        df = spark.createDataFrame(
            [
                {"value": "A"},
                {"value": "B"},
            ]
        )

        result = df.withColumn(
            "when_result",
            F.when(F.col("value") == "A", F.lit(100.5))
            .otherwise(F.lit(200.7))
            .cast("int"),
        )

        rows = result.collect()

        assert len(rows) == 2
        # Values should be integers (truncated)
        assert rows[0]["when_result"] == 100
        assert rows[1]["when_result"] == 200

    def test_casewhen_cast_to_double(self, spark):
        """Test CaseWhen.cast() to double."""
        df = spark.createDataFrame(
            [
                {"value": "A"},
                {"value": "B"},
            ]
        )

        result = df.withColumn(
            "when_result",
            F.when(F.col("value") == "A", F.lit(100))
            .otherwise(F.lit(200))
            .cast("double"),
        )

        rows = result.collect()

        assert len(rows) == 2
        # Values should be doubles
        assert rows[0]["when_result"] == 100.0
        assert rows[1]["when_result"] == 200.0

        # Verify schema shows double type
        schema = result.schema
        # Use PySpark-compatible field access
        when_result_field = next(
            (f for f in schema.fields if f.name == "when_result"), None
        )
        assert when_result_field is not None
        assert isinstance(when_result_field.dataType, DoubleType)

    def test_casewhen_cast_with_multiple_when(self, spark):
        """Test CaseWhen.cast() with multiple when conditions."""
        df = spark.createDataFrame(
            [
                {"value": 1},
                {"value": 2},
                {"value": 3},
            ]
        )

        result = df.withColumn(
            "category",
            F.when(F.col("value") == 1, "low")
            .when(F.col("value") == 2, "medium")
            .otherwise("high")
            .cast("string"),
        )

        rows = result.collect()

        assert len(rows) == 3
        assert rows[0]["category"] == "low"
        assert rows[1]["category"] == "medium"
        assert rows[2]["category"] == "high"

    def test_casewhen_cast_with_null_values(self, spark):
        """Test CaseWhen.cast() with null values."""
        df = spark.createDataFrame(
            [
                {"value": "A"},
                {"value": None},
            ]
        )

        result = df.withColumn(
            "when_result",
            F.when(F.col("value") == "A", F.lit(100))
            .otherwise(F.lit(None))
            .cast("long"),
        )

        rows = result.collect()

        assert len(rows) == 2
        assert rows[0]["when_result"] == 100
        assert rows[1]["when_result"] is None

    def test_casewhen_cast_in_select(self, spark):
        """Test CaseWhen.cast() in select operation."""
        df = spark.createDataFrame(
            [
                {"value": "A"},
                {"value": "B"},
            ]
        )

        result = df.select(
            F.when(F.col("value") == "A", F.lit(100))
            .otherwise(F.lit(200))
            .cast("long")
            .alias("result")
        )

        rows = result.collect()

        assert len(rows) == 2
        assert rows[0]["result"] == 100
        assert rows[1]["result"] == 200

    def test_casewhen_cast_with_datatype_object(self, spark):
        """Test CaseWhen.cast() with DataType object instead of string."""
        df = spark.createDataFrame(
            [
                {"value": "A"},
                {"value": "B"},
            ]
        )

        result = df.withColumn(
            "when_result",
            F.when(F.col("value") == "A", F.lit(100))
            .otherwise(F.lit(200))
            .cast(LongType()),
        )

        rows = result.collect()

        assert len(rows) == 2
        assert rows[0]["when_result"] == 100
        assert rows[1]["when_result"] == 200

        # Verify schema
        schema = result.schema
        # Use PySpark-compatible field access
        when_result_field = next(
            (f for f in schema.fields if f.name == "when_result"), None
        )
        assert when_result_field is not None
        assert isinstance(when_result_field.dataType, LongType)


class TestWindowFunctionCast:
    """Test WindowFunction.cast() method."""

    def test_window_function_cast_to_long(self, spark):
        """Test WindowFunction.cast() to long."""
        df = spark.createDataFrame(
            [
                {"id": 1, "value": 10},
                {"id": 2, "value": 20},
                {"id": 3, "value": 30},
            ]
        )

        window_spec = Window.orderBy("id")
        result = df.withColumn(
            "rank_long",
            F.row_number().over(window_spec).cast("long"),
        )

        rows = result.collect()

        assert len(rows) == 3
        # Verify the cast worked
        assert rows[0]["rank_long"] == 1
        assert rows[1]["rank_long"] == 2
        assert rows[2]["rank_long"] == 3

        # Verify schema shows long type
        schema = result.schema
        # Use PySpark-compatible field access
        rank_field = next((f for f in schema.fields if f.name == "rank_long"), None)
        assert rank_field is not None
        assert isinstance(rank_field.dataType, LongType)

    def test_window_function_cast_to_string(self, spark):
        """Test WindowFunction.cast() to string."""
        df = spark.createDataFrame(
            [
                {"id": 1, "value": 10},
                {"id": 2, "value": 20},
                {"id": 3, "value": 30},
            ]
        )

        window_spec = Window.orderBy("id")
        result = df.withColumn(
            "rank_str",
            F.row_number().over(window_spec).cast("string"),
        )

        rows = result.collect()

        assert len(rows) == 3
        # Values should be strings
        assert rows[0]["rank_str"] == "1"
        assert rows[1]["rank_str"] == "2"
        assert rows[2]["rank_str"] == "3"

        # Verify schema shows string type
        schema = result.schema
        # Use PySpark-compatible field access
        rank_field = next((f for f in schema.fields if f.name == "rank_str"), None)
        assert rank_field is not None
        assert isinstance(rank_field.dataType, StringType)

    def test_window_function_cast_to_double(self, spark):
        """Test WindowFunction.cast() to double."""
        df = spark.createDataFrame(
            [
                {"id": 1, "value": 10},
                {"id": 2, "value": 20},
                {"id": 3, "value": 30},
            ]
        )

        window_spec = Window.orderBy("id")
        result = df.withColumn(
            "rank_double",
            F.row_number().over(window_spec).cast("double"),
        )

        rows = result.collect()

        assert len(rows) == 3
        # Values should be doubles
        assert rows[0]["rank_double"] == 1.0
        assert rows[1]["rank_double"] == 2.0
        assert rows[2]["rank_double"] == 3.0

        # Verify schema shows double type
        schema = result.schema
        # Use PySpark-compatible field access
        rank_field = next((f for f in schema.fields if f.name == "rank_double"), None)
        assert rank_field is not None
        assert isinstance(rank_field.dataType, DoubleType)

    def test_window_function_cast_with_partition(self, spark):
        """Test WindowFunction.cast() with partitioned window."""
        df = spark.createDataFrame(
            [
                {"category": "A", "value": 10},
                {"category": "A", "value": 20},
                {"category": "B", "value": 30},
                {"category": "B", "value": 40},
            ]
        )

        window_spec = Window.partitionBy("category").orderBy("value")
        result = df.withColumn(
            "rank_long",
            F.row_number().over(window_spec).cast("long"),
        )

        rows = result.collect()

        assert len(rows) == 4
        # Verify ranks are per partition
        assert rows[0]["rank_long"] == 1  # First in category A
        assert rows[1]["rank_long"] == 2  # Second in category A
        assert rows[2]["rank_long"] == 1  # First in category B
        assert rows[3]["rank_long"] == 2  # Second in category B

    def test_window_function_cast_with_sum(self, spark):
        """Test WindowFunction.cast() with sum() window function."""
        df = spark.createDataFrame(
            [
                {"id": 1, "value": 10},
                {"id": 2, "value": 20},
                {"id": 3, "value": 30},
            ]
        )

        window_spec = Window.orderBy("id").rowsBetween(
            Window.unboundedPreceding, Window.currentRow
        )
        result = df.withColumn(
            "sum_long",
            F.sum("value").over(window_spec).cast("long"),
        )

        rows = result.collect()

        assert len(rows) == 3
        # Verify cumulative sum
        assert rows[0]["sum_long"] == 10
        assert rows[1]["sum_long"] == 30
        assert rows[2]["sum_long"] == 60

    def test_window_function_cast_in_select(self, spark):
        """Test WindowFunction.cast() in select operation."""
        df = spark.createDataFrame(
            [
                {"id": 1, "value": 10},
                {"id": 2, "value": 20},
            ]
        )

        window_spec = Window.orderBy("id")
        result = df.select(F.row_number().over(window_spec).cast("long").alias("rank"))

        rows = result.collect()

        assert len(rows) == 2
        assert rows[0]["rank"] == 1
        assert rows[1]["rank"] == 2

    def test_window_function_cast_with_datatype_object(self, spark):
        """Test WindowFunction.cast() with DataType object instead of string."""
        df = spark.createDataFrame(
            [
                {"id": 1, "value": 10},
                {"id": 2, "value": 20},
            ]
        )

        window_spec = Window.orderBy("id")
        result = df.withColumn(
            "rank_long",
            F.row_number().over(window_spec).cast(LongType()),
        )

        rows = result.collect()

        assert len(rows) == 2
        assert rows[0]["rank_long"] == 1
        assert rows[1]["rank_long"] == 2

        # Verify schema
        schema = result.schema
        # Use PySpark-compatible field access
        rank_field = next((f for f in schema.fields if f.name == "rank_long"), None)
        assert rank_field is not None
        assert isinstance(rank_field.dataType, LongType)


class TestCaseWhenWindowFunctionCastParity:
    """PySpark parity tests for CaseWhen and WindowFunction cast operations."""

    def test_casewhen_cast_parity_issue_243(self, spark):
        """Test CaseWhen.cast() parity with PySpark (exact issue #243 scenario)."""
        if not _is_pyspark_mode():
            pytest.skip(
                "PySpark parity test - run with MOCK_SPARK_TEST_BACKEND=pyspark"
            )

        df = spark.createDataFrame(
            [
                {"value": "A"},
                {"value": "B"},
            ]
        )

        # Exact code from issue #243
        result = df.withColumn(
            "when_result",
            F.when(F.col("value") == "A", F.lit(100))
            .otherwise(F.lit(200))
            .cast("long"),
        )

        rows = result.collect()

        # Verify PySpark behavior
        assert len(rows) == 2
        assert rows[0]["when_result"] == 100
        assert rows[1]["when_result"] == 200

        # Verify schema matches PySpark
        schema = result.schema
        # Use PySpark-compatible field access
        when_result_field = next(
            (f for f in schema.fields if f.name == "when_result"), None
        )
        assert when_result_field is not None
        # PySpark returns LongType for cast("long")
        assert isinstance(when_result_field.dataType, LongType)

    def test_window_function_cast_parity(self, spark):
        """Test WindowFunction.cast() parity with PySpark."""
        if not _is_pyspark_mode():
            pytest.skip(
                "PySpark parity test - run with MOCK_SPARK_TEST_BACKEND=pyspark"
            )

        df = spark.createDataFrame(
            [
                {"id": 1, "value": 10},
                {"id": 2, "value": 20},
                {"id": 3, "value": 30},
            ]
        )

        window_spec = Window.orderBy("id")
        result = df.withColumn(
            "rank_long",
            F.row_number().over(window_spec).cast("long"),
        )

        rows = result.collect()

        # Verify PySpark behavior
        assert len(rows) == 3
        assert rows[0]["rank_long"] == 1
        assert rows[1]["rank_long"] == 2
        assert rows[2]["rank_long"] == 3

        # Verify schema matches PySpark
        schema = result.schema
        # Use PySpark-compatible field access
        rank_field = next((f for f in schema.fields if f.name == "rank_long"), None)
        assert rank_field is not None
        assert isinstance(rank_field.dataType, LongType)

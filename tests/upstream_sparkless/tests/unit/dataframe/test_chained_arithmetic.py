"""
Tests for chained arithmetic operations with reverse operators.

These tests ensure that:
1. Literals can appear on the left side of arithmetic operations (e.g., `2 * F.col("col")`)
2. Chained arithmetic operations work correctly (e.g., `F.col("a") + 2 * F.col("b") + 0.01`)
3. All arithmetic operations support reverse operators
4. Behavior matches PySpark exactly

These tests work with both sparkless (mock) and PySpark backends.
Set MOCK_SPARK_TEST_BACKEND=pyspark to run with real PySpark.
"""

from tests.fixtures.spark_imports import get_spark_imports
from tests.fixtures.spark_backend import get_backend_type, BackendType

# Get imports based on backend
imports = get_spark_imports()
SparkSession = imports.SparkSession
DoubleType = imports.DoubleType
IntegerType = imports.IntegerType
StringType = imports.StringType
StructType = imports.StructType
StructField = imports.StructField
F = imports.F  # Functions module for backend-appropriate F.col() etc.


def _is_pyspark_mode() -> bool:
    """Check if running in PySpark mode."""
    backend = get_backend_type()
    return backend == BackendType.PYSPARK


class TestChainedArithmetic:
    """Test chained arithmetic operations with reverse operators."""

    def test_reverse_multiplication(self, spark):
        """Test reverse multiplication: `2 * F.col("col")`."""
        schema = StructType([StructField("number_2", DoubleType(), True)])
        df = spark.createDataFrame(
            [
                {"number_2": 1.0},
                {"number_2": 2.0},
                {"number_2": 3.0},
            ],
            schema=schema,
        )

        result = df.withColumn("result", 2 * F.col("number_2"))

        rows = result.collect()
        assert len(rows) == 3
        assert rows[0]["result"] == 2.0  # 2 * 1.0
        assert rows[1]["result"] == 4.0  # 2 * 2.0
        assert rows[2]["result"] == 6.0  # 2 * 3.0

    def test_reverse_addition(self, spark):
        """Test reverse addition: `2 + F.col("col")`."""
        schema = StructType([StructField("number_2", DoubleType(), True)])
        df = spark.createDataFrame(
            [
                {"number_2": 1.0},
                {"number_2": 2.0},
            ],
            schema=schema,
        )

        result = df.withColumn("result", 2 + F.col("number_2"))

        rows = result.collect()
        assert len(rows) == 2
        assert rows[0]["result"] == 3.0  # 2 + 1.0
        assert rows[1]["result"] == 4.0  # 2 + 2.0

    def test_reverse_subtraction(self, spark):
        """Test reverse subtraction: `2 - F.col("col")`."""
        schema = StructType([StructField("number_2", DoubleType(), True)])
        df = spark.createDataFrame(
            [
                {"number_2": 1.0},
                {"number_2": 2.0},
            ],
            schema=schema,
        )

        result = df.withColumn("result", 2 - F.col("number_2"))

        rows = result.collect()
        assert len(rows) == 2
        assert rows[0]["result"] == 1.0  # 2 - 1.0
        assert rows[1]["result"] == 0.0  # 2 - 2.0

    def test_reverse_division(self, spark):
        """Test reverse division: `2 / F.col("col")`."""
        schema = StructType([StructField("number_2", DoubleType(), True)])
        df = spark.createDataFrame(
            [
                {"number_2": 1.0},
                {"number_2": 2.0},
            ],
            schema=schema,
        )

        result = df.withColumn("result", 2 / F.col("number_2"))

        rows = result.collect()
        assert len(rows) == 2
        assert rows[0]["result"] == 2.0  # 2 / 1.0
        assert rows[1]["result"] == 1.0  # 2 / 2.0

    def test_reverse_modulo(self, spark):
        """Test reverse modulo: `2 % F.col("col")`."""
        schema = StructType([StructField("number_2", DoubleType(), True)])
        df = spark.createDataFrame(
            [
                {"number_2": 3.0},
                {"number_2": 2.0},
            ],
            schema=schema,
        )

        result = df.withColumn("result", 2 % F.col("number_2"))

        rows = result.collect()
        assert len(rows) == 2
        assert rows[0]["result"] == 2.0  # 2 % 3.0
        assert rows[1]["result"] == 0.0  # 2 % 2.0

    def test_chained_arithmetic_issue_237_example(self, spark):
        """Test the exact example from issue #237."""
        schema = StructType(
            [
                StructField("number_1", DoubleType(), True),
                StructField("number_2", DoubleType(), True),
            ]
        )
        df = spark.createDataFrame(
            [
                {"number_1": 1.0, "number_2": 1.0},
                {"number_1": 2.0, "number_2": 2.0},
                {"number_1": 3.0, "number_2": 3.0},
            ],
            schema=schema,
        )

        result = df.withColumn(
            "result", F.col("number_1") + 2 * F.col("number_2") + 0.01
        )

        rows = result.collect()
        assert len(rows) == 3
        assert rows[0]["result"] == 3.01  # 1.0 + 2 * 1.0 + 0.01 = 1.0 + 2.0 + 0.01
        assert rows[1]["result"] == 6.01  # 2.0 + 2 * 2.0 + 0.01 = 2.0 + 4.0 + 0.01
        assert rows[2]["result"] == 9.01  # 3.0 + 2 * 3.0 + 0.01 = 3.0 + 6.0 + 0.01

    def test_complex_chained_operations(self, spark):
        """Test complex chained operations with multiple literals and columns."""
        schema = StructType(
            [
                StructField("a", DoubleType(), True),
                StructField("b", DoubleType(), True),
            ]
        )
        df = spark.createDataFrame(
            [
                {"a": 10.0, "b": 5.0},
                {"a": 20.0, "b": 10.0},
            ],
            schema=schema,
        )

        result = df.withColumn("result", F.col("a") * 3 - F.col("b") / 2 + 1.5)

        rows = result.collect()
        assert len(rows) == 2
        # 10.0 * 3 - 5.0 / 2 + 1.5 = 30.0 - 2.5 + 1.5 = 29.0
        assert rows[0]["result"] == 29.0
        # 20.0 * 3 - 10.0 / 2 + 1.5 = 60.0 - 5.0 + 1.5 = 56.5
        assert rows[1]["result"] == 56.5

    def test_all_reverse_operations(self, spark):
        """Test all reverse operations in one expression."""
        schema = StructType([StructField("col", DoubleType(), True)])
        df = spark.createDataFrame(
            [
                {"col": 2.0},
            ],
            schema=schema,
        )

        # Test all reverse operations
        result = (
            df.withColumn("add", 5 + F.col("col"))
            .withColumn("sub", 5 - F.col("col"))
            .withColumn("mul", 5 * F.col("col"))
            .withColumn("div", 5 / F.col("col"))
            .withColumn("mod", 5 % F.col("col"))
        )

        rows = result.collect()
        assert rows[0]["add"] == 7.0  # 5 + 2.0
        assert rows[0]["sub"] == 3.0  # 5 - 2.0
        assert rows[0]["mul"] == 10.0  # 5 * 2.0
        assert rows[0]["div"] == 2.5  # 5 / 2.0
        assert rows[0]["mod"] == 1.0  # 5 % 2.0

    def test_reverse_operations_with_integers(self, spark):
        """Test reverse operations with integer literals."""
        schema = StructType([StructField("col", IntegerType(), True)])
        df = spark.createDataFrame(
            [
                {"col": 3},
            ],
            schema=schema,
        )

        result = df.withColumn("result", 10 * F.col("col"))

        rows = result.collect()
        assert rows[0]["result"] == 30  # 10 * 3

    def test_reverse_operations_with_floats(self, spark):
        """Test reverse operations with float literals."""
        schema = StructType([StructField("col", DoubleType(), True)])
        df = spark.createDataFrame(
            [
                {"col": 2.0},
            ],
            schema=schema,
        )

        result = df.withColumn("result", 3.5 * F.col("col"))

        rows = result.collect()
        assert rows[0]["result"] == 7.0  # 3.5 * 2.0

    def test_nested_chained_operations(self, spark):
        """Test deeply nested chained operations."""
        schema = StructType(
            [
                StructField("a", DoubleType(), True),
                StructField("b", DoubleType(), True),
                StructField("c", DoubleType(), True),
            ]
        )
        df = spark.createDataFrame(
            [
                {"a": 1.0, "b": 2.0, "c": 3.0},
            ],
            schema=schema,
        )

        result = df.withColumn(
            "result", 2 * F.col("a") + 3 * F.col("b") - 4 * F.col("c") + 1.0
        )

        rows = result.collect()
        # 2 * 1.0 + 3 * 2.0 - 4 * 3.0 + 1.0 = 2.0 + 6.0 - 12.0 + 1.0 = -3.0
        assert rows[0]["result"] == -3.0

    def test_reverse_operations_in_select(self, spark):
        """Test reverse operations in select statements."""
        schema = StructType([StructField("col", DoubleType(), True)])
        df = spark.createDataFrame(
            [
                {"col": 5.0},
            ],
            schema=schema,
        )

        result = df.select((10 * F.col("col")).alias("result"))

        rows = result.collect()
        assert rows[0]["result"] == 50.0  # 10 * 5.0

    def test_reverse_operations_in_filter(self, spark):
        """Test reverse operations in filter conditions."""
        schema = StructType([StructField("value", DoubleType(), True)])
        df = spark.createDataFrame(
            [
                {"value": 5.0},
                {"value": 10.0},
                {"value": 15.0},
            ],
            schema=schema,
        )

        # Filter where 2 * value > 10
        result = df.filter(2 * F.col("value") > 10)

        rows = result.collect()
        assert len(rows) == 2  # 2 * 10.0 = 20 > 10, 2 * 15.0 = 30 > 10
        assert rows[0]["value"] == 10.0
        assert rows[1]["value"] == 15.0

    def test_mixed_forward_and_reverse_operations(self, spark):
        """Test mixing forward and reverse operations."""
        schema = StructType(
            [
                StructField("a", DoubleType(), True),
                StructField("b", DoubleType(), True),
            ]
        )
        df = spark.createDataFrame(
            [
                {"a": 2.0, "b": 3.0},
            ],
            schema=schema,
        )

        # Mix: col * literal and literal * col
        result = df.withColumn("result", F.col("a") * 5 + 10 * F.col("b"))

        rows = result.collect()
        assert rows[0]["result"] == 40.0  # 2.0 * 5 + 10 * 3.0 = 10.0 + 30.0

    def test_reverse_operations_with_null_values(self, spark):
        """Test reverse operations with null column values."""
        schema = StructType([StructField("col", DoubleType(), True)])
        df = spark.createDataFrame(
            [
                {"col": 2.0},
                {"col": None},
                {"col": 4.0},
            ],
            schema=schema,
        )

        result = df.withColumn("result", 10 * F.col("col"))

        rows = result.collect()
        assert rows[0]["result"] == 20.0  # 10 * 2.0
        assert rows[1]["result"] is None  # 10 * None
        assert rows[2]["result"] == 40.0  # 10 * 4.0

    def test_reverse_operations_with_negative_numbers(self, spark):
        """Test reverse operations with negative literals."""
        schema = StructType([StructField("col", DoubleType(), True)])
        df = spark.createDataFrame(
            [
                {"col": 5.0},
            ],
            schema=schema,
        )

        result = df.withColumn("result", -2 * F.col("col"))

        rows = result.collect()
        assert rows[0]["result"] == -10.0  # -2 * 5.0

    def test_reverse_operations_chained_with_arithmetic(self, spark):
        """Test reverse operations in complex arithmetic chains."""
        schema = StructType(
            [
                StructField("x", DoubleType(), True),
                StructField("y", DoubleType(), True),
            ]
        )
        df = spark.createDataFrame(
            [
                {"x": 1.0, "y": 2.0},
            ],
            schema=schema,
        )

        # Complex: (2 * x) + (3 * y) - (4 * x) + 1
        result = df.withColumn(
            "result", 2 * F.col("x") + 3 * F.col("y") - 4 * F.col("x") + 1.0
        )

        rows = result.collect()
        # 2 * 1.0 + 3 * 2.0 - 4 * 1.0 + 1.0 = 2.0 + 6.0 - 4.0 + 1.0 = 5.0
        assert rows[0]["result"] == 5.0

    def test_operator_precedence(self, spark):
        """Test that operator precedence is correctly handled in chained operations."""
        schema = StructType([StructField("a", DoubleType(), True)])
        df = spark.createDataFrame(
            [
                {"a": 2.0},
            ],
            schema=schema,
        )

        # Test precedence: multiplication before addition
        # 1 + 2 * a should be 1 + (2 * a) = 1 + 4 = 5, not (1 + 2) * a = 6
        result = df.withColumn("result", 1 + 2 * F.col("a"))

        rows = result.collect()
        assert rows[0]["result"] == 5.0  # 1 + (2 * 2.0) = 1 + 4.0 = 5.0

    def test_operator_precedence_with_parentheses_equivalent(self, spark):
        """Test that explicit parentheses work correctly."""
        schema = StructType([StructField("a", DoubleType(), True)])
        df = spark.createDataFrame(
            [
                {"a": 2.0},
            ],
            schema=schema,
        )

        # Test that (1 + 2) * a gives different result than 1 + 2 * a
        result = df.withColumn("result", (1 + 2) * F.col("a"))

        rows = result.collect()
        assert rows[0]["result"] == 6.0  # (1 + 2) * 2.0 = 3 * 2.0 = 6.0

    def test_all_operators_in_single_expression(self, spark):
        """Test all arithmetic operators in a single chained expression."""
        schema = StructType([StructField("a", DoubleType(), True)])
        df = spark.createDataFrame(
            [
                {"a": 3.0},
            ],
            schema=schema,
        )

        # Test: 10 + 2 * a - 5 / a + 3 % a
        # Order: 2 * 3 = 6, 5 / 3 = 1.67, 3 % 3 = 0
        # 10 + 6 - 1.67 + 0 = 14.33
        result = df.withColumn(
            "result", 10 + 2 * F.col("a") - 5 / F.col("a") + 3 % F.col("a")
        )

        rows = result.collect()
        # 10 + (2 * 3.0) - (5 / 3.0) + (3 % 3.0) = 10 + 6.0 - 1.67 + 0.0 = 14.33
        assert abs(rows[0]["result"] - 14.333333333333334) < 0.0001

    def test_reverse_operations_with_zero(self, spark):
        """Test reverse operations with zero literals."""
        schema = StructType([StructField("col", DoubleType(), True)])
        df = spark.createDataFrame(
            [
                {"col": 5.0},
            ],
            schema=schema,
        )

        result = (
            df.withColumn("mul", 0 * F.col("col"))
            .withColumn("add", 0 + F.col("col"))
            .withColumn("sub", 0 - F.col("col"))
        )

        rows = result.collect()
        assert rows[0]["mul"] == 0.0  # 0 * 5.0
        assert rows[0]["add"] == 5.0  # 0 + 5.0
        assert rows[0]["sub"] == -5.0  # 0 - 5.0

    def test_reverse_operations_with_one(self, spark):
        """Test reverse operations with one as literal."""
        schema = StructType([StructField("col", DoubleType(), True)])
        df = spark.createDataFrame(
            [
                {"col": 5.0},
            ],
            schema=schema,
        )

        result = df.withColumn("mul", 1 * F.col("col")).withColumn(
            "div", 1 / F.col("col")
        )

        rows = result.collect()
        assert rows[0]["mul"] == 5.0  # 1 * 5.0
        assert rows[0]["div"] == 0.2  # 1 / 5.0

    def test_reverse_operations_with_negative_literals(self, spark):
        """Test reverse operations with negative literals."""
        schema = StructType([StructField("col", DoubleType(), True)])
        df = spark.createDataFrame(
            [
                {"col": 5.0},
            ],
            schema=schema,
        )

        result = (
            df.withColumn("mul", -2 * F.col("col"))
            .withColumn("add", -3 + F.col("col"))
            .withColumn("sub", -4 - F.col("col"))
        )

        rows = result.collect()
        assert rows[0]["mul"] == -10.0  # -2 * 5.0
        assert rows[0]["add"] == 2.0  # -3 + 5.0
        assert rows[0]["sub"] == -9.0  # -4 - 5.0

    def test_reverse_operations_with_decimal_literals(self, spark):
        """Test reverse operations with decimal literals."""
        schema = StructType([StructField("col", DoubleType(), True)])
        df = spark.createDataFrame(
            [
                {"col": 4.0},
            ],
            schema=schema,
        )

        result = (
            df.withColumn("mul", 0.5 * F.col("col"))
            .withColumn("div", 0.5 / F.col("col"))
            .withColumn("add", 0.5 + F.col("col"))
        )

        rows = result.collect()
        assert rows[0]["mul"] == 2.0  # 0.5 * 4.0
        assert rows[0]["div"] == 0.125  # 0.5 / 4.0
        assert rows[0]["add"] == 4.5  # 0.5 + 4.0

    def test_chained_operations_with_mixed_types(self, spark):
        """Test chained operations mixing integers and floats."""
        schema = StructType([StructField("col", DoubleType(), True)])
        df = spark.createDataFrame(
            [
                {"col": 2.0},
            ],
            schema=schema,
        )

        # Mix int and float literals
        result = df.withColumn("result", 3 * F.col("col") + 1.5 * F.col("col"))

        rows = result.collect()
        assert rows[0]["result"] == 9.0  # 3 * 2.0 + 1.5 * 2.0 = 6.0 + 3.0

    def test_very_long_chained_expression(self, spark):
        """Test very long chained arithmetic expression."""
        schema = StructType([StructField("a", DoubleType(), True)])
        df = spark.createDataFrame(
            [
                {"a": 2.0},
            ],
            schema=schema,
        )

        # Long chain: 1 + 2 * a + 3 * a - 4 * a + 5 * a
        result = df.withColumn(
            "result",
            1 + 2 * F.col("a") + 3 * F.col("a") - 4 * F.col("a") + 5 * F.col("a"),
        )

        rows = result.collect()
        # 1 + (2 * 2) + (3 * 2) - (4 * 2) + (5 * 2) = 1 + 4 + 6 - 8 + 10 = 13
        assert rows[0]["result"] == 13.0

    def test_reverse_operations_in_orderby(self, spark):
        """Test reverse operations in orderBy clauses."""
        schema = StructType([StructField("value", DoubleType(), True)])
        df = spark.createDataFrame(
            [
                {"value": 3.0},
                {"value": 1.0},
                {"value": 2.0},
            ],
            schema=schema,
        )

        # Order by 2 * value
        result = df.orderBy(2 * F.col("value"))

        rows = result.collect()
        assert len(rows) == 3
        assert rows[0]["value"] == 1.0  # 2 * 1.0 = 2.0
        assert rows[1]["value"] == 2.0  # 2 * 2.0 = 4.0
        assert rows[2]["value"] == 3.0  # 2 * 3.0 = 6.0

    def test_reverse_operations_in_groupby_aggregation(self, spark):
        """Test reverse operations in groupBy aggregations."""
        schema = StructType(
            [
                StructField("category", StringType(), True),
                StructField("value", DoubleType(), True),
            ]
        )
        df = spark.createDataFrame(
            [
                {"category": "A", "value": 2.0},
                {"category": "A", "value": 3.0},
                {"category": "B", "value": 4.0},
            ],
            schema=schema,
        )

        # Group by category and sum 2 * value
        result = df.groupBy("category").agg(F.sum(2 * F.col("value")).alias("total"))

        rows = result.collect()
        assert len(rows) == 2
        row_a = next(r for r in rows if r["category"] == "A")
        row_b = next(r for r in rows if r["category"] == "B")
        assert row_a["total"] == 10.0  # 2 * 2.0 + 2 * 3.0 = 4.0 + 6.0
        assert row_b["total"] == 8.0  # 2 * 4.0

    def test_reverse_operations_with_when_otherwise(self, spark):
        """Test reverse operations in conditional expressions."""
        schema = StructType([StructField("value", DoubleType(), True)])
        df = spark.createDataFrame(
            [
                {"value": 5.0},
                {"value": 15.0},
            ],
            schema=schema,
        )

        result = df.withColumn(
            "result",
            F.when(2 * F.col("value") > 10, 3 * F.col("value")).otherwise(
                1 * F.col("value")
            ),
        )

        rows = result.collect()
        assert rows[0]["result"] == 5.0  # 2 * 5.0 = 10, not > 10, so 1 * 5.0 = 5.0
        assert rows[1]["result"] == 45.0  # 2 * 15.0 = 30 > 10, so 3 * 15.0 = 45.0

    def test_reverse_operations_with_cast(self, spark):
        """Test reverse operations chained with cast operations."""
        schema = StructType([StructField("col", DoubleType(), True)])
        df = spark.createDataFrame(
            [
                {"col": 2.5},
            ],
            schema=schema,
        )

        # (2 * col) cast to int
        result = df.withColumn("result", (2 * F.col("col")).cast("int"))

        rows = result.collect()
        assert rows[0]["result"] == 5  # (2 * 2.5) = 5.0, cast to int = 5

    def test_reverse_operations_with_string_columns(self, spark):
        """Test reverse operations with string columns (should coerce to numeric)."""
        schema = StructType([StructField("string_col", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"string_col": "10.0"},
                {"string_col": "20"},
            ],
            schema=schema,
        )

        result = df.withColumn("result", 2 * F.col("string_col"))

        rows = result.collect()
        assert rows[0]["result"] == 20.0  # 2 * 10.0
        assert rows[1]["result"] == 40.0  # 2 * 20

    def test_reverse_operations_division_by_zero(self, spark):
        """Test reverse division by zero."""
        schema = StructType([StructField("col", DoubleType(), True)])
        df = spark.createDataFrame(
            [
                {"col": 0.0},
                {"col": 5.0},
            ],
            schema=schema,
        )

        result = df.withColumn("result", 10 / F.col("col"))

        rows = result.collect()
        # Division by zero should return None/null
        assert rows[0]["result"] is None  # 10 / 0.0
        assert rows[1]["result"] == 2.0  # 10 / 5.0

    def test_reverse_operations_modulo_by_zero(self, spark):
        """Test reverse modulo by zero."""
        schema = StructType([StructField("col", DoubleType(), True)])
        df = spark.createDataFrame(
            [
                {"col": 0.0},
                {"col": 3.0},
            ],
            schema=schema,
        )

        result = df.withColumn("result", 10 % F.col("col"))

        rows = result.collect()
        # Modulo by zero should return None/null
        assert rows[0]["result"] is None  # 10 % 0.0
        assert rows[1]["result"] == 1.0  # 10 % 3.0

    def test_reverse_operations_with_null_literals(self, spark):
        """Test reverse operations when column has null values."""
        schema = StructType([StructField("col", DoubleType(), True)])
        df = spark.createDataFrame(
            [
                {"col": 5.0},
                {"col": None},
                {"col": 10.0},
            ],
            schema=schema,
        )

        result = df.withColumn("result", 2 * F.col("col"))

        rows = result.collect()
        assert rows[0]["result"] == 10.0  # 2 * 5.0
        assert rows[1]["result"] is None  # 2 * None
        assert rows[2]["result"] == 20.0  # 2 * 10.0

    def test_very_complex_nested_expression(self, spark):
        """Test very complex nested expression with multiple reverse operations."""
        schema = StructType(
            [
                StructField("a", DoubleType(), True),
                StructField("b", DoubleType(), True),
                StructField("c", DoubleType(), True),
            ]
        )
        df = spark.createDataFrame(
            [
                {"a": 1.0, "b": 2.0, "c": 3.0},
            ],
            schema=schema,
        )

        # Very complex: 2 * a + 3 * b - 4 * c + 5 * a - 6 * b + 7 * c
        result = df.withColumn(
            "result",
            2 * F.col("a")
            + 3 * F.col("b")
            - 4 * F.col("c")
            + 5 * F.col("a")
            - 6 * F.col("b")
            + 7 * F.col("c"),
        )

        rows = result.collect()
        # 2*1 + 3*2 - 4*3 + 5*1 - 6*2 + 7*3 = 2 + 6 - 12 + 5 - 12 + 21 = 10
        assert rows[0]["result"] == 10.0

    def test_reverse_operations_with_select_multiple_columns(self, spark):
        """Test reverse operations in select with multiple computed columns."""
        schema = StructType([StructField("col", DoubleType(), True)])
        df = spark.createDataFrame(
            [
                {"col": 3.0},
            ],
            schema=schema,
        )

        result = df.select(
            (2 * F.col("col")).alias("double"),
            (3 + F.col("col")).alias("add"),
            (10 - F.col("col")).alias("sub"),
        )

        rows = result.collect()
        assert rows[0]["double"] == 6.0  # 2 * 3.0
        assert rows[0]["add"] == 6.0  # 3 + 3.0
        assert rows[0]["sub"] == 7.0  # 10 - 3.0

    def test_reverse_operations_preserve_precision(self, spark):
        """Test that reverse operations preserve decimal precision."""
        schema = StructType([StructField("col", DoubleType(), True)])
        df = spark.createDataFrame(
            [
                {"col": 0.1},
            ],
            schema=schema,
        )

        result = df.withColumn("result", 3 * F.col("col"))

        rows = result.collect()
        # Floating point arithmetic - expect approximate value
        assert abs(rows[0]["result"] - 0.3) < 0.0001  # 3 * 0.1

    def test_reverse_operations_with_large_numbers(self, spark):
        """Test reverse operations with large numbers."""
        schema = StructType([StructField("col", DoubleType(), True)])
        df = spark.createDataFrame(
            [
                {"col": 1000000.0},
            ],
            schema=schema,
        )

        result = df.withColumn("result", 2 * F.col("col"))

        rows = result.collect()
        assert rows[0]["result"] == 2000000.0  # 2 * 1000000.0

    def test_reverse_operations_with_small_numbers(self, spark):
        """Test reverse operations with very small numbers."""
        schema = StructType([StructField("col", DoubleType(), True)])
        df = spark.createDataFrame(
            [
                {"col": 0.0001},
            ],
            schema=schema,
        )

        result = df.withColumn("result", 1000 * F.col("col"))

        rows = result.collect()
        assert abs(rows[0]["result"] - 0.1) < 0.0001  # 1000 * 0.0001

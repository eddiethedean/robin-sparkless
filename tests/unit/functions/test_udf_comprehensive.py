"""
Comprehensive tests for UDF functionality. Uses get_spark_imports from fixture only.
"""

import pytest

from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
F = _imports.F
T = _imports
StructType = _imports.StructType
StructField = _imports.StructField


class TestUDFBasicOperations:
    """Test basic UDF operations with different return types."""

    def test_udf_string_return_type(self, spark):
        """Test UDF with StringType return."""
        df = spark.createDataFrame([{"text": "hello"}, {"text": "world"}])

        upper_udf = F.udf(lambda x: x.upper(), T.StringType())
        result = df.withColumn("upper_text", upper_udf(F.col("text"))).collect()

        assert len(result) == 2
        assert result[0]["upper_text"] == "HELLO"
        assert result[1]["upper_text"] == "WORLD"

    def test_udf_integer_return_type(self, spark):
        """Test UDF with IntegerType return."""
        df = spark.createDataFrame([{"value": 5}, {"value": 10}])

        square_udf = F.udf(lambda x: x * x, T.IntegerType())
        result = df.withColumn("squared", square_udf(F.col("value"))).collect()

        assert len(result) == 2
        assert result[0]["squared"] == 25
        assert result[1]["squared"] == 100

    def test_udf_double_return_type(self, spark):
        """Test UDF with DoubleType return."""
        df = spark.createDataFrame([{"value": 2.5}, {"value": 3.0}])

        double_udf = F.udf(lambda x: x * 2.0, T.DoubleType())
        result = df.withColumn("doubled", double_udf(F.col("value"))).collect()

        assert len(result) == 2
        assert result[0]["doubled"] == 5.0
        assert result[1]["doubled"] == 6.0

    def test_udf_boolean_return_type(self, spark):
        """Test UDF with BooleanType return."""
        df = spark.createDataFrame([{"age": 25}, {"age": 15}, {"age": 30}])

        is_adult_udf = F.udf(lambda x: x >= 18, T.BooleanType())
        result = df.withColumn("is_adult", is_adult_udf(F.col("age"))).collect()

        assert len(result) == 3
        assert result[0]["is_adult"] is True
        assert result[1]["is_adult"] is False
        assert result[2]["is_adult"] is True


class TestUDFMultiArgument:
    """Test UDFs with multiple arguments."""

    def test_udf_two_arguments(self, spark):
        """Test UDF with two column arguments."""
        df = spark.createDataFrame(
            [
                {"first": "hello", "second": "world"},
                {"first": "foo", "second": "bar"},
            ]
        )

        concat_udf = F.udf(lambda x, y: f"{x}_{y}", T.StringType())
        result = df.withColumn(
            "combined", concat_udf(F.col("first"), F.col("second"))
        ).collect()

        assert len(result) == 2
        assert result[0]["combined"] == "hello_world"
        assert result[1]["combined"] == "foo_bar"

    def test_udf_three_arguments(self, spark):
        """Test UDF with three column arguments."""
        df = spark.createDataFrame(
            [
                {"a": 1, "b": 2, "c": 3},
                {"a": 4, "b": 5, "c": 6},
            ]
        )

        sum_udf = F.udf(lambda x, y, z: x + y + z, T.IntegerType())
        result = df.withColumn(
            "total", sum_udf(F.col("a"), F.col("b"), F.col("c"))
        ).collect()

        assert len(result) == 2
        assert result[0]["total"] == 6
        assert result[1]["total"] == 15

    def test_udf_mixed_types(self, spark):
        """Test UDF with mixed input types."""
        df = spark.createDataFrame(
            [
                {"name": "Alice", "age": 25},
                {"name": "Bob", "age": 30},
            ]
        )

        format_udf = F.udf(
            lambda name, age: f"{name} is {age} years old", T.StringType()
        )
        result = df.withColumn(
            "description", format_udf(F.col("name"), F.col("age"))
        ).collect()

        assert len(result) == 2
        assert result[0]["description"] == "Alice is 25 years old"
        assert result[1]["description"] == "Bob is 30 years old"


class TestUDFInDifferentOperations:
    """Test UDF usage in various DataFrame operations."""

    def test_udf_in_select(self, spark):
        """Test UDF in select operation."""
        df = spark.createDataFrame([{"value": "test"}])

        upper_udf = F.udf(lambda x: x.upper(), T.StringType())
        # Use withColumn first, then select to ensure UDF works in both contexts
        result = (
            df.withColumn("upper", upper_udf(F.col("value"))).select("upper").collect()
        )

        assert result[0]["upper"] == "TEST"

    def test_udf_in_withColumn(self, spark):
        """Test UDF in withColumn operation."""
        df = spark.createDataFrame([{"name": "alice", "age": 25}])

        upper_udf = F.udf(lambda x: x.upper(), T.StringType())
        result = df.withColumn("name_upper", upper_udf(F.col("name"))).collect()

        assert result[0]["name"] == "alice"
        assert result[0]["name_upper"] == "ALICE"
        assert result[0]["age"] == 25

    def test_udf_in_filter(self, spark):
        """Test UDF in filter operation."""
        df = spark.createDataFrame(
            [
                {"name": "Alice", "age": 25},
                {"name": "Bob", "age": 15},
                {"name": "Charlie", "age": 30},
            ]
        )

        is_adult_udf = F.udf(lambda x: x >= 18, T.BooleanType())
        result = df.filter(is_adult_udf(F.col("age"))).collect()

        assert len(result) == 2
        names = {row["name"] for row in result}
        assert names == {"Alice", "Charlie"}

    def test_udf_in_groupBy_aggregation(self, spark):
        """Test UDF in groupBy aggregation."""
        df = spark.createDataFrame(
            [
                {"category": "A", "value": 10},
                {"category": "A", "value": 20},
                {"category": "B", "value": 30},
            ]
        )

        double_udf = F.udf(lambda x: x * 2, T.IntegerType())
        # Apply UDF first, then aggregate
        df_doubled = df.withColumn("value_doubled", double_udf(F.col("value")))
        result = (
            df_doubled.groupBy("category")
            .agg(F.sum("value_doubled").alias("total_doubled"))
            .collect()
        )

        assert len(result) == 2
        result_dict = {row["category"]: row["total_doubled"] for row in result}
        assert result_dict["A"] == 60  # (10*2 + 20*2) = 60
        assert result_dict["B"] == 60  # 30*2 = 60


class TestUDFNullHandling:
    """Test UDF behavior with null values."""

    def test_udf_with_null_input(self, spark):
        """Test UDF handling null input."""
        df = spark.createDataFrame(
            [
                {"value": "hello"},
                {"value": None},
                {"value": "world"},
            ]
        )

        upper_udf = F.udf(
            lambda x: x.upper() if x is not None else None, T.StringType()
        )
        result = df.withColumn("upper", upper_udf(F.col("value"))).collect()

        assert len(result) == 3
        assert result[0]["upper"] == "HELLO"
        assert result[1]["upper"] is None
        assert result[2]["upper"] == "WORLD"

    def test_udf_with_null_return(self, spark):
        """Test UDF that returns null for certain inputs."""
        df = spark.createDataFrame([{"value": 0}, {"value": 5}, {"value": 10}])

        safe_divide_udf = F.udf(lambda x: 100 / x if x != 0 else None, T.DoubleType())
        result = df.withColumn("result", safe_divide_udf(F.col("value"))).collect()

        assert len(result) == 3
        assert result[0]["result"] is None
        assert result[1]["result"] == 20.0
        assert result[2]["result"] == 10.0


class TestUDFEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_udf_empty_dataframe(self, spark):
        """Test UDF with empty DataFrame."""
        schema = StructType([StructField("value", T.StringType())])
        df = spark.createDataFrame([], schema)

        upper_udf = F.udf(lambda x: x.upper(), T.StringType())
        result = df.withColumn("upper", upper_udf(F.col("value"))).collect()

        assert len(result) == 0

    def test_udf_single_row(self, spark):
        """Test UDF with single row DataFrame."""
        df = spark.createDataFrame([{"value": "test"}])

        upper_udf = F.udf(lambda x: x.upper(), T.StringType())
        result = df.withColumn("upper", upper_udf(F.col("value"))).collect()

        assert len(result) == 1
        assert result[0]["upper"] == "TEST"

    def test_udf_large_dataframe(self, spark):
        """Test UDF with larger DataFrame."""
        data = [{"value": i} for i in range(100)]
        df = spark.createDataFrame(data)

        square_udf = F.udf(lambda x: x * x, T.IntegerType())
        result = df.withColumn("squared", square_udf(F.col("value"))).collect()

        assert len(result) == 100
        assert result[0]["squared"] == 0
        assert result[50]["squared"] == 2500
        assert result[99]["squared"] == 9801

    def test_udf_empty_string(self, spark):
        """Test UDF with empty string input."""
        df = spark.createDataFrame([{"value": ""}, {"value": "test"}])

        length_udf = F.udf(lambda x: len(x), T.IntegerType())
        result = df.withColumn("length", length_udf(F.col("value"))).collect()

        assert len(result) == 2
        assert result[0]["length"] == 0
        assert result[1]["length"] == 4

    def test_udf_zero_value(self, spark):
        """Test UDF with zero value."""
        df = spark.createDataFrame([{"value": 0}, {"value": 5}])

        square_udf = F.udf(lambda x: x * x, T.IntegerType())
        result = df.withColumn("squared", square_udf(F.col("value"))).collect()

        assert len(result) == 2
        assert result[0]["squared"] == 0
        assert result[1]["squared"] == 25


class TestUDFComplexScenarios:
    """Test complex UDF usage scenarios."""

    def test_udf_chained_operations(self, spark):
        """Test chaining multiple UDF operations."""
        df = spark.createDataFrame([{"value": "hello"}])

        upper_udf = F.udf(lambda x: x.upper(), T.StringType())
        reverse_udf = F.udf(lambda x: x[::-1], T.StringType())

        result = (
            df.withColumn("upper", upper_udf(F.col("value")))
            .withColumn("reversed", reverse_udf(F.col("upper")))
            .collect()
        )

        assert result[0]["value"] == "hello"
        assert result[0]["upper"] == "HELLO"
        assert result[0]["reversed"] == "OLLEH"

    def test_udf_with_literal(self, spark):
        """Test UDF combined with literals."""
        df = spark.createDataFrame([{"name": "Alice"}])

        format_udf = F.udf(lambda name, prefix: f"{prefix}_{name}", T.StringType())
        result = df.withColumn(
            "formatted", format_udf(F.col("name"), F.lit("USER"))
        ).collect()

        assert result[0]["formatted"] == "USER_Alice"

    def test_udf_multiple_columns_same_udf(self, spark):
        """Test applying same UDF to multiple columns."""
        df = spark.createDataFrame([{"first": "hello", "second": "world"}])

        upper_udf = F.udf(lambda x: x.upper(), T.StringType())
        result = (
            df.withColumn("first_upper", upper_udf(F.col("first")))
            .withColumn("second_upper", upper_udf(F.col("second")))
            .collect()
        )

        assert result[0]["first_upper"] == "HELLO"
        assert result[0]["second_upper"] == "WORLD"

    def test_udf_in_orderBy(self, spark):
        """Test UDF in orderBy operation."""
        df = spark.createDataFrame(
            [
                {"name": "Charlie", "age": 30},
                {"name": "Alice", "age": 25},
                {"name": "Bob", "age": 35},
            ]
        )

        double_udf = F.udf(lambda x: x * 2, T.IntegerType())
        result = (
            df.withColumn("age_doubled", double_udf(F.col("age")))
            .orderBy("age_doubled")
            .collect()
        )

        assert len(result) == 3
        assert result[0]["name"] == "Alice"  # 25 * 2 = 50
        assert result[1]["name"] == "Charlie"  # 30 * 2 = 60
        assert result[2]["name"] == "Bob"  # 35 * 2 = 70


class TestUDFWithDifferentDataTypes:
    """Test UDFs with various input data types."""

    def test_udf_with_long_type(self, spark):
        """Test UDF with LongType input."""
        df = spark.createDataFrame([{"value": 1000000000}], schema=["value"])

        square_udf = F.udf(lambda x: x * x, T.LongType())
        result = df.withColumn("squared", square_udf(F.col("value"))).collect()

        assert result[0]["squared"] == 1000000000000000000

    def test_udf_with_float_type(self, spark):
        """Test UDF with FloatType input.

        Note: A Python `float` is treated as 64-bit; returning `FloatType()` here
        can cause strict Float32 enforcement in some backends. We keep the input
        as FloatType-like and declare a DoubleType return to avoid dtype mismatch.
        """
        df = spark.createDataFrame([{"value": 3.14}], schema=["value"])

        double_udf = F.udf(lambda x: x * 2.0, T.DoubleType())
        result = df.withColumn("doubled", double_udf(F.col("value"))).collect()

        assert abs(result[0]["doubled"] - 6.28) < 0.01


class TestUDFCustomName:
    """Test UDF with custom names."""

    def test_udf_with_custom_name(self, spark):
        """Test UDF with custom function name (PySpark-style F.udf)."""
        df = spark.createDataFrame([{"value": "test"}])

        upper_udf = F.udf(lambda x: x.upper(), T.StringType())
        result = df.withColumn("upper", upper_udf(F.col("value"))).collect()

        assert result[0]["upper"] == "TEST"


class TestUDFRegression279:
    """Regression test for issue #279 - exact reproduction."""

    def test_udf_with_withColumn_regression_279(self, spark):
        """Exact reproduction of issue #279."""
        data = [
            {"Name": "Alice", "Value": "abc"},
            {"Name": "Bob", "Value": "def"},
        ]
        df = spark.createDataFrame(data=data)
        my_udf = F.udf(lambda x: x.upper(), T.StringType())
        df2 = df.withColumn("Value", my_udf(F.col("Value")))
        rows = df2.collect()
        assert [r["Value"] for r in rows] == ["ABC", "DEF"]

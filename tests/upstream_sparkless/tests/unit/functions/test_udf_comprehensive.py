"""
Comprehensive tests for UDF functionality in Sparkless.

Tests cover various data types, operations, edge cases, and usage patterns.
"""

import sparkless.sql.functions as F
import sparkless.sql.types as T
from sparkless.sql import SparkSession


class TestUDFBasicOperations:
    """Test basic UDF operations with different return types."""

    def test_udf_string_return_type(self):
        """Test UDF with StringType return."""
        spark = SparkSession.builder.appName("test").getOrCreate()
        try:
            df = spark.createDataFrame([{"text": "hello"}, {"text": "world"}])

            upper_udf = F.udf(lambda x: x.upper(), T.StringType())
            result = df.withColumn("upper_text", upper_udf(F.col("text"))).collect()

            assert len(result) == 2
            assert result[0]["upper_text"] == "HELLO"
            assert result[1]["upper_text"] == "WORLD"
        finally:
            spark.stop()

    def test_udf_integer_return_type(self):
        """Test UDF with IntegerType return."""
        spark = SparkSession.builder.appName("test").getOrCreate()
        try:
            df = spark.createDataFrame([{"value": 5}, {"value": 10}])

            square_udf = F.udf(lambda x: x * x, T.IntegerType())
            result = df.withColumn("squared", square_udf(F.col("value"))).collect()

            assert len(result) == 2
            assert result[0]["squared"] == 25
            assert result[1]["squared"] == 100
        finally:
            spark.stop()

    def test_udf_double_return_type(self):
        """Test UDF with DoubleType return."""
        spark = SparkSession.builder.appName("test").getOrCreate()
        try:
            df = spark.createDataFrame([{"value": 2.5}, {"value": 3.0}])

            double_udf = F.udf(lambda x: x * 2.0, T.DoubleType())
            result = df.withColumn("doubled", double_udf(F.col("value"))).collect()

            assert len(result) == 2
            assert result[0]["doubled"] == 5.0
            assert result[1]["doubled"] == 6.0
        finally:
            spark.stop()

    def test_udf_boolean_return_type(self):
        """Test UDF with BooleanType return."""
        spark = SparkSession.builder.appName("test").getOrCreate()
        try:
            df = spark.createDataFrame([{"age": 25}, {"age": 15}, {"age": 30}])

            is_adult_udf = F.udf(lambda x: x >= 18, T.BooleanType())
            result = df.withColumn("is_adult", is_adult_udf(F.col("age"))).collect()

            assert len(result) == 3
            assert result[0]["is_adult"] is True
            assert result[1]["is_adult"] is False
            assert result[2]["is_adult"] is True
        finally:
            spark.stop()


class TestUDFMultiArgument:
    """Test UDFs with multiple arguments."""

    def test_udf_two_arguments(self):
        """Test UDF with two column arguments."""
        from sparkless.functions.udf import UserDefinedFunction

        spark = SparkSession.builder.appName("test").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"first": "hello", "second": "world"},
                    {"first": "foo", "second": "bar"},
                ]
            )

            concat_udf = UserDefinedFunction(lambda x, y: f"{x}_{y}", T.StringType())
            result = df.withColumn(
                "combined", concat_udf(F.col("first"), F.col("second"))
            ).collect()

            assert len(result) == 2
            assert result[0]["combined"] == "hello_world"
            assert result[1]["combined"] == "foo_bar"
        finally:
            spark.stop()

    def test_udf_three_arguments(self):
        """Test UDF with three column arguments."""
        from sparkless.functions.udf import UserDefinedFunction

        spark = SparkSession.builder.appName("test").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"a": 1, "b": 2, "c": 3},
                    {"a": 4, "b": 5, "c": 6},
                ]
            )

            sum_udf = UserDefinedFunction(lambda x, y, z: x + y + z, T.IntegerType())
            result = df.withColumn(
                "total", sum_udf(F.col("a"), F.col("b"), F.col("c"))
            ).collect()

            assert len(result) == 2
            assert result[0]["total"] == 6
            assert result[1]["total"] == 15
        finally:
            spark.stop()

    def test_udf_mixed_types(self):
        """Test UDF with mixed input types."""
        from sparkless.functions.udf import UserDefinedFunction

        spark = SparkSession.builder.appName("test").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"name": "Alice", "age": 25},
                    {"name": "Bob", "age": 30},
                ]
            )

            format_udf = UserDefinedFunction(
                lambda name, age: f"{name} is {age} years old", T.StringType()
            )
            result = df.withColumn(
                "description", format_udf(F.col("name"), F.col("age"))
            ).collect()

            assert len(result) == 2
            assert result[0]["description"] == "Alice is 25 years old"
            assert result[1]["description"] == "Bob is 30 years old"
        finally:
            spark.stop()


class TestUDFInDifferentOperations:
    """Test UDF usage in various DataFrame operations."""

    def test_udf_in_select(self):
        """Test UDF in select operation."""
        spark = SparkSession.builder.appName("test").getOrCreate()
        try:
            df = spark.createDataFrame([{"value": "test"}])

            upper_udf = F.udf(lambda x: x.upper(), T.StringType())
            # Use withColumn first, then select to ensure UDF works in both contexts
            result = (
                df.withColumn("upper", upper_udf(F.col("value")))
                .select("upper")
                .collect()
            )

            assert result[0]["upper"] == "TEST"
        finally:
            spark.stop()

    def test_udf_in_withColumn(self):
        """Test UDF in withColumn operation."""
        spark = SparkSession.builder.appName("test").getOrCreate()
        try:
            df = spark.createDataFrame([{"name": "alice", "age": 25}])

            upper_udf = F.udf(lambda x: x.upper(), T.StringType())
            result = df.withColumn("name_upper", upper_udf(F.col("name"))).collect()

            assert result[0]["name"] == "alice"
            assert result[0]["name_upper"] == "ALICE"
            assert result[0]["age"] == 25
        finally:
            spark.stop()

    def test_udf_in_filter(self):
        """Test UDF in filter operation."""
        spark = SparkSession.builder.appName("test").getOrCreate()
        try:
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
        finally:
            spark.stop()

    def test_udf_in_groupBy_aggregation(self):
        """Test UDF in groupBy aggregation."""
        spark = SparkSession.builder.appName("test").getOrCreate()
        try:
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
        finally:
            spark.stop()


class TestUDFNullHandling:
    """Test UDF behavior with null values."""

    def test_udf_with_null_input(self):
        """Test UDF handling null input."""
        spark = SparkSession.builder.appName("test").getOrCreate()
        try:
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
        finally:
            spark.stop()

    def test_udf_with_null_return(self):
        """Test UDF that returns null for certain inputs."""
        spark = SparkSession.builder.appName("test").getOrCreate()
        try:
            df = spark.createDataFrame([{"value": 0}, {"value": 5}, {"value": 10}])

            safe_divide_udf = F.udf(
                lambda x: 100 / x if x != 0 else None, T.DoubleType()
            )
            result = df.withColumn("result", safe_divide_udf(F.col("value"))).collect()

            assert len(result) == 3
            assert result[0]["result"] is None
            assert result[1]["result"] == 20.0
            assert result[2]["result"] == 10.0
        finally:
            spark.stop()


class TestUDFEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_udf_empty_dataframe(self):
        """Test UDF with empty DataFrame."""
        from sparkless.spark_types import StructType, StructField

        spark = SparkSession.builder.appName("test").getOrCreate()
        try:
            schema = StructType([StructField("value", T.StringType())])
            df = spark.createDataFrame([], schema)

            upper_udf = F.udf(lambda x: x.upper(), T.StringType())
            result = df.withColumn("upper", upper_udf(F.col("value"))).collect()

            assert len(result) == 0
        finally:
            spark.stop()

    def test_udf_single_row(self):
        """Test UDF with single row DataFrame."""
        spark = SparkSession.builder.appName("test").getOrCreate()
        try:
            df = spark.createDataFrame([{"value": "test"}])

            upper_udf = F.udf(lambda x: x.upper(), T.StringType())
            result = df.withColumn("upper", upper_udf(F.col("value"))).collect()

            assert len(result) == 1
            assert result[0]["upper"] == "TEST"
        finally:
            spark.stop()

    def test_udf_large_dataframe(self):
        """Test UDF with larger DataFrame."""
        spark = SparkSession.builder.appName("test").getOrCreate()
        try:
            data = [{"value": i} for i in range(100)]
            df = spark.createDataFrame(data)

            square_udf = F.udf(lambda x: x * x, T.IntegerType())
            result = df.withColumn("squared", square_udf(F.col("value"))).collect()

            assert len(result) == 100
            assert result[0]["squared"] == 0
            assert result[50]["squared"] == 2500
            assert result[99]["squared"] == 9801
        finally:
            spark.stop()

    def test_udf_empty_string(self):
        """Test UDF with empty string input."""
        spark = SparkSession.builder.appName("test").getOrCreate()
        try:
            df = spark.createDataFrame([{"value": ""}, {"value": "test"}])

            length_udf = F.udf(lambda x: len(x), T.IntegerType())
            result = df.withColumn("length", length_udf(F.col("value"))).collect()

            assert len(result) == 2
            assert result[0]["length"] == 0
            assert result[1]["length"] == 4
        finally:
            spark.stop()

    def test_udf_zero_value(self):
        """Test UDF with zero value."""
        spark = SparkSession.builder.appName("test").getOrCreate()
        try:
            df = spark.createDataFrame([{"value": 0}, {"value": 5}])

            square_udf = F.udf(lambda x: x * x, T.IntegerType())
            result = df.withColumn("squared", square_udf(F.col("value"))).collect()

            assert len(result) == 2
            assert result[0]["squared"] == 0
            assert result[1]["squared"] == 25
        finally:
            spark.stop()


class TestUDFComplexScenarios:
    """Test complex UDF usage scenarios."""

    def test_udf_chained_operations(self):
        """Test chaining multiple UDF operations."""
        spark = SparkSession.builder.appName("test").getOrCreate()
        try:
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
        finally:
            spark.stop()

    def test_udf_with_literal(self):
        """Test UDF combined with literals."""
        from sparkless.functions.udf import UserDefinedFunction

        spark = SparkSession.builder.appName("test").getOrCreate()
        try:
            df = spark.createDataFrame([{"name": "Alice"}])

            format_udf = UserDefinedFunction(
                lambda name, prefix: f"{prefix}_{name}", T.StringType()
            )
            result = df.withColumn(
                "formatted", format_udf(F.col("name"), F.lit("USER"))
            ).collect()

            assert result[0]["formatted"] == "USER_Alice"
        finally:
            spark.stop()

    def test_udf_multiple_columns_same_udf(self):
        """Test applying same UDF to multiple columns."""
        spark = SparkSession.builder.appName("test").getOrCreate()
        try:
            df = spark.createDataFrame([{"first": "hello", "second": "world"}])

            upper_udf = F.udf(lambda x: x.upper(), T.StringType())
            result = (
                df.withColumn("first_upper", upper_udf(F.col("first")))
                .withColumn("second_upper", upper_udf(F.col("second")))
                .collect()
            )

            assert result[0]["first_upper"] == "HELLO"
            assert result[0]["second_upper"] == "WORLD"
        finally:
            spark.stop()

    def test_udf_in_orderBy(self):
        """Test UDF in orderBy operation."""
        spark = SparkSession.builder.appName("test").getOrCreate()
        try:
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
        finally:
            spark.stop()


class TestUDFWithDifferentDataTypes:
    """Test UDFs with various input data types."""

    def test_udf_with_long_type(self):
        """Test UDF with LongType input."""
        spark = SparkSession.builder.appName("test").getOrCreate()
        try:
            df = spark.createDataFrame([{"value": 1000000000}], schema=["value"])

            square_udf = F.udf(lambda x: x * x, T.LongType())
            result = df.withColumn("squared", square_udf(F.col("value"))).collect()

            assert result[0]["squared"] == 1000000000000000000
        finally:
            spark.stop()

    def test_udf_with_float_type(self):
        """Test UDF with FloatType input.

        Note: A Python `float` is treated as 64-bit; returning `FloatType()` here
        can cause strict Float32 enforcement in some backends. We keep the input
        as FloatType-like and declare a DoubleType return to avoid dtype mismatch.
        """
        spark = SparkSession.builder.appName("test").getOrCreate()
        try:
            df = spark.createDataFrame([{"value": 3.14}], schema=["value"])

            double_udf = F.udf(lambda x: x * 2.0, T.DoubleType())
            result = df.withColumn("doubled", double_udf(F.col("value"))).collect()

            assert abs(result[0]["doubled"] - 6.28) < 0.01
        finally:
            spark.stop()


class TestUDFCustomName:
    """Test UDF with custom names."""

    def test_udf_with_custom_name(self):
        """Test UDF with custom function name."""
        spark = SparkSession.builder.appName("test").getOrCreate()
        try:
            from sparkless.functions.udf import UserDefinedFunction

            df = spark.createDataFrame([{"value": "test"}])

            upper_udf = UserDefinedFunction(
                lambda x: x.upper(), T.StringType(), name="my_upper"
            )
            result = df.withColumn("upper", upper_udf(F.col("value"))).collect()

            assert result[0]["upper"] == "TEST"
        finally:
            spark.stop()


class TestUDFRegression279:
    """Regression test for issue #279 - exact reproduction."""

    def test_udf_with_withColumn_regression_279(self):
        """Exact reproduction of issue #279."""
        spark = SparkSession.builder.appName("Example").getOrCreate()
        try:
            data = [
                {"Name": "Alice", "Value": "abc"},
                {"Name": "Bob", "Value": "def"},
            ]

            df = spark.createDataFrame(data=data)

            my_udf = F.udf(lambda x: x.upper(), T.StringType())
            df2 = df.withColumn("Value", my_udf(F.col("Value")))

            rows = df2.collect()
            assert [r["Value"] for r in rows] == ["ABC", "DEF"]
        finally:
            spark.stop()

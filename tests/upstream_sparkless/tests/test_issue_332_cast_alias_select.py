"""
Unit tests for Issue #332: Column resolution fails with cast+alias+select.

Tests that column names are correctly resolved when combining aggregation, cast, alias, and select.
"""

from sparkless.sql import SparkSession
from sparkless import functions as F
from sparkless.sql import types as T


class TestIssue332CastAliasSelect:
    """Test cast+alias+select column resolution."""

    def test_cast_alias_select_basic(self):
        """Test basic cast+alias+select scenario."""
        spark = SparkSession.builder.appName("issue-332").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 2},
                    {"Name": "Bob", "Value": 3},
                    {"Name": "Bob", "Value": 4},
                ]
            )

            result = (
                df.groupBy("Name")
                .agg(F.mean("Value").cast(T.DoubleType()).alias("AvgValue"))
                .select("Name", "AvgValue")
            )

            rows = result.collect()
            assert len(rows) == 2
            assert rows[0]["Name"] == "Alice"
            assert rows[0]["AvgValue"] == 1.5
            assert rows[1]["Name"] == "Bob"
            assert rows[1]["AvgValue"] == 3.5
        finally:
            spark.stop()

    def test_cast_alias_select_multiple_aggregations(self):
        """Test multiple aggregations with cast+alias."""
        spark = SparkSession.builder.appName("issue-332").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1, "Score": 10},
                    {"Name": "Alice", "Value": 2, "Score": 20},
                    {"Name": "Bob", "Value": 3, "Score": 30},
                    {"Name": "Bob", "Value": 4, "Score": 40},
                ]
            )

            result = (
                df.groupBy("Name")
                .agg(
                    F.mean("Value").cast(T.DoubleType()).alias("AvgValue"),
                    F.sum("Score").cast(T.IntegerType()).alias("TotalScore"),
                )
                .select("Name", "AvgValue", "TotalScore")
            )

            rows = result.collect()
            assert len(rows) == 2
            assert rows[0]["Name"] == "Alice"
            assert rows[0]["AvgValue"] == 1.5
            assert rows[0]["TotalScore"] == 30
        finally:
            spark.stop()

    def test_cast_alias_select_different_cast_types(self):
        """Test different cast types with alias."""
        spark = SparkSession.builder.appName("issue-332").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 2},
                ]
            )

            # Test StringType cast
            result1 = (
                df.groupBy("Name")
                .agg(F.mean("Value").cast(T.StringType()).alias("AvgValueStr"))
                .select("Name", "AvgValueStr")
            )
            rows1 = result1.collect()
            assert len(rows1) == 1
            assert isinstance(rows1[0]["AvgValueStr"], str)

            # Test IntegerType cast
            result2 = (
                df.groupBy("Name")
                .agg(F.sum("Value").cast(T.IntegerType()).alias("SumValue"))
                .select("Name", "SumValue")
            )
            rows2 = result2.collect()
            assert len(rows2) == 1
            assert isinstance(rows2[0]["SumValue"], int)
        finally:
            spark.stop()

    def test_cast_alias_select_with_withcolumn(self):
        """Test cast+alias in withColumn then select."""
        spark = SparkSession.builder.appName("issue-332").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 20},
                ]
            )

            # withColumn uses the first argument as column name, alias is ignored
            result = df.withColumn(
                "ValueDouble", F.col("Value").cast(T.DoubleType())
            ).select("Name", "ValueDouble")

            rows = result.collect()
            assert len(rows) == 2
            assert "ValueDouble" in result.columns
            assert rows[0]["ValueDouble"] == 10.0
        finally:
            spark.stop()

    def test_cast_alias_select_nested_operations(self):
        """Test nested operations: cast+alias in groupBy then select."""
        spark = SparkSession.builder.appName("issue-332").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1, "Category": "A"},
                    {"Name": "Alice", "Value": 2, "Category": "A"},
                    {"Name": "Bob", "Value": 3, "Category": "B"},
                    {"Name": "Bob", "Value": 4, "Category": "B"},
                ]
            )

            result = (
                df.groupBy("Name", "Category")
                .agg(F.mean("Value").cast(T.DoubleType()).alias("AvgValue"))
                .select("Name", "Category", "AvgValue")
            )

            rows = result.collect()
            assert len(rows) == 2
            assert rows[0]["Name"] == "Alice"
            assert rows[0]["AvgValue"] == 1.5
        finally:
            spark.stop()

    def test_cast_alias_select_with_filter(self):
        """Test cast+alias+select with filter."""
        spark = SparkSession.builder.appName("issue-332").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 2},
                    {"Name": "Bob", "Value": 3},
                    {"Name": "Bob", "Value": 4},
                ]
            )

            result = (
                df.groupBy("Name")
                .agg(F.mean("Value").cast(T.DoubleType()).alias("AvgValue"))
                .filter(F.col("AvgValue") > 2.0)
                .select("Name", "AvgValue")
            )

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Name"] == "Bob"
            assert rows[0]["AvgValue"] == 3.5
        finally:
            spark.stop()

    def test_cast_alias_select_with_orderby(self):
        """Test cast+alias+select with orderBy."""
        spark = SparkSession.builder.appName("issue-332").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 2},
                    {"Name": "Bob", "Value": 3},
                    {"Name": "Bob", "Value": 4},
                ]
            )

            result = (
                df.groupBy("Name")
                .agg(F.mean("Value").cast(T.DoubleType()).alias("AvgValue"))
                .orderBy("AvgValue", ascending=False)
                .select("Name", "AvgValue")
            )

            rows = result.collect()
            assert len(rows) == 2
            assert rows[0]["Name"] == "Bob"
            assert rows[0]["AvgValue"] == 3.5
            assert rows[1]["Name"] == "Alice"
            assert rows[1]["AvgValue"] == 1.5
        finally:
            spark.stop()

    def test_cast_alias_select_backward_compatibility(self):
        """Test that aggregation+alias without cast still works (backward compatibility)."""
        spark = SparkSession.builder.appName("issue-332").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 2},
                ]
            )

            result = (
                df.groupBy("Name")
                .agg(F.mean("Value").alias("AvgValue"))
                .select("Name", "AvgValue")
            )

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Name"] == "Alice"
            assert rows[0]["AvgValue"] == 1.5
        finally:
            spark.stop()

    def test_cast_alias_select_empty_dataframe(self):
        """Test cast+alias+select on empty DataFrame."""
        spark = SparkSession.builder.appName("issue-332").getOrCreate()
        try:
            from sparkless.spark_types import (
                StructType,
                StructField,
                StringType,
                IntegerType,
            )

            schema = StructType(
                [
                    StructField("Name", StringType(), True),
                    StructField("Value", IntegerType(), True),
                ]
            )
            df = spark.createDataFrame([], schema=schema)

            result = (
                df.groupBy("Name")
                .agg(F.mean("Value").cast(T.DoubleType()).alias("AvgValue"))
                .select("Name", "AvgValue")
            )

            rows = result.collect()
            assert len(rows) == 0
            assert "AvgValue" in result.columns
        finally:
            spark.stop()

    def test_cast_alias_select_all_null_values(self):
        """Test cast+alias+select when all values are null."""
        spark = SparkSession.builder.appName("issue-332").getOrCreate()
        try:
            from sparkless.spark_types import (
                StructType,
                StructField,
                StringType,
                IntegerType,
            )

            schema = StructType(
                [
                    StructField("Name", StringType(), True),
                    StructField("Value", IntegerType(), True),
                ]
            )
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": None},
                    {"Name": "Alice", "Value": None},
                ],
                schema=schema,
            )

            result = (
                df.groupBy("Name")
                .agg(F.mean("Value").cast(T.DoubleType()).alias("AvgValue"))
                .select("Name", "AvgValue")
            )

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Name"] == "Alice"
            assert rows[0]["AvgValue"] is None
        finally:
            spark.stop()

    def test_cast_alias_select_mixed_null_values(self):
        """Test cast+alias+select with mixed null and non-null values."""
        spark = SparkSession.builder.appName("issue-332").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": None},
                    {"Name": "Alice", "Value": 3},
                ]
            )

            result = (
                df.groupBy("Name")
                .agg(F.mean("Value").cast(T.DoubleType()).alias("AvgValue"))
                .select("Name", "AvgValue")
            )

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Name"] == "Alice"
            # Mean of [1, None, 3] = 2.0 (nulls are ignored)
            assert rows[0]["AvgValue"] == 2.0
        finally:
            spark.stop()

    def test_cast_alias_select_all_aggregation_functions(self):
        """Test cast+alias+select with all major aggregation functions."""
        spark = SparkSession.builder.appName("issue-332").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 2},
                    {"Name": "Alice", "Value": 3},
                ]
            )

            result = (
                df.groupBy("Name")
                .agg(
                    F.count("Value").cast(T.IntegerType()).alias("Count"),
                    F.sum("Value").cast(T.IntegerType()).alias("Sum"),
                    F.avg("Value").cast(T.DoubleType()).alias("Avg"),
                    F.min("Value").cast(T.IntegerType()).alias("Min"),
                    F.max("Value").cast(T.IntegerType()).alias("Max"),
                )
                .select("Name", "Count", "Sum", "Avg", "Min", "Max")
            )

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Count"] == 3
            assert rows[0]["Sum"] == 6
            assert rows[0]["Avg"] == 2.0
            assert rows[0]["Min"] == 1
            assert rows[0]["Max"] == 3
        finally:
            spark.stop()

    def test_cast_alias_select_complex_nested_operations(self):
        """Test cast+alias+select with complex nested operations."""
        spark = SparkSession.builder.appName("issue-332").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1, "Category": "A", "Score": 10},
                    {"Name": "Alice", "Value": 2, "Category": "A", "Score": 20},
                    {"Name": "Bob", "Value": 3, "Category": "B", "Score": 30},
                ]
            )

            result = (
                df.groupBy("Name", "Category")
                .agg(
                    F.mean("Value").cast(T.DoubleType()).alias("AvgValue"),
                    F.sum("Score").cast(T.IntegerType()).alias("TotalScore"),
                )
                .filter(F.col("AvgValue") > 1.0)
                .orderBy("AvgValue", ascending=False)
                .select("Name", "Category", "AvgValue", "TotalScore")
            )

            rows = result.collect()
            assert len(rows) == 2
            assert rows[0]["Name"] == "Bob"
            assert rows[0]["AvgValue"] == 3.0
        finally:
            spark.stop()

    def test_cast_alias_select_with_join(self):
        """Test cast+alias+select with join operations."""
        spark = SparkSession.builder.appName("issue-332").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"ID": 1, "Value": 10},
                    {"ID": 1, "Value": 20},
                    {"ID": 2, "Value": 30},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"ID": 1, "Name": "Alice"},
                    {"ID": 2, "Name": "Bob"},
                ]
            )

            result = (
                df1.groupBy("ID")
                .agg(F.mean("Value").cast(T.DoubleType()).alias("AvgValue"))
                .join(df2, on="ID", how="inner")
                .select("Name", "AvgValue")
            )

            rows = result.collect()
            assert len(rows) == 2
            names = {row["Name"]: row["AvgValue"] for row in rows}
            assert names["Alice"] == 15.0
            assert names["Bob"] == 30.0
        finally:
            spark.stop()

    def test_cast_alias_select_with_union(self):
        """Test cast+alias+select with union operations."""
        spark = SparkSession.builder.appName("issue-332").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 2},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Name": "Bob", "Value": 3},
                    {"Name": "Bob", "Value": 4},
                ]
            )

            result1 = (
                df1.groupBy("Name")
                .agg(F.mean("Value").cast(T.DoubleType()).alias("AvgValue"))
                .select("Name", "AvgValue")
            )

            result2 = (
                df2.groupBy("Name")
                .agg(F.mean("Value").cast(T.DoubleType()).alias("AvgValue"))
                .select("Name", "AvgValue")
            )

            union_result = result1.union(result2)
            rows = union_result.collect()

            assert len(rows) == 2
            names = {row["Name"]: row["AvgValue"] for row in rows}
            assert names["Alice"] == 1.5
            assert names["Bob"] == 3.5
        finally:
            spark.stop()

    def test_cast_alias_select_with_window_function(self):
        """Test cast+alias+select with window functions."""
        spark = SparkSession.builder.appName("issue-332").getOrCreate()
        try:
            from sparkless.window import Window

            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1, "Category": "A"},
                    {"Name": "Alice", "Value": 2, "Category": "A"},
                    {"Name": "Bob", "Value": 3, "Category": "B"},
                ]
            )

            # Window function needs to use columns available after groupBy
            window_spec = Window.partitionBy("Category").orderBy("AvgValue")
            result = (
                df.groupBy("Name", "Category")
                .agg(F.mean("Value").cast(T.DoubleType()).alias("AvgValue"))
                .withColumn("Rank", F.row_number().over(window_spec))
                .select("Name", "Category", "AvgValue", "Rank")
            )

            rows = result.collect()
            assert len(rows) == 2
            assert "AvgValue" in result.columns
            assert "Rank" in result.columns
        finally:
            spark.stop()

    def test_cast_alias_select_multiple_casts_same_column(self):
        """Test multiple casts on the same column with different aliases."""
        spark = SparkSession.builder.appName("issue-332").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 2},
                ]
            )

            result = (
                df.groupBy("Name")
                .agg(
                    F.mean("Value").cast(T.DoubleType()).alias("AvgValueDouble"),
                    F.mean("Value").cast(T.StringType()).alias("AvgValueString"),
                    F.mean("Value").cast(T.IntegerType()).alias("AvgValueInt"),
                )
                .select("Name", "AvgValueDouble", "AvgValueString", "AvgValueInt")
            )

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["AvgValueDouble"] == 1.5
            assert isinstance(rows[0]["AvgValueString"], str)
            assert rows[0]["AvgValueInt"] == 1  # or 2, depending on rounding
        finally:
            spark.stop()

    def test_cast_alias_select_schema_verification(self):
        """Test that schema correctly reflects aliased cast columns."""
        spark = SparkSession.builder.appName("issue-332").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 2},
                ]
            )

            result = (
                df.groupBy("Name")
                .agg(F.mean("Value").cast(T.DoubleType()).alias("AvgValue"))
                .select("Name", "AvgValue")
            )

            # Verify schema
            assert "AvgValue" in result.columns
            assert "CAST(avg(Value) AS" not in " ".join(result.columns)

            # Verify data type in schema
            field = next(f for f in result.schema.fields if f.name == "AvgValue")
            assert field is not None
            from sparkless.spark_types import DoubleType

            assert isinstance(field.dataType, DoubleType)
        finally:
            spark.stop()

    def test_cast_alias_select_with_limit(self):
        """Test cast+alias+select with limit operation."""
        spark = SparkSession.builder.appName("issue-332").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 2},
                    {"Name": "Bob", "Value": 3},
                    {"Name": "Bob", "Value": 4},
                    {"Name": "Charlie", "Value": 5},
                ]
            )

            result = (
                df.groupBy("Name")
                .agg(F.mean("Value").cast(T.DoubleType()).alias("AvgValue"))
                .orderBy("AvgValue", ascending=False)
                .limit(2)
                .select("Name", "AvgValue")
            )

            rows = result.collect()
            assert len(rows) == 2
            assert rows[0]["Name"] == "Charlie"
            assert rows[0]["AvgValue"] == 5.0
        finally:
            spark.stop()

    def test_cast_alias_select_with_distinct(self):
        """Test cast+alias+select with distinct operation."""
        spark = SparkSession.builder.appName("issue-332").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                ]
            )

            result = (
                df.groupBy("Name")
                .agg(F.mean("Value").cast(T.DoubleType()).alias("AvgValue"))
                .select("AvgValue")
                .distinct()
            )

            rows = result.collect()
            assert len(rows) == 2  # Two distinct average values
        finally:
            spark.stop()

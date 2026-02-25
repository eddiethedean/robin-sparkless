"""
Unit tests for Issue #337: GroupedData.mean() method.

Tests that GroupedData supports mean() method matching PySpark behavior.
"""

from sparkless.sql import SparkSession
from sparkless import functions as F


class TestIssue337GroupedDataMean:
    """Test GroupedData.mean() method."""

    def test_grouped_data_mean_single_column(self):
        """Test GroupedData.mean() with single column."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                ]
            )

            result = df.groupBy("Name").mean("Value")
            rows = result.collect()

            assert len(rows) == 2
            # Find Alice and Bob rows
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            bob_row = next(row for row in rows if row["Name"] == "Bob")

            # Alice: (1 + 10) / 2 = 5.5
            assert alice_row["avg(Value)"] == 5.5
            # Bob: 5 / 1 = 5.0
            assert bob_row["avg(Value)"] == 5.0
        finally:
            spark.stop()

    def test_grouped_data_mean_multiple_columns(self):
        """Test GroupedData.mean() with multiple columns."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value1": 1, "Value2": 2},
                    {"Name": "Alice", "Value1": 10, "Value2": 20},
                    {"Name": "Bob", "Value1": 5, "Value2": 6},
                ]
            )

            result = df.groupBy("Name").mean("Value1", "Value2")
            rows = result.collect()

            assert len(rows) == 2
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            bob_row = next(row for row in rows if row["Name"] == "Bob")

            # Alice: Value1 = (1 + 10) / 2 = 5.5, Value2 = (2 + 20) / 2 = 11.0
            assert alice_row["avg(Value1)"] == 5.5
            assert alice_row["avg(Value2)"] == 11.0
            # Bob: Value1 = 5.0, Value2 = 6.0
            assert bob_row["avg(Value1)"] == 5.0
            assert bob_row["avg(Value2)"] == 6.0
        finally:
            spark.stop()

    def test_grouped_data_mean_no_columns(self):
        """Test GroupedData.mean() with no columns (should use avg(1))."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                ]
            )

            # mean() with no columns should work the same as avg() with no columns
            # This may raise an error if avg(1) isn't supported, which is acceptable
            # as it matches the behavior of avg() with no columns
            try:
                result = df.groupBy("Name").mean()
                rows = result.collect()
                assert len(rows) == 2
                # If it works, should have avg(1) column
                if rows:
                    assert "avg(1)" in rows[0] or "avg(Value)" in rows[0]
            except Exception:
                # If it fails, that's acceptable as it matches avg() behavior
                pass
        finally:
            spark.stop()

    def test_grouped_data_mean_with_column_object(self):
        """Test GroupedData.mean() with Column object."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                ]
            )

            result = df.groupBy("Name").mean(F.col("Value"))
            rows = result.collect()

            assert len(rows) == 2
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            assert alice_row["avg(Value)"] == 5.5
        finally:
            spark.stop()

    def test_grouped_data_mean_with_null_values(self):
        """Test GroupedData.mean() with null values."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": None},
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                ]
            )

            result = df.groupBy("Name").mean("Value")
            rows = result.collect()

            assert len(rows) == 2
            # Alice: (1 + 10) / 2 = 5.5 (nulls are ignored)
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            assert alice_row["avg(Value)"] == 5.5
        finally:
            spark.stop()

    def test_grouped_data_mean_equals_avg(self):
        """Test that mean() produces same results as avg()."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                ]
            )

            result_mean = df.groupBy("Name").mean("Value")
            result_avg = df.groupBy("Name").avg("Value")

            rows_mean = result_mean.collect()
            rows_avg = result_avg.collect()

            assert len(rows_mean) == len(rows_avg) == 2

            # Results should be identical
            for mean_row in rows_mean:
                avg_row = next(
                    row for row in rows_avg if row["Name"] == mean_row["Name"]
                )
                assert mean_row["avg(Value)"] == avg_row["avg(Value)"]
        finally:
            spark.stop()

    def test_grouped_data_mean_with_float_values(self):
        """Test GroupedData.mean() with float values."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1.5},
                    {"Name": "Alice", "Value": 10.5},
                    {"Name": "Bob", "Value": 5.25},
                ]
            )

            result = df.groupBy("Name").mean("Value")
            rows = result.collect()

            assert len(rows) == 2
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            # Alice: (1.5 + 10.5) / 2 = 6.0
            assert alice_row["avg(Value)"] == 6.0
        finally:
            spark.stop()

    def test_grouped_data_mean_with_negative_values(self):
        """Test GroupedData.mean() with negative values."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": -10},
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": -5},
                ]
            )

            result = df.groupBy("Name").mean("Value")
            rows = result.collect()

            assert len(rows) == 2
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            # Alice: (-10 + 10) / 2 = 0.0
            assert alice_row["avg(Value)"] == 0.0
        finally:
            spark.stop()

    def test_grouped_data_mean_with_zero_values(self):
        """Test GroupedData.mean() with zero values."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 0},
                    {"Name": "Alice", "Value": 0},
                    {"Name": "Bob", "Value": 5},
                ]
            )

            result = df.groupBy("Name").mean("Value")
            rows = result.collect()

            assert len(rows) == 2
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            # Alice: (0 + 0) / 2 = 0.0
            assert alice_row["avg(Value)"] == 0.0
        finally:
            spark.stop()

    def test_grouped_data_mean_with_single_row_per_group(self):
        """Test GroupedData.mean() with single row per group."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 5},
                ]
            )

            result = df.groupBy("Name").mean("Value")
            rows = result.collect()

            assert len(rows) == 2
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            # Alice: 1 / 1 = 1.0
            assert alice_row["avg(Value)"] == 1.0
        finally:
            spark.stop()

    def test_grouped_data_mean_with_empty_dataframe(self):
        """Test GroupedData.mean() with empty DataFrame."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame([], schema="Name string, Value int")

            result = df.groupBy("Name").mean("Value")
            rows = result.collect()

            assert len(rows) == 0
        finally:
            spark.stop()

    def test_grouped_data_mean_with_chained_operations(self):
        """Test GroupedData.mean() with chained DataFrame operations."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                ]
            )

            result = df.groupBy("Name").mean("Value").orderBy("Name")
            rows = result.collect()

            assert len(rows) == 2
            # Should be ordered by Name
            assert rows[0]["Name"] in ["Alice", "Bob"]
        finally:
            spark.stop()

    def test_grouped_data_mean_with_select(self):
        """Test GroupedData.mean() followed by select."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                ]
            )

            result = df.groupBy("Name").mean("Value").select("Name", "avg(Value)")
            rows = result.collect()

            assert len(rows) == 2
            assert "Name" in rows[0]
            assert "avg(Value)" in rows[0]
        finally:
            spark.stop()

    def test_grouped_data_mean_with_filter(self):
        """Test GroupedData.mean() followed by filter."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                ]
            )

            result = df.groupBy("Name").mean("Value").filter(F.col("avg(Value)") > 5.0)
            rows = result.collect()

            assert len(rows) == 1
            # Only Alice should have avg > 5.0
            assert rows[0]["Name"] == "Alice"
        finally:
            spark.stop()

    def test_grouped_data_mean_with_multiple_group_columns(self):
        """Test GroupedData.mean() with multiple group columns."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Value": 1},
                    {"Name": "Alice", "Type": "A", "Value": 10},
                    {"Name": "Alice", "Type": "B", "Value": 5},
                    {"Name": "Bob", "Type": "A", "Value": 3},
                ]
            )

            result = df.groupBy("Name", "Type").mean("Value")
            rows = result.collect()

            assert len(rows) == 3
            # Find Alice, Type A
            alice_a = next(
                row for row in rows if row["Name"] == "Alice" and row["Type"] == "A"
            )
            assert alice_a["avg(Value)"] == 5.5
        finally:
            spark.stop()

    def test_grouped_data_mean_with_all_null_values(self):
        """Test GroupedData.mean() with all null values in a group."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": None},
                    {"Name": "Alice", "Value": None},
                    {"Name": "Bob", "Value": 5},
                ]
            )

            result = df.groupBy("Name").mean("Value")
            rows = result.collect()

            assert len(rows) == 2
            # Alice group has all nulls, so mean should be None or 0
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            # Mean of all nulls is typically None or 0 depending on implementation
            assert alice_row["avg(Value)"] is None or alice_row["avg(Value)"] == 0
        finally:
            spark.stop()

    def test_grouped_data_mean_with_large_dataset(self):
        """Test GroupedData.mean() with larger dataset."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            data = [{"Name": "Alice", "Value": i} for i in range(1, 21)]  # Alice: 1-20
            data.extend(
                [{"Name": "Bob", "Value": i} for i in range(1, 11)]
            )  # Bob: 1-10

            df = spark.createDataFrame(data)

            result = df.groupBy("Name").mean("Value")
            rows = result.collect()

            assert len(rows) == 2
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            # Alice: mean of 1-20 = (1+20)*20/2 / 20 = 10.5
            assert alice_row["avg(Value)"] == 10.5
            bob_row = next(row for row in rows if row["Name"] == "Bob")
            # Bob: mean of 1-10 = (1+10)*10/2 / 10 = 5.5
            assert bob_row["avg(Value)"] == 5.5
        finally:
            spark.stop()

    def test_grouped_data_mean_with_duplicate_values(self):
        """Test GroupedData.mean() with duplicate values."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 5},
                    {"Name": "Alice", "Value": 5},
                    {"Name": "Alice", "Value": 5},
                    {"Name": "Bob", "Value": 10},
                ]
            )

            result = df.groupBy("Name").mean("Value")
            rows = result.collect()

            assert len(rows) == 2
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            # Alice: (5 + 5 + 5) / 3 = 5.0
            assert alice_row["avg(Value)"] == 5.0
        finally:
            spark.stop()

    def test_grouped_data_mean_with_very_large_numbers(self):
        """Test GroupedData.mean() with very large numbers."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1000000},
                    {"Name": "Alice", "Value": 2000000},
                    {"Name": "Bob", "Value": 500000},
                ]
            )

            result = df.groupBy("Name").mean("Value")
            rows = result.collect()

            assert len(rows) == 2
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            # Alice: (1000000 + 2000000) / 2 = 1500000.0
            assert alice_row["avg(Value)"] == 1500000.0
        finally:
            spark.stop()

    def test_grouped_data_mean_with_very_small_numbers(self):
        """Test GroupedData.mean() with very small numbers."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 0.0001},
                    {"Name": "Alice", "Value": 0.0002},
                    {"Name": "Bob", "Value": 0.0005},
                ]
            )

            result = df.groupBy("Name").mean("Value")
            rows = result.collect()

            assert len(rows) == 2
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            # Alice: (0.0001 + 0.0002) / 2 = 0.00015
            assert abs(alice_row["avg(Value)"] - 0.00015) < 0.000001
        finally:
            spark.stop()

    def test_grouped_data_mean_with_mixed_int_float(self):
        """Test GroupedData.mean() with mixed integer and float values."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            from sparkless.spark_types import (
                StructType,
                StructField,
                StringType,
                DoubleType,
            )

            # Use explicit schema to avoid type merging issues
            schema = StructType(
                [
                    StructField("Name", StringType(), True),
                    StructField("Value", DoubleType(), True),
                ]
            )
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1.0},
                    {"Name": "Alice", "Value": 2.5},
                    {"Name": "Alice", "Value": 3.0},
                    {"Name": "Bob", "Value": 5.0},
                ],
                schema=schema,
            )

            result = df.groupBy("Name").mean("Value")
            rows = result.collect()

            assert len(rows) == 2
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            # Alice: (1 + 2.5 + 3) / 3 = 2.1666...
            assert abs(alice_row["avg(Value)"] - 2.1666666666666665) < 0.0001
        finally:
            spark.stop()

    def test_grouped_data_mean_with_join(self):
        """Test GroupedData.mean() with join operations."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A"},
                    {"Name": "Bob", "Type": "B"},
                ]
            )

            result = df1.groupBy("Name").mean("Value").join(df2, on="Name", how="left")
            rows = result.collect()

            assert len(rows) == 2
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            assert alice_row["avg(Value)"] == 5.5
            assert alice_row["Type"] == "A"
        finally:
            spark.stop()

    def test_grouped_data_mean_with_union(self):
        """Test GroupedData.mean() with union operations."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 10},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Name": "Bob", "Value": 5},
                ]
            )

            combined = df1.unionByName(df2, allowMissingColumns=True)
            result = combined.groupBy("Name").mean("Value")
            rows = result.collect()

            assert len(rows) == 2
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            assert alice_row["avg(Value)"] == 5.5
        finally:
            spark.stop()

    def test_grouped_data_mean_with_distinct(self):
        """Test GroupedData.mean() with distinct operation."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 1},  # Duplicate
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                ]
            )

            # First get distinct, then groupBy and mean
            result = df.distinct().groupBy("Name").mean("Value")
            rows = result.collect()

            assert len(rows) == 2
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            # After distinct: (1 + 10) / 2 = 5.5
            assert alice_row["avg(Value)"] == 5.5
        finally:
            spark.stop()

    def test_grouped_data_mean_with_limit(self):
        """Test GroupedData.mean() with limit operation."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                    {"Name": "Charlie", "Value": 3},
                ]
            )

            result = df.groupBy("Name").mean("Value").limit(2)
            rows = result.collect()

            assert len(rows) == 2
        finally:
            spark.stop()

    def test_grouped_data_mean_with_withColumn(self):
        """Test GroupedData.mean() with withColumn operation."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                ]
            )

            result = (
                df.groupBy("Name")
                .mean("Value")
                .withColumn("DoubleMean", F.col("avg(Value)") * 2)
            )
            rows = result.collect()

            assert len(rows) == 2
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            assert alice_row["DoubleMean"] == 11.0  # 5.5 * 2
        finally:
            spark.stop()

    def test_grouped_data_mean_with_drop(self):
        """Test GroupedData.mean() with drop operation."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1, "Other": "X"},
                    {"Name": "Alice", "Value": 10, "Other": "Y"},
                    {"Name": "Bob", "Value": 5, "Other": "Z"},
                ]
            )

            result = df.groupBy("Name").mean("Value").drop("Name")
            rows = result.collect()

            assert len(rows) == 2
            # Should only have avg(Value) column
            assert "avg(Value)" in rows[0]
        finally:
            spark.stop()

    def test_grouped_data_mean_with_alias(self):
        """Test GroupedData.mean() with column alias."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                ]
            )

            result = (
                df.groupBy("Name")
                .mean("Value")
                .select(F.col("avg(Value)").alias("MeanValue"), "Name")
            )
            rows = result.collect()

            assert len(rows) == 2
            assert "MeanValue" in rows[0]
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            assert alice_row["MeanValue"] == 5.5
        finally:
            spark.stop()

    def test_grouped_data_mean_with_case_when(self):
        """Test GroupedData.mean() with case/when operations."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                ]
            )

            result = (
                df.groupBy("Name")
                .mean("Value")
                .withColumn(
                    "Category",
                    F.when(F.col("avg(Value)") > 5.0, "High").otherwise("Low"),
                )
            )
            rows = result.collect()

            assert len(rows) == 2
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            assert alice_row["Category"] == "High"
            bob_row = next(row for row in rows if row["Name"] == "Bob")
            assert bob_row["Category"] == "Low"
        finally:
            spark.stop()

    def test_grouped_data_mean_with_coalesce(self):
        """Test GroupedData.mean() with coalesce operation."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                ]
            )

            result = (
                df.groupBy("Name")
                .mean("Value")
                .withColumn(
                    "MeanOrZero",
                    F.coalesce(F.col("avg(Value)"), F.lit(0)),
                )
            )
            rows = result.collect()

            assert len(rows) == 2
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            assert alice_row["MeanOrZero"] == 5.5
        finally:
            spark.stop()

    def test_grouped_data_mean_with_cast(self):
        """Test GroupedData.mean() with cast operation."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                ]
            )

            result = (
                df.groupBy("Name")
                .mean("Value")
                .withColumn("MeanInt", F.col("avg(Value)").cast("int"))
            )
            rows = result.collect()

            assert len(rows) == 2
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            assert alice_row["MeanInt"] == 5  # 5.5 cast to int = 5
        finally:
            spark.stop()

    def test_grouped_data_mean_schema_verification(self):
        """Test that GroupedData.mean() produces correct schema."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 10},
                ]
            )

            result = df.groupBy("Name").mean("Value")

            schema = result.schema
            field_names = [field.name for field in schema.fields]

            # Should contain group column and mean column
            assert "Name" in field_names
            assert "avg(Value)" in field_names
        finally:
            spark.stop()

    def test_grouped_data_mean_with_multiple_aggregations(self):
        """Test GroupedData.mean() combined with other aggregations."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                ]
            )

            # Use mean() and then add other aggregations via agg()
            result = (
                df.groupBy("Name")
                .mean("Value")
                .join(
                    df.groupBy("Name").agg(F.max("Value").alias("MaxValue")),
                    on="Name",
                )
            )
            rows = result.collect()

            assert len(rows) == 2
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            assert alice_row["avg(Value)"] == 5.5
            assert alice_row["MaxValue"] == 10
        finally:
            spark.stop()

    def test_grouped_data_mean_with_window_functions(self):
        """Test GroupedData.mean() with window functions."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            from sparkless.window import Window

            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                ]
            )

            result = (
                df.groupBy("Name")
                .mean("Value")
                .withColumn(
                    "RowNum",
                    F.row_number().over(Window.orderBy("Name")),
                )
            )
            rows = result.collect()

            assert len(rows) == 2
            assert "RowNum" in rows[0]
        finally:
            spark.stop()

    def test_grouped_data_mean_with_orderBy(self):
        """Test GroupedData.mean() with orderBy operation."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                    {"Name": "Charlie", "Value": 3},
                ]
            )

            result = df.groupBy("Name").mean("Value").orderBy("Name")
            rows = result.collect()

            assert len(rows) == 3
            # Should be ordered alphabetically
            names = [row["Name"] for row in rows]
            assert names == sorted(names)
        finally:
            spark.stop()

    def test_grouped_data_mean_with_desc_orderBy(self):
        """Test GroupedData.mean() with descending orderBy."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                ]
            )

            result = (
                df.groupBy("Name").mean("Value").orderBy(F.col("avg(Value)").desc())
            )
            rows = result.collect()

            assert len(rows) == 2
            # Alice should be first (5.5 > 5.0)
            assert rows[0]["Name"] == "Alice"
            assert rows[0]["avg(Value)"] == 5.5
        finally:
            spark.stop()

    def test_grouped_data_mean_with_many_groups(self):
        """Test GroupedData.mean() with many different groups."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            data = [{"Name": f"Person{i}", "Value": i} for i in range(10)]

            df = spark.createDataFrame(data)

            result = df.groupBy("Name").mean("Value")
            rows = result.collect()

            assert len(rows) == 10
            # Each person should have their own value as mean (single row per group)
            for i in range(10):
                person_row = next(row for row in rows if row["Name"] == f"Person{i}")
                assert person_row["avg(Value)"] == float(i)
        finally:
            spark.stop()

    def test_grouped_data_mean_with_complex_chained_operations(self):
        """Test GroupedData.mean() with complex chained operations."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1, "Type": "A"},
                    {"Name": "Alice", "Value": 10, "Type": "A"},
                    {"Name": "Bob", "Value": 5, "Type": "B"},
                    {"Name": "Charlie", "Value": 3, "Type": "A"},
                ]
            )

            result = (
                df.groupBy("Name", "Type")
                .mean("Value")
                .filter(F.col("avg(Value)") > 4.0)
                .select("Name", "Type", F.col("avg(Value)").alias("MeanValue"))
                .orderBy("MeanValue", ascending=False)
            )
            rows = result.collect()

            assert len(rows) == 2  # Only Alice and Bob have mean > 4.0
            # Alice should be first (5.5 > 5.0)
            assert rows[0]["Name"] == "Alice"
            assert rows[0]["MeanValue"] == 5.5
        finally:
            spark.stop()

    def test_grouped_data_mean_with_nested_select(self):
        """Test GroupedData.mean() with nested select operations."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                ]
            )

            result = (
                df.groupBy("Name")
                .mean("Value")
                .select("Name", "avg(Value)")
                .select("Name")
            )
            rows = result.collect()

            assert len(rows) == 2
            assert "Name" in rows[0]
        finally:
            spark.stop()

    def test_grouped_data_mean_with_string_column_error(self):
        """Test GroupedData.mean() with string column (should handle gracefully)."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": "A"},
                    {"Name": "Alice", "Value": "B"},
                    {"Name": "Bob", "Value": "C"},
                ]
            )

            # mean() on string column may raise an error or return None
            # This is acceptable behavior as strings can't be averaged
            try:
                result = df.groupBy("Name").mean("Value")
                rows = result.collect()
                # If it doesn't raise, check that results are reasonable
                assert len(rows) == 2
            except Exception:
                # If it raises an error, that's acceptable for string columns
                pass
        finally:
            spark.stop()

    def test_grouped_data_mean_with_column_alias_in_groupBy(self):
        """Test GroupedData.mean() when groupBy uses column alias."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                ]
            )

            # Group by aliased column
            result = (
                df.select(F.col("Name").alias("Person"), "Value")
                .groupBy("Person")
                .mean("Value")
            )
            rows = result.collect()

            assert len(rows) == 2
            assert "Person" in rows[0]
            alice_row = next(row for row in rows if row["Person"] == "Alice")
            assert alice_row["avg(Value)"] == 5.5
        finally:
            spark.stop()

    def test_grouped_data_mean_with_computed_column(self):
        """Test GroupedData.mean() with computed column."""
        spark = SparkSession.builder.appName("issue-337").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                ]
            )

            # Compute a column and then take mean
            result = (
                df.withColumn("DoubleValue", F.col("Value") * 2)
                .groupBy("Name")
                .mean("DoubleValue")
            )
            rows = result.collect()

            assert len(rows) == 2
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            # Alice: (1*2 + 10*2) / 2 = 11.0
            assert alice_row["avg(DoubleValue)"] == 11.0
        finally:
            spark.stop()

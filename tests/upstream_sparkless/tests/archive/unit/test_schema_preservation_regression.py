"""
Tests for Regression 3: Schema Preservation During Transformations

Tests that empty DataFrames preserve their schema through transformations (select, filter, withColumn, etc.).
"""

from sparkless import SparkSession, F
from sparkless.spark_types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)


class TestSchemaPreservationRegression:
    """Test schema preservation during DataFrame transformations."""

    def test_empty_dataframe_select_preserves_schema(self):
        """Test that select operation preserves schema for empty DataFrames."""
        spark = SparkSession("test")

        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
                StructField("score", DoubleType()),
            ]
        )

        empty_df = spark.createDataFrame([], schema)

        # Select should preserve selected columns
        result_df = empty_df.select("id", "name")

        assert len(result_df.columns) == 2
        assert "id" in result_df.columns
        assert "name" in result_df.columns
        assert "score" not in result_df.columns

    def test_empty_dataframe_filter_preserves_schema(self):
        """Test that filter operation preserves schema for empty DataFrames."""
        spark = SparkSession("test")

        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
            ]
        )

        empty_df = spark.createDataFrame([], schema)

        # Filter should preserve all columns
        result_df = empty_df.filter(F.col("id") > 0)

        assert len(result_df.columns) == 2
        assert "id" in result_df.columns
        assert "name" in result_df.columns

    def test_empty_dataframe_withcolumn_preserves_schema(self):
        """Test that withColumn operation preserves schema for empty DataFrames."""
        spark = SparkSession("test")

        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
            ]
        )

        empty_df = spark.createDataFrame([], schema)

        # withColumn should preserve existing columns and add new one
        result_df = empty_df.withColumn("score", F.lit(0.0))

        assert len(result_df.columns) == 3
        assert "id" in result_df.columns
        assert "name" in result_df.columns
        assert "score" in result_df.columns

    def test_empty_dataframe_multiple_transformations(self):
        """Test that multiple transformations preserve schema for empty DataFrames."""
        spark = SparkSession("test")

        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
                StructField("value", DoubleType()),
            ]
        )

        empty_df = spark.createDataFrame([], schema)

        # Apply multiple transformations
        result_df = (
            empty_df.select("id", "name")
            .withColumn("new_col", F.lit("test"))
            .filter(F.col("id") > 0)
        )

        assert len(result_df.columns) == 3
        assert "id" in result_df.columns
        assert "name" in result_df.columns
        assert "new_col" in result_df.columns

    def test_empty_dataframe_join_preserves_schema(self):
        """Test that join operation preserves schema for empty DataFrames."""
        spark = SparkSession("test")

        schema1 = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
            ]
        )

        schema2 = StructType(
            [
                StructField("id", IntegerType()),
                StructField("score", DoubleType()),
            ]
        )

        empty_df1 = spark.createDataFrame([], schema1)
        empty_df2 = spark.createDataFrame([], schema2)

        # Join should preserve columns from both DataFrames
        result_df = empty_df1.join(empty_df2, on="id", how="inner")

        # Should have columns from both DataFrames (id appears once, name from df1, score from df2)
        assert "id" in result_df.columns
        assert "name" in result_df.columns
        assert "score" in result_df.columns

    def test_empty_bronze_to_silver_pipeline(self):
        """Test that empty bronze DataFrame preserves schema through silver step.

        This simulates the pipeline scenario mentioned in the regression report.
        """
        spark = SparkSession("test")

        # Bronze schema
        bronze_schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("raw_data", StringType()),
            ]
        )

        # Create empty bronze DataFrame
        bronze_df = spark.createDataFrame([], bronze_schema)

        # Silver step - should preserve columns even though DataFrame is empty
        silver_df = bronze_df.select("id", F.col("raw_data").alias("processed_data"))

        # Should have columns from silver transformation
        assert len(silver_df.columns) == 2
        assert "id" in silver_df.columns
        assert "processed_data" in silver_df.columns

    def test_empty_bronze_to_silver_to_gold_pipeline(self):
        """Test that empty bronze DataFrame preserves schema through silver and gold steps.

        This simulates the complete pipeline scenario mentioned in the regression report.
        """
        spark = SparkSession("test")

        # Bronze schema
        bronze_schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("raw_data", StringType()),
            ]
        )

        # Create empty bronze DataFrame
        bronze_df = spark.createDataFrame([], bronze_schema)

        # Silver step
        silver_df = bronze_df.select("id", F.col("raw_data").alias("processed_data"))

        # Gold step - should preserve columns from silver
        gold_df = silver_df.select("id", F.col("processed_data").alias("final_data"))

        # Should have columns from gold transformation
        assert len(gold_df.columns) == 2
        assert "id" in gold_df.columns
        assert "final_data" in gold_df.columns

    def test_empty_dataframe_with_validation_rules(self):
        """Test that validation rules can reference columns in empty DataFrames.

        This is the specific use case mentioned in the regression report where
        validation rules fail because columns don't exist.
        """
        spark = SparkSession("test")

        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
            ]
        )

        empty_df = spark.createDataFrame([], schema)

        # Validation rules should be able to reference columns even if DataFrame is empty
        # This simulates the validation that was failing
        assert "id" in empty_df.columns
        assert "name" in empty_df.columns

        # Should be able to select columns referenced in validation
        result_df = empty_df.select("id")
        assert "id" in result_df.columns

    def test_empty_dataframe_groupby_preserves_schema(self):
        """Test that groupBy operation preserves schema information for empty DataFrames."""
        spark = SparkSession("test")

        schema = StructType(
            [
                StructField("category", StringType()),
                StructField("value", DoubleType()),
            ]
        )

        empty_df = spark.createDataFrame([], schema)

        # GroupBy should work even with empty DataFrame
        grouped = empty_df.groupBy("category")

        # Aggregation should preserve schema
        result_df = grouped.agg(F.sum("value").alias("total"))

        assert len(result_df.columns) == 2
        assert "category" in result_df.columns
        assert "total" in result_df.columns

    def test_empty_dataframe_orderby_preserves_schema(self):
        """Test that orderBy operation preserves schema for empty DataFrames."""
        spark = SparkSession("test")

        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
            ]
        )

        empty_df = spark.createDataFrame([], schema)

        # orderBy should preserve all columns
        result_df = empty_df.orderBy("id")

        assert len(result_df.columns) == 2
        assert "id" in result_df.columns
        assert "name" in result_df.columns

    def test_empty_dataframe_lazy_evaluation_preserves_schema(self):
        """Test that lazy evaluation preserves schema for empty DataFrames."""
        spark = SparkSession("test")

        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
            ]
        )

        empty_df = spark.createDataFrame([], schema)

        # Apply lazy operations
        lazy_df = empty_df.select("id").filter(F.col("id") > 0)

        # Schema should be accessible even before materialization
        assert len(lazy_df.columns) == 1
        assert "id" in lazy_df.columns

        # Materialize and verify schema is still correct
        lazy_df.collect()
        assert len(lazy_df.columns) == 1
        assert "id" in lazy_df.columns

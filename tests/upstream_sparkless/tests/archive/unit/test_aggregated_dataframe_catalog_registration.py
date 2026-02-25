"""
Tests for aggregated DataFrame catalog registration.

This module tests that aggregated DataFrames (from groupBy().agg()) are correctly
registered in the catalog after saveAsTable(), ensuring they are immediately
queryable via spark.table() just like simple DataFrames.
"""

import pytest
from sparkless import SparkSession
from sparkless.sql import functions as F


@pytest.mark.unit
class TestAggregatedDataFrameCatalogRegistration:
    """Test aggregated DataFrame catalog registration fixes."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        session = SparkSession("agg_catalog_test")
        # Create test schema
        session.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
        yield session
        session.stop()

    def test_basic_groupby_agg_catalog_registration(self, spark):
        """Test basic groupBy().agg() catalog registration."""
        source = spark.createDataFrame(
            [("user1", 100), ("user2", 200)], ["user_id", "value"]
        )
        agg_df = source.groupBy("user_id").agg(F.count("*").alias("count"))

        # Write aggregated DataFrame
        agg_df.write.mode("overwrite").saveAsTable("test_schema.basic_agg")

        # CRITICAL: Must work immediately (no delay, no retry)
        table = spark.table("test_schema.basic_agg")

        # Verify table is accessible and has correct data
        assert table is not None, "Table should not be None"
        assert table.count() == 2, "Table should have 2 rows"
        assert "user_id" in table.columns, "Table should have user_id column"
        assert "count" in table.columns, "Table should have count column"

        # Verify data integrity
        rows = sorted(table.collect(), key=lambda x: x["user_id"])
        assert len(rows) == 2
        assert rows[0]["user_id"] == "user1"
        assert rows[0]["count"] == 1
        assert rows[1]["user_id"] == "user2"
        assert rows[1]["count"] == 1

    def test_multiple_agg_functions(self, spark):
        """Test DataFrame with multiple aggregation functions."""
        source = spark.createDataFrame(
            [("user1", 100), ("user2", 200), ("user1", 150)],
            ["user_id", "value"],
        )

        agg_df = source.groupBy("user_id").agg(
            F.count("*").alias("count"),
            F.sum("value").alias("total"),
            F.avg("value").alias("average"),
            F.max("value").alias("maximum"),
            F.min("value").alias("minimum"),
        )

        agg_df.write.mode("overwrite").saveAsTable("test_schema.multi_agg")

        # Must work immediately
        table = spark.table("test_schema.multi_agg")
        assert table.count() == 2, "Should have 2 groups"
        assert set(table.columns) == {
            "user_id",
            "count",
            "total",
            "average",
            "maximum",
            "minimum",
        }

        # Verify data integrity
        rows = sorted(table.collect(), key=lambda x: x["user_id"])
        assert len(rows) == 2

        # Check user1 (has 2 values: 100, 150)
        user1_row = next(r for r in rows if r["user_id"] == "user1")
        assert user1_row["count"] == 2
        assert user1_row["total"] == 250
        assert user1_row["average"] == 125.0
        assert user1_row["maximum"] == 150
        assert user1_row["minimum"] == 100

        # Check user2 (has 1 value: 200)
        user2_row = next(r for r in rows if r["user_id"] == "user2")
        assert user2_row["count"] == 1
        assert user2_row["total"] == 200
        assert user2_row["average"] == 200.0
        assert user2_row["maximum"] == 200
        assert user2_row["minimum"] == 200

    def test_nested_aggregations(self, spark):
        """Test complex nested aggregation operations."""
        source = spark.createDataFrame(
            [("user1", "A", 100), ("user1", "B", 200), ("user2", "A", 150)],
            ["user_id", "category", "value"],
        )

        # First aggregation
        first_agg = source.groupBy("user_id", "category").agg(
            F.sum("value").alias("category_total")
        )

        # Second aggregation on first result
        second_agg = first_agg.groupBy("user_id").agg(
            F.sum("category_total").alias("user_total")
        )

        second_agg.write.mode("overwrite").saveAsTable("test_schema.nested_agg")

        # Must work immediately
        table = spark.table("test_schema.nested_agg")
        assert table.count() == 2
        assert "user_id" in table.columns
        assert "user_total" in table.columns

        # Verify data integrity
        rows = sorted(table.collect(), key=lambda x: x["user_id"])
        assert len(rows) == 2
        # user1: (100 + 200) = 300
        assert rows[0]["user_id"] == "user1"
        assert rows[0]["user_total"] == 300
        # user2: 150
        assert rows[1]["user_id"] == "user2"
        assert rows[1]["user_total"] == 150

    def test_aggregated_table_immediate_access(self, spark):
        """Test that aggregated tables are immediately accessible after saveAsTable()."""
        source = spark.createDataFrame(
            [("user1", 100), ("user2", 200), ("user1", 150)],
            ["user_id", "value"],
        )

        agg_df = source.groupBy("user_id").agg(
            F.count("*").alias("count"), F.sum("value").alias("total")
        )

        # Write aggregated DataFrame
        agg_df.write.mode("overwrite").saveAsTable("test_schema.immediate_test")

        # CRITICAL: Should be immediately accessible (no delay, no retry)
        # This is what the bug was preventing
        table = spark.table("test_schema.immediate_test")

        # Verify table is accessible
        assert table is not None
        assert table.count() > 0

        # Verify schema matches
        expected_columns = set(agg_df.columns)
        actual_columns = set(table.columns)
        assert expected_columns == actual_columns, (
            f"Column mismatch: expected {expected_columns}, got {actual_columns}"
        )

    def test_aggregated_table_schema_preservation(self, spark):
        """Test that aggregated DataFrame schemas are correctly preserved in catalog."""
        source = spark.createDataFrame(
            [("user1", 100), ("user2", 200), ("user1", 150)],
            ["user_id", "value"],
        )

        agg_df = source.groupBy("user_id").agg(
            F.count("*").alias("count"),
            F.sum("value").alias("total"),
            F.avg("value").alias("avg_val"),
        )

        # Write aggregated DataFrame
        agg_df.write.mode("overwrite").saveAsTable("test_schema.schema_test")

        # Immediately read back
        table = spark.table("test_schema.schema_test")

        # Verify schema matches exactly
        assert len(table.schema.fields) == len(agg_df.schema.fields)
        assert set(table.schema.fieldNames()) == set(agg_df.schema.fieldNames())

        # Verify field types match
        for agg_field in agg_df.schema.fields:
            table_field = next(
                f for f in table.schema.fields if f.name == agg_field.name
            )
            assert table_field.dataType == agg_field.dataType, (
                f"Type mismatch for {agg_field.name}: expected {agg_field.dataType}, got {table_field.dataType}"
            )

    def test_count_aggregation_catalog_registration(self, spark):
        """Test count aggregation catalog registration."""
        source = spark.createDataFrame(
            [("user1", 100), ("user2", 200), ("user1", 150)],
            ["user_id", "value"],
        )

        agg_df = source.groupBy("user_id").agg(F.count("*").alias("count"))

        agg_df.write.mode("overwrite").saveAsTable("test_schema.count_agg")

        # Must work immediately
        table = spark.table("test_schema.count_agg")
        assert table.count() == 2
        assert "user_id" in table.columns
        assert "count" in table.columns

    def test_sum_aggregation_catalog_registration(self, spark):
        """Test sum aggregation catalog registration."""
        source = spark.createDataFrame(
            [("user1", 100), ("user2", 200), ("user1", 150)],
            ["user_id", "value"],
        )

        agg_df = source.groupBy("user_id").agg(F.sum("value").alias("total"))

        agg_df.write.mode("overwrite").saveAsTable("test_schema.sum_agg")

        # Must work immediately
        table = spark.table("test_schema.sum_agg")
        assert table.count() == 2
        assert "user_id" in table.columns
        assert "total" in table.columns

    def test_avg_aggregation_catalog_registration(self, spark):
        """Test average aggregation catalog registration."""
        source = spark.createDataFrame(
            [("user1", 100), ("user2", 200), ("user1", 150)],
            ["user_id", "value"],
        )

        agg_df = source.groupBy("user_id").agg(F.avg("value").alias("avg_val"))

        agg_df.write.mode("overwrite").saveAsTable("test_schema.avg_agg")

        # Must work immediately
        table = spark.table("test_schema.avg_agg")
        assert table.count() == 2
        assert "user_id" in table.columns
        assert "avg_val" in table.columns

    def test_max_aggregation_catalog_registration(self, spark):
        """Test max aggregation catalog registration."""
        source = spark.createDataFrame(
            [("user1", 100), ("user2", 200), ("user1", 150)],
            ["user_id", "value"],
        )

        agg_df = source.groupBy("user_id").agg(F.max("value").alias("max_val"))

        agg_df.write.mode("overwrite").saveAsTable("test_schema.max_agg")

        # Must work immediately
        table = spark.table("test_schema.max_agg")
        assert table.count() == 2
        assert "user_id" in table.columns
        assert "max_val" in table.columns

    def test_min_aggregation_catalog_registration(self, spark):
        """Test min aggregation catalog registration."""
        source = spark.createDataFrame(
            [("user1", 100), ("user2", 200), ("user1", 150)],
            ["user_id", "value"],
        )

        agg_df = source.groupBy("user_id").agg(F.min("value").alias("min_val"))

        agg_df.write.mode("overwrite").saveAsTable("test_schema.min_agg")

        # Must work immediately
        table = spark.table("test_schema.min_agg")
        assert table.count() == 2
        assert "user_id" in table.columns
        assert "min_val" in table.columns

    def test_aggregated_table_with_append_mode(self, spark):
        """Test aggregated DataFrame with append mode."""
        source1 = spark.createDataFrame(
            [("user1", 100), ("user2", 200)], ["user_id", "value"]
        )
        source2 = spark.createDataFrame([("user3", 300)], ["user_id", "value"])

        agg_df1 = source1.groupBy("user_id").agg(F.count("*").alias("count"))
        agg_df2 = source2.groupBy("user_id").agg(F.count("*").alias("count"))

        # Create table with overwrite
        agg_df1.write.mode("overwrite").saveAsTable("test_schema.append_agg")

        # Verify table exists and is accessible
        table1 = spark.table("test_schema.append_agg")
        assert table1.count() == 2

        # Append aggregated DataFrame
        agg_df2.write.mode("append").saveAsTable("test_schema.append_agg")

        # Must work immediately after append
        table2 = spark.table("test_schema.append_agg")
        assert table2.count() == 3

    def test_aggregated_table_with_error_mode(self, spark):
        """Test aggregated DataFrame with error mode."""
        source = spark.createDataFrame(
            [("user1", 100), ("user2", 200)], ["user_id", "value"]
        )

        agg_df = source.groupBy("user_id").agg(F.count("*").alias("count"))

        # Create table
        agg_df.write.mode("error").saveAsTable("test_schema.error_agg")

        # Must work immediately
        table = spark.table("test_schema.error_agg")
        assert table.count() == 2

        # Try to create again with error mode - should raise exception
        with pytest.raises(Exception):
            agg_df.write.mode("error").saveAsTable("test_schema.error_agg")

    def test_aggregated_table_with_ignore_mode(self, spark):
        """Test aggregated DataFrame with ignore mode."""
        source = spark.createDataFrame(
            [("user1", 100), ("user2", 200)], ["user_id", "value"]
        )

        agg_df = source.groupBy("user_id").agg(F.count("*").alias("count"))

        # Create table
        agg_df.write.mode("ignore").saveAsTable("test_schema.ignore_agg")

        # Must work immediately
        table1 = spark.table("test_schema.ignore_agg")
        assert table1.count() == 2

        # Try to create again with ignore mode - should be ignored
        agg_df.write.mode("ignore").saveAsTable("test_schema.ignore_agg")

        # Table should still be accessible and unchanged
        table2 = spark.table("test_schema.ignore_agg")
        assert table2.count() == 2

    def test_catalog_table_exists_after_aggregated_save(self, spark):
        """Test that catalog.tableExists() works after saving aggregated DataFrame."""
        source = spark.createDataFrame(
            [("user1", 100), ("user2", 200)], ["user_id", "value"]
        )

        agg_df = source.groupBy("user_id").agg(F.count("*").alias("count"))

        # Write aggregated DataFrame
        agg_df.write.mode("overwrite").saveAsTable("test_schema.catalog_test")

        # Verify table exists in catalog
        assert spark.catalog.tableExists("test_schema", "catalog_test") is True
        assert spark.catalog.tableExists("test_schema.catalog_test") is True

    def test_catalog_list_tables_includes_aggregated(self, spark):
        """Test that listTables() includes aggregated tables."""
        source = spark.createDataFrame(
            [("user1", 100), ("user2", 200)], ["user_id", "value"]
        )

        agg_df = source.groupBy("user_id").agg(F.count("*").alias("count"))

        # Write aggregated DataFrame
        agg_df.write.mode("overwrite").saveAsTable("test_schema.list_test")

        # List tables in schema
        tables = spark.catalog.listTables("test_schema")
        table_names = [t.name for t in tables]
        assert "list_test" in table_names

    def test_storage_instance_synchronization(self, spark):
        """Test that aggregated DataFrames use the same storage instance as source."""
        source = spark.createDataFrame(
            [("user1", 100), ("user2", 200)], ["user_id", "value"]
        )

        # Verify source DataFrame uses spark._storage
        assert source.storage is spark._storage, (
            "Source DataFrame should use spark._storage"
        )

        # Create aggregated DataFrame
        agg_df = source.groupBy("user_id").agg(F.count("*").alias("count"))

        # CRITICAL: Aggregated DataFrame should use the same storage instance
        assert agg_df.storage is spark._storage, (
            "Aggregated DataFrame should use spark._storage"
        )
        assert agg_df.storage is source.storage, (
            "Aggregated DataFrame should use same storage as source"
        )

        # Verify writer also uses the same storage
        writer = agg_df.write
        assert writer.storage is spark._storage, (
            "DataFrameWriter should use spark._storage"
        )

        # Write table and verify it's accessible
        agg_df.write.mode("overwrite").saveAsTable("test_schema.storage_sync_test")
        table = spark.table("test_schema.storage_sync_test")
        assert table.count() == 2

    def test_triple_nested_aggregations(self, spark):
        """Test three levels of nested aggregations."""
        source = spark.createDataFrame(
            [
                ("user1", "cat1", "sub1", 10),
                ("user1", "cat1", "sub2", 20),
                ("user1", "cat2", "sub1", 30),
                ("user2", "cat1", "sub1", 40),
            ],
            ["user_id", "category", "subcategory", "value"],
        )

        # First aggregation: group by user_id, category, subcategory
        first_agg = source.groupBy("user_id", "category", "subcategory").agg(
            F.sum("value").alias("sub_total")
        )

        # Verify storage synchronization
        assert first_agg.storage is spark._storage

        # Second aggregation: group by user_id, category
        second_agg = first_agg.groupBy("user_id", "category").agg(
            F.sum("sub_total").alias("cat_total")
        )

        # Verify storage synchronization
        assert second_agg.storage is spark._storage

        # Third aggregation: group by user_id
        third_agg = second_agg.groupBy("user_id").agg(
            F.sum("cat_total").alias("user_total")
        )

        # Verify storage synchronization
        assert third_agg.storage is spark._storage

        # Write and verify immediate access
        third_agg.write.mode("overwrite").saveAsTable("test_schema.triple_nested")
        table = spark.table("test_schema.triple_nested")
        assert table.count() == 2
        assert "user_id" in table.columns
        assert "user_total" in table.columns

    def test_empty_aggregated_dataframe(self, spark):
        """Test aggregated DataFrame with empty source data."""
        from sparkless.spark_types import (
            StructType,
            StructField,
            StringType,
            IntegerType,
        )

        schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("value", IntegerType(), True),
            ]
        )
        source = spark.createDataFrame([], schema)

        # Create aggregated DataFrame (will be empty)
        agg_df = source.groupBy("user_id").agg(F.count("*").alias("count"))

        # Verify storage synchronization even for empty DataFrames
        assert agg_df.storage is spark._storage

        # Write empty aggregated DataFrame
        agg_df.write.mode("overwrite").saveAsTable("test_schema.empty_agg")

        # Must work immediately
        table = spark.table("test_schema.empty_agg")
        assert table.count() == 0
        assert "user_id" in table.columns
        assert "count" in table.columns

    def test_single_row_aggregation(self, spark):
        """Test aggregation with single row in source."""
        source = spark.createDataFrame([("user1", 100)], ["user_id", "value"])

        agg_df = source.groupBy("user_id").agg(
            F.count("*").alias("count"), F.sum("value").alias("total")
        )

        # Verify storage synchronization
        assert agg_df.storage is spark._storage

        agg_df.write.mode("overwrite").saveAsTable("test_schema.single_row_agg")

        # Must work immediately
        table = spark.table("test_schema.single_row_agg")
        assert table.count() == 1
        assert table.collect()[0]["count"] == 1
        assert table.collect()[0]["total"] == 100

    def test_cube_operation_storage_sync(self, spark):
        """Test that cube operations maintain storage synchronization."""
        source = spark.createDataFrame(
            [("user1", "A", 100), ("user1", "B", 200), ("user2", "A", 150)],
            ["user_id", "category", "value"],
        )

        cube_df = source.cube("user_id", "category").agg(F.sum("value").alias("total"))

        # Verify storage synchronization
        assert cube_df.storage is spark._storage

        cube_df.write.mode("overwrite").saveAsTable("test_schema.cube_test")

        # Must work immediately
        table = spark.table("test_schema.cube_test")
        assert table.count() > 0
        assert "user_id" in table.columns
        assert "category" in table.columns
        assert "total" in table.columns

    def test_rollup_operation_storage_sync(self, spark):
        """Test that rollup operations maintain storage synchronization."""
        source = spark.createDataFrame(
            [("user1", "A", 100), ("user1", "B", 200), ("user2", "A", 150)],
            ["user_id", "category", "value"],
        )

        rollup_df = source.rollup("user_id", "category").agg(
            F.sum("value").alias("total")
        )

        # Verify storage synchronization
        assert rollup_df.storage is spark._storage

        rollup_df.write.mode("overwrite").saveAsTable("test_schema.rollup_test")

        # Must work immediately
        table = spark.table("test_schema.rollup_test")
        assert table.count() > 0
        assert "user_id" in table.columns
        assert "category" in table.columns
        assert "total" in table.columns

    def test_pivot_operation_storage_sync(self, spark):
        """Test that pivot operations maintain storage synchronization."""
        source = spark.createDataFrame(
            [
                ("user1", "Q1", 100),
                ("user1", "Q2", 200),
                ("user2", "Q1", 150),
                ("user2", "Q2", 250),
            ],
            ["user_id", "quarter", "value"],
        )

        pivot_df = source.groupBy("user_id").pivot("quarter").agg(F.sum("value"))

        # Verify storage synchronization
        assert pivot_df.storage is spark._storage

        pivot_df.write.mode("overwrite").saveAsTable("test_schema.pivot_test")

        # Must work immediately
        table = spark.table("test_schema.pivot_test")
        assert table.count() == 2
        assert "user_id" in table.columns

    def test_multiple_aggregations_in_sequence(self, spark):
        """Test multiple aggregations created in sequence."""
        source = spark.createDataFrame(
            [("user1", 100), ("user2", 200), ("user1", 150)],
            ["user_id", "value"],
        )

        # Create multiple aggregated DataFrames
        agg1 = source.groupBy("user_id").agg(F.count("*").alias("count"))
        agg2 = source.groupBy("user_id").agg(F.sum("value").alias("total"))
        agg3 = source.groupBy("user_id").agg(F.avg("value").alias("average"))

        # Verify all use the same storage
        assert agg1.storage is spark._storage
        assert agg2.storage is spark._storage
        assert agg3.storage is spark._storage

        # Write all to different tables
        agg1.write.mode("overwrite").saveAsTable("test_schema.seq_agg1")
        agg2.write.mode("overwrite").saveAsTable("test_schema.seq_agg2")
        agg3.write.mode("overwrite").saveAsTable("test_schema.seq_agg3")

        # All must be immediately accessible
        table1 = spark.table("test_schema.seq_agg1")
        table2 = spark.table("test_schema.seq_agg2")
        table3 = spark.table("test_schema.seq_agg3")

        assert table1.count() == 2
        assert table2.count() == 2
        assert table3.count() == 2

    def test_aggregation_after_other_operations(self, spark):
        """Test aggregation after filter, select, and other operations."""
        source = spark.createDataFrame(
            [("user1", 100, "A"), ("user2", 200, "B"), ("user1", 150, "A")],
            ["user_id", "value", "category"],
        )

        # Apply multiple transformations before aggregation
        filtered = source.filter(F.col("category") == "A")
        selected = filtered.select("user_id", "value")
        agg_df = selected.groupBy("user_id").agg(F.sum("value").alias("total"))

        # Verify storage synchronization through transformations
        assert filtered.storage is spark._storage
        assert selected.storage is spark._storage
        assert agg_df.storage is spark._storage

        agg_df.write.mode("overwrite").saveAsTable("test_schema.transformed_agg")

        # Must work immediately
        table = spark.table("test_schema.transformed_agg")
        assert table.count() == 1  # Only user1 with category A
        assert table.collect()[0]["total"] == 250  # 100 + 150

    def test_aggregation_with_join(self, spark):
        """Test aggregation after joining DataFrames."""
        df1 = spark.createDataFrame(
            [("user1", "dept1"), ("user2", "dept2")], ["user_id", "dept_id"]
        )
        df2 = spark.createDataFrame(
            [("dept1", 100), ("dept1", 150), ("dept2", 200)],
            ["dept_id", "value"],
        )

        # Join then aggregate
        joined = df1.join(df2, "dept_id")
        agg_df = joined.groupBy("user_id").agg(F.sum("value").alias("total"))

        # Verify storage synchronization
        assert joined.storage is spark._storage
        assert agg_df.storage is spark._storage

        agg_df.write.mode("overwrite").saveAsTable("test_schema.join_agg")

        # Must work immediately
        table = spark.table("test_schema.join_agg")
        assert table.count() == 2

    def test_multiple_schemas_with_aggregations(self, spark):
        """Test aggregated tables in multiple schemas."""
        spark.sql("CREATE SCHEMA IF NOT EXISTS schema1")
        spark.sql("CREATE SCHEMA IF NOT EXISTS schema2")

        source1 = spark.createDataFrame([("user1", 100)], ["user_id", "value"])
        source2 = spark.createDataFrame([("user2", 200)], ["user_id", "value"])

        agg1 = source1.groupBy("user_id").agg(F.count("*").alias("count"))
        agg2 = source2.groupBy("user_id").agg(F.count("*").alias("count"))

        # Verify storage synchronization
        assert agg1.storage is spark._storage
        assert agg2.storage is spark._storage

        # Write to different schemas
        agg1.write.mode("overwrite").saveAsTable("schema1.agg_table")
        agg2.write.mode("overwrite").saveAsTable("schema2.agg_table")

        # Both must be immediately accessible
        table1 = spark.table("schema1.agg_table")
        table2 = spark.table("schema2.agg_table")

        assert table1.count() == 1
        assert table2.count() == 1

    def test_aggregation_with_window_functions(self, spark):
        """Test aggregation after window functions."""
        source = spark.createDataFrame(
            [("user1", 100), ("user1", 150), ("user2", 200)],
            ["user_id", "value"],
        )

        # Add window function column
        from sparkless.window import Window

        windowed = source.withColumn(
            "rank", F.row_number().over(Window.partitionBy("user_id").orderBy("value"))
        )

        # Then aggregate
        agg_df = windowed.groupBy("user_id").agg(
            F.sum("value").alias("total"), F.max("rank").alias("max_rank")
        )

        # Verify storage synchronization
        assert windowed.storage is spark._storage
        assert agg_df.storage is spark._storage

        agg_df.write.mode("overwrite").saveAsTable("test_schema.window_agg")

        # Must work immediately
        table = spark.table("test_schema.window_agg")
        assert table.count() == 2

    def test_aggregation_with_complex_expressions(self, spark):
        """Test aggregation with complex column expressions."""
        source = spark.createDataFrame(
            [("user1", 100, 10), ("user2", 200, 20), ("user1", 150, 15)],
            ["user_id", "value", "multiplier"],
        )

        # Aggregate with computed columns
        agg_df = source.groupBy("user_id").agg(
            F.sum(F.col("value") * F.col("multiplier")).alias("weighted_sum"),
            F.avg("value").alias("avg_value"),
        )

        # Verify storage synchronization
        assert agg_df.storage is spark._storage

        agg_df.write.mode("overwrite").saveAsTable("test_schema.complex_expr_agg")

        # Must work immediately
        table = spark.table("test_schema.complex_expr_agg")
        assert table.count() == 2

        # Verify data integrity
        user1_row = next(r for r in table.collect() if r["user_id"] == "user1")
        # user1: (100*10) + (150*15) = 1000 + 2250 = 3250
        assert user1_row["weighted_sum"] == 3250

    def test_storage_sync_through_chain(self, spark):
        """Test storage synchronization through a long chain of operations."""
        source = spark.createDataFrame(
            [("user1", 100), ("user2", 200), ("user1", 150)],
            ["user_id", "value"],
        )

        # Long chain: filter -> select -> groupBy -> agg -> filter -> groupBy -> agg
        step1 = source.filter(F.col("value") > 50)
        step2 = step1.select("user_id", "value")
        step3 = step2.groupBy("user_id").agg(F.sum("value").alias("total"))
        step4 = step3.filter(F.col("total") > 100)
        step5 = step4.groupBy().agg(F.sum("total").alias("grand_total"))

        # Verify storage synchronization at each step
        assert step1.storage is spark._storage
        assert step2.storage is spark._storage
        assert step3.storage is spark._storage
        assert step4.storage is spark._storage
        assert step5.storage is spark._storage

        step5.write.mode("overwrite").saveAsTable("test_schema.chain_agg")

        # Must work immediately
        table = spark.table("test_schema.chain_agg")
        assert table.count() == 1

    def test_aggregation_with_null_values(self, spark):
        """Test aggregation with null values in source data."""
        source = spark.createDataFrame(
            [("user1", 100), ("user1", None), ("user2", 200), ("user2", None)],
            ["user_id", "value"],
        )

        agg_df = source.groupBy("user_id").agg(
            F.count("*").alias("count"),
            F.sum("value").alias("total"),
            F.avg("value").alias("average"),
        )

        # Verify storage synchronization
        assert agg_df.storage is spark._storage

        agg_df.write.mode("overwrite").saveAsTable("test_schema.null_agg")

        # Must work immediately
        table = spark.table("test_schema.null_agg")
        assert table.count() == 2

        # Verify null handling
        user1_row = next(r for r in table.collect() if r["user_id"] == "user1")
        assert user1_row["count"] == 2  # count(*) includes nulls
        assert user1_row["total"] == 100  # sum ignores nulls

    def test_aggregation_with_distinct(self, spark):
        """Test aggregation after distinct operation."""
        source = spark.createDataFrame(
            [("user1", 100), ("user1", 100), ("user2", 200)], ["user_id", "value"]
        )

        # Distinct then aggregate
        distinct_df = source.distinct()
        agg_df = distinct_df.groupBy("user_id").agg(F.sum("value").alias("total"))

        # Verify storage synchronization
        assert distinct_df.storage is spark._storage
        assert agg_df.storage is spark._storage

        agg_df.write.mode("overwrite").saveAsTable("test_schema.distinct_agg")

        # Must work immediately
        table = spark.table("test_schema.distinct_agg")
        assert table.count() == 2

    def test_large_dataset_aggregation(self, spark):
        """Test aggregation with larger dataset."""
        # Create larger dataset
        data = [("user" + str(i % 10), i * 10) for i in range(100)]
        source = spark.createDataFrame(data, ["user_id", "value"])

        agg_df = source.groupBy("user_id").agg(
            F.count("*").alias("count"),
            F.sum("value").alias("total"),
            F.avg("value").alias("average"),
        )

        # Verify storage synchronization
        assert agg_df.storage is spark._storage

        agg_df.write.mode("overwrite").saveAsTable("test_schema.large_agg")

        # Must work immediately
        table = spark.table("test_schema.large_agg")
        assert table.count() == 10  # 10 unique users

    def test_aggregation_with_union(self, spark):
        """Test aggregation after union operation."""
        df1 = spark.createDataFrame([("user1", 100)], ["user_id", "value"])
        df2 = spark.createDataFrame([("user1", 150)], ["user_id", "value"])

        # Union then aggregate
        unioned = df1.union(df2)
        agg_df = unioned.groupBy("user_id").agg(F.sum("value").alias("total"))

        # Verify storage synchronization
        assert unioned.storage is spark._storage
        assert agg_df.storage is spark._storage

        agg_df.write.mode("overwrite").saveAsTable("test_schema.union_agg")

        # Must work immediately
        table = spark.table("test_schema.union_agg")
        assert table.count() == 1
        assert table.collect()[0]["total"] == 250

    def test_storage_instance_verification_at_write_time(self, spark):
        """Test that storage instances are synchronized at write time."""
        source = spark.createDataFrame(
            [("user1", 100), ("user2", 200)], ["user_id", "value"]
        )

        agg_df = source.groupBy("user_id").agg(F.count("*").alias("count"))

        # Get writer and verify storage
        writer = agg_df.write
        assert writer.storage is spark._storage
        assert writer.storage is source.storage
        assert writer.storage is agg_df.storage

        # Write table
        writer.mode("overwrite").saveAsTable("test_schema.write_time_test")

        # Verify table is accessible
        table = spark.table("test_schema.write_time_test")
        assert table.count() == 2

    def test_multiple_writers_same_aggregation(self, spark):
        """Test multiple writers from the same aggregated DataFrame."""
        source = spark.createDataFrame(
            [("user1", 100), ("user2", 200)], ["user_id", "value"]
        )

        agg_df = source.groupBy("user_id").agg(F.count("*").alias("count"))

        # Create multiple writers
        writer1 = agg_df.write
        writer2 = agg_df.write

        # All should use the same storage
        assert writer1.storage is spark._storage
        assert writer2.storage is spark._storage
        assert writer1.storage is writer2.storage

        # Write to different tables
        writer1.mode("overwrite").saveAsTable("test_schema.multi_writer1")
        writer2.mode("overwrite").saveAsTable("test_schema.multi_writer2")

        # Both must be accessible
        table1 = spark.table("test_schema.multi_writer1")
        table2 = spark.table("test_schema.multi_writer2")
        assert table1.count() == 2
        assert table2.count() == 2

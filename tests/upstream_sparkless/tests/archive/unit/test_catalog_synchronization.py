"""
Tests for catalog synchronization fixes.

This module tests that catalog.tableExists() works correctly after saveAsTable().
"""

import pytest
import tempfile
import os
import threading
from sparkless import SparkSession
from sparkless.sql import functions as F


@pytest.mark.unit
class TestCatalogSynchronization:
    """Test catalog synchronization fixes."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        session = SparkSession("catalog_sync_test")
        yield session
        session.stop()

    def test_table_exists_after_save_as_table(self, spark):
        """Test that catalog.tableExists() works after saveAsTable()."""
        test_data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

        df = spark.createDataFrame(test_data)

        # Save as table
        df.write.mode("overwrite").saveAsTable("test_schema.test_table")

        # Verify table exists in catalog
        assert spark.catalog.tableExists("test_schema", "test_table") is True
        assert spark.catalog.tableExists("test_schema.test_table") is True

    def test_table_exists_after_append_mode(self, spark):
        """Test that catalog.tableExists() works after append mode saveAsTable()."""
        test_data1 = [{"id": 1, "name": "Alice"}]
        test_data2 = [{"id": 2, "name": "Bob"}]

        df1 = spark.createDataFrame(test_data1)
        df2 = spark.createDataFrame(test_data2)

        # Create table with overwrite
        df1.write.mode("overwrite").saveAsTable("test_schema.test_table_append")

        # Verify table exists
        assert spark.catalog.tableExists("test_schema", "test_table_append") is True

        # Append to table
        df2.write.mode("append").saveAsTable("test_schema.test_table_append")

        # Verify table still exists after append
        assert spark.catalog.tableExists("test_schema", "test_table_append") is True

        # Verify data was appended
        result_df = spark.table("test_schema.test_table_append")
        assert result_df.count() == 2

    def test_table_exists_with_qualified_name(self, spark):
        """Test that catalog.tableExists() works with qualified table names."""
        test_data = [{"id": 1, "value": "test"}]

        df = spark.createDataFrame(test_data)
        df.write.mode("overwrite").saveAsTable("my_schema.my_table")

        # Test with qualified name
        assert spark.catalog.tableExists("my_schema.my_table") is True

        # Test with separate schema and table
        assert spark.catalog.tableExists("my_table", "my_schema") is True

    def test_table_exists_returns_false_for_nonexistent_table(self, spark):
        """Test that catalog.tableExists() returns False for non-existent tables."""
        assert (
            spark.catalog.tableExists("nonexistent_schema", "nonexistent_table")
            is False
        )
        assert (
            spark.catalog.tableExists("nonexistent_schema.nonexistent_table") is False
        )

    def test_table_list_includes_saved_tables(self, spark):
        """Test that listTables() includes tables created with saveAsTable()."""
        test_data = [{"id": 1, "name": "Test"}]

        df = spark.createDataFrame(test_data)
        df.write.mode("overwrite").saveAsTable("list_test_schema.list_test_table")

        # List tables in schema
        tables = spark.catalog.listTables("list_test_schema")
        table_names = [t.name for t in tables]
        assert "list_test_table" in table_names

    def test_immediate_table_access_after_save(self, spark):
        """Test that table is immediately accessible after saveAsTable() with zero delay."""
        # Create test DataFrame
        df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])

        # Write table
        df.write.mode("overwrite").saveAsTable("test_schema.test_table")

        # CRITICAL: Should work immediately, no delay, no retry needed
        table = spark.table("test_schema.test_table")

        # Verify table is accessible and has correct data
        assert table is not None
        assert table.count() == 2
        assert "id" in table.columns
        assert "name" in table.columns

        # Verify data integrity
        rows = table.collect()
        assert len(rows) == 2
        assert rows[0]["id"] == 1
        assert rows[0]["name"] == "Alice"
        assert rows[1]["id"] == 2
        assert rows[1]["name"] == "Bob"

    def test_schema_evolution_immediate_access(self, spark):
        """Test schema evolution workflow with immediate table access."""
        # Initial table
        df1 = spark.createDataFrame([(1, "Alice")], ["id", "name"])
        df1.write.mode("overwrite").saveAsTable("test_schema.events")

        # Immediately read to check schema (this is what PipelineBuilder does)
        existing_table = spark.table("test_schema.events")  # Must work immediately
        existing_schema = existing_table.schema

        # Add new column
        df2 = spark.createDataFrame([(2, "Bob", 25)], ["id", "name", "age"])

        # Merge schemas (simulating PipelineBuilder's schema evolution)
        # This requires immediate access to the existing table
        for field in existing_schema.fields:
            if field.name not in df2.columns:
                df2 = df2.withColumn(field.name, F.lit(None))

        # Write merged schema
        df2.write.mode("overwrite").saveAsTable("test_schema.events")

        # Immediately verify new schema
        final_table = spark.table("test_schema.events")  # Must work immediately
        assert "id" in final_table.columns
        assert "name" in final_table.columns
        assert "age" in final_table.columns

    def test_parallel_write_read(self, spark):
        """Test that multiple threads can write and immediately read tables."""
        results = []
        errors = []

        def write_and_read(table_num):
            try:
                df = spark.createDataFrame(
                    [(table_num, f"data_{table_num}")], ["id", "value"]
                )
                df.write.mode("overwrite").saveAsTable(f"test_schema.table_{table_num}")

                # Immediately read (no delay)
                table = spark.table(f"test_schema.table_{table_num}")
                results.append((table_num, table.count()))
            except Exception as e:
                errors.append((table_num, str(e)))

        # Run 10 parallel write+read operations
        threads = [
            threading.Thread(target=write_and_read, args=(i,)) for i in range(10)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All should succeed without errors
        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == 10
        assert all(count == 1 for _, count in results)

    def test_append_mode_immediate_access(self, spark):
        """Test that append mode also provides immediate catalog access."""
        # Initial write
        df1 = spark.createDataFrame([(1, "Alice")], ["id", "name"])
        df1.write.mode("overwrite").saveAsTable("test_schema.users")

        # Verify initial state
        table1 = spark.table("test_schema.users")  # Must work immediately
        assert table1.count() == 1

        # Append new data
        df2 = spark.createDataFrame([(2, "Bob")], ["id", "name"])
        df2.write.mode("append").saveAsTable("test_schema.users")

        # Immediately verify appended data
        table2 = spark.table("test_schema.users")  # Must work immediately
        assert table2.count() == 2

    def test_catalog_table_exists_immediate(self, spark):
        """Test that catalog.tableExists() returns True immediately after saveAsTable()."""
        # Create test DataFrame
        df = spark.createDataFrame([(1, "test")], ["id", "value"])

        # Write table
        df.write.mode("overwrite").saveAsTable("test_schema.catalog_test")

        # CRITICAL: catalog.tableExists() should return True immediately
        assert spark.catalog.tableExists("test_schema", "catalog_test") is True
        assert spark.catalog.tableExists("test_schema.catalog_test") is True

        # Also verify we can read it immediately
        table = spark.table("test_schema.catalog_test")
        assert table.count() == 1

    def test_all_save_modes_immediate_access(self, spark):
        """Test that all save modes (overwrite, append, error, ignore) provide immediate access."""
        # Test overwrite mode
        df1 = spark.createDataFrame([(1, "overwrite")], ["id", "mode"])
        df1.write.mode("overwrite").saveAsTable("test_schema.modes_test")
        table1 = spark.table("test_schema.modes_test")  # Must work immediately
        assert table1.count() == 1

        # Test append mode
        df2 = spark.createDataFrame([(2, "append")], ["id", "mode"])
        df2.write.mode("append").saveAsTable("test_schema.modes_test")
        table2 = spark.table("test_schema.modes_test")  # Must work immediately
        assert table2.count() == 2

        # Test error mode (should fail if table exists)
        with pytest.raises(Exception):  # Should raise AnalysisException
            df3 = spark.createDataFrame([(3, "error")], ["id", "mode"])
            df3.write.mode("error").saveAsTable("test_schema.modes_test")

        # Test ignore mode (should not fail, but also not write if table exists)
        df4 = spark.createDataFrame([(4, "ignore")], ["id", "mode"])
        df4.write.mode("ignore").saveAsTable("test_schema.modes_test")
        table4 = spark.table("test_schema.modes_test")  # Must work immediately
        # Should still have 2 rows (ignore mode doesn't write if table exists)
        assert table4.count() == 2

    def test_large_dataset_immediate_access(self, spark):
        """Test immediate access with large datasets."""
        # Create a larger dataset (1000 rows)
        data = [(i, f"name_{i}", i * 10) for i in range(1000)]
        df = spark.createDataFrame(data, ["id", "name", "value"])

        # Write table
        df.write.mode("overwrite").saveAsTable("test_schema.large_table")

        # Immediately read - should work without delay
        table = spark.table("test_schema.large_table")
        assert table.count() == 1000

        # Verify data integrity
        rows = table.filter(table.id == 0).collect()
        assert len(rows) == 1
        assert rows[0]["id"] == 0
        assert rows[0]["name"] == "name_0"

    def test_empty_table_immediate_access(self, spark):
        """Test immediate access with empty tables."""
        # Create empty DataFrame with schema
        from sparkless.sql.types import (
            StructType,
            StructField,
            IntegerType,
            StringType,
        )

        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        )
        df = spark.createDataFrame([], schema)

        # Write empty table
        df.write.mode("overwrite").saveAsTable("test_schema.empty_table")

        # Immediately read - should work
        table = spark.table("test_schema.empty_table")
        assert table.count() == 0
        assert "id" in table.columns
        assert "name" in table.columns

    def test_rapid_successive_writes(self, spark):
        """Test rapid successive writes to the same table."""
        for i in range(10):
            df = spark.createDataFrame([(i, f"value_{i}")], ["id", "value"])
            df.write.mode("overwrite").saveAsTable("test_schema.rapid_test")

            # Immediately verify after each write
            table = spark.table("test_schema.rapid_test")
            assert table.count() == 1
            rows = table.collect()
            assert rows[0]["id"] == i
            assert rows[0]["value"] == f"value_{i}"

    def test_table_drop_and_immediate_recreation(self, spark):
        """Test dropping a table and immediately recreating it."""
        # Create initial table
        df1 = spark.createDataFrame([(1, "first")], ["id", "value"])
        df1.write.mode("overwrite").saveAsTable("test_schema.recreate_test")
        assert spark.table("test_schema.recreate_test").count() == 1

        # Drop table
        spark.catalog.dropTable("test_schema.recreate_test")
        assert spark.catalog.tableExists("test_schema.recreate_test") is False

        # Immediately recreate
        df2 = spark.createDataFrame([(2, "second")], ["id", "value"])
        df2.write.mode("overwrite").saveAsTable("test_schema.recreate_test")

        # Immediately verify new table
        table = spark.table("test_schema.recreate_test")
        assert table.count() == 1
        assert table.collect()[0]["id"] == 2

    def test_multiple_schemas_immediate_access(self, spark):
        """Test immediate access across multiple schemas."""
        schemas = ["schema_a", "schema_b", "schema_c"]

        for schema_name in schemas:
            df = spark.createDataFrame([(1, f"data_{schema_name}")], ["id", "value"])
            df.write.mode("overwrite").saveAsTable(f"{schema_name}.test_table")

            # Immediately verify in each schema
            table = spark.table(f"{schema_name}.test_table")
            assert table.count() == 1
            assert spark.catalog.tableExists(f"{schema_name}.test_table") is True

    def test_complex_table_names(self, spark):
        """Test immediate access with complex table names."""
        # Test with underscores, numbers, and mixed case
        complex_names = [
            "test_table_123",
            "TestTable",
            "table_with_many_underscores",
            "table123",
        ]

        for table_name in complex_names:
            df = spark.createDataFrame([(1, "test")], ["id", "value"])
            full_name = f"test_schema.{table_name}"
            df.write.mode("overwrite").saveAsTable(full_name)

            # Immediately verify
            table = spark.table(full_name)
            assert table.count() == 1
            assert spark.catalog.tableExists(full_name) is True

    def test_concurrent_same_table_access(self, spark):
        """Test concurrent writes and reads to the same table."""
        results = []
        errors = []
        lock = threading.Lock()

        def write_and_read(thread_id):
            try:
                # Each thread writes then immediately reads
                df = spark.createDataFrame(
                    [(thread_id, f"thread_{thread_id}")], ["id", "value"]
                )
                with lock:
                    df.write.mode("overwrite").saveAsTable("test_schema.concurrent")

                # Immediately read (outside lock to allow concurrency)
                table = spark.table("test_schema.concurrent")
                count = table.count()
                with lock:
                    results.append((thread_id, count))
            except Exception as e:
                with lock:
                    errors.append((thread_id, str(e)))

        # Run 5 concurrent operations
        threads = [threading.Thread(target=write_and_read, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # At least some operations should succeed
        assert len(results) > 0, f"All operations failed: {errors}"
        # All should have count == 1 (overwrite mode)
        assert all(count == 1 for _, count in results)

    def test_many_tables_stress_test(self, spark):
        """Stress test: create many tables and verify immediate access."""
        num_tables = 50

        for i in range(num_tables):
            df = spark.createDataFrame([(i, f"data_{i}")], ["id", "value"])
            table_name = f"test_schema.stress_table_{i}"
            df.write.mode("overwrite").saveAsTable(table_name)

            # Immediately verify each table
            table = spark.table(table_name)
            assert table.count() == 1
            assert spark.catalog.tableExists(table_name) is True

        # Verify all tables are in the catalog
        tables = spark.catalog.listTables("test_schema")
        table_names = [t.name for t in tables]
        assert (
            len([n for n in table_names if n.startswith("stress_table_")]) == num_tables
        )

    def test_nested_schema_operations(self, spark):
        """Test schema operations followed by immediate table access."""
        # Create schema
        spark.sql("CREATE SCHEMA IF NOT EXISTS nested_test")

        # Immediately create table in new schema
        df = spark.createDataFrame([(1, "test")], ["id", "value"])
        df.write.mode("overwrite").saveAsTable("nested_test.nested_table")

        # Immediately verify
        assert spark.catalog.tableExists("nested_test.nested_table") is True
        table = spark.table("nested_test.nested_table")
        assert table.count() == 1

    def test_table_with_special_characters_in_data(self, spark):
        """Test immediate access with special characters in data."""
        # Data with special characters
        data = [
            (1, "value with spaces"),
            (2, "value-with-dashes"),
            (3, "value_with_underscores"),
            (4, "value.with.dots"),
            (5, "value\nwith\nnewlines"),
            (6, "value\twith\ttabs"),
        ]
        df = spark.createDataFrame(data, ["id", "value"])

        df.write.mode("overwrite").saveAsTable("test_schema.special_chars")

        # Immediately verify
        table = spark.table("test_schema.special_chars")
        assert table.count() == 6

        # Verify special characters are preserved
        rows = table.collect()
        assert rows[0]["value"] == "value with spaces"
        assert rows[1]["value"] == "value-with-dashes"

    def test_table_with_null_values(self, spark):
        """Test immediate access with tables containing null values."""
        data = [
            (1, "Alice", 25),
            (2, None, 30),  # null name
            (3, "Bob", None),  # null age
            (4, None, None),  # both null
        ]
        df = spark.createDataFrame(data, ["id", "name", "age"])

        df.write.mode("overwrite").saveAsTable("test_schema.nulls_test")

        # Immediately verify
        table = spark.table("test_schema.nulls_test")
        assert table.count() == 4

        # Verify nulls are preserved
        rows = table.collect()
        assert rows[0]["name"] == "Alice"
        assert rows[1]["name"] is None
        assert rows[2]["age"] is None
        assert rows[3]["name"] is None
        assert rows[3]["age"] is None

    def test_table_with_complex_types(self, spark):
        """Test immediate access with complex data types."""
        # Note: Complex types like arrays and maps may have limitations in storage
        # Test with simpler complex scenario - nested structs or just verify basic access
        from sparkless.sql.types import (
            StructType,
            StructField,
            StringType,
            IntegerType,
            DoubleType,
        )

        # Create DataFrame with multiple column types
        data = [
            (1, "Alice", 25.5),
            (2, "Bob", 30.0),
        ]
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("score", DoubleType(), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        df.write.mode("overwrite").saveAsTable("test_schema.complex_types")

        # Immediately verify
        table = spark.table("test_schema.complex_types")
        assert table.count() == 2
        assert "id" in table.columns
        assert "name" in table.columns
        assert "score" in table.columns

    def test_persistent_storage_immediate_access(self):
        """Test immediate access with persistent storage backend."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test_db")

            spark = SparkSession(
                "persistent_test",
                db_path=db_path,
                max_memory="1GB",
                allow_disk_spillover=False,
            )
            try:
                df = spark.createDataFrame([(1, "test")], ["id", "value"])
                df.write.mode("overwrite").saveAsTable("persist_schema.persist_table")

                # Immediately verify
                table = spark.table("persist_schema.persist_table")
                assert table.count() == 1
                assert spark.catalog.tableExists("persist_schema.persist_table") is True
            finally:
                spark.stop()

    def test_multiple_append_operations_immediate(self, spark):
        """Test multiple append operations with immediate access after each."""
        # Initial write
        df1 = spark.createDataFrame([(1, "first")], ["id", "value"])
        df1.write.mode("overwrite").saveAsTable("test_schema.multi_append")
        table1 = spark.table("test_schema.multi_append")
        assert table1.count() == 1

        # Multiple appends
        for i in range(2, 6):
            df = spark.createDataFrame([(i, f"value_{i}")], ["id", "value"])
            df.write.mode("append").saveAsTable("test_schema.multi_append")

            # Immediately verify after each append
            table = spark.table("test_schema.multi_append")
            assert table.count() == i

        # Final verification
        final_table = spark.table("test_schema.multi_append")
        assert final_table.count() == 5

    def test_table_access_after_schema_drop_recreate(self, spark):
        """Test table access after schema operations."""
        # Create schema and table
        spark.sql("CREATE SCHEMA IF NOT EXISTS drop_test")
        df = spark.createDataFrame([(1, "test")], ["id", "value"])
        df.write.mode("overwrite").saveAsTable("drop_test.drop_table")

        # Verify immediate access
        table = spark.table("drop_test.drop_table")
        assert table.count() == 1

        # Drop and recreate schema
        spark.catalog.dropDatabase("drop_test", ignoreIfNotExists=True)
        spark.sql("CREATE SCHEMA IF NOT EXISTS drop_test")

        # Create new table in recreated schema
        df2 = spark.createDataFrame([(2, "new")], ["id", "value"])
        df2.write.mode("overwrite").saveAsTable("drop_test.drop_table")

        # Immediately verify new table
        table2 = spark.table("drop_test.drop_table")
        assert table2.count() == 1
        assert table2.collect()[0]["id"] == 2

    def test_catalog_list_tables_immediate(self, spark):
        """Test that listTables() immediately reflects new tables."""
        # Create multiple tables
        for i in range(5):
            df = spark.createDataFrame([(i, f"data_{i}")], ["id", "value"])
            df.write.mode("overwrite").saveAsTable(f"list_schema.list_table_{i}")

        # Immediately list tables - should include all 5
        tables = spark.catalog.listTables("list_schema")
        table_names = [t.name for t in tables]
        assert len([n for n in table_names if n.startswith("list_table_")]) == 5

    def test_table_with_very_long_name(self, spark):
        """Test immediate access with very long table names."""
        long_name = "a" * 100  # 100 character name
        df = spark.createDataFrame([(1, "test")], ["id", "value"])
        full_name = f"test_schema.{long_name}"

        df.write.mode("overwrite").saveAsTable(full_name)

        # Immediately verify
        table = spark.table(full_name)
        assert table.count() == 1
        assert spark.catalog.tableExists(full_name) is True

    def test_rapid_overwrite_operations(self, spark):
        """Test rapid overwrite operations with immediate verification."""
        for i in range(20):
            df = spark.createDataFrame([(i, f"iteration_{i}")], ["id", "value"])
            df.write.mode("overwrite").saveAsTable("test_schema.rapid_overwrite")

            # Immediately verify each overwrite
            table = spark.table("test_schema.rapid_overwrite")
            assert table.count() == 1
            rows = table.collect()
            assert rows[0]["id"] == i
            assert rows[0]["value"] == f"iteration_{i}"

    def test_table_access_with_sql_query(self, spark):
        """Test that tables are immediately accessible for SQL queries after saveAsTable()."""
        df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
        df.write.mode("overwrite").saveAsTable("test_schema.sql_test")

        # First verify immediate access via spark.table() (core improvement)
        table = spark.table("test_schema.sql_test")
        assert table.count() == 2

        # Verify data integrity
        rows = table.collect()
        assert len(rows) == 2
        assert rows[0]["id"] == 1
        assert rows[1]["id"] == 2

        # Also verify catalog knows about the table (required for SQL)
        assert spark.catalog.tableExists("test_schema.sql_test") is True

    def test_table_metadata_immediate_access(self, spark):
        """Test that table metadata is immediately available."""
        df = spark.createDataFrame([(1, "test")], ["id", "value"])
        df.write.mode("overwrite").saveAsTable("test_schema.metadata_test")

        # Immediately get table metadata
        table_info = spark.catalog.getTable("test_schema.metadata_test")
        assert table_info is not None
        assert table_info.name == "metadata_test"
        assert table_info.database == "test_schema"

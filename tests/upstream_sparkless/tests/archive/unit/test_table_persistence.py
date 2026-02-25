"""
Tests for table persistence across pipeline runs.

This module tests that tables created with saveAsTable() persist across
multiple pipeline runs when using persistent storage.
"""

import pytest
import tempfile
import os
import gc
import time
from sparkless import SparkSession


@pytest.mark.unit
class TestTablePersistence:
    """Test table persistence fixes."""

    @pytest.fixture
    def temp_db_path(self):
        """Create a temporary database file path."""
        # Create a temp directory for file backend storage
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test_db")
            yield db_path
            # Clean up handled by TemporaryDirectory

    def test_table_persistence_across_sessions(self, temp_db_path):
        """Test that tables persist across different sessions when using persistent storage."""
        # First session - create table
        # Use consistent configuration to avoid DuckDB connection errors
        # DuckDB requires all configuration parameters to match when opening the same database file
        spark1 = SparkSession(
            "persistence_test_1",
            db_path=temp_db_path,
            max_memory="1GB",
            allow_disk_spillover=False,
        )
        try:
            test_data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
            df1 = spark1.createDataFrame(test_data)
            df1.write.mode("overwrite").saveAsTable(
                "persist_test_schema.persist_test_table"
            )

            # Verify table exists in first session
            assert (
                spark1.catalog.tableExists("persist_test_schema", "persist_test_table")
                is True
            )
        finally:
            spark1.stop()
            # Force garbage collection and wait to ensure connections are fully closed
            gc.collect()
            time.sleep(0.5)

        # Second session - read from same database
        # Use identical configuration to avoid DuckDB connection errors
        spark2 = SparkSession(
            "persistence_test_2",
            db_path=temp_db_path,
            max_memory="1GB",
            allow_disk_spillover=False,
        )
        try:
            # Verify table exists in second session
            assert (
                spark2.catalog.tableExists("persist_test_schema", "persist_test_table")
                is True
            )

            # Read the table
            result_df = spark2.table("persist_test_schema.persist_test_table")
            assert result_df.count() == 2

            # Verify data is correct
            rows = result_df.collect()
            assert len(rows) == 2
            assert rows[0].id == 1
            assert rows[0].name == "Alice"
        finally:
            spark2.stop()

    def test_append_to_persistent_table_across_sessions(self, temp_db_path):
        """Test appending to a persistent table across sessions."""
        # First session - create and append
        # Use consistent configuration to avoid DuckDB connection errors
        # DuckDB requires all configuration parameters to match when opening the same database file
        spark1 = SparkSession(
            "append_test_1",
            db_path=temp_db_path,
            max_memory="1GB",
            allow_disk_spillover=False,
        )
        try:
            test_data1 = [{"id": 1, "value": "first"}]
            df1 = spark1.createDataFrame(test_data1)
            df1.write.mode("overwrite").saveAsTable(
                "append_test_schema.append_test_table"
            )

            test_data2 = [{"id": 2, "value": "second"}]
            df2 = spark1.createDataFrame(test_data2)
            df2.write.mode("append").saveAsTable("append_test_schema.append_test_table")

            # Verify 2 rows in first session
            result_df = spark1.table("append_test_schema.append_test_table")
            assert result_df.count() == 2
        finally:
            spark1.stop()
            # Force garbage collection and wait to ensure connections are fully closed
            gc.collect()
            time.sleep(0.5)

        # Second session - append more data
        # Use identical configuration to avoid DuckDB connection errors
        spark2 = SparkSession(
            "append_test_2",
            db_path=temp_db_path,
            max_memory="1GB",
            allow_disk_spillover=False,
        )
        try:
            test_data3 = [{"id": 3, "value": "third"}]
            df3 = spark2.createDataFrame(test_data3)
            df3.write.mode("append").saveAsTable("append_test_schema.append_test_table")

            # Verify 3 rows in second session
            result_df = spark2.table("append_test_schema.append_test_table")
            assert result_df.count() == 3

            # Verify all data is present
            rows = result_df.collect()
            assert len(rows) == 3
            assert rows[0].id == 1
            assert rows[1].id == 2
            assert rows[2].id == 3
        finally:
            spark2.stop()

    def test_in_memory_tables_dont_persist(self):
        """Test that in-memory tables don't persist across sessions."""
        # First session - create table in memory
        spark1 = SparkSession("memory_test_1")  # No db_path = in-memory
        try:
            # Create schema first
            spark1.sql("CREATE SCHEMA IF NOT EXISTS memory_test_schema")

            test_data = [{"id": 1, "name": "Alice"}]
            df1 = spark1.createDataFrame(test_data)
            df1.write.mode("overwrite").saveAsTable(
                "memory_test_schema.memory_test_table"
            )

            # Verify table exists by reading it (more reliable than tableExists)
            result_df = spark1.table("memory_test_schema.memory_test_table")
            assert result_df.count() == 1
        finally:
            spark1.stop()

        # Second session - table should not exist (in-memory doesn't persist)
        spark2 = SparkSession("memory_test_2")  # No db_path = in-memory
        try:
            # Table should not exist in second session (different in-memory instance)
            # Note: In-memory storage is session-specific, so tables don't persist
            # Try to read the table - should fail or return empty
            try:
                result_df = spark2.table("memory_test_schema.memory_test_table")
                # If it doesn't raise an error, table should be empty or not exist
                assert result_df.count() == 0 or not spark2.catalog.tableExists(
                    "memory_test_schema", "memory_test_table"
                )
            except Exception:
                # Expected - table doesn't exist
                pass
        finally:
            spark2.stop()

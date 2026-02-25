"""
Comprehensive tests for Catalog.createDatabase() method.

This module tests the createDatabase() method in various scenarios including
error cases, edge cases, and integration with SQL operations.
"""

import pytest
from sparkless.sql import SparkSession
from sparkless.core.exceptions.analysis import AnalysisException
from sparkless.core.exceptions.validation import IllegalArgumentException


@pytest.mark.compatibility
class TestCatalogCreateDatabase:
    """Test Catalog.createDatabase() in various scenarios."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        session = SparkSession("catalog_test")
        yield session
        session.stop()

    def test_create_database_basic(self, spark):
        """Test basic database creation."""
        spark.catalog.createDatabase("test_db", ignoreIfExists=True)

        # Verify database was created
        databases = spark.catalog.listDatabases()
        db_names = [db.name for db in databases]
        assert "test_db" in db_names

    def test_create_database_ignore_if_exists_true(self, spark):
        """Test createDatabase with ignoreIfExists=True is idempotent."""
        # Create database first time
        spark.catalog.createDatabase("test_db", ignoreIfExists=True)

        # Create again - should not raise
        spark.catalog.createDatabase("test_db", ignoreIfExists=True)

        # Should still exist
        databases = spark.catalog.listDatabases()
        db_names = [db.name for db in databases]
        assert db_names.count("test_db") == 1

    def test_create_database_ignore_if_exists_false_raises(self, spark):
        """Test createDatabase with ignoreIfExists=False raises if exists."""
        # Create database first time
        spark.catalog.createDatabase("test_db", ignoreIfExists=True)

        # Try to create again with ignoreIfExists=False - should raise
        with pytest.raises(AnalysisException, match="already exists"):
            spark.catalog.createDatabase("test_db", ignoreIfExists=False)

    def test_create_database_invalid_name_empty_string(self, spark):
        """Test createDatabase raises error for empty string."""
        with pytest.raises(IllegalArgumentException, match="cannot be empty"):
            spark.catalog.createDatabase("")

    def test_create_database_invalid_name_not_string(self, spark):
        """Test createDatabase raises error for non-string name."""
        with pytest.raises(IllegalArgumentException, match="must be a string"):
            spark.catalog.createDatabase(123)

        with pytest.raises(IllegalArgumentException, match="must be a string"):
            spark.catalog.createDatabase(None)

    def test_create_database_multiple_databases(self, spark):
        """Test creating multiple databases."""
        for i in range(5):
            db_name = f"test_db_{i}"
            spark.catalog.createDatabase(db_name, ignoreIfExists=True)

        # Verify all were created
        databases = spark.catalog.listDatabases()
        db_names = [db.name for db in databases]
        for i in range(5):
            assert f"test_db_{i}" in db_names

    def test_create_database_special_characters(self, spark):
        """Test creating database with special characters in name."""
        # Test with underscore (should work)
        spark.catalog.createDatabase("test_db_123", ignoreIfExists=True)
        databases = spark.catalog.listDatabases()
        db_names = [db.name for db in databases]
        assert "test_db_123" in db_names

    def test_create_database_integration_with_sql(self, spark):
        """Test createDatabase works correctly with SQL operations."""
        # Create database via catalog
        spark.catalog.createDatabase("sql_test_db", ignoreIfExists=True)

        # Use database via catalog (USE might not be fully supported)
        spark.catalog.setCurrentDatabase("sql_test_db")

        # Verify current database
        assert spark.catalog.currentDatabase() == "sql_test_db"

        # Create table in the database
        spark.sql("CREATE TABLE sql_test_db.test_table (id INT, name STRING)")

        # Verify table exists
        assert spark.catalog.tableExists("sql_test_db", "test_table")

    def test_create_database_integration_with_storage(self, spark):
        """Test createDatabase integrates correctly with storage API."""
        # Create database via catalog
        spark.catalog.createDatabase("storage_test_db", ignoreIfExists=True)

        # Verify it's accessible via storage
        assert spark._storage.schema_exists("storage_test_db")

        # Create table via storage
        from sparkless.spark_types import (
            StructType,
            StructField,
            StringType,
            IntegerType,
        )

        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        )
        spark._storage.create_table("storage_test_db", "test_table", schema)

        # Verify via catalog
        assert spark.catalog.tableExists("storage_test_db", "test_table")

    def test_create_database_default_ignore_if_exists(self, spark):
        """Test createDatabase defaults to ignoreIfExists=True."""
        # Create database without specifying ignoreIfExists
        spark.catalog.createDatabase("default_test_db")

        # Create again - should not raise (defaults to True)
        spark.catalog.createDatabase("default_test_db")

        # Should still exist
        databases = spark.catalog.listDatabases()
        db_names = [db.name for db in databases]
        assert "default_test_db" in db_names

    def test_create_database_case_sensitive(self, spark):
        """Test database names are case-sensitive."""
        spark.catalog.createDatabase("TestDB", ignoreIfExists=True)
        spark.catalog.createDatabase("testdb", ignoreIfExists=True)

        # Both should exist as separate databases
        databases = spark.catalog.listDatabases()
        db_names = [db.name for db in databases]
        assert "TestDB" in db_names
        assert "testdb" in db_names
        assert db_names.count("TestDB") == 1
        assert db_names.count("testdb") == 1


@pytest.mark.compatibility
class TestCatalogCreateDatabaseEdgeCases:
    """Test edge cases and error scenarios for createDatabase()."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        session = SparkSession("edge_case_test")
        yield session
        session.stop()

    def test_create_database_very_long_name(self, spark):
        """Test creating database with very long name."""
        long_name = "a" * 1000
        spark.catalog.createDatabase(long_name, ignoreIfExists=True)

        # Verify it was created
        databases = spark.catalog.listDatabases()
        db_names = [db.name for db in databases]
        assert long_name in db_names

    def test_create_database_unicode_name(self, spark):
        """Test creating database with unicode characters."""
        unicode_name = "测试数据库_123"
        spark.catalog.createDatabase(unicode_name, ignoreIfExists=True)

        # Verify it was created
        databases = spark.catalog.listDatabases()
        db_names = [db.name for db in databases]
        assert unicode_name in db_names

    def test_create_database_concurrent_creation(self, spark):
        """Test concurrent database creation (should be idempotent)."""
        import threading

        results = []

        def create_db(name: str):
            try:
                spark.catalog.createDatabase(name, ignoreIfExists=True)
                results.append(f"created_{name}")
            except Exception as e:
                results.append(f"error_{name}: {str(e)}")

        # Create same database from multiple threads
        threads = []
        for i in range(5):
            t = threading.Thread(target=create_db, args=("concurrent_db",))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # All should succeed (idempotent)
        assert len(results) == 5
        assert all("created_concurrent_db" in r for r in results)

        # Database should exist
        databases = spark.catalog.listDatabases()
        db_names = [db.name for db in databases]
        assert db_names.count("concurrent_db") == 1

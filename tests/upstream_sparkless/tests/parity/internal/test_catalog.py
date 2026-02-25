"""
PySpark parity tests for Catalog operations.

Tests validate that Sparkless Catalog operations behave identically to PySpark.
"""

import pytest
from tests.fixtures.parity_base import ParityTestBase
from tests.fixtures.spark_backend import get_backend_type, BackendType


def _is_pyspark_mode() -> bool:
    """Check if running in PySpark mode."""
    backend = get_backend_type()
    return backend == BackendType.PYSPARK


class TestCatalogParity(ParityTestBase):
    """Test Catalog operations parity with PySpark."""

    def test_list_databases(self, spark):
        """Test listDatabases matches PySpark behavior."""
        databases = spark.catalog.listDatabases()

        # Should at least have default database
        assert len(databases) >= 1
        db_names = [db.name for db in databases]
        assert "default" in db_names

    @pytest.mark.skipif(
        _is_pyspark_mode(),
        reason="createDatabase is a sparkless-specific Catalog API - PySpark uses SQL CREATE DATABASE",
    )
    def test_create_database_catalog(self, spark):
        """Test createDatabase via catalog matches PySpark behavior.

        Note: This is a sparkless-specific API. PySpark uses SQL CREATE DATABASE instead.
        """
        # Create database
        spark.catalog.createDatabase("test_catalog_db", ignoreIfExists=True)

        # Verify it exists
        databases = spark.catalog.listDatabases()
        db_names = [db.name for db in databases]
        assert "test_catalog_db" in db_names

        # Cleanup
        spark.catalog.dropDatabase("test_catalog_db", ignoreIfNotExists=True)

    @pytest.mark.skipif(
        _is_pyspark_mode(),
        reason="createDatabase is a sparkless-specific Catalog API - PySpark uses SQL CREATE DATABASE",
    )
    def test_drop_database_catalog(self, spark):
        """Test dropDatabase via catalog matches PySpark behavior.

        Note: This is a sparkless-specific API. PySpark uses SQL DROP DATABASE instead.
        """
        # Create database
        spark.catalog.createDatabase("test_drop_db", ignoreIfExists=True)

        # Drop it
        spark.catalog.dropDatabase("test_drop_db", ignoreIfNotExists=True)

        # Verify it doesn't exist
        databases = spark.catalog.listDatabases()
        db_names = [db.name for db in databases]
        assert "test_drop_db" not in db_names

    @pytest.mark.skipif(
        _is_pyspark_mode(),
        reason="createDatabase is a sparkless-specific Catalog API - PySpark uses SQL CREATE DATABASE",
    )
    def test_set_current_database(self, spark):
        """Test setCurrentDatabase matches PySpark behavior.

        Note: This is a sparkless-specific API. PySpark uses SQL USE DATABASE instead.
        """
        # Create database
        spark.catalog.createDatabase("test_current_db2", ignoreIfExists=True)

        # Set as current
        spark.catalog.setCurrentDatabase("test_current_db2")

        # Create table in current database
        data = [("Alice", 25)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").saveAsTable("catalog_test_table")

        # Verify it exists
        assert spark.catalog.tableExists("catalog_test_table", "test_current_db2")

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS test_current_db2.catalog_test_table")
        spark.catalog.dropDatabase("test_current_db2", ignoreIfNotExists=True)

    def test_list_tables(self, spark):
        """Test listTables matches PySpark behavior."""
        # Create a table
        data = [("Alice", 25)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").saveAsTable("list_tables_test")

        # List tables
        tables = spark.catalog.listTables()
        table_names = [t.name for t in tables]

        # Should contain our table
        assert "list_tables_test" in table_names

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS list_tables_test")

    @pytest.mark.skipif(
        _is_pyspark_mode(),
        reason="createDatabase is a sparkless-specific Catalog API - PySpark uses SQL CREATE DATABASE",
    )
    def test_list_tables_in_database(self, spark):
        """Test listTables with database parameter matches PySpark behavior.

        Note: This test uses sparkless-specific createDatabase API.
        """
        # Create database
        spark.catalog.createDatabase("list_db", ignoreIfExists=True)

        # Create table in that database
        data = [("Alice", 25)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").saveAsTable("list_db.list_table")

        # List tables in database
        tables = spark.catalog.listTables("list_db")
        table_names = [t.name for t in tables]

        assert "list_table" in table_names

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS list_db.list_table")
        spark.catalog.dropDatabase("list_db", ignoreIfNotExists=True)

    def test_table_exists(self, spark):
        """Test tableExists matches PySpark behavior."""
        # Create table
        data = [("Alice", 25)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").saveAsTable("exists_test")

        # Check existence
        assert spark.catalog.tableExists("exists_test")
        assert not spark.catalog.tableExists("non_existent_table")

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS exists_test")

    @pytest.mark.skipif(
        _is_pyspark_mode(),
        reason="createDatabase is a sparkless-specific Catalog API - PySpark uses SQL CREATE DATABASE",
    )
    def test_table_exists_in_database(self, spark):
        """Test tableExists with database parameter matches PySpark behavior.

        Note: This test uses sparkless-specific createDatabase API.
        """
        # Create database
        spark.catalog.createDatabase("exists_db", ignoreIfExists=True)

        # Create table
        data = [("Alice", 25)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").saveAsTable("exists_db.exists_table")

        # Check existence
        assert spark.catalog.tableExists("exists_table", "exists_db")
        assert not spark.catalog.tableExists("exists_table", "default")

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS exists_db.exists_table")
        spark.catalog.dropDatabase("exists_db", ignoreIfNotExists=True)

    def test_get_table(self, spark):
        """Test getTable matches PySpark behavior."""
        # Create table
        data = [("Alice", 25)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").saveAsTable("get_table_test")

        # Get table
        table = spark.catalog.getTable("get_table_test")

        # Verify table properties
        assert table.name == "get_table_test"
        assert table.database == "default"

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS get_table_test")

    @pytest.mark.skipif(
        _is_pyspark_mode(),
        reason="createDatabase is a sparkless-specific Catalog API - PySpark uses SQL CREATE DATABASE",
    )
    def test_get_table_in_database(self, spark):
        """Test getTable with database parameter matches PySpark behavior.

        Note: This test uses sparkless-specific createDatabase API.
        """
        # Create database
        spark.catalog.createDatabase("get_db", ignoreIfExists=True)

        # Create table
        data = [("Alice", 25)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").saveAsTable("get_db.get_table")

        # Get table
        table = spark.catalog.getTable("get_db", "get_table")

        # Verify table properties
        assert table.name == "get_table"
        assert table.database == "get_db"

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS get_db.get_table")
        spark.catalog.dropDatabase("get_db", ignoreIfNotExists=True)

    def test_cache_table(self, spark):
        """Test cacheTable matches PySpark behavior."""
        # Create table
        data = [("Alice", 25), ("Bob", 30)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").saveAsTable("cache_test")

        # Cache table
        spark.catalog.cacheTable("cache_test")

        # Verify it's cached (by checking if we can query it)
        result = spark.sql("SELECT * FROM cache_test")
        assert result.count() == 2

        # Uncache
        spark.catalog.uncacheTable("cache_test")

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS cache_test")

    def test_uncache_table(self, spark):
        """Test uncacheTable matches PySpark behavior."""
        # Create table
        data = [("Alice", 25)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").saveAsTable("uncache_test")

        # Cache then uncache
        spark.catalog.cacheTable("uncache_test")
        spark.catalog.uncacheTable("uncache_test")

        # Should still be queryable
        result = spark.sql("SELECT * FROM uncache_test")
        assert result.count() == 1

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS uncache_test")

    def test_is_cached(self, spark):
        """Test isCached matches PySpark behavior."""
        # Create table
        data = [("Alice", 25)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").saveAsTable("is_cached_test")

        # Initially not cached
        assert not spark.catalog.isCached("is_cached_test")

        # Cache it
        spark.catalog.cacheTable("is_cached_test")

        # Should be cached
        assert spark.catalog.isCached("is_cached_test")

        # Uncache
        spark.catalog.uncacheTable("is_cached_test")

        # Should not be cached
        assert not spark.catalog.isCached("is_cached_test")

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS is_cached_test")

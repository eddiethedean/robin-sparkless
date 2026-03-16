"""
PySpark parity tests for Catalog operations.

Tests validate that Sparkless Catalog operations behave identically to PySpark.
"""

import pytest
from sparkless.testing import is_pyspark_mode
from tests.tools.parity_base import ParityTestBase


class TestCatalogParity(ParityTestBase):
    """Test Catalog operations parity with PySpark."""

    def test_list_databases(self, spark):
        """Test listDatabases matches PySpark behavior."""
        databases = spark.catalog.listDatabases()

        # Should at least have default database
        assert len(databases) >= 1
        db_names = [db.name for db in databases]
        assert "default" in db_names

    def test_create_database_catalog(self, spark):
        """Test createDatabase via catalog matches PySpark behavior.

        Note: This is a sparkless-specific API. PySpark uses SQL CREATE DATABASE instead.
        """
        # Real PySpark Catalog does not expose createDatabase; this helper is sparkless-only.
        with pytest.raises(AttributeError):
            spark.catalog.createDatabase("test_catalog_db", ignoreIfExists=True)

    def test_drop_database_catalog(self, spark):
        """Test dropDatabase via catalog matches PySpark behavior.

        Note: This is a sparkless-specific API. PySpark uses SQL DROP DATABASE instead.
        """
        with pytest.raises(AttributeError):
            spark.catalog.dropDatabase("test_drop_db", ignoreIfNotExists=True)

    def test_set_current_database(self, spark):
        """Test setCurrentDatabase matches PySpark behavior.

        Note: This is a sparkless-specific API. PySpark uses SQL USE DATABASE instead.
        """
        spark.sql("CREATE DATABASE IF NOT EXISTS test_current_db2")
        spark.catalog.setCurrentDatabase("test_current_db2")
        assert spark.catalog.currentDatabase() == "test_current_db2"

        # Cleanup
        spark.sql("DROP DATABASE IF EXISTS test_current_db2")

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

    def test_list_tables_in_database(self, spark):
        """Test listTables with database parameter matches PySpark behavior.

        Note: This test uses sparkless-specific createDatabase API.
        """
        spark.sql("CREATE DATABASE IF NOT EXISTS list_db")
        df = spark.createDataFrame([("Alice", 25)], ["name", "age"])
        df.write.mode("overwrite").saveAsTable("list_db.list_table")

        tables = spark.catalog.listTables("list_db")
        table_names = [t.name for t in tables]
        assert "list_table" in table_names

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS list_db.list_table")
        spark.sql("DROP DATABASE IF EXISTS list_db")

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

    def test_table_exists_in_database(self, spark):
        """Test tableExists with database parameter matches PySpark behavior.

        Note: This test uses sparkless-specific createDatabase API.
        """
        spark.sql("CREATE DATABASE IF NOT EXISTS exists_db")
        df = spark.createDataFrame([("Alice", 25)], ["name", "age"])
        df.write.mode("overwrite").saveAsTable("exists_db.exists_table")

        assert spark.catalog.tableExists("exists_table", "exists_db")

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS exists_db.exists_table")
        spark.sql("DROP DATABASE IF EXISTS exists_db")

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

    def test_get_table_in_database(self, spark):
        """Test getTable with database parameter matches PySpark behavior.

        Note: This test uses sparkless-specific createDatabase API.
        """
        with pytest.raises(TypeError):
            # PySpark's signature is getTable(tableName) only (db is separate).
            spark.catalog.getTable("get_db", "get_table")

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

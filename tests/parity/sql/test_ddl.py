"""
PySpark parity tests for SQL DDL operations.

Tests validate that Sparkless SQL DDL statements behave identically to PySpark.
"""

import pytest

try:
    from pyspark.errors import AnalysisException
except ImportError:
    AnalysisException = None  # type: ignore[misc, assignment]

from sparkless.errors import SparklessError
from tests.tools.parity_base import ParityTestBase
from sparkless.testing import is_pyspark_mode


class TestSQLDDLParity(ParityTestBase):
    """Test SQL DDL operations parity with PySpark."""

    def test_create_database(self, spark):
        """Test CREATE DATABASE matches PySpark behavior."""
        # Create database
        spark.sql("CREATE DATABASE IF NOT EXISTS test_db")

        # Verify it exists
        databases = spark.catalog.listDatabases()
        db_names = [db.name for db in databases]
        assert "test_db" in db_names

        # Cleanup
        spark.sql("DROP DATABASE IF EXISTS test_db")

    def test_create_database_if_not_exists(self, spark):
        """Test CREATE DATABASE IF NOT EXISTS matches PySpark behavior."""
        # First create
        spark.sql("CREATE DATABASE IF NOT EXISTS test_db2")

        # Second create should not fail
        spark.sql("CREATE DATABASE IF NOT EXISTS test_db2")

        # Verify it exists
        databases = spark.catalog.listDatabases()
        db_names = [db.name for db in databases]
        assert "test_db2" in db_names

        # Cleanup
        spark.sql("DROP DATABASE IF EXISTS test_db2")

    def test_drop_database(self, spark):
        """Test DROP DATABASE matches PySpark behavior."""
        # Create database
        spark.sql("CREATE DATABASE IF NOT EXISTS test_db3")

        # Drop it
        spark.sql("DROP DATABASE IF EXISTS test_db3")

        # Verify it doesn't exist
        databases = spark.catalog.listDatabases()
        db_names = [db.name for db in databases]
        assert "test_db3" not in db_names

    def test_create_table_from_dataframe(self, spark):
        """Test CREATE TABLE from DataFrame matches PySpark behavior."""
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        df = spark.createDataFrame(data, ["name", "age"])

        # Create table
        df.write.mode("overwrite").saveAsTable("test_users")

        # Verify table exists
        assert spark.catalog.tableExists("test_users")

        # Verify we can query it
        result = spark.sql("SELECT * FROM test_users")
        assert result.count() == 3

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS test_users")

    @pytest.mark.hive
    def test_create_table_with_select(self, spark):
        """Test CREATE TABLE AS SELECT matches PySpark behavior.

        Runs in PySpark with Hive support enabled (@pytest.mark.hive) so CTAS succeeds.
        Sparkless does not support CTAS yet (see issue #1508); skipped in sparkless mode.
        """
        if not is_pyspark_mode():
            pytest.skip(
                "See https://github.com/eddiethedean/robin-sparkless/issues/1508 – "
                "sparkless CTAS parity gap; unskip once sparkless matches PySpark."
            )
        data = [("Alice", 25, "IT"), ("Bob", 30, "HR"), ("Charlie", 35, "IT")]
        df = spark.createDataFrame(data, ["name", "age", "dept"])
        df.write.mode("overwrite").saveAsTable("employees")

        # With Hive support (enabled via @pytest.mark.hive), CTAS succeeds.
        spark.sql(
            "CREATE TABLE IF NOT EXISTS it_employees AS SELECT name, age FROM employees WHERE dept = 'IT'"
        )

        # Verify table exists and has expected data
        assert spark.catalog.tableExists("it_employees")
        result = spark.sql("SELECT * FROM it_employees ORDER BY name")
        rows = result.collect()
        assert len(rows) == 2
        assert rows[0]["name"] == "Alice" and rows[0]["age"] == 25
        assert rows[1]["name"] == "Charlie" and rows[1]["age"] == 35

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS employees")
        spark.sql("DROP TABLE IF EXISTS it_employees")

    def test_create_table_with_select_fails_without_hive(self, spark):
        """Test that CREATE TABLE AS SELECT fails when Hive support is not enabled.

        Uses the default session (no @pytest.mark.hive). PySpark and Sparkless both
        raise with NOT_SUPPORTED_COMMAND_WITHOUT_HIVE_SUPPORT in the message.
        """
        data = [("a", 1, "X"), ("b", 2, "Y")]
        df = spark.createDataFrame(data, ["name", "age", "dept"])
        df.write.mode("overwrite").saveAsTable("ctas_no_hive_src")

        _raise = (AnalysisException, SparklessError) if AnalysisException is not None else (SparklessError,)
        try:
            with pytest.raises(_raise) as excinfo:
                spark.sql(
                    "CREATE TABLE ctas_no_hive_dst AS SELECT name, age FROM ctas_no_hive_src"
                )
            msg = str(excinfo.value)
            assert "NOT_SUPPORTED_COMMAND_WITHOUT_HIVE_SUPPORT" in msg
        finally:
            spark.sql("DROP TABLE IF EXISTS ctas_no_hive_src")
            spark.sql("DROP TABLE IF EXISTS ctas_no_hive_dst")

    def test_drop_table(self, spark):
        """Test DROP TABLE matches PySpark behavior."""
        data = [("Alice", 25)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").saveAsTable("temp_table")

        # Verify exists
        assert spark.catalog.tableExists("temp_table")

        # Drop it
        spark.sql("DROP TABLE IF EXISTS temp_table")

        # Verify doesn't exist
        assert not spark.catalog.tableExists("temp_table")

    def test_drop_table_if_exists(self, spark):
        """Test DROP TABLE IF EXISTS matches PySpark behavior."""
        # Should not fail even if table doesn't exist
        spark.sql("DROP TABLE IF EXISTS non_existent_table")

        # Should work on existing table
        data = [("Alice", 25)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").saveAsTable("temp_table2")
        spark.sql("DROP TABLE IF EXISTS temp_table2")
        assert not spark.catalog.tableExists("temp_table2")

    def test_create_schema(self, spark):
        """Test CREATE SCHEMA matches PySpark behavior (same as DATABASE)."""
        # SCHEMA is synonymous with DATABASE in Spark
        spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")

        # Verify it exists (checking as database)
        databases = spark.catalog.listDatabases()
        db_names = [db.name for db in databases]
        assert "test_schema" in db_names

        # Cleanup
        spark.sql("DROP SCHEMA IF EXISTS test_schema")

    def test_set_current_database(self, spark):
        """Test setting current database matches PySpark behavior."""
        # Create database
        spark.sql("CREATE DATABASE IF NOT EXISTS test_current_db")

        # Set as current
        spark.catalog.setCurrentDatabase("test_current_db")

        # Create table in current database
        data = [("Alice", 25)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").saveAsTable("current_db_table")

        # Verify it exists in the current database
        assert spark.catalog.tableExists("current_db_table", "test_current_db")

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS test_current_db.current_db_table")
        spark.sql("DROP DATABASE IF EXISTS test_current_db")

    def test_table_in_specific_database(self, spark):
        """Test creating table in specific database matches PySpark behavior."""
        # Create database
        spark.sql("CREATE DATABASE IF NOT EXISTS test_db_specific")

        # Create table in specific database
        data = [("Alice", 25)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").option("path", "/tmp/test_path").saveAsTable(
            "test_db_specific.specific_table"
        )

        # Verify it exists
        assert spark.catalog.tableExists("specific_table", "test_db_specific")

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS test_db_specific.specific_table")
        spark.sql("DROP DATABASE IF EXISTS test_db_specific")

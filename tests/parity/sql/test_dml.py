"""
PySpark parity tests for SQL DML operations.

Tests validate that Sparkless SQL DML statements behave identically to PySpark.
"""

import pytest
from tests.fixtures.parity_base import ParityTestBase
from tests.fixtures.spark_backend import get_backend_type, BackendType


def _is_pyspark_mode() -> bool:
    """Check if running in PySpark mode."""
    backend = get_backend_type()
    return backend == BackendType.PYSPARK


class TestSQLDMLParity(ParityTestBase):
    """Test SQL DML operations parity with PySpark."""

    def test_insert_into_table(self, spark):
        """Test INSERT INTO matches PySpark behavior."""
        # Create table
        data = [("Alice", 25)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").saveAsTable("insert_test")

        # Insert new row
        spark.sql("INSERT INTO insert_test VALUES ('Bob', 30)")

        # Verify data
        result = spark.sql("SELECT * FROM insert_test ORDER BY name")
        rows = result.collect()
        assert len(rows) == 2
        assert rows[0]["name"] == "Alice"
        assert rows[1]["name"] == "Bob"

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS insert_test")

    def test_insert_into_specific_columns(self, spark):
        """Test INSERT INTO with specific columns matches PySpark behavior."""
        # Create table
        data = [("Alice", 25, "IT")]
        df = spark.createDataFrame(data, ["name", "age", "dept"])
        df.write.mode("overwrite").saveAsTable("insert_specific")

        # Insert with specific columns
        spark.sql("INSERT INTO insert_specific (name, age) VALUES ('Bob', 30)")

        # Verify data
        result = spark.sql("SELECT * FROM insert_specific ORDER BY name")
        rows = result.collect()
        assert len(rows) == 2
        # Bob should have NULL for dept
        assert rows[1]["name"] == "Bob"
        assert rows[1]["age"] == 30

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS insert_specific")

    def test_insert_multiple_values(self, spark):
        """Test INSERT with multiple VALUES matches PySpark behavior."""
        # Create table
        data = [("Alice", 25)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").saveAsTable("insert_multi")

        # Insert multiple rows
        spark.sql("INSERT INTO insert_multi VALUES ('Bob', 30), ('Charlie', 35)")

        # Verify data
        result = spark.sql("SELECT * FROM insert_multi ORDER BY name")
        rows = result.collect()
        assert len(rows) == 3

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS insert_multi")

    @pytest.mark.skipif(
        _is_pyspark_mode(),
        reason="UPDATE TABLE is not supported in PySpark - this is a sparkless-specific feature",
    )
    def test_update_table(self, spark):
        """Test UPDATE matches PySpark behavior.

        Note: This is a sparkless-specific feature. PySpark does not support UPDATE TABLE.
        """
        # Create table
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").saveAsTable("update_test")

        # Update rows
        spark.sql("UPDATE update_test SET age = 26 WHERE name = 'Alice'")

        # Verify update
        result = spark.sql("SELECT * FROM update_test WHERE name = 'Alice'")
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["age"] == 26

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS update_test")

    @pytest.mark.skipif(
        _is_pyspark_mode(),
        reason="UPDATE TABLE is not supported in PySpark - this is a sparkless-specific feature",
    )
    def test_update_multiple_columns(self, spark):
        """Test UPDATE multiple columns matches PySpark behavior.

        Note: This is a sparkless-specific feature. PySpark does not support UPDATE TABLE.
        """
        # Create table
        data = [("Alice", 25, "IT")]
        df = spark.createDataFrame(data, ["name", "age", "dept"])
        df.write.mode("overwrite").saveAsTable("update_multi")

        # Update multiple columns
        spark.sql("UPDATE update_multi SET age = 26, dept = 'HR' WHERE name = 'Alice'")

        # Verify update
        result = spark.sql("SELECT * FROM update_multi WHERE name = 'Alice'")
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["age"] == 26
        assert rows[0]["dept"] == "HR"

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS update_multi")

    @pytest.mark.skipif(
        _is_pyspark_mode(),
        reason="DELETE FROM TABLE is not supported in PySpark - this is a sparkless-specific feature",
    )
    def test_delete_from_table(self, spark):
        """Test DELETE FROM matches PySpark behavior.

        Note: This is a sparkless-specific feature. PySpark does not support DELETE FROM TABLE.
        """
        # Create table
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").saveAsTable("delete_test")

        # Delete rows
        spark.sql("DELETE FROM delete_test WHERE age > 30")

        # Verify deletion
        result = spark.sql("SELECT * FROM delete_test ORDER BY name")
        rows = result.collect()
        assert len(rows) == 2
        assert rows[0]["name"] == "Alice"
        assert rows[1]["name"] == "Bob"

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS delete_test")

    @pytest.mark.skipif(
        _is_pyspark_mode(),
        reason="DELETE FROM TABLE is not supported in PySpark - this is a sparkless-specific feature",
    )
    def test_delete_all_rows(self, spark):
        """Test DELETE without WHERE matches PySpark behavior.

        Note: This is a sparkless-specific feature. PySpark does not support DELETE FROM TABLE.
        """
        # Create table
        data = [("Alice", 25), ("Bob", 30)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").saveAsTable("delete_all")

        # Delete all
        spark.sql("DELETE FROM delete_all")

        # Verify all deleted
        result = spark.sql("SELECT * FROM delete_all")
        assert result.count() == 0

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS delete_all")

    def test_insert_from_select(self, spark):
        """Test INSERT INTO ... SELECT matches PySpark behavior."""
        # Create source table
        data = [("Alice", 25, "IT"), ("Bob", 30, "HR"), ("Charlie", 35, "IT")]
        df = spark.createDataFrame(data, ["name", "age", "dept"])
        df.write.mode("overwrite").saveAsTable("source_table")

        # Create target table
        empty_df = spark.createDataFrame([], "name string, age int")
        empty_df.write.mode("overwrite").saveAsTable("target_table")

        # Insert from select
        spark.sql(
            "INSERT INTO target_table SELECT name, age FROM source_table WHERE dept = 'IT'"
        )

        # Verify
        result = spark.sql("SELECT * FROM target_table ORDER BY name")
        rows = result.collect()
        assert len(rows) == 2

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS source_table")
        spark.sql("DROP TABLE IF EXISTS target_table")

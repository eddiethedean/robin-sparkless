"""
PySpark parity tests for SQL DML operations.

Tests validate that Sparkless SQL DML statements behave identically to PySpark.
"""

import pytest

try:
    from pyspark.errors import AnalysisException, UnsupportedOperationException
except ImportError:
    AnalysisException = None  # type: ignore[misc, assignment]
    UnsupportedOperationException = None  # type: ignore[misc, assignment]

from sparkless.errors import SparklessError
from tests.tools.parity_base import ParityTestBase

# Exception types for pytest.raises; when PySpark is not installed only SparklessError is used.
_RAISE_ANALYSIS = (AnalysisException, SparklessError) if AnalysisException is not None else (SparklessError,)
_RAISE_UNSUPPORTED = (UnsupportedOperationException, SparklessError) if UnsupportedOperationException is not None else (SparklessError,)


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

    def test_update_table(self, spark):
        """Test UPDATE matches PySpark behavior (rejected for default catalog)."""
        # Create table
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").saveAsTable("update_test")

        # PySpark and Sparkless reject UPDATE for default catalog tables (#1507).
        with pytest.raises(_RAISE_UNSUPPORTED) as excinfo:
            spark.sql("UPDATE update_test SET age = 26 WHERE name = 'Alice'")
        assert "UPDATE TABLE is not supported temporarily" in str(excinfo.value)

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS update_test")

    def test_update_multiple_columns(self, spark):
        """Test UPDATE multiple columns matches PySpark behavior (rejected for default catalog)."""
        # Create table
        data = [("Alice", 25, "IT")]
        df = spark.createDataFrame(data, ["name", "age", "dept"])
        df.write.mode("overwrite").saveAsTable("update_multi")

        with pytest.raises(_RAISE_UNSUPPORTED) as excinfo:
            spark.sql(
                "UPDATE update_multi SET age = 26, dept = 'HR' WHERE name = 'Alice'"
            )
        assert "UPDATE TABLE is not supported temporarily" in str(excinfo.value)

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS update_multi")

    def test_delete_from_table(self, spark):
        """Test DELETE FROM matches PySpark behavior (rejected for default catalog)."""
        # Create table
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").saveAsTable("delete_test")

        with pytest.raises(_RAISE_ANALYSIS) as excinfo:
            spark.sql("DELETE FROM delete_test WHERE age > 30")
        assert "UNSUPPORTED_FEATURE.TABLE_OPERATION" in str(excinfo.value)

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS delete_test")

    def test_delete_all_rows(self, spark):
        """Test DELETE without WHERE matches PySpark behavior (rejected for default catalog)."""
        # Create table
        data = [("Alice", 25), ("Bob", 30)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").saveAsTable("delete_all")

        with pytest.raises(_RAISE_ANALYSIS) as excinfo:
            spark.sql("DELETE FROM delete_all")
        assert "UNSUPPORTED_FEATURE.TABLE_OPERATION" in str(excinfo.value)

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

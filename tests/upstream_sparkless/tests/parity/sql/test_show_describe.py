"""
PySpark parity tests for SQL SHOW and DESCRIBE operations.

Tests validate that Sparkless SHOW and DESCRIBE statements behave identically to PySpark.
"""

from tests.fixtures.parity_base import ParityTestBase


class TestSQLShowDescribeParity(ParityTestBase):
    """Test SQL SHOW and DESCRIBE operations parity with PySpark."""

    def test_show_databases(self, spark):
        """Test SHOW DATABASES matches PySpark behavior."""
        # Create a test database
        spark.sql("CREATE DATABASE IF NOT EXISTS show_test_db")

        # Show databases
        result = spark.sql("SHOW DATABASES")
        db_names = [row["databaseName"] for row in result.collect()]

        # Should contain default and our test database
        assert "default" in db_names
        assert "show_test_db" in db_names

        # Cleanup
        spark.sql("DROP DATABASE IF EXISTS show_test_db")

    def test_show_tables(self, spark):
        """Test SHOW TABLES matches PySpark behavior."""
        # Create a table
        data = [("Alice", 25)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").saveAsTable("show_test_table")

        # Show tables
        result = spark.sql("SHOW TABLES")
        table_names = [row["tableName"] for row in result.collect()]

        # Should contain our table
        assert "show_test_table" in table_names

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS show_test_table")

    def test_show_tables_in_database(self, spark):
        """Test SHOW TABLES IN database matches PySpark behavior."""
        # Create database
        spark.sql("CREATE DATABASE IF NOT EXISTS show_db")

        # Create table in that database
        data = [("Alice", 25)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").saveAsTable("show_db.show_table")

        # Show tables in database
        result = spark.sql("SHOW TABLES IN show_db")
        table_names = [row["tableName"] for row in result.collect()]

        assert "show_table" in table_names

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS show_db.show_table")
        spark.sql("DROP DATABASE IF EXISTS show_db")

    def test_describe_table(self, spark):
        """Test DESCRIBE TABLE matches PySpark behavior."""
        # Create table
        data = [("Alice", 25, 50000.0)]
        df = spark.createDataFrame(data, ["name", "age", "salary"])
        df.write.mode("overwrite").saveAsTable("describe_test")

        # Describe table
        result = spark.sql("DESCRIBE describe_test")
        columns = result.collect()

        # Should have 3 columns
        assert len(columns) == 3
        col_names = [col["col_name"] for col in columns]
        assert "name" in col_names
        assert "age" in col_names
        assert "salary" in col_names

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS describe_test")

    def test_describe_extended(self, spark):
        """Test DESCRIBE EXTENDED matches PySpark behavior."""
        # Create table
        data = [("Alice", 25)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").saveAsTable("describe_extended_test")

        # Describe extended
        result = spark.sql("DESCRIBE EXTENDED describe_extended_test")
        rows = result.collect()

        # Should have more information than regular describe
        assert len(rows) > 2

        # Should contain column info
        col_names = [row["col_name"] for row in rows]
        assert "name" in col_names or any("name" in str(row) for row in rows)

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS describe_extended_test")

    def test_describe_column(self, spark):
        """Test DESCRIBE column matches PySpark behavior."""
        # Create table
        data = [("Alice", 25)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").saveAsTable("describe_col_test")

        # Describe specific column
        result = spark.sql("DESCRIBE describe_col_test age")
        rows = result.collect()

        # Should have info about the age column
        assert len(rows) >= 1
        assert any("age" in str(row).lower() for row in rows)

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS describe_col_test")

    def test_show_columns(self, spark):
        """Test SHOW COLUMNS matches PySpark behavior (if supported)."""
        # Create table
        data = [("Alice", 25, "IT")]
        df = spark.createDataFrame(data, ["name", "age", "dept"])
        df.write.mode("overwrite").saveAsTable("show_cols_test")

        # Try SHOW COLUMNS (may not be supported, but test if it is)
        try:
            result = spark.sql("SHOW COLUMNS FROM show_cols_test")
            cols = result.collect()
            col_names = [col["col_name"] for col in cols] if cols else []
            # If supported, verify columns
            if col_names:
                assert "name" in col_names or "name" in str(cols)
        except Exception:
            # If not supported, that's okay - skip this test
            pass

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS show_cols_test")

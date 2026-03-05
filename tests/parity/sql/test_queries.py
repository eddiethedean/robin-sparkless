"""
PySpark parity tests for SQL query execution.

Tests validate that Sparkless SQL queries behave identically to PySpark.
"""

from tests.fixtures.parity_base import ParityTestBase


class TestSQLQueriesParity(ParityTestBase):
    """Test SQL query execution parity with PySpark."""

    def test_basic_select(self, spark, table_prefix):
        """Test basic SELECT matches PySpark behavior."""
        expected = self.load_expected("sql_operations", "basic_select")

        # Create table from input data (unique name for shared session)
        tbl = f"{table_prefix}_test_table"
        df = spark.createDataFrame(expected["input_data"])
        df.write.mode("overwrite").saveAsTable(tbl)

        # Execute SQL query - use the query from expected output
        result = spark.sql(f"SELECT id, name, age FROM {tbl}")

        self.assert_parity(result, expected)

    def test_filtered_select(self, spark, table_prefix):
        """Test filtered SELECT matches PySpark behavior."""
        expected = self.load_expected("sql_operations", "filtered_select")

        tbl = f"{table_prefix}_test_table"
        df = spark.createDataFrame(expected["input_data"])
        df.write.mode("overwrite").saveAsTable(tbl)

        result = spark.sql(f"SELECT * FROM {tbl} WHERE age > 30")

        self.assert_parity(result, expected)

    def test_group_by(self, spark, table_prefix):
        """Test GROUP BY matches PySpark behavior."""
        expected = self.load_expected("sql_operations", "group_by")

        tbl = f"{table_prefix}_test_table"
        df = spark.createDataFrame(expected["input_data"])
        df.write.mode("overwrite").saveAsTable(tbl)

        # Use the query from expected output: GROUP BY (age > 30)
        result = spark.sql(f"SELECT COUNT(*) as count FROM {tbl} GROUP BY (age > 30)")

        self.assert_parity(result, expected)

    def test_aggregation(self, spark, table_prefix):
        """Test aggregation in SQL matches PySpark behavior."""
        expected = self.load_expected("sql_operations", "aggregation")

        tbl = f"{table_prefix}_test_table"
        df = spark.createDataFrame(expected["input_data"])
        df.write.mode("overwrite").saveAsTable(tbl)

        result = spark.sql(f"SELECT AVG(salary) as avg_salary FROM {tbl}")

        self.assert_parity(result, expected)

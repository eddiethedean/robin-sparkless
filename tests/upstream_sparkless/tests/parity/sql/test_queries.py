"""
PySpark parity tests for SQL query execution.

Tests validate that Sparkless SQL queries behave identically to PySpark.
"""

from tests.fixtures.parity_base import ParityTestBase


class TestSQLQueriesParity(ParityTestBase):
    """Test SQL query execution parity with PySpark."""

    def test_basic_select(self, spark):
        """Test basic SELECT matches PySpark behavior."""
        expected = self.load_expected("sql_operations", "basic_select")

        # Create table from input data
        df = spark.createDataFrame(expected["input_data"])
        df.write.mode("overwrite").saveAsTable("test_table")

        # Execute SQL query - use the query from expected output
        # The expected output was generated with "SELECT id, name, age FROM employees"
        # So we need to use the same query pattern
        result = spark.sql("SELECT id, name, age FROM test_table")

        self.assert_parity(result, expected)

    def test_filtered_select(self, spark):
        """Test filtered SELECT matches PySpark behavior."""
        expected = self.load_expected("sql_operations", "filtered_select")

        df = spark.createDataFrame(expected["input_data"])
        df.write.mode("overwrite").saveAsTable("test_table")

        result = spark.sql("SELECT * FROM test_table WHERE age > 30")

        self.assert_parity(result, expected)

    def test_group_by(self, spark):
        """Test GROUP BY matches PySpark behavior."""
        expected = self.load_expected("sql_operations", "group_by")

        df = spark.createDataFrame(expected["input_data"])
        df.write.mode("overwrite").saveAsTable("test_table")

        # Use the query from expected output: GROUP BY (age > 30)
        # This groups by boolean: False (age <= 30) -> 2 rows, True (age > 30) -> 1 row
        result = spark.sql(
            "SELECT COUNT(*) as count FROM test_table GROUP BY (age > 30)"
        )

        self.assert_parity(result, expected)

    def test_aggregation(self, spark):
        """Test aggregation in SQL matches PySpark behavior."""
        expected = self.load_expected("sql_operations", "aggregation")

        df = spark.createDataFrame(expected["input_data"])
        df.write.mode("overwrite").saveAsTable("test_table")

        # Use the query from expected output: SELECT AVG(salary) as avg_salary FROM employees
        # This aggregates over all rows (no GROUP BY)
        result = spark.sql("SELECT AVG(salary) as avg_salary FROM test_table")

        self.assert_parity(result, expected)

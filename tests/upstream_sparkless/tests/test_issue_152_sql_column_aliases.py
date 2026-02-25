"""
Test for issue #152 (BUG-004): SQL column aliases not properly parsed in SELECT statements.

Issue #152 reports that SQL SELECT statements with column aliases (e.g., `SELECT col AS alias`)
are not properly parsed. The executor tries to access columns using the full alias expression
instead of parsing it correctly.

Error:
    SparkColumnNotFoundError: 'DataFrame' object has no attribute 'name as dept_name'.
    Available columns: dept_id, name
"""

from tests.fixtures.spark_imports import get_spark_imports

# Get the appropriate imports based on backend (sparkless or PySpark)
imports = get_spark_imports()
F = imports.F


class TestIssue152SQLColumnAliases:
    """Test cases for issue #152: SQL column aliases parsing."""

    def test_sql_with_inner_join_and_aliases(self, spark):
        """Test that SQL queries with JOIN and column aliases work correctly.

        This test verifies the fix for issue #152 where queries like:
            SELECT e.name, d.name as dept_name
            FROM employees e
            INNER JOIN departments d ON e.dept_id = d.id
        would fail with "name as dept_name" not being parsed correctly.
        """
        # Create test data
        employees_data = [("Alice", 1), ("Bob", 2)]
        employees_df = spark.createDataFrame(employees_data, ["name", "dept_id"])
        employees_df.write.mode("overwrite").saveAsTable("employees")

        departments_data = [(1, "IT"), (2, "HR")]
        departments_df = spark.createDataFrame(departments_data, ["id", "name"])
        departments_df.write.mode("overwrite").saveAsTable("departments")

        # Execute SQL query with aliases
        result = spark.sql(
            """
            SELECT e.name, d.name as dept_name
            FROM employees e
            INNER JOIN departments d ON e.dept_id = d.id
            """
        )

        # Verify the query executes without errors
        rows = result.collect()
        assert len(rows) == 2

        # Verify column names are correct
        # After JOIN, columns are prefixed with table alias (e.name -> e_name)
        # But aliased columns use their alias (d.name as dept_name -> dept_name)
        assert "e_name" in result.columns or "name" in result.columns
        assert "dept_name" in result.columns

        # Verify data is correct
        row_dicts = [row.asDict() for row in rows]
        # Use the actual column name (e_name or name)
        name_col = "e_name" if "e_name" in result.columns else "name"
        assert any(
            row[name_col] == "Alice" and row["dept_name"] == "IT" for row in row_dicts
        )
        assert any(
            row[name_col] == "Bob" and row["dept_name"] == "HR" for row in row_dicts
        )

    def test_sql_with_left_join_and_aliases(self, spark):
        """Test that SQL queries with LEFT JOIN and column aliases work correctly."""
        # Create test data
        employees_data = [("Alice", 1), ("Bob", 99)]  # Bob has invalid dept_id
        employees_df = spark.createDataFrame(employees_data, ["name", "dept_id"])
        employees_df.write.mode("overwrite").saveAsTable("employees")

        departments_data = [(1, "IT")]
        departments_df = spark.createDataFrame(departments_data, ["id", "name"])
        departments_df.write.mode("overwrite").saveAsTable("departments")

        # Execute SQL query with aliases
        result = spark.sql(
            """
            SELECT e.name, d.name as dept_name
            FROM employees e
            LEFT JOIN departments d ON e.dept_id = d.id
            """
        )

        # Verify the query executes without errors
        rows = result.collect()
        assert len(rows) == 2

        # Verify column names are correct
        assert "e_name" in result.columns or "name" in result.columns
        assert "dept_name" in result.columns

        # Verify data (Bob should have NULL dept_name)
        row_dicts = [row.asDict() for row in rows]
        name_col = "e_name" if "e_name" in result.columns else "name"
        alice_row = next(row for row in row_dicts if row[name_col] == "Alice")
        assert alice_row["dept_name"] == "IT"

        bob_row = next(row for row in row_dicts if row[name_col] == "Bob")
        assert bob_row["dept_name"] is None

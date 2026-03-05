"""
PySpark parity tests for advanced SQL operations.

Tests validate that Sparkless advanced SQL queries behave identically to PySpark.
"""

from tests.fixtures.parity_base import ParityTestBase


class TestSQLAdvancedParity(ParityTestBase):
    """Test advanced SQL operations parity with PySpark."""

    def test_sql_with_inner_join(self, spark):
        """Test SQL INNER JOIN matches PySpark behavior."""
        # Create employees table
        emp_data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
        emp_df = spark.createDataFrame(emp_data, ["name", "dept_id"])
        emp_df.write.mode("overwrite").saveAsTable("employees")

        # Create departments table
        dept_data = [(1, "IT"), (2, "HR")]
        dept_df = spark.createDataFrame(dept_data, ["id", "name"])
        dept_df.write.mode("overwrite").saveAsTable("departments")

        # Join via SQL
        result = spark.sql("""
            SELECT e.name, d.name as dept_name
            FROM employees e
            INNER JOIN departments d ON e.dept_id = d.id
        """)

        rows = result.collect()
        assert len(rows) == 2  # Only Alice and Bob have matching dept

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS employees")
        spark.sql("DROP TABLE IF EXISTS departments")

    def test_sql_with_left_join(self, spark):
        """Test SQL LEFT JOIN matches PySpark behavior."""
        # Create employees table
        emp_data = [("Alice", 1), ("Bob", 2), ("Charlie", 99)]
        emp_df = spark.createDataFrame(emp_data, ["name", "dept_id"])
        emp_df.write.mode("overwrite").saveAsTable("employees2")

        # Create departments table
        dept_data = [(1, "IT"), (2, "HR")]
        dept_df = spark.createDataFrame(dept_data, ["id", "name"])
        dept_df.write.mode("overwrite").saveAsTable("departments2")

        # Left join via SQL
        result = spark.sql("""
            SELECT e.name, d.name as dept_name
            FROM employees2 e
            LEFT JOIN departments2 d ON e.dept_id = d.id
        """)

        rows = result.collect()
        assert len(rows) == 3  # Charlie should have NULL dept

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS employees2")
        spark.sql("DROP TABLE IF EXISTS departments2")

    def test_sql_with_order_by(self, spark):
        """Test SQL ORDER BY matches PySpark behavior."""
        # Create table
        data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").saveAsTable("order_test")

        # Order by age ascending
        result = spark.sql("SELECT * FROM order_test ORDER BY age")
        rows = result.collect()
        assert rows[0]["name"] == "Bob"
        assert rows[1]["name"] == "Alice"
        assert rows[2]["name"] == "Charlie"

        # Order by age descending
        result = spark.sql("SELECT * FROM order_test ORDER BY age DESC")
        rows = result.collect()
        assert rows[0]["name"] == "Charlie"
        assert rows[2]["name"] == "Bob"

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS order_test")

    def test_sql_with_limit(self, spark):
        """Test SQL LIMIT matches PySpark behavior."""
        # Create table
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35), ("David", 40)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").saveAsTable("limit_test")

        # Limit to 2 rows
        result = spark.sql("SELECT * FROM limit_test LIMIT 2")
        assert result.count() == 2

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS limit_test")

    def test_sql_with_having(self, spark):
        """Test SQL HAVING clause matches PySpark behavior."""
        # Create table
        # IT: (50000 + 60001) / 2 = 55000.5 > 55000
        # HR: 55000 (not > 55000)
        data = [("Alice", "IT", 50000), ("Bob", "IT", 60001), ("Charlie", "HR", 55000)]
        df = spark.createDataFrame(data, ["name", "dept", "salary"])
        df.write.mode("overwrite").saveAsTable("having_test")

        # Group by with HAVING
        result = spark.sql("""
            SELECT dept, AVG(salary) as avg_salary
            FROM having_test
            GROUP BY dept
            HAVING AVG(salary) > 55000
        """)

        rows = result.collect()
        assert len(rows) == 1  # Only IT has avg > 55000
        assert rows[0]["dept"] == "IT"

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS having_test")

    def test_sql_with_union(self, spark):
        """Test SQL UNION matches PySpark behavior."""
        # Create two tables
        data1 = [("Alice", 25), ("Bob", 30)]
        df1 = spark.createDataFrame(data1, ["name", "age"])
        df1.write.mode("overwrite").saveAsTable("union_table1")

        data2 = [("Charlie", 35), ("David", 40)]
        df2 = spark.createDataFrame(data2, ["name", "age"])
        df2.write.mode("overwrite").saveAsTable("union_table2")

        # Union
        result = spark.sql("""
            SELECT name, age FROM union_table1
            UNION
            SELECT name, age FROM union_table2
        """)

        assert result.count() == 4

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS union_table1")
        spark.sql("DROP TABLE IF EXISTS union_table2")

    def test_sql_with_subquery(self, spark):
        """Test SQL subquery matches PySpark behavior."""
        # Create table
        data = [("Alice", 50000), ("Bob", 60000), ("Charlie", 70000)]
        df = spark.createDataFrame(data, ["name", "salary"])
        df.write.mode("overwrite").saveAsTable("subquery_test")

        # Subquery to find above average
        result = spark.sql("""
            SELECT name, salary
            FROM subquery_test
            WHERE salary > (SELECT AVG(salary) FROM subquery_test)
        """)

        rows = result.collect()
        # Only Charlie should be above average (60000)
        assert len(rows) == 1
        assert rows[0]["name"] == "Charlie"

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS subquery_test")

    def test_sql_with_case_when(self, spark):
        """Test SQL CASE WHEN matches PySpark behavior."""
        # Create table
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").saveAsTable("case_test")

        # CASE WHEN statement
        result = spark.sql("""
            SELECT name, age,
                   CASE WHEN age < 30 THEN 'Young'
                        WHEN age < 35 THEN 'Middle'
                        ELSE 'Senior' END as category
            FROM case_test
        """)

        rows = result.collect()
        assert len(rows) == 3
        assert rows[0]["category"] == "Young"
        assert rows[1]["category"] == "Middle"
        assert rows[2]["category"] == "Senior"

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS case_test")

    def test_sql_with_like(self, spark):
        """Test SQL LIKE matches PySpark behavior."""
        # Create table
        data = [("Alice",), ("Bob",), ("Charlie",), ("David",)]
        df = spark.createDataFrame(data, ["name"])
        df.write.mode("overwrite").saveAsTable("like_test")

        # LIKE pattern
        result = spark.sql("SELECT * FROM like_test WHERE name LIKE 'A%'")
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS like_test")

    def test_sql_with_in_clause(self, spark):
        """Test SQL IN clause matches PySpark behavior."""
        # Create table
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").saveAsTable("in_test")

        # IN clause
        result = spark.sql("SELECT * FROM in_test WHERE age IN (25, 35)")
        rows = result.collect()
        assert len(rows) == 2
        names = {row["name"] for row in rows}
        assert "Alice" in names
        assert "Charlie" in names

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS in_test")

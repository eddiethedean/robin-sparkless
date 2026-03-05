"""
PySpark parity tests for DataFrame join operations.

Tests validate that Sparkless join operations behave identically to PySpark.
"""

from tests.fixtures.parity_base import ParityTestBase


class TestJoinParity(ParityTestBase):
    """Test DataFrame join operations parity with PySpark."""

    def test_inner_join(self, spark):
        """Test inner join matches PySpark behavior."""
        expected = self.load_expected("joins", "inner_join")

        # Join tests use hardcoded data structure matching compatibility tests
        employees_data = [
            {"id": 1, "name": "Alice", "dept_id": 10, "salary": 50000},
            {"id": 2, "name": "Bob", "dept_id": 20, "salary": 60000},
            {"id": 3, "name": "Charlie", "dept_id": 10, "salary": 70000},
            {"id": 4, "name": "David", "dept_id": 30, "salary": 55000},
        ]

        departments_data = [
            {"dept_id": 10, "name": "IT", "location": "NYC"},
            {"dept_id": 20, "name": "HR", "location": "LA"},
            {"dept_id": 40, "name": "Finance", "location": "Chicago"},
        ]

        emp_df = spark.createDataFrame(employees_data)
        dept_df = spark.createDataFrame(departments_data)
        result = emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "inner")

        self.assert_parity(result, expected)

    def test_left_join(self, spark):
        """Test left join matches PySpark behavior."""
        expected = self.load_expected("joins", "left_join")

        employees_data = [
            {"id": 1, "name": "Alice", "dept_id": 10, "salary": 50000},
            {"id": 2, "name": "Bob", "dept_id": 20, "salary": 60000},
            {"id": 3, "name": "Charlie", "dept_id": 10, "salary": 70000},
            {"id": 4, "name": "David", "dept_id": 30, "salary": 55000},
        ]

        departments_data = [
            {"dept_id": 10, "name": "IT", "location": "NYC"},
            {"dept_id": 20, "name": "HR", "location": "LA"},
            {"dept_id": 40, "name": "Finance", "location": "Chicago"},
        ]

        emp_df = spark.createDataFrame(employees_data)
        dept_df = spark.createDataFrame(departments_data)
        result = emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "left")
        result = result.orderBy("id")  # Sort for consistent comparison

        self.assert_parity(result, expected)

    def test_right_join(self, spark):
        """Test right join matches PySpark behavior."""
        expected = self.load_expected("joins", "right_join")

        employees_data = [
            {"id": 1, "name": "Alice", "dept_id": 10, "salary": 50000},
            {"id": 2, "name": "Bob", "dept_id": 20, "salary": 60000},
            {"id": 3, "name": "Charlie", "dept_id": 10, "salary": 70000},
            {"id": 4, "name": "David", "dept_id": 30, "salary": 55000},
        ]

        departments_data = [
            {"dept_id": 10, "name": "IT", "location": "NYC"},
            {"dept_id": 20, "name": "HR", "location": "LA"},
            {"dept_id": 40, "name": "Finance", "location": "Chicago"},
        ]

        emp_df = spark.createDataFrame(employees_data)
        dept_df = spark.createDataFrame(departments_data)
        result = emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "right")
        result = result.orderBy("id")  # Sort for consistent comparison

        self.assert_parity(result, expected)

    def test_outer_join(self, spark):
        """Test outer join matches PySpark behavior."""
        expected = self.load_expected("joins", "outer_join")

        employees_data = [
            {"id": 1, "name": "Alice", "dept_id": 10, "salary": 50000},
            {"id": 2, "name": "Bob", "dept_id": 20, "salary": 60000},
            {"id": 3, "name": "Charlie", "dept_id": 10, "salary": 70000},
            {"id": 4, "name": "David", "dept_id": 30, "salary": 55000},
        ]

        departments_data = [
            {"dept_id": 10, "name": "IT", "location": "NYC"},
            {"dept_id": 20, "name": "HR", "location": "LA"},
            {"dept_id": 40, "name": "Finance", "location": "Chicago"},
        ]

        emp_df = spark.createDataFrame(employees_data)
        dept_df = spark.createDataFrame(departments_data)
        result = emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "outer")
        result = result.orderBy("id")  # Sort for consistent comparison

        self.assert_parity(result, expected)

    def test_cross_join(self, spark):
        """Test cross join matches PySpark behavior."""
        expected = self.load_expected("joins", "cross_join")

        employees_data = [
            {"id": 1, "name": "Alice", "dept_id": 10, "salary": 50000},
            {"id": 2, "name": "Bob", "dept_id": 20, "salary": 60000},
            {"id": 3, "name": "Charlie", "dept_id": 10, "salary": 70000},
            {"id": 4, "name": "David", "dept_id": 30, "salary": 55000},
        ]

        departments_data = [
            {"dept_id": 10, "name": "IT", "location": "NYC"},
            {"dept_id": 20, "name": "HR", "location": "LA"},
            {"dept_id": 40, "name": "Finance", "location": "Chicago"},
        ]

        emp_df = spark.createDataFrame(employees_data)
        dept_df = spark.createDataFrame(departments_data)
        result = emp_df.crossJoin(dept_df)

        self.assert_parity(result, expected)

    def test_semi_join(self, spark):
        """Test semi join matches PySpark behavior."""
        expected = self.load_expected("joins", "semi_join")

        employees_data = [
            {"id": 1, "name": "Alice", "dept_id": 10, "salary": 50000},
            {"id": 2, "name": "Bob", "dept_id": 20, "salary": 60000},
            {"id": 3, "name": "Charlie", "dept_id": 10, "salary": 70000},
            {"id": 4, "name": "David", "dept_id": 30, "salary": 55000},
        ]

        departments_data = [
            {"dept_id": 10, "name": "IT", "location": "NYC"},
            {"dept_id": 20, "name": "HR", "location": "LA"},
            {"dept_id": 40, "name": "Finance", "location": "Chicago"},
        ]

        emp_df = spark.createDataFrame(employees_data)
        dept_df = spark.createDataFrame(departments_data)
        result = emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "left_semi")

        self.assert_parity(result, expected)

    def test_anti_join(self, spark):
        """Test anti join matches PySpark behavior."""
        expected = self.load_expected("joins", "anti_join")

        employees_data = [
            {"id": 1, "name": "Alice", "dept_id": 10, "salary": 50000},
            {"id": 2, "name": "Bob", "dept_id": 20, "salary": 60000},
            {"id": 3, "name": "Charlie", "dept_id": 10, "salary": 70000},
            {"id": 4, "name": "David", "dept_id": 30, "salary": 55000},
        ]

        departments_data = [
            {"dept_id": 10, "name": "IT", "location": "NYC"},
            {"dept_id": 20, "name": "HR", "location": "LA"},
            {"dept_id": 40, "name": "Finance", "location": "Chicago"},
        ]

        emp_df = spark.createDataFrame(employees_data)
        dept_df = spark.createDataFrame(departments_data)
        result = emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "left_anti")

        self.assert_parity(result, expected)

"""
PySpark parity tests for DataFrame join operations.

Tests validate that Sparkless join operations behave identically to PySpark.
Expected outputs use aliased right-side columns (dept_id_right, name_right) for
unique names; we select with those aliases and orderBy id for stable comparison.
"""

from tests.fixtures.parity_base import ParityTestBase
from tests.fixtures.spark_imports import get_spark_imports

F = get_spark_imports().F


def _join_result_with_aliases(emp_df, dept_df, join_type):
    """Join and select with dept_id_right/name_right so schema matches expected_output."""
    result = emp_df.join(
        dept_df, emp_df.dept_id == dept_df.dept_id, join_type
    ).select(
        emp_df.dept_id,
        emp_df.id,
        emp_df.name,
        emp_df.salary,
        dept_df.dept_id.alias("dept_id_right"),
        dept_df.location,
        dept_df.name.alias("name_right"),
    )
    return result.orderBy("id")


def _employees_and_departments(spark):
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
    return emp_df, dept_df


class TestJoinParity(ParityTestBase):
    """Test DataFrame join operations parity with PySpark."""

    def test_inner_join(self, spark):
        """Test inner join matches PySpark behavior."""
        expected = self.load_expected("joins", "inner_join")
        emp_df, dept_df = _employees_and_departments(spark)
        result = _join_result_with_aliases(emp_df, dept_df, "inner")
        self.assert_parity(result, expected)

    def test_left_join(self, spark):
        """Test left join matches PySpark behavior."""
        expected = self.load_expected("joins", "left_join")
        emp_df, dept_df = _employees_and_departments(spark)
        result = _join_result_with_aliases(emp_df, dept_df, "left")
        self.assert_parity(result, expected)

    def test_right_join(self, spark):
        """Test right join matches PySpark behavior."""
        expected = self.load_expected("joins", "right_join")
        emp_df, dept_df = _employees_and_departments(spark)
        result = _join_result_with_aliases(emp_df, dept_df, "right")
        self.assert_parity(result, expected)

    def test_outer_join(self, spark):
        """Test outer join matches PySpark behavior."""
        expected = self.load_expected("joins", "outer_join")
        emp_df, dept_df = _employees_and_departments(spark)
        result = _join_result_with_aliases(emp_df, dept_df, "outer")
        self.assert_parity(result, expected)

    def test_cross_join(self, spark):
        """Test cross join matches PySpark behavior."""
        expected = self.load_expected("joins", "cross_join")
        emp_df, dept_df = _employees_and_departments(spark)
        # Match expected schema column order: dept_id, name, id, salary, dept_id_right, name_right, location
        result = emp_df.crossJoin(dept_df).select(
            emp_df.dept_id,
            emp_df.name,
            emp_df.id,
            emp_df.salary,
            dept_df.dept_id.alias("dept_id_right"),
            dept_df.name.alias("name_right"),
            dept_df.location,
        ).orderBy("id", "dept_id_right")
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

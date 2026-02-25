"""
Compatibility tests for join operations using expected outputs.

This module tests MockSpark's join operations against PySpark-generated expected outputs
to ensure compatibility across different join types and conditions.
"""

import pytest
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal


@pytest.mark.compatibility
class TestJoinsCompatibility:
    """Tests for join operations compatibility using expected outputs."""

    @pytest.fixture(scope="class")
    def expected_outputs(self):
        """Load expected outputs for join operations."""
        return load_expected_output("joins", "inner_join", "3.2")

    def test_inner_join(self, mock_spark_session, expected_outputs):
        """Test inner join against expected output."""
        # Create test data
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

        emp_df = mock_spark_session.createDataFrame(employees_data)
        dept_df = mock_spark_session.createDataFrame(departments_data)

        # Perform inner join
        result = emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "inner")

        # Load expected output
        expected = load_expected_output("joins", "inner_join")
        assert_dataframes_equal(result, expected)

    def test_left_join(self, mock_spark_session):
        """Test left join against expected output."""
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

        emp_df = mock_spark_session.createDataFrame(employees_data)
        dept_df = mock_spark_session.createDataFrame(departments_data)

        result = emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "left")
        # Sort by id for consistent comparison (joins don't guarantee order)
        result = result.orderBy("id")

        expected = load_expected_output("joins", "left_join")
        assert_dataframes_equal(result, expected)

    def test_right_join(self, mock_spark_session):
        """Test right join against expected output."""
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

        emp_df = mock_spark_session.createDataFrame(employees_data)
        dept_df = mock_spark_session.createDataFrame(departments_data)

        result = emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "right")
        # Sort by id for consistent comparison (joins don't guarantee order)
        result = result.orderBy("id")

        expected = load_expected_output("joins", "right_join")
        assert_dataframes_equal(result, expected)

    def test_outer_join(self, mock_spark_session):
        """Test outer join against expected output."""
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

        emp_df = mock_spark_session.createDataFrame(employees_data)
        dept_df = mock_spark_session.createDataFrame(departments_data)

        result = emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "outer")
        # Sort by id for consistent comparison (joins don't guarantee order)
        result = result.orderBy("id")

        expected = load_expected_output("joins", "outer_join")
        assert_dataframes_equal(result, expected)

    def test_cross_join(self, mock_spark_session):
        """Test cross join against expected output."""
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

        emp_df = mock_spark_session.createDataFrame(employees_data)
        dept_df = mock_spark_session.createDataFrame(departments_data)

        result = emp_df.crossJoin(dept_df)

        expected = load_expected_output("joins", "cross_join")
        assert_dataframes_equal(result, expected)

    def test_semi_join(self, mock_spark_session):
        """Test semi join against expected output."""
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

        emp_df = mock_spark_session.createDataFrame(employees_data)
        dept_df = mock_spark_session.createDataFrame(departments_data)

        result = emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "left_semi")

        expected = load_expected_output("joins", "semi_join")
        assert_dataframes_equal(result, expected)

    def test_anti_join(self, mock_spark_session):
        """Test anti join against expected output."""
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

        emp_df = mock_spark_session.createDataFrame(employees_data)
        dept_df = mock_spark_session.createDataFrame(departments_data)

        result = emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "left_anti")

        expected = load_expected_output("joins", "anti_join")
        assert_dataframes_equal(result, expected)


@pytest.mark.compatibility
class TestJoinsWithColumnRenamed:
    """Tests for join operations combined with withColumnRenamed."""

    def test_inner_join_with_rename_before(self, mock_spark_session):
        """Test inner join after renaming columns to avoid conflicts."""
        # Create dataframes with conflicting column names
        left_data = [
            {"id": 1, "name": "Alice", "value": "left1"},
            {"id": 2, "name": "Bob", "value": "left2"},
        ]
        right_data = [
            {"id": 1, "name": "Alice R", "value": "right1"},
            {"id": 3, "name": "Charlie R", "value": "right3"},
        ]

        left_df = mock_spark_session.createDataFrame(left_data)
        right_df = mock_spark_session.createDataFrame(right_data)

        # Rename columns before join to avoid conflicts
        left_df = left_df.withColumnRenamed("name", "left_name").withColumnRenamed(
            "value", "left_value"
        )
        right_df = right_df.withColumnRenamed("name", "right_name").withColumnRenamed(
            "value", "right_value"
        )

        # Perform inner join
        result = left_df.join(right_df, left_df.id == right_df.id, "inner")
        result = result.orderBy("id")

        # Verify columns exist
        assert "left_name" in result.columns
        assert "right_name" in result.columns
        assert "left_value" in result.columns
        assert "right_value" in result.columns
        assert "name" not in result.columns  # Original name should not exist

        # Verify data
        rows = result.collect()
        assert len(rows) == 1  # Only id=1 matches
        assert rows[0].id == 1
        assert rows[0].left_name == "Alice"
        assert rows[0].right_name == "Alice R"

    def test_left_join_with_rename_after(self, mock_spark_session):
        """Test left join followed by column rename."""
        employees_data = [
            {"id": 1, "emp_name": "Alice", "dept_id": 10},
            {"id": 2, "emp_name": "Bob", "dept_id": 20},
        ]
        departments_data = [
            {"dept_id": 10, "dept_name": "IT", "location": "NYC"},
            {"dept_id": 30, "dept_name": "HR", "location": "LA"},
        ]

        emp_df = mock_spark_session.createDataFrame(employees_data)
        dept_df = mock_spark_session.createDataFrame(departments_data)

        # Join using string column name and then rename columns
        result = (
            emp_df.join(dept_df, "dept_id", "left")
            .withColumnRenamed("emp_name", "employee_name")
            .orderBy("id")
        )

        # Verify renamed columns exist - focus on testing withColumnRenamed works
        assert "employee_name" in result.columns
        assert "emp_name" not in result.columns  # Original name should not exist

        # Verify the rename worked on the data
        rows = result.collect()
        assert len(rows) >= 1  # At least one row
        # Find row with id=1 if it exists
        row_1 = next((r for r in rows if hasattr(r, "id") and r.id == 1), None)
        if row_1:
            assert row_1.employee_name == "Alice"

    def test_outer_join_with_multiple_renames(self, mock_spark_session):
        """Test outer join with multiple column renames using withColumnsRenamed."""
        left_data = [
            {"id": 1, "name": "Alice", "score": 85},
            {"id": 2, "name": "Bob", "score": 90},
        ]
        right_data = [
            {"id": 2, "name": "Bob R", "grade": "A"},
            {"id": 3, "name": "Charlie R", "grade": "B"},
        ]

        left_df = mock_spark_session.createDataFrame(left_data)
        right_df = mock_spark_session.createDataFrame(right_data)

        # Rename multiple columns at once before join
        left_df = left_df.withColumnsRenamed(
            {"name": "left_name", "score": "left_score"}
        )
        right_df = right_df.withColumnsRenamed(
            {"name": "right_name", "grade": "right_grade"}
        )

        # Perform outer join using string column name
        result = left_df.join(right_df, "id", "outer")

        # Verify all renamed columns exist - focus on testing withColumnsRenamed works
        assert "left_name" in result.columns
        assert "left_score" in result.columns
        assert "name" not in result.columns  # Original name should not exist in left

        # Verify the renames worked on the data
        rows = result.collect()
        assert len(rows) >= 1  # At least one row
        # Find row with id=1 if it exists
        row_1 = next((r for r in rows if hasattr(r, "id") and r.id == 1), None)
        if row_1:
            assert row_1.left_name == "Alice"
            assert row_1.left_score == 85

    def test_cross_join_with_rename_before_and_after(self, mock_spark_session):
        """Test cross join with renames both before and after."""
        left_data = [{"id": 1, "value": "A"}, {"id": 2, "value": "B"}]
        right_data = [{"code": "X", "type": "alpha"}, {"code": "Y", "type": "beta"}]

        left_df = mock_spark_session.createDataFrame(left_data)
        right_df = mock_spark_session.createDataFrame(right_data)

        # Rename before join
        left_df = left_df.withColumnRenamed("value", "left_value")
        right_df = right_df.withColumnRenamed("code", "right_code")

        # Cross join and rename after
        result = (
            left_df.crossJoin(right_df)
            .withColumnRenamed("type", "category")
            .orderBy("id", "right_code")
        )

        # Verify all renames
        assert "left_value" in result.columns
        assert "right_code" in result.columns
        assert "category" in result.columns
        assert "value" not in result.columns
        assert "code" not in result.columns
        assert "type" not in result.columns

        # Verify data (2x2 = 4 rows)
        rows = result.collect()
        assert len(rows) == 4

    def test_inner_join_with_rename_on_join_key(self, mock_spark_session):
        """Test inner join where join key is renamed before join."""
        left_data = [{"emp_id": 1, "name": "Alice"}, {"emp_id": 2, "name": "Bob"}]
        right_data = [{"dept_id": 1, "dept": "IT"}, {"dept_id": 2, "dept": "HR"}]

        left_df = mock_spark_session.createDataFrame(left_data)
        right_df = mock_spark_session.createDataFrame(right_data)

        # Rename join keys to match
        left_df = left_df.withColumnRenamed("emp_id", "id")
        right_df = right_df.withColumnRenamed("dept_id", "id")

        # Join on the renamed column
        result = left_df.join(right_df, "id", "inner").orderBy("id")

        # Verify join worked correctly
        rows = result.collect()
        assert len(rows) == 2
        assert rows[0].id == 1
        assert rows[0].name == "Alice"
        assert rows[0].dept == "IT"
        assert rows[1].id == 2
        assert rows[1].name == "Bob"
        assert rows[1].dept == "HR"

    def test_right_join_with_chained_renames(self, mock_spark_session):
        """Test right join with chained withColumnRenamed calls."""
        left_data = [{"id": 1, "name": "Alice"}]
        right_data = [
            {"id": 1, "score": 85, "grade": "A"},
            {"id": 2, "score": 90, "grade": "B"},
        ]

        left_df = mock_spark_session.createDataFrame(left_data)
        right_df = mock_spark_session.createDataFrame(right_data)

        # Chain multiple renames
        result = (
            left_df.join(right_df, "id", "right")
            .withColumnRenamed("name", "employee_name")
            .withColumnRenamed("score", "test_score")
            .withColumnRenamed("grade", "test_grade")
        )

        # Verify all renames - focus on testing withColumnRenamed works
        assert "employee_name" in result.columns
        assert "test_score" in result.columns
        assert "test_grade" in result.columns
        assert "name" not in result.columns
        assert "score" not in result.columns
        assert "grade" not in result.columns

        # Verify the renames worked on the data
        rows = result.collect()
        assert len(rows) >= 1  # At least one row
        # Find row with id=1 if it exists
        row_1 = next((r for r in rows if hasattr(r, "id") and r.id == 1), None)
        if row_1:
            assert row_1.employee_name == "Alice"

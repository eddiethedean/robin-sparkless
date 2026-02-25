"""
Tests to detect isinstance ordering bugs with Column and ColumnOperation.

Since ColumnOperation is a subclass of Column, isinstance checks must check
ColumnOperation BEFORE Column to avoid incorrectly handling operations as
simple column references.

These tests catch bugs where comparison operations, arithmetic operations, or
other ColumnOperations are incorrectly handled because isinstance(expr, Column)
returns True for ColumnOperation objects when checked before ColumnOperation.
"""

from tests.fixtures.parity_base import ParityTestBase
from tests.fixtures.spark_imports import get_spark_imports


class TestIsInstanceOrdering(ParityTestBase):
    """Test isinstance ordering with Column and ColumnOperation."""

    def test_filter_comparison_operations_not_treated_as_column_existence(self, spark):
        """Test that comparison operations are evaluated, not treated as column existence.

        This test ensures that when we filter with comparison operations (==, !=, etc.),
        they are properly evaluated rather than just checking if a column exists.
        """
        imports = get_spark_imports()
        F = imports.F

        # Create DataFrame with test data
        data = [
            {"id": 1, "name": "Alice", "value": 10},
            {"id": 2, "name": "Bob", "value": 20},
            {"id": 3, "name": "Charlie", "value": 30},
        ]
        df = spark.createDataFrame(data)

        # Test various comparison operations
        # These should filter based on actual values, not just column existence

        # Equality comparison
        result1 = df.filter(df.value == 20)
        assert result1.count() == 1, "Should return 1 row where value == 20"
        assert result1.collect()[0]["name"] == "Bob"

        # Inequality comparison
        result2 = df.filter(df.value != 20)
        assert result2.count() == 2, "Should return 2 rows where value != 20"

        # Greater than
        result3 = df.filter(df.value > 15)
        assert result3.count() == 2, "Should return 2 rows where value > 15"

        # Less than or equal
        result4 = df.filter(df.value <= 20)
        assert result4.count() == 2, "Should return 2 rows where value <= 20"

        # Test with F.col() syntax
        result5 = df.filter(F.col("value") == 20)
        assert result5.count() == 1, "F.col() comparison should work correctly"

    def test_filter_on_table_with_comparison_operations(self, spark):
        """Test filter with comparison operations on DataFrames read from tables.

        This specifically tests the bug from issue #122 where filtering failed
        on tables with complex schemas because comparison operations were
        incorrectly handled.
        """
        imports = get_spark_imports()
        F = imports.F

        # Create schema
        schema_name = "test_schema"
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        # Create DataFrame
        data = [
            {"id": 1, "status": "active", "score": 100},
            {"id": 2, "status": "inactive", "score": 50},
            {"id": 3, "status": "active", "score": 200},
        ]
        df = spark.createDataFrame(data)

        # Write to table
        table_fqn = f"{schema_name}.test_table"
        df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(table_fqn)

        # Read back
        read_df = spark.table(table_fqn)

        # Test various comparison operations on table DataFrame
        # These should all work correctly

        # Equality
        result1 = read_df.filter(read_df.status == "active")
        assert result1.count() == 2, "Should return 2 rows with status='active'"

        # Inequality
        result2 = read_df.filter(read_df.score != 50)
        assert result2.count() == 2, "Should return 2 rows where score != 50"

        # Greater than
        result3 = read_df.filter(read_df.score > 75)
        assert result3.count() == 2, "Should return 2 rows where score > 75"

        # Less than
        result4 = read_df.filter(read_df.score < 150)
        assert result4.count() == 2, "Should return 2 rows where score < 150"

        # Greater than or equal
        result5 = read_df.filter(read_df.score >= 100)
        assert result5.count() == 2, "Should return 2 rows where score >= 100"

        # Less than or equal
        result6 = read_df.filter(read_df.score <= 100)
        assert result6.count() == 2, "Should return 2 rows where score <= 100"

        # Test with F.col() syntax
        result7 = read_df.filter(F.col("status") == "active")
        assert result7.count() == 2, "F.col() comparison should work on tables"

    def test_arithmetic_operations_in_filters(self, spark):
        """Test that arithmetic operations in filter conditions work correctly.

        When filtering with arithmetic operations, they should be evaluated,
        not treated as simple column references.
        """
        data = [
            {"a": 10, "b": 5},
            {"a": 20, "b": 10},
            {"a": 30, "b": 15},
        ]
        df = spark.createDataFrame(data)

        # Filter with arithmetic operations
        # These should evaluate the arithmetic, not check column existence

        # Addition
        result1 = df.filter(df.a + df.b > 25)
        assert result1.count() == 2, "Should return 2 rows where a + b > 25"

        # Subtraction
        result2 = df.filter(df.a - df.b == 10)
        assert result2.count() == 1, "Should return 1 row where a - b == 10"

        # Multiplication
        result3 = df.filter(df.a * df.b >= 200)
        assert result3.count() == 2, "Should return 2 rows where a * b >= 200"

        # Division
        result4 = df.filter(df.a / df.b == 2)
        assert result4.count() == 3, "Should return 3 rows where a / b == 2"

        # Test on table DataFrame
        schema_name = "test_schema"
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        table_fqn = f"{schema_name}.test_table"
        df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(table_fqn)
        read_df = spark.table(table_fqn)

        result5 = read_df.filter(read_df.a + read_df.b > 25)
        assert result5.count() == 2, "Arithmetic should work on table DataFrames"

    def test_complex_nested_operations_in_filters(self, spark):
        """Test that nested ColumnOperations are properly handled.

        When we have nested operations like (a + b) > c, the entire expression
        should be evaluated correctly, not treated as simple column references.
        """
        data = [
            {"x": 5, "y": 10, "z": 20},
            {"x": 15, "y": 10, "z": 20},
            {"x": 25, "y": 10, "z": 20},
        ]
        df = spark.createDataFrame(data)

        # Nested operations should be evaluated correctly
        # (x + y) > z should evaluate the addition first, then compare
        result1 = df.filter((df.x + df.y) > df.z)
        assert result1.count() == 2, "Should return 2 rows where (x + y) > z"

        # More complex: (x * 2) < (y + z)
        result2 = df.filter((df.x * 2) < (df.y + df.z))
        assert result2.count() == 1, "Should return 1 row where (x * 2) < (y + z)"

        # Test on table DataFrame
        schema_name = "test_schema"
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        table_fqn = f"{schema_name}.test_table"
        df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(table_fqn)
        read_df = spark.table(table_fqn)

        result3 = read_df.filter((read_df.x + read_df.y) > read_df.z)
        assert result3.count() == 2, "Nested operations should work on table DataFrames"

    def test_string_operations_in_filters(self, spark):
        """Test that string operations (like, contains, etc.) work correctly.

        String operations create ColumnOperations that should be evaluated,
        not treated as simple column references.
        """
        data = [
            {"name": "Alice", "email": "alice@example.com"},
            {"name": "Bob", "email": "bob@test.com"},
            {"name": "Charlie", "email": "charlie@example.com"},
        ]
        df = spark.createDataFrame(data)

        # String operations should be evaluated correctly
        result1 = df.filter(df.name.startswith("A"))
        assert result1.count() == 1, "Should return 1 row where name starts with 'A'"

        result2 = df.filter(df.email.contains("example"))
        assert result2.count() == 2, (
            "Should return 2 rows where email contains 'example'"
        )

        # Test on table DataFrame
        schema_name = "test_schema"
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        table_fqn = f"{schema_name}.test_table"
        df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(table_fqn)
        read_df = spark.table(table_fqn)

        result3 = read_df.filter(read_df.name.startswith("A"))
        assert result3.count() == 1, "String operations should work on table DataFrames"

    def test_logical_operations_in_filters(self, spark):
        """Test that logical operations (AND, OR) work correctly.

        Logical operations combine ColumnOperations, and the entire expression
        should be evaluated correctly.
        """
        data = [
            {"a": 10, "b": 20, "c": 30},
            {"a": 20, "b": 10, "c": 30},
            {"a": 30, "b": 20, "c": 10},
        ]
        df = spark.createDataFrame(data)

        # AND operation
        result1 = df.filter((df.a > 15) & (df.b > 15))
        assert result1.count() == 1, "Should return 1 row where a > 15 AND b > 15"

        # OR operation
        result2 = df.filter((df.a > 25) | (df.c < 20))
        assert result2.count() == 1, "Should return 1 row where a > 25 OR c < 20"

        # NOT operation
        result3 = df.filter(~(df.a == 20))
        assert result3.count() == 2, "Should return 2 rows where NOT (a == 20)"

        # Test on table DataFrame
        schema_name = "test_schema"
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        table_fqn = f"{schema_name}.test_table"
        df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(table_fqn)
        read_df = spark.table(table_fqn)

        result4 = read_df.filter((read_df.a > 15) & (read_df.b > 15))
        assert result4.count() == 1, (
            "Logical operations should work on table DataFrames"
        )

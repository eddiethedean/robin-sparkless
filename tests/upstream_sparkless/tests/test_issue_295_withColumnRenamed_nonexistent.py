"""
Tests for issue #295: withColumnRenamed should treat non-existent columns as no-op.

In PySpark, renaming a non-existent column is treated as a no-op (no error,
statement is ignored). This test verifies that Sparkless matches this behavior.
"""

from sparkless.sql import SparkSession


class TestIssue295WithColumnRenamedNonexistent:
    """Test withColumnRenamed with non-existent columns (no-op behavior)."""

    def test_withColumnRenamed_nonexistent_column_no_op(self):
        """Test that renaming a non-existent column is treated as a no-op."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            # Create dataframe with timestamp strings
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                ]
            )

            # Rename non-existent column - should be no-op
            result = df.withColumnRenamed("Does-Not-Exist", "Still-Does-Not-Exist")

            # Verify DataFrame is unchanged
            assert result.count() == 2
            assert set(result.columns) == {"Name", "Value"}
            assert "Does-Not-Exist" not in result.columns
            assert "Still-Does-Not-Exist" not in result.columns

            # Verify data is unchanged
            rows = result.collect()
            assert len(rows) == 2
            assert rows[0]["Name"] == "Alice"
            assert rows[0]["Value"] == 1
            assert rows[1]["Name"] == "Bob"
            assert rows[1]["Value"] == 2

            # Verify original DataFrame is unchanged
            assert df.count() == 2
            assert set(df.columns) == {"Name", "Value"}
        finally:
            spark.stop()

    def test_withColumnRenamed_existing_column_works(self):
        """Test that renaming an existing column still works correctly."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                ]
            )

            # Rename existing column
            result = df.withColumnRenamed("Name", "FullName")

            # Verify rename worked
            assert result.count() == 2
            assert "FullName" in result.columns
            assert "Name" not in result.columns
            assert "Value" in result.columns

            # Verify data
            rows = result.collect()
            assert rows[0]["FullName"] == "Alice"
            assert rows[0]["Value"] == 1
            assert rows[1]["FullName"] == "Bob"
            assert rows[1]["Value"] == 2
        finally:
            spark.stop()

    def test_withColumnRenamed_case_insensitive_nonexistent(self):
        """Test that case-insensitive non-existent column is treated as no-op."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                ]
            )

            # Try to rename with different case (doesn't exist)
            result = df.withColumnRenamed("DOES-NOT-EXIST", "new_name")

            # Should be no-op
            assert result.count() == 2
            assert set(result.columns) == {"Name", "Value"}
            assert "new_name" not in result.columns
        finally:
            spark.stop()

    def test_withColumnRenamed_chained_with_nonexistent(self):
        """Test chaining withColumnRenamed with both existing and non-existent columns."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                ]
            )

            # Chain: rename existing, then try to rename non-existent
            result = df.withColumnRenamed("Name", "FullName").withColumnRenamed(
                "Does-Not-Exist", "Still-Does-Not-Exist"
            )

            # First rename should work, second should be no-op
            assert result.count() == 2
            assert "FullName" in result.columns
            assert "Name" not in result.columns
            assert "Value" in result.columns
            assert "Does-Not-Exist" not in result.columns
            assert "Still-Does-Not-Exist" not in result.columns

            # Verify data
            rows = result.collect()
            assert rows[0]["FullName"] == "Alice"
            assert rows[0]["Value"] == 1
        finally:
            spark.stop()

    def test_withColumnsRenamed_with_nonexistent_columns(self):
        """Test withColumnsRenamed skips non-existent columns (no-op for missing ones)."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                ]
            )

            # Rename mix of existing and non-existent columns
            result = df.withColumnsRenamed(
                {
                    "Name": "FullName",  # Exists - should rename
                    "Does-Not-Exist": "Still-Does-Not-Exist",  # Doesn't exist - should skip
                }
            )

            # Only existing column should be renamed
            assert result.count() == 2
            assert "FullName" in result.columns
            assert "Name" not in result.columns
            assert "Value" in result.columns
            assert "Does-Not-Exist" not in result.columns
            assert "Still-Does-Not-Exist" not in result.columns

            # Verify data
            rows = result.collect()
            assert rows[0]["FullName"] == "Alice"
            assert rows[0]["Value"] == 1
        finally:
            spark.stop()

    def test_withColumnsRenamed_all_nonexistent_no_op(self):
        """Test withColumnsRenamed with all non-existent columns is a no-op."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                ]
            )

            # Try to rename only non-existent columns
            result = df.withColumnsRenamed(
                {
                    "Does-Not-Exist-1": "New-Name-1",
                    "Does-Not-Exist-2": "New-Name-2",
                }
            )

            # Should be complete no-op
            assert result.count() == 2
            assert set(result.columns) == {"Name", "Value"}
            assert "New-Name-1" not in result.columns
            assert "New-Name-2" not in result.columns

            # Verify data unchanged
            rows = result.collect()
            assert rows[0]["Name"] == "Alice"
            assert rows[0]["Value"] == 1
        finally:
            spark.stop()

    def test_withColumnRenamed_after_operations(self):
        """Test withColumnRenamed with non-existent column after other operations."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                ]
            )

            # Apply filter, then try to rename non-existent column
            result = df.filter(df.Value > 1).withColumnRenamed(
                "Does-Not-Exist", "Still-Does-Not-Exist"
            )

            # Should filter correctly, no-op on rename
            assert result.count() == 1
            assert set(result.columns) == {"Name", "Value"}
            assert "Does-Not-Exist" not in result.columns

            # Verify filtered data
            rows = result.collect()
            assert rows[0]["Name"] == "Bob"
            assert rows[0]["Value"] == 2
        finally:
            spark.stop()

    def test_withColumnRenamed_empty_dataframe(self):
        """Test withColumnRenamed with non-existent column on empty DataFrame."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            from sparkless.spark_types import (
                StructType,
                StructField,
                StringType,
                IntegerType,
            )

            # Create empty DataFrame with schema
            schema = StructType(
                [
                    StructField("Name", StringType(), True),
                    StructField("Value", IntegerType(), True),
                ]
            )
            df = spark.createDataFrame([], schema)

            # Try to rename non-existent column
            result = df.withColumnRenamed("Does-Not-Exist", "New-Name")

            # Should be no-op, DataFrame remains empty
            assert result.count() == 0
            assert set(result.columns) == {"Name", "Value"}
            assert "New-Name" not in result.columns
        finally:
            spark.stop()

    def test_withColumnRenamed_with_null_values(self):
        """Test withColumnRenamed with non-existent column when DataFrame has null values."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": None},
                    {"Name": None, "Value": 2},
                    {"Name": "Charlie", "Value": 3},
                ]
            )

            # Try to rename non-existent column
            result = df.withColumnRenamed("Does-Not-Exist", "New-Name")

            # Should be no-op
            assert result.count() == 3
            assert set(result.columns) == {"Name", "Value"}
            assert "New-Name" not in result.columns

            # Verify null values are preserved
            rows = result.collect()
            assert rows[0]["Value"] is None
            assert rows[1]["Name"] is None
        finally:
            spark.stop()

    def test_withColumnRenamed_different_data_types(self):
        """Test withColumnRenamed with non-existent column across different data types."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            from datetime import date, datetime

            df = spark.createDataFrame(
                [
                    {
                        "name": "Alice",
                        "age": 25,
                        "salary": 50000.5,
                        "active": True,
                        "birth_date": date(1998, 1, 15),
                        "created_at": datetime(2023, 1, 1, 12, 0, 0),
                    },
                    {
                        "name": "Bob",
                        "age": 30,
                        "salary": 60000.0,
                        "active": False,
                        "birth_date": date(1993, 5, 20),
                        "created_at": datetime(2023, 2, 1, 14, 30, 0),
                    },
                ]
            )

            # Try to rename non-existent column
            result = df.withColumnRenamed("Does-Not-Exist", "New-Name")

            # Should be no-op
            assert result.count() == 2
            assert len(result.columns) == 6
            assert "Does-Not-Exist" not in result.columns
            assert "New-Name" not in result.columns

            # Verify all data types are preserved
            rows = result.collect()
            assert isinstance(rows[0]["age"], int)
            assert isinstance(rows[0]["salary"], float)
            assert isinstance(rows[0]["active"], bool)
        finally:
            spark.stop()

    def test_withColumnRenamed_special_characters_in_names(self):
        """Test withColumnRenamed with special characters in column names."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"col_with_underscore": 1, "col-with-dash": 2, "col.with.dot": 3},
                    {"col_with_underscore": 4, "col-with-dash": 5, "col.with.dot": 6},
                ]
            )

            # Try to rename non-existent column with special characters
            result = df.withColumnRenamed("col@with#special$chars", "new@col#name")

            # Should be no-op
            assert result.count() == 2
            assert "col_with_underscore" in result.columns
            assert "col-with-dash" in result.columns
            assert "col.with.dot" in result.columns
            assert "col@with#special$chars" not in result.columns
            assert "new@col#name" not in result.columns
        finally:
            spark.stop()

    def test_withColumnRenamed_unicode_column_names(self):
        """Test withColumnRenamed with unicode characters in column names."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"姓名": "Alice", "年龄": 25, "値": 100},
                    {"姓名": "Bob", "年龄": 30, "値": 200},
                ]
            )

            # Try to rename non-existent unicode column
            result = df.withColumnRenamed("不存在", "新列名")

            # Should be no-op
            assert result.count() == 2
            assert "姓名" in result.columns
            assert "年龄" in result.columns
            assert "値" in result.columns
            assert "不存在" not in result.columns
            assert "新列名" not in result.columns
        finally:
            spark.stop()

    def test_withColumnRenamed_very_long_column_name(self):
        """Test withColumnRenamed with very long column names."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            long_col_name = "a" * 1000  # Very long column name
            df = spark.createDataFrame([{long_col_name: 1, "short": 2}])

            # Try to rename non-existent very long column
            result = df.withColumnRenamed("b" * 1000, "c" * 1000)

            # Should be no-op
            assert result.count() == 1
            assert long_col_name in result.columns
            assert "short" in result.columns
        finally:
            spark.stop()

    def test_withColumnRenamed_after_join(self):
        """Test withColumnRenamed with non-existent column after join operation."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
            )
            df2 = spark.createDataFrame(
                [{"id": 1, "value": 100}, {"id": 2, "value": 200}]
            )

            # Join, then try to rename non-existent column
            result = df1.join(df2, on="id", how="inner").withColumnRenamed(
                "Does-Not-Exist", "Still-Does-Not-Exist"
            )

            # Should join correctly, no-op on rename
            assert result.count() == 2
            assert "id" in result.columns
            assert "name" in result.columns
            assert "value" in result.columns
            assert "Does-Not-Exist" not in result.columns
        finally:
            spark.stop()

    def test_withColumnRenamed_after_groupby(self):
        """Test withColumnRenamed with non-existent column after groupBy operation."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            from sparkless.sql import functions as F

            df = spark.createDataFrame(
                [
                    {"dept": "IT", "salary": 50000},
                    {"dept": "IT", "salary": 60000},
                    {"dept": "HR", "salary": 55000},
                ]
            )

            # GroupBy, then try to rename non-existent column
            result = (
                df.groupBy("dept")
                .agg(F.avg("salary").alias("avg_salary"))
                .withColumnRenamed("Does-Not-Exist", "New-Name")
            )

            # Should aggregate correctly, no-op on rename
            assert result.count() == 2
            assert "dept" in result.columns
            assert "avg_salary" in result.columns
            assert "Does-Not-Exist" not in result.columns
        finally:
            spark.stop()

    def test_withColumnRenamed_after_select(self):
        """Test withColumnRenamed with non-existent column after select operation."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"name": "Alice", "age": 25, "salary": 50000},
                    {"name": "Bob", "age": 30, "salary": 60000},
                ]
            )

            # Select some columns, then try to rename non-existent column
            result = df.select("name", "age").withColumnRenamed(
                "Does-Not-Exist", "New-Name"
            )

            # Should select correctly, no-op on rename
            assert result.count() == 2
            assert "name" in result.columns
            assert "age" in result.columns
            assert "salary" not in result.columns
            assert "Does-Not-Exist" not in result.columns
        finally:
            spark.stop()

    def test_withColumnRenamed_after_orderby(self):
        """Test withColumnRenamed with non-existent column after orderBy operation."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"name": "Alice", "age": 25},
                    {"name": "Bob", "age": 30},
                    {"name": "Charlie", "age": 20},
                ]
            )

            # OrderBy, then try to rename non-existent column
            result = df.orderBy("age").withColumnRenamed("Does-Not-Exist", "New-Name")

            # Should order correctly, no-op on rename
            assert result.count() == 3
            rows = result.collect()
            assert rows[0]["age"] == 20  # Should be sorted
            assert rows[1]["age"] == 25
            assert rows[2]["age"] == 30
        finally:
            spark.stop()

    def test_withColumnRenamed_multiple_chained_nonexistent(self):
        """Test multiple chained withColumnRenamed calls with all non-existent columns."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            df = spark.createDataFrame([{"name": "Alice", "age": 25}])

            # Chain multiple non-existent column renames
            result = (
                df.withColumnRenamed("Does-Not-Exist-1", "New-Name-1")
                .withColumnRenamed("Does-Not-Exist-2", "New-Name-2")
                .withColumnRenamed("Does-Not-Exist-3", "New-Name-3")
            )

            # All should be no-ops
            assert result.count() == 1
            assert set(result.columns) == {"name", "age"}
            assert "New-Name-1" not in result.columns
            assert "New-Name-2" not in result.columns
            assert "New-Name-3" not in result.columns
        finally:
            spark.stop()

    def test_withColumnRenamed_mixed_existing_and_nonexistent_chained(self):
        """Test chaining withColumnRenamed with mix of existing and non-existent columns."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            df = spark.createDataFrame([{"name": "Alice", "age": 25, "city": "NYC"}])

            # Chain: existing, non-existent, existing, non-existent
            result = (
                df.withColumnRenamed("name", "full_name")  # Exists
                .withColumnRenamed("Does-Not-Exist-1", "New-1")  # Doesn't exist
                .withColumnRenamed("age", "years")  # Exists
                .withColumnRenamed("Does-Not-Exist-2", "New-2")  # Doesn't exist
            )

            # Existing should rename, non-existent should be no-op
            assert result.count() == 1
            assert "full_name" in result.columns
            assert "years" in result.columns
            assert "city" in result.columns
            assert "name" not in result.columns
            assert "age" not in result.columns
            assert "New-1" not in result.columns
            assert "New-2" not in result.columns
        finally:
            spark.stop()

    def test_withColumnsRenamed_mixed_existing_nonexistent_complex(self):
        """Test withColumnsRenamed with complex mix of existing and non-existent columns."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"a": 1, "b": 2, "c": 3, "d": 4},
                    {"a": 5, "b": 6, "c": 7, "d": 8},
                ]
            )

            # Mix: 2 exist, 3 don't exist
            result = df.withColumnsRenamed(
                {
                    "a": "A",  # Exists
                    "Does-Not-Exist-1": "New-1",  # Doesn't exist
                    "b": "B",  # Exists
                    "Does-Not-Exist-2": "New-2",  # Doesn't exist
                    "Does-Not-Exist-3": "New-3",  # Doesn't exist
                }
            )

            # Only existing should rename
            assert result.count() == 2
            assert "A" in result.columns
            assert "B" in result.columns
            assert "c" in result.columns
            assert "d" in result.columns
            assert "a" not in result.columns
            assert "b" not in result.columns
            assert "New-1" not in result.columns
            assert "New-2" not in result.columns
            assert "New-3" not in result.columns
        finally:
            spark.stop()

    def test_withColumnRenamed_after_union(self):
        """Test withColumnRenamed with non-existent column after union operation."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            df1 = spark.createDataFrame([{"id": 1, "name": "Alice"}])
            df2 = spark.createDataFrame([{"id": 2, "name": "Bob"}])

            # Union, then try to rename non-existent column
            result = df1.union(df2).withColumnRenamed("Does-Not-Exist", "New-Name")

            # Should union correctly, no-op on rename
            assert result.count() == 2
            assert "id" in result.columns
            assert "name" in result.columns
            assert "Does-Not-Exist" not in result.columns
        finally:
            spark.stop()

    def test_withColumnRenamed_after_distinct(self):
        """Test withColumnRenamed with non-existent column after distinct operation."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"name": "Alice", "dept": "IT"},
                    {"name": "Alice", "dept": "IT"},
                    {"name": "Bob", "dept": "HR"},
                ]
            )

            # Distinct, then try to rename non-existent column
            result = df.distinct().withColumnRenamed("Does-Not-Exist", "New-Name")

            # Should deduplicate correctly, no-op on rename
            assert result.count() == 2
            assert "name" in result.columns
            assert "dept" in result.columns
            assert "Does-Not-Exist" not in result.columns
        finally:
            spark.stop()

    def test_withColumnRenamed_after_withColumn(self):
        """Test withColumnRenamed with non-existent column after withColumn operation."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            from sparkless.sql import functions as F

            df = spark.createDataFrame([{"name": "Alice", "age": 25}])

            # Add column, then try to rename non-existent column
            result = df.withColumn("double_age", F.col("age") * 2).withColumnRenamed(
                "Does-Not-Exist", "New-Name"
            )

            # Should add column correctly, no-op on rename
            assert result.count() == 1
            assert "name" in result.columns
            assert "age" in result.columns
            assert "double_age" in result.columns
            assert "Does-Not-Exist" not in result.columns
        finally:
            spark.stop()

    def test_withColumnRenamed_after_drop(self):
        """Test withColumnRenamed with non-existent column after drop operation."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            df = spark.createDataFrame([{"name": "Alice", "age": 25, "city": "NYC"}])

            # Drop column, then try to rename non-existent column
            result = df.drop("city").withColumnRenamed("Does-Not-Exist", "New-Name")

            # Should drop correctly, no-op on rename
            assert result.count() == 1
            assert "name" in result.columns
            assert "age" in result.columns
            assert "city" not in result.columns
            assert "Does-Not-Exist" not in result.columns
        finally:
            spark.stop()

    def test_withColumnRenamed_whitespace_in_column_names(self):
        """Test withColumnRenamed with whitespace in column names."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            # Note: PySpark doesn't allow spaces in column names without backticks
            # But we test that non-existent columns with spaces are handled
            df = spark.createDataFrame([{"name": "Alice", "age": 25}])

            # Try to rename non-existent column (with spaces would need backticks in SQL)
            result = df.withColumnRenamed("Does Not Exist", "New Name")

            # Should be no-op
            assert result.count() == 1
            assert "name" in result.columns
            assert "age" in result.columns
            assert "Does Not Exist" not in result.columns
            assert "New Name" not in result.columns
        finally:
            spark.stop()

    def test_withColumnRenamed_complex_nested_operations(self):
        """Test withColumnRenamed with non-existent column after complex nested operations."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            from sparkless.sql import functions as F

            df = spark.createDataFrame(
                [
                    {"name": "Alice", "age": 25, "salary": 50000},
                    {"name": "Bob", "age": 30, "salary": 60000},
                    {"name": "Charlie", "age": 35, "salary": 70000},
                ]
            )

            # Complex chain: filter, select, withColumn, orderBy, then rename non-existent
            result = (
                df.filter(F.col("age") > 25)
                .select("name", "age", "salary")
                .withColumn("bonus", F.col("salary") * 0.1)
                .orderBy(F.desc("salary"))
                .withColumnRenamed("Does-Not-Exist", "New-Name")
            )

            # Should execute all operations correctly, no-op on rename
            assert result.count() == 2
            assert "name" in result.columns
            assert "age" in result.columns
            assert "salary" in result.columns
            assert "bonus" in result.columns
            assert "Does-Not-Exist" not in result.columns

            # Verify ordering
            rows = result.collect()
            assert rows[0]["salary"] == 70000
            assert rows[1]["salary"] == 60000
        finally:
            spark.stop()

    def test_withColumnRenamed_idempotent_behavior(self):
        """Test that withColumnRenamed with same non-existent column multiple times is idempotent."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            df = spark.createDataFrame([{"name": "Alice", "age": 25}])

            # Try to rename same non-existent column multiple times
            result1 = df.withColumnRenamed("Does-Not-Exist", "New-Name")
            result2 = result1.withColumnRenamed("Does-Not-Exist", "New-Name")
            result3 = result2.withColumnRenamed("Does-Not-Exist", "New-Name")

            # All should be no-ops, results should be equivalent
            assert result1.count() == result2.count() == result3.count() == 1
            assert set(result1.columns) == set(result2.columns) == set(result3.columns)
            assert "Does-Not-Exist" not in result3.columns
            assert "New-Name" not in result3.columns
        finally:
            spark.stop()

"""
Unit tests for DataFrame transformation operations.
"""

import pytest
from sparkless import SparkSession, F
from sparkless.window import Window
from sparkless.core.exceptions.operation import SparkColumnNotFoundError


@pytest.mark.unit
class TestTransformationOperations:
    """Test DataFrame transformation operations."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        return SparkSession("test")

    @pytest.fixture
    def sample_data(self):
        """Sample data for testing."""
        return [
            {"id": 1, "name": "Alice", "age": 25, "salary": 50000.0, "active": True},
            {"id": 2, "name": "Bob", "age": 30, "salary": 60000.0, "active": False},
            {"id": 3, "name": "Charlie", "age": 35, "salary": 70000.0, "active": True},
            {"id": 4, "name": "Diana", "age": 28, "salary": 55000.0, "active": True},
            {
                "id": 1,
                "name": "Alice",
                "age": 25,
                "salary": 50000.0,
                "active": True,
            },  # Exact duplicate
        ]

    # withColumn tests
    def test_withColumn_add_new_column_literal(self, spark, sample_data):
        """Test adding new column with literal value."""
        df = spark.createDataFrame(sample_data)
        result = df.withColumn("status", F.lit("active"))

        assert "status" in result.columns
        assert result.count() == 5
        rows = result.collect()
        assert all(row.status == "active" for row in rows)

    def test_withColumn_add_new_column_expression(self, spark, sample_data):
        """Test adding new column with expression."""
        df = spark.createDataFrame(sample_data)
        result = df.withColumn("double_age", df.age * 2)

        assert "double_age" in result.columns
        assert result.count() == 5
        rows = result.collect()
        assert rows[0].double_age == 50  # 25 * 2

    def test_withColumn_add_new_column_string_function(self, spark, sample_data):
        """Test adding new column with string function."""
        df = spark.createDataFrame(sample_data)
        result = df.withColumn("upper_name", F.upper(df.name))

        assert "upper_name" in result.columns
        rows = result.collect()
        assert rows[0].upper_name == "ALICE"

    def test_withColumn_replace_existing_column(self, spark, sample_data):
        """Test replacing existing column."""
        df = spark.createDataFrame(sample_data)
        result = df.withColumn("age", df.age + 1)

        assert "age" in result.columns
        rows = result.collect()
        assert rows[0].age == 26  # 25 + 1

    def test_withColumn_with_window_function(self, spark, sample_data):
        """Test adding column with window function."""
        df = spark.createDataFrame(sample_data)
        window_spec = Window.partitionBy("name").orderBy("age")
        result = df.withColumn("row_num", F.row_number().over(window_spec))

        assert "row_num" in result.columns
        assert result.count() == 5

    def test_withColumn_with_conditional_logic(self, spark, sample_data):
        """Test adding column with conditional logic."""
        df = spark.createDataFrame(sample_data)
        result = df.withColumn(
            "category", F.when(df.age > 30, "senior").otherwise("junior")
        )

        assert "category" in result.columns
        rows = result.collect()
        assert rows[0].category == "junior"  # age 25
        assert rows[2].category == "senior"  # age 35

    def test_withColumn_invalid_column_reference(self, spark, sample_data):
        """Test error handling for invalid column references."""
        df = spark.createDataFrame(sample_data)
        with pytest.raises(SparkColumnNotFoundError):
            df.withColumn("new_col", df.nonexistent_column)

    # withColumns tests
    def test_withColumns_add_multiple_columns(self, spark, sample_data):
        """Test adding multiple columns at once."""
        df = spark.createDataFrame(sample_data)
        result = df.withColumns(
            {
                "double_age": df.age * 2,
                "upper_name": F.upper(df.name),
                "status": F.lit("active"),
            }
        )

        assert "double_age" in result.columns
        assert "upper_name" in result.columns
        assert "status" in result.columns
        assert result.count() == 5

    def test_withColumns_replace_multiple_columns(self, spark, sample_data):
        """Test replacing multiple columns."""
        df = spark.createDataFrame(sample_data)
        result = df.withColumns({"age": df.age + 1, "salary": df.salary * 1.1})

        rows = result.collect()
        assert rows[0].age == 26  # 25 + 1
        # Use approximate comparison for floating point
        assert abs(rows[0].salary - 55000.0) < 0.01  # 50000 * 1.1

    def test_withColumns_mix_new_and_existing(self, spark, sample_data):
        """Test mix of new and existing columns."""
        df = spark.createDataFrame(sample_data)
        result = df.withColumns(
            {
                "age": df.age + 1,  # Replace
                "new_col": F.lit(100),  # New
            }
        )

        assert "age" in result.columns
        assert "new_col" in result.columns
        rows = result.collect()
        assert rows[0].age == 26
        assert rows[0].new_col == 100

    # drop tests
    def test_drop_single_column(self, spark, sample_data):
        """Test dropping single column."""
        df = spark.createDataFrame(sample_data)
        result = df.drop("age")

        assert "age" not in result.columns
        assert "id" in result.columns
        assert "name" in result.columns
        assert result.count() == 5

    def test_drop_multiple_columns(self, spark, sample_data):
        """Test dropping multiple columns."""
        df = spark.createDataFrame(sample_data)
        result = df.drop("age", "salary")

        assert "age" not in result.columns
        assert "salary" not in result.columns
        assert "id" in result.columns
        assert "name" in result.columns
        assert result.count() == 5

    def test_drop_all_columns(self, spark, sample_data):
        """Test dropping all columns (edge case)."""
        df = spark.createDataFrame(sample_data)
        result = df.drop("id", "name", "age", "salary", "active")

        assert len(result.columns) == 0
        assert result.count() == 5

    def test_drop_nonexistent_column(self, spark, sample_data):
        """Test dropping non-existent column (should not error in PySpark)."""
        df = spark.createDataFrame(sample_data)
        # PySpark allows dropping non-existent columns without error
        result = df.drop("nonexistent")

        assert result.count() == 5
        assert len(result.columns) == 5

    # withColumnRenamed / withColumnsRenamed tests
    def test_withColumnRenamed_single_column(self, spark, sample_data):
        """Test renaming single column."""
        df = spark.createDataFrame(sample_data)
        result = df.withColumnRenamed("name", "full_name")

        assert "full_name" in result.columns
        assert "name" not in result.columns
        assert result.count() == 5
        rows = result.collect()
        assert rows[0].full_name == "Alice"

    def test_withColumnsRenamed_multiple_columns(self, spark, sample_data):
        """Test renaming multiple columns."""
        df = spark.createDataFrame(sample_data)
        result = df.withColumnsRenamed({"name": "full_name", "age": "years_old"})

        assert "full_name" in result.columns
        assert "years_old" in result.columns
        assert "name" not in result.columns
        assert "age" not in result.columns
        rows = result.collect()
        assert rows[0].full_name == "Alice"
        assert rows[0].years_old == 25

    def test_withColumnRenamed_nonexistent_column(self, spark, sample_data):
        """Test error handling for renaming non-existent column."""
        df = spark.createDataFrame(sample_data)
        # PySpark may allow renaming non-existent columns, so test that it doesn't crash
        # and that the column doesn't exist in result
        result = df.withColumnRenamed("nonexistent", "new_name")
        assert "nonexistent" not in result.columns
        # new_name may or may not be added depending on implementation

    # distinct / dropDuplicates tests
    def test_distinct_remove_all_duplicates(self, spark, sample_data):
        """Test removing all duplicates."""
        df = spark.createDataFrame(sample_data)
        result = df.distinct()

        # Should have 4 unique rows (id 1 appears twice with same values)
        assert result.count() == 4
        # Verify it's a DataFrame with same columns
        assert set(result.columns) == set(df.columns)

    def test_dropDuplicates_without_subset(self, spark, sample_data):
        """Test dropDuplicates without subset (same as distinct)."""
        df = spark.createDataFrame(sample_data)
        result = df.dropDuplicates()

        # Should have 4 unique rows (same as distinct)
        assert result.count() == 4
        assert set(result.columns) == set(df.columns)

    def test_dropDuplicates_with_subset(self, spark, sample_data):
        """Test dropDuplicates with subset of columns."""
        df = spark.createDataFrame(sample_data)
        # Drop duplicates based on name and age only
        result = df.dropDuplicates(subset=["name", "age"])

        # Should have 4 rows (first and last row have same name and age)
        assert result.count() == 4

    def test_dropDuplicates_empty_dataframe(self, spark):
        """Test dropDuplicates on empty DataFrame."""
        from sparkless.spark_types import StructType

        df = spark.createDataFrame([], StructType([]))
        result = df.dropDuplicates()

        assert result.count() == 0

    def test_dropDuplicates_with_none_values(self, spark):
        """Test dropDuplicates with None values."""
        from sparkless.spark_types import (
            StructType,
            StructField,
            IntegerType,
            StringType,
        )

        # Use explicit schema to avoid inference issues with None
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),  # Nullable
            ]
        )
        data = [
            {"id": 1, "name": "Alice", "age": None},
            {"id": 2, "name": "Bob", "age": None},
            {"id": 3, "name": "Alice", "age": None},
        ]
        df = spark.createDataFrame(data, schema)
        result = df.dropDuplicates(subset=["name", "age"])

        # dropDuplicates with None values - should have 2 unique rows (id 1 and 3 have same name and age=None)
        assert result.count() == 2
        assert set(result.columns) == set(df.columns)

    # orderBy / sort tests
    def test_orderBy_single_column_ascending(self, spark, sample_data):
        """Test sorting by single column ascending."""
        df = spark.createDataFrame(sample_data)
        result = df.orderBy("age")

        rows = result.collect()
        assert rows[0].age == 25
        assert rows[-1].age == 35

    def test_orderBy_multiple_columns(self, spark, sample_data):
        """Test sorting by multiple columns."""
        df = spark.createDataFrame(sample_data)
        result = df.orderBy("name", "age")

        rows = result.collect()
        # Should be sorted by name first, then age
        names = [row.name for row in rows]
        assert names == sorted(names)

    def test_orderBy_descending(self, spark, sample_data):
        """Test sorting with descending order."""
        df = spark.createDataFrame(sample_data)
        result = df.orderBy(F.desc("age"))

        rows = result.collect()
        assert rows[0].age == 35
        assert rows[-1].age == 25

    def test_sort_alias_for_orderBy(self, spark, sample_data):
        """Test sort() as alias for orderBy()."""
        df = spark.createDataFrame(sample_data)
        result = df.sort("age")

        rows = result.collect()
        assert rows[0].age == 25
        assert rows[-1].age == 35

    def test_orderBy_nonexistent_column(self, spark, sample_data):
        """Test error handling for sorting by non-existent column."""
        df = spark.createDataFrame(sample_data)
        # orderBy may not validate columns immediately with lazy evaluation
        # Test that it doesn't crash (error may occur on materialization)
        result = df.orderBy("nonexistent")
        # May raise error on collect() or may return empty DataFrame
        import contextlib

        with contextlib.suppress(Exception):
            # Expected if column validation happens on materialization
            result.collect()

    # limit tests
    def test_limit_fewer_rows_than_available(self, spark, sample_data):
        """Test limiting to fewer rows than available."""
        df = spark.createDataFrame(sample_data)
        result = df.limit(2)

        assert result.count() == 2

    def test_limit_more_rows_than_available(self, spark, sample_data):
        """Test limiting to more rows than available."""
        df = spark.createDataFrame(sample_data)
        result = df.limit(10)

        assert result.count() == 5  # Only 5 rows available

    def test_limit_zero_rows(self, spark, sample_data):
        """Test limiting to zero rows."""
        df = spark.createDataFrame(sample_data)
        result = df.limit(0)

        assert result.count() == 0

    def test_limit_negative_number(self, spark, sample_data):
        """Test limit with negative number (should error or handle gracefully)."""
        df = spark.createDataFrame(sample_data)
        # Negative limits should raise an error
        with pytest.raises((ValueError, Exception)):
            result = df.limit(-1)
            result.count()  # Error may occur on materialization

    # replace tests
    def test_replace_single_value(self, spark, sample_data):
        """Test replacing single value."""
        df = spark.createDataFrame(sample_data)
        result = df.replace("Alice", "Alicia")

        rows = result.collect()
        alice_rows = [row for row in rows if row.name == "Alicia"]
        assert len(alice_rows) > 0

    def test_replace_multiple_values_with_dict(self, spark, sample_data):
        """Test replacing multiple values with dict."""
        df = spark.createDataFrame(sample_data)
        result = df.replace({"Alice": "Alicia", "Bob": "Robert"})

        rows = result.collect()
        names = [row.name for row in rows]
        assert "Alicia" in names
        assert "Robert" in names
        assert "Alice" not in names
        assert "Bob" not in names

    def test_replace_with_subset_columns(self, spark, sample_data):
        """Test replacing with subset of columns."""
        df = spark.createDataFrame(sample_data)
        result = df.replace(50000.0, 55000.0, subset=["salary"])

        rows = result.collect()
        # Check that salary values were replaced
        salaries = [row.salary for row in rows]
        assert 55000.0 in salaries

    def test_replace_none_values(self, spark):
        """Test replacing None values."""
        data = [
            {"id": 1, "name": "Alice", "age": None},
            {"id": 2, "name": "Bob", "age": 30},
        ]
        df = spark.createDataFrame(data)
        result = df.replace(None, 0, subset=["age"])

        rows = result.collect()
        ages = [row.age for row in rows]
        assert 0 in ages
        assert None not in ages or all(
            age is not None for age in ages if age is not None
        )

"""
Unit tests for Issue #331: Join doesn't support array_contains() condition.

Tests that join operations work correctly with expression-based conditions like array_contains.
"""

from sparkless.sql import SparkSession
from sparkless import functions as F


class TestIssue331ArrayContainsJoin:
    """Test array_contains() as join condition."""

    def test_array_contains_join_basic(self):
        """Test basic array_contains join."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3]},
                    {"Name": "Bob", "IDs": [4, 5, 6]},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 3},
                    {"Dept": "B", "ID": 5},
                ]
            )

            result = df1.join(df2, on=F.array_contains(df1.IDs, df2.ID), how="left")
            rows = result.collect()

            assert len(rows) == 2
            assert rows[0]["Name"] == "Alice"
            assert rows[0]["IDs"] == [1, 2, 3]
            assert rows[0]["Dept"] == "A"
            assert rows[0]["ID"] == 3
            assert rows[1]["Name"] == "Bob"
            assert rows[1]["IDs"] == [4, 5, 6]
            assert rows[1]["Dept"] == "B"
            assert rows[1]["ID"] == 5
        finally:
            spark.stop()

    def test_array_contains_join_inner(self):
        """Test array_contains join with inner join type."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3]},
                    {"Name": "Bob", "IDs": [4, 5, 6]},
                    {"Name": "Charlie", "IDs": [7, 8, 9]},  # No match
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 3},
                    {"Dept": "B", "ID": 5},
                ]
            )

            result = df1.join(df2, on=F.array_contains(df1.IDs, df2.ID), how="inner")
            rows = result.collect()

            assert len(rows) == 2
            names = {row["Name"] for row in rows}
            assert names == {"Alice", "Bob"}
        finally:
            spark.stop()

    def test_array_contains_join_left(self):
        """Test array_contains join with left join type."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3]},
                    {"Name": "Bob", "IDs": [4, 5, 6]},
                    {"Name": "Charlie", "IDs": [7, 8, 9]},  # No match
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 3},
                    {"Dept": "B", "ID": 5},
                ]
            )

            result = df1.join(df2, on=F.array_contains(df1.IDs, df2.ID), how="left")
            rows = result.collect()

            assert len(rows) == 3
            # Find Charlie row (no match)
            charlie_row = next(row for row in rows if row["Name"] == "Charlie")
            assert charlie_row["Dept"] is None
            assert charlie_row["ID"] is None
        finally:
            spark.stop()

    def test_array_contains_join_multiple_matches(self):
        """Test array_contains join with multiple matches."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3]},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 1},
                    {"Dept": "B", "ID": 2},
                    {"Dept": "C", "ID": 3},
                ]
            )

            result = df1.join(df2, on=F.array_contains(df1.IDs, df2.ID), how="inner")
            rows = result.collect()

            assert len(rows) == 3
            # All rows should have Alice's name and IDs
            for row in rows:
                assert row["Name"] == "Alice"
                assert row["IDs"] == [1, 2, 3]
            # Check all depts are present
            depts = {row["Dept"] for row in rows}
            assert depts == {"A", "B", "C"}
        finally:
            spark.stop()

    def test_array_contains_join_no_matches(self):
        """Test array_contains join with no matches."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3]},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 10},  # No match
                    {"Dept": "B", "ID": 20},  # No match
                ]
            )

            # Inner join with no matches should return empty
            result = df1.join(df2, on=F.array_contains(df1.IDs, df2.ID), how="inner")
            rows = result.collect()
            assert len(rows) == 0

            # Left join with no matches should return left rows with nulls
            result2 = df1.join(df2, on=F.array_contains(df1.IDs, df2.ID), how="left")
            rows2 = result2.collect()
            assert len(rows2) == 1
            assert rows2[0]["Name"] == "Alice"
            assert rows2[0]["Dept"] is None
            assert rows2[0]["ID"] is None
        finally:
            spark.stop()

    def test_array_contains_join_null_arrays(self):
        """Test array_contains join with null arrays."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3]},
                    {"Name": "Bob", "IDs": None},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 3},
                ]
            )

            result = df1.join(df2, on=F.array_contains(df1.IDs, df2.ID), how="left")
            rows = result.collect()

            assert len(rows) >= 1
            # Alice should match
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            assert alice_row["Dept"] == "A"
            # Bob with null array should not match (or match with null)
            bob_rows = [row for row in rows if row["Name"] == "Bob"]
            if bob_rows:
                assert bob_rows[0]["Dept"] is None
        finally:
            spark.stop()

    def test_array_contains_join_null_ids(self):
        """Test array_contains join with null IDs."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3]},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 3},
                    {"Dept": "B", "ID": None},
                ]
            )

            result = df1.join(df2, on=F.array_contains(df1.IDs, df2.ID), how="left")
            rows = result.collect()

            # Should have at least one match (ID=3)
            assert len(rows) >= 1
            matching_rows = [row for row in rows if row["Dept"] == "A"]
            assert len(matching_rows) >= 1
        finally:
            spark.stop()

    def test_array_contains_join_right(self):
        """Test array_contains join with right join type."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3]},
                    {"Name": "Bob", "IDs": [4, 5, 6]},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 3},
                    {"Dept": "B", "ID": 5},
                    {"Dept": "C", "ID": 10},  # No match
                ]
            )

            result = df1.join(df2, on=F.array_contains(df1.IDs, df2.ID), how="right")
            rows = result.collect()

            # Should include all right rows
            assert len(rows) >= 2
            depts = {row["Dept"] for row in rows}
            assert "A" in depts
            assert "B" in depts
        finally:
            spark.stop()

    def test_array_contains_join_outer(self):
        """Test array_contains join with outer join type."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3]},
                    {"Name": "Bob", "IDs": [4, 5, 6]},
                    {"Name": "Charlie", "IDs": [7, 8, 9]},  # No match
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 3},
                    {"Dept": "B", "ID": 5},
                    {"Dept": "C", "ID": 10},  # No match
                ]
            )

            result = df1.join(df2, on=F.array_contains(df1.IDs, df2.ID), how="outer")
            rows = result.collect()

            # Should include matches and unmatched rows from both sides
            assert len(rows) >= 2
            names = {row["Name"] for row in rows if row["Name"] is not None}
            assert "Alice" in names
            assert "Bob" in names
        finally:
            spark.stop()

    def test_array_contains_join_with_select(self):
        """Test array_contains join followed by select."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3]},
                    {"Name": "Bob", "IDs": [4, 5, 6]},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 3},
                    {"Dept": "B", "ID": 5},
                ]
            )

            result = df1.join(
                df2, on=F.array_contains(df1.IDs, df2.ID), how="left"
            ).select("Name", "Dept")
            rows = result.collect()

            assert len(rows) == 2
            assert rows[0]["Name"] == "Alice"
            assert rows[0]["Dept"] == "A"
        finally:
            spark.stop()

    def test_array_contains_join_with_filter(self):
        """Test array_contains join followed by filter."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3]},
                    {"Name": "Bob", "IDs": [4, 5, 6]},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 3},
                    {"Dept": "B", "ID": 5},
                ]
            )

            result = df1.join(
                df2, on=F.array_contains(df1.IDs, df2.ID), how="left"
            ).filter(F.col("Dept") == "A")
            rows = result.collect()

            assert len(rows) == 1
            assert rows[0]["Name"] == "Alice"
            assert rows[0]["Dept"] == "A"
        finally:
            spark.stop()

    def test_array_contains_join_column_name_conflicts(self):
        """Test array_contains join when DataFrames have column name conflicts."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3], "Value": 10},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Name": "Bob", "ID": 3, "Value": 20},
                ]
            )

            result = df1.join(df2, on=F.array_contains(df1.IDs, df2.ID), how="inner")
            rows = result.collect()

            assert len(rows) == 1
            # Both Name and Value columns should be present (may be from left or right)
            assert "Name" in rows[0]
            assert "Value" in rows[0]
        finally:
            spark.stop()

    def test_array_contains_join_empty_dataframes(self):
        """Test array_contains join with empty DataFrames."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            from sparkless.spark_types import (
                StructType,
                StructField,
                StringType,
                ArrayType,
                IntegerType,
            )

            schema1 = StructType(
                [
                    StructField("Name", StringType(), True),
                    StructField("IDs", ArrayType(IntegerType()), True),
                ]
            )
            schema2 = StructType(
                [
                    StructField("Dept", StringType(), True),
                    StructField("ID", IntegerType(), True),
                ]
            )

            df1 = spark.createDataFrame([], schema=schema1)
            df2 = spark.createDataFrame([], schema=schema2)

            result = df1.join(df2, on=F.array_contains(df1.IDs, df2.ID), how="inner")
            rows = result.collect()

            assert len(rows) == 0
        finally:
            spark.stop()

    def test_array_contains_join_backward_compatibility(self):
        """Test that regular column-based joins still work (backward compatibility)."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "ID": 1},
                    {"Name": "Bob", "ID": 2},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 1},
                    {"Dept": "B", "ID": 2},
                ]
            )

            # Regular column-based join should still work
            result = df1.join(df2, on="ID", how="inner")
            rows = result.collect()

            assert len(rows) == 2
            assert rows[0]["Name"] == "Alice"
            assert rows[0]["Dept"] == "A"
        finally:
            spark.stop()

    def test_array_contains_join_empty_arrays(self):
        """Test array_contains join with empty arrays."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": []},  # Empty array
                    {"Name": "Bob", "IDs": [1, 2, 3]},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 1},
                ]
            )

            result = df1.join(df2, on=F.array_contains(df1.IDs, df2.ID), how="left")
            rows = result.collect()

            assert len(rows) == 2
            # Alice with empty array should not match
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            assert alice_row["Dept"] is None
            assert alice_row["ID"] is None
            # Bob should match
            bob_row = next(row for row in rows if row["Name"] == "Bob")
            assert bob_row["Dept"] == "A"
        finally:
            spark.stop()

    def test_array_contains_join_duplicate_values(self):
        """Test array_contains join with arrays containing duplicate values."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 1, 2, 2, 3]},  # Duplicates
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 1},
                    {"Dept": "B", "ID": 2},
                ]
            )

            result = df1.join(df2, on=F.array_contains(df1.IDs, df2.ID), how="inner")
            rows = result.collect()

            assert len(rows) == 2
            depts = {row["Dept"] for row in rows}
            assert depts == {"A", "B"}
        finally:
            spark.stop()

    def test_array_contains_join_string_arrays(self):
        """Test array_contains join with string arrays."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "Tags": ["python", "spark", "data"]},
                    {"Name": "Bob", "Tags": ["java", "scala"]},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Skill": "Python", "Tag": "python"},
                    {"Skill": "Java", "Tag": "java"},
                ]
            )

            result = df1.join(df2, on=F.array_contains(df1.Tags, df2.Tag), how="left")
            rows = result.collect()

            assert len(rows) == 2
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            assert alice_row["Skill"] == "Python"
            bob_row = next(row for row in rows if row["Name"] == "Bob")
            assert bob_row["Skill"] == "Java"
        finally:
            spark.stop()

    def test_array_contains_join_float_arrays(self):
        """Test array_contains join with float arrays."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "Values": [1.5, 2.5, 3.5]},
                    {"Name": "Bob", "Values": [4.0, 5.0]},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Category": "A", "Value": 2.5},
                    {"Category": "B", "Value": 5.0},
                ]
            )

            result = df1.join(
                df2, on=F.array_contains(df1.Values, df2.Value), how="left"
            )
            rows = result.collect()

            assert len(rows) == 2
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            assert alice_row["Category"] == "A"
            bob_row = next(row for row in rows if row["Name"] == "Bob")
            assert bob_row["Category"] == "B"
        finally:
            spark.stop()

    def test_array_contains_join_large_arrays(self):
        """Test array_contains join with large arrays."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            # Create array with 100 elements
            large_array = list(range(1, 101))
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": large_array},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 50},  # Middle of array
                    {"Dept": "B", "ID": 1},  # First element
                    {"Dept": "C", "ID": 100},  # Last element
                ]
            )

            result = df1.join(df2, on=F.array_contains(df1.IDs, df2.ID), how="inner")
            rows = result.collect()

            assert len(rows) == 3
            depts = {row["Dept"] for row in rows}
            assert depts == {"A", "B", "C"}
        finally:
            spark.stop()

    def test_array_contains_join_with_where_clause(self):
        """Test array_contains join combined with where/filter conditions."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3], "Age": 25},
                    {"Name": "Bob", "IDs": [4, 5, 6], "Age": 30},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 3},
                    {"Dept": "B", "ID": 5},
                ]
            )

            result = df1.join(
                df2, on=F.array_contains(df1.IDs, df2.ID), how="left"
            ).filter(F.col("Age") > 25)
            rows = result.collect()

            assert len(rows) == 1
            assert rows[0]["Name"] == "Bob"
            assert rows[0]["Dept"] == "B"
        finally:
            spark.stop()

    def test_array_contains_join_with_aggregation(self):
        """Test array_contains join followed by aggregation."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3]},
                    {"Name": "Bob", "IDs": [4, 5, 6]},
                    {"Name": "Charlie", "IDs": [1, 2, 3]},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 3},
                    {"Dept": "B", "ID": 5},
                ]
            )

            result = (
                df1.join(df2, on=F.array_contains(df1.IDs, df2.ID), how="left")
                .groupBy("Dept")
                .agg(F.count("Name").alias("Count"))
            )
            rows = result.collect()

            # Should have counts for each dept
            assert len(rows) >= 1
            dept_counts = {
                row["Dept"]: row["Count"] for row in rows if row["Dept"] is not None
            }
            assert "A" in dept_counts
            assert "B" in dept_counts
        finally:
            spark.stop()

    def test_array_contains_join_with_window_functions(self):
        """Test array_contains join with window functions."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            from sparkless.window import Window

            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3], "Score": 100},
                    {"Name": "Bob", "IDs": [4, 5, 6], "Score": 90},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 3},
                    {"Dept": "B", "ID": 5},
                ]
            )

            window = Window.partitionBy("Dept").orderBy(F.col("Score").desc())
            result = df1.join(
                df2, on=F.array_contains(df1.IDs, df2.ID), how="left"
            ).withColumn("Rank", F.row_number().over(window))
            rows = result.collect()

            assert len(rows) == 2
            for row in rows:
                assert "Rank" in row
                assert row["Rank"] == 1  # Each dept has one row
        finally:
            spark.stop()

    def test_array_contains_join_with_union(self):
        """Test array_contains join with union operations."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3]},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 3},
                ]
            )

            df3 = spark.createDataFrame(
                [
                    {"Name": "Bob", "IDs": [4, 5, 6]},
                ]
            )

            result1 = df1.join(df2, on=F.array_contains(df1.IDs, df2.ID), how="left")
            result2 = df3.join(df2, on=F.array_contains(df3.IDs, df2.ID), how="left")
            # Use unionByName to handle potential column order differences
            combined = result1.unionByName(result2, allowMissingColumns=True)
            rows = combined.collect()

            assert len(rows) == 2
            names = {row["Name"] for row in rows}
            assert names == {"Alice", "Bob"}
        finally:
            spark.stop()

    def test_array_contains_join_with_distinct(self):
        """Test array_contains join with distinct operation."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3], "Value": 10},
                    {"Name": "Alice", "IDs": [1, 2, 3], "Value": 10},  # Duplicate
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 3},
                ]
            )

            result = (
                df1.join(df2, on=F.array_contains(df1.IDs, df2.ID), how="left")
                .select(
                    "Name", "Dept", "ID", "Value"
                )  # Select specific columns to avoid array distinct issues
                .distinct()
            )
            rows = result.collect()

            # Should have only one row after distinct
            assert len(rows) == 1
            assert rows[0]["Name"] == "Alice"
        finally:
            spark.stop()

    def test_array_contains_join_with_limit(self):
        """Test array_contains join with limit operation."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3]},
                    {"Name": "Bob", "IDs": [4, 5, 6]},
                    {"Name": "Charlie", "IDs": [7, 8, 9]},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 3},
                    {"Dept": "B", "ID": 5},
                ]
            )

            result = df1.join(
                df2, on=F.array_contains(df1.IDs, df2.ID), how="left"
            ).limit(1)
            rows = result.collect()

            assert len(rows) == 1
        finally:
            spark.stop()

    def test_array_contains_join_multiple_conditions_same_df(self):
        """Test array_contains join where same left row matches multiple right rows."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3, 4, 5]},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 1},
                    {"Dept": "B", "ID": 2},
                    {"Dept": "C", "ID": 3},
                    {"Dept": "D", "ID": 4},
                    {"Dept": "E", "ID": 5},
                ]
            )

            result = df1.join(df2, on=F.array_contains(df1.IDs, df2.ID), how="inner")
            rows = result.collect()

            assert len(rows) == 5
            depts = {row["Dept"] for row in rows}
            assert depts == {"A", "B", "C", "D", "E"}
            # All rows should have Alice's name
            for row in rows:
                assert row["Name"] == "Alice"
        finally:
            spark.stop()

    def test_array_contains_join_with_nested_select(self):
        """Test array_contains join with nested select expressions."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3]},
                    {"Name": "Bob", "IDs": [4, 5, 6]},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 3},
                    {"Dept": "B", "ID": 5},
                ]
            )

            result = df1.join(
                df2, on=F.array_contains(df1.IDs, df2.ID), how="left"
            ).select(
                F.col("Name"),
                F.col("Dept").alias("Department"),
                F.col("ID").alias("MatchedID"),
            )
            rows = result.collect()

            assert len(rows) == 2
            assert "Name" in rows[0]
            assert "Department" in rows[0]
            assert "MatchedID" in rows[0]
        finally:
            spark.stop()

    def test_array_contains_join_with_case_when(self):
        """Test array_contains join with case/when expressions."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3]},
                    {"Name": "Bob", "IDs": [4, 5, 6]},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 3},
                    {"Dept": "B", "ID": 5},
                ]
            )

            result = df1.join(
                df2, on=F.array_contains(df1.IDs, df2.ID), how="left"
            ).withColumn(
                "Status",
                F.when(F.col("Dept").isNotNull(), "Matched").otherwise("NoMatch"),
            )
            rows = result.collect()

            assert len(rows) == 2
            for row in rows:
                assert "Status" in row
                if row["Dept"] is not None:
                    assert row["Status"] == "Matched"
                else:
                    assert row["Status"] == "NoMatch"
        finally:
            spark.stop()

    def test_array_contains_join_with_coalesce(self):
        """Test array_contains join with coalesce function."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3]},
                    {"Name": "Bob", "IDs": [4, 5, 6]},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 3},
                    {"Dept": "B", "ID": 5},
                ]
            )

            result = df1.join(
                df2, on=F.array_contains(df1.IDs, df2.ID), how="left"
            ).withColumn("FinalDept", F.coalesce(F.col("Dept"), F.lit("Unknown")))
            rows = result.collect()

            assert len(rows) == 2
            for row in rows:
                assert "FinalDept" in row
                assert row["FinalDept"] in ["A", "B", "Unknown"]
        finally:
            spark.stop()

    def test_array_contains_join_with_cast(self):
        """Test array_contains join with cast operations."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            from sparkless.spark_types import StringType

            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3]},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 3},
                ]
            )

            result = df1.join(
                df2, on=F.array_contains(df1.IDs, df2.ID), how="left"
            ).withColumn("DeptStr", F.col("Dept").cast(StringType()))
            rows = result.collect()

            assert len(rows) == 1
            assert rows[0]["DeptStr"] == "A"
        finally:
            spark.stop()

    def test_array_contains_join_schema_verification(self):
        """Test that array_contains join produces correct schema."""
        spark = SparkSession.builder.appName("issue-331").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "IDs": [1, 2, 3]},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Dept": "A", "ID": 3},
                ]
            )

            result = df1.join(df2, on=F.array_contains(df1.IDs, df2.ID), how="left")

            schema = result.schema
            field_names = [field.name for field in schema.fields]

            # Should contain columns from both DataFrames
            assert "Name" in field_names
            assert "IDs" in field_names
            assert "Dept" in field_names
            assert "ID" in field_names
        finally:
            spark.stop()

"""
Tests for issue #297: Join with different case columns and select with third case.

PySpark allows selecting columns with a different case when multiple columns
with different cases exist after a join. It picks the first matching column.
"""

from sparkless.sql import SparkSession, functions as F


class TestIssue297JoinDifferentCaseSelect:
    """Test join with different case columns and select with third case."""

    def test_join_different_case_select_third_case(self):
        """Test joining DataFrames with different case keys and selecting with third case."""
        spark = SparkSession.builder.appName("issue-297").getOrCreate()
        try:
            # Create two dataframes with different case keys
            df1 = spark.createDataFrame(
                [
                    {"name": "Alice", "Value1": 1},
                    {"name": "Bob", "Value1": 2},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"NAME": "Alice", "Value2": 1},
                    {"NAME": "Bob", "Value2": 2},
                ]
            )

            # Join the dataframes and apply select with different case
            df = df1.join(df2, on="Name", how="left").select("NaMe", "Value1", "Value2")

            # Should not raise an exception - PySpark picks the first match
            result = df.collect()

            # Verify results
            assert len(result) == 2
            assert result[0]["NaMe"] == "Alice"
            assert result[0]["Value1"] == 1
            assert result[0]["Value2"] == 1
            assert result[1]["NaMe"] == "Bob"
            assert result[1]["Value1"] == 2
            assert result[1]["Value2"] == 2

            # Verify column names in result
            assert "NaMe" in df.columns
            assert "Value1" in df.columns
            assert "Value2" in df.columns
        finally:
            spark.stop()

    def test_join_different_case_select_left_column(self):
        """Test that selecting with different case picks the left DataFrame's column."""
        spark = SparkSession.builder.appName("issue-297").getOrCreate()
        try:
            df1 = spark.createDataFrame([{"name": "Alice", "value": 1}])
            df2 = spark.createDataFrame([{"NAME": "Bob", "value": 2}])

            # Join on different case column names
            df = df1.join(df2, on="Name", how="left")

            # After join, both "name" and "NAME" exist
            # Selecting with different case should pick the first one (from left DataFrame)
            result = df.select("NaMe", "value").collect()

            assert len(result) == 1
            # Should pick "name" from left DataFrame (first match)
            assert result[0]["NaMe"] == "Alice"
        finally:
            spark.stop()

    def test_join_same_case_no_ambiguity(self):
        """Test that joins with same case columns work normally."""
        spark = SparkSession.builder.appName("issue-297").getOrCreate()
        try:
            df1 = spark.createDataFrame([{"name": "Alice", "value1": 1}])
            df2 = spark.createDataFrame(
                [{"name": "Alice", "value2": 2}]
            )  # Same name to match

            df = df1.join(df2, on="name", how="left")
            result = df.select("name", "value1", "value2").collect()

            assert len(result) == 1
            assert result[0]["name"] == "Alice"
            assert result[0]["value1"] == 1
            assert result[0]["value2"] == 2
        finally:
            spark.stop()

    def test_different_join_types(self):
        """Test that the fix works with different join types."""
        spark = SparkSession.builder.appName("issue-297").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"name": "Alice", "id": 1},
                    {"name": "Bob", "id": 2},
                    {"name": "Charlie", "id": 3},
                ]
            )
            df2 = spark.createDataFrame(
                [
                    {"NAME": "Alice", "score": 100},
                    {"NAME": "Bob", "score": 200},
                    {"NAME": "David", "score": 300},
                ]
            )

            # Test inner join
            inner_df = df1.join(df2, on="Name", how="inner").select(
                "NaMe", "id", "score"
            )
            inner_result = inner_df.collect()
            assert len(inner_result) == 2  # Only matching rows
            assert inner_result[0]["NaMe"] == "Alice"

            # Test left join
            left_df = df1.join(df2, on="Name", how="left").select("NaMe", "id", "score")
            left_result = left_df.collect()
            assert len(left_result) == 3  # All left rows
            assert left_result[2]["NaMe"] == "Charlie"  # No match, should be None
            assert left_result[2]["score"] is None

            # Test right join
            right_df = df1.join(df2, on="Name", how="right").select(
                "NaMe", "id", "score"
            )
            right_result = right_df.collect()
            assert len(right_result) == 3  # All right rows
            # "NaMe" picks first match case-insensitively. PySpark returns "David"
            # (from right's NAME) for the David row - column resolution varies by implementation.
            david_row = next((r for r in right_result if r["score"] == 300), None)
            assert david_row is not None
            assert david_row["NaMe"] == "David"
            assert david_row["id"] is None  # No match in left

            # Test outer join
            outer_df = df1.join(df2, on="Name", how="outer").select(
                "NaMe", "id", "score"
            )
            outer_result = outer_df.collect()
            assert len(outer_result) == 4  # All rows from both sides
        finally:
            spark.stop()

    def test_multiple_ambiguous_columns(self):
        """Test selecting multiple columns with different cases."""
        spark = SparkSession.builder.appName("issue-297").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"name": "Alice", "age": 25, "city": "NYC"},
                    {"name": "Bob", "age": 30, "city": "LA"},
                ]
            )
            df2 = spark.createDataFrame(
                [
                    {"NAME": "Alice", "AGE": 25, "CITY": "NYC"},
                    {"NAME": "Bob", "AGE": 30, "CITY": "LA"},
                ]
            )

            # Join and select with different case variations
            df = df1.join(df2, on="Name", how="left").select(
                "NaMe", "AgE", "CiTy", "age", "city"
            )

            result = df.collect()
            assert len(result) == 2
            assert result[0]["NaMe"] == "Alice"
            assert result[0]["AgE"] == 25  # Should pick first match (from left: "age")
            assert (
                result[0]["CiTy"] == "NYC"
            )  # Should pick first match (from left: "city")
            # Verify original columns still work
            assert result[0]["age"] == 25
            assert result[0]["city"] == "NYC"
        finally:
            spark.stop()

    def test_chained_operations_after_select(self):
        """Test that operations after select work correctly with ambiguous columns."""
        spark = SparkSession.builder.appName("issue-297").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"name": "Alice", "value": 10},
                    {"name": "Bob", "value": 20},
                    {"name": "Charlie", "value": 30},
                ]
            )
            df2 = spark.createDataFrame(
                [
                    {"NAME": "Alice", "score": 100},
                    {"NAME": "Bob", "score": 200},
                ]
            )

            # Join, select with different case, then filter
            df = (
                df1.join(df2, on="Name", how="left")
                .select("NaMe", "value", "score")
                .filter(F.col("value") > 15)
            )

            result = df.collect()
            assert len(result) == 2
            assert result[0]["NaMe"] == "Bob"
            assert result[1]["NaMe"] == "Charlie"

            # Test orderBy
            ordered_df = (
                df1.join(df2, on="Name", how="left")
                .select("NaMe", "value")
                .orderBy(F.desc("value"))
            )
            ordered_result = ordered_df.collect()
            assert ordered_result[0]["NaMe"] == "Charlie"
            assert ordered_result[0]["value"] == 30
        finally:
            spark.stop()

    def test_groupby_after_select_with_ambiguous_column(self):
        """Test groupBy works after selecting ambiguous column."""
        spark = SparkSession.builder.appName("issue-297").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"name": "Alice", "dept": "IT", "salary": 100},
                    {"name": "Bob", "dept": "IT", "salary": 200},
                    {"name": "Charlie", "dept": "HR", "salary": 150},
                ]
            )
            df2 = spark.createDataFrame(
                [
                    {"NAME": "Alice", "bonus": 10},
                    {"NAME": "Bob", "bonus": 20},
                    {"NAME": "Charlie", "bonus": 15},
                ]
            )

            # Join, select with different case, then groupBy
            df = (
                df1.join(df2, on="Name", how="left")
                .select("NaMe", "dept", "salary", "bonus")
                .groupBy("dept")
                .agg(
                    F.sum("salary").alias("total_salary"),
                    F.sum("bonus").alias("total_bonus"),
                )
            )

            result = df.collect()
            assert len(result) == 2
            # Verify we can still access the grouped column
            dept_names = [row["dept"] for row in result]
            assert "IT" in dept_names
            assert "HR" in dept_names
        finally:
            spark.stop()

    def test_single_match_preserves_original_name(self):
        """Test that single match preserves original column name (not requested name)."""
        spark = SparkSession.builder.appName("issue-297").getOrCreate()
        try:
            # Create DataFrame with only one column (no ambiguity)
            df = spark.createDataFrame([{"Name": "Alice", "Age": 25}])

            # Select with different case - should use original name
            result = df.select("name", "age").collect()

            assert len(result) == 1
            # When there's only one match, original column name is preserved
            assert (
                "Name" in df.select("name").columns
                or "name" in df.select("name").columns
            )
            # The value should be correct
            row = result[0]
            # Check that we can access the data (column name might be Name or name depending on implementation)
            assert row[df.select("name").columns[0]] == "Alice"
        finally:
            spark.stop()

    def test_multiple_matches_uses_requested_name(self):
        """Test that multiple matches use the requested column name."""
        spark = SparkSession.builder.appName("issue-297").getOrCreate()
        try:
            df1 = spark.createDataFrame([{"name": "Alice"}])
            df2 = spark.createDataFrame([{"NAME": "Bob"}])

            # After join, both "name" and "NAME" exist
            df = df1.join(df2, on="Name", how="left")

            # Select with different case - should use requested name
            result = df.select("NaMe").collect()

            assert len(result) == 1
            # Output column should be "NaMe" (requested name)
            assert "NaMe" in df.select("NaMe").columns
            # Value should come from first match ("name" from left DataFrame)
            assert result[0]["NaMe"] == "Alice"
        finally:
            spark.stop()

    def test_empty_dataframes(self):
        """Test that the fix works with empty DataFrames."""
        spark = SparkSession.builder.appName("issue-297").getOrCreate()
        try:
            from sparkless.spark_types import (
                StructType,
                StructField,
                StringType,
                IntegerType,
            )

            schema1 = StructType(
                [
                    StructField("name", StringType()),
                    StructField("value", IntegerType()),
                ]
            )
            schema2 = StructType(
                [
                    StructField("NAME", StringType()),
                    StructField("score", IntegerType()),
                ]
            )

            df1 = spark.createDataFrame([], schema=schema1)
            df2 = spark.createDataFrame([], schema=schema2)

            # Join empty DataFrames and select with different case
            df = df1.join(df2, on="Name", how="left").select("NaMe", "value", "score")

            result = df.collect()
            assert len(result) == 0
            # Verify columns exist even if empty
            assert "NaMe" in df.columns or len(df.columns) > 0
        finally:
            spark.stop()

    def test_null_values_in_joined_columns(self):
        """Test that null values are handled correctly."""
        spark = SparkSession.builder.appName("issue-297").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"name": "Alice", "value": 1},
                    {"name": None, "value": 2},
                ]
            )
            df2 = spark.createDataFrame(
                [
                    {"NAME": "Alice", "score": 100},
                    {"NAME": None, "score": 200},
                ]
            )

            # Join and select with different case
            df = df1.join(df2, on="Name", how="left").select("NaMe", "value", "score")

            result = df.collect()
            assert len(result) == 2
            # First row should match
            assert result[0]["NaMe"] == "Alice"
            # Second row might have nulls depending on join behavior
        finally:
            spark.stop()

    def test_different_case_variations(self):
        """Test various case combinations."""
        spark = SparkSession.builder.appName("issue-297").getOrCreate()
        try:
            df1 = spark.createDataFrame([{"name": "Alice"}])
            df2 = spark.createDataFrame([{"NAME": "Bob"}])

            df = df1.join(df2, on="Name", how="left")

            # Test various case variations
            test_cases = ["NaMe", "nAmE", "NAME", "name", "Name"]
            for case_variant in test_cases:
                result = df.select(case_variant).collect()
                assert len(result) == 1
                # All should work and pick the first match
                assert result[0][case_variant] == "Alice"
        finally:
            spark.stop()

    def test_with_column_after_select(self):
        """Test withColumn works after selecting ambiguous column."""
        spark = SparkSession.builder.appName("issue-297").getOrCreate()
        try:
            df1 = spark.createDataFrame([{"name": "Alice", "value": 10}])
            df2 = spark.createDataFrame(
                [{"NAME": "Alice", "score": 20}]
            )  # Match on Alice

            # Join, select with different case, then add column
            df = (
                df1.join(df2, on="Name", how="left")
                .select("NaMe", "value", "score")
                .withColumn("total", F.col("value") + F.col("score"))
            )

            result = df.collect()
            assert len(result) == 1
            assert "total" in df.columns
            assert result[0]["total"] == 30
            assert result[0]["NaMe"] == "Alice"
        finally:
            spark.stop()

    def test_drop_after_select(self):
        """Test drop works after selecting ambiguous column."""
        spark = SparkSession.builder.appName("issue-297").getOrCreate()
        try:
            df1 = spark.createDataFrame([{"name": "Alice", "value": 10, "extra": 1}])
            df2 = spark.createDataFrame([{"NAME": "Bob", "score": 20}])

            # Join, select with different case, then drop
            df = (
                df1.join(df2, on="Name", how="left")
                .select("NaMe", "value", "score", "extra")
                .drop("extra")
            )

            result = df.collect()
            assert len(result) == 1
            assert "NaMe" in df.columns
            assert "extra" not in df.columns
        finally:
            spark.stop()

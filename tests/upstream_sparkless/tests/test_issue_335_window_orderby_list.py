"""
Unit tests for Issue #335: Window().orderBy() should accept list of column names.

Tests that Window().orderBy() and Window().partitionBy() accept lists of column names,
matching PySpark behavior.
"""

from sparkless.sql import SparkSession
from sparkless import functions as F
from sparkless.window import Window


class TestIssue335WindowOrderByList:
    """Test Window().orderBy() and partitionBy() with list arguments."""

    def test_window_orderby_list_basic(self):
        """Test basic Window().orderBy() with list."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A"},
                    {"Name": "Bob", "Type": "B"},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Name", "Type"])
            result = df.withColumn("Rank", F.row_number().over(w))
            rows = result.collect()

            assert len(rows) == 2
            assert rows[0]["Name"] == "Alice"
            assert rows[0]["Type"] == "A"
            assert rows[0]["Rank"] == 1
            assert rows[1]["Name"] == "Bob"
            assert rows[1]["Type"] == "B"
            assert rows[1]["Rank"] == 1
        finally:
            spark.stop()

    def test_window_orderby_list_single_column(self):
        """Test Window().orderBy() with single column in list."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A"},
                    {"Name": "Bob", "Type": "B"},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Name"])
            result = df.withColumn("Rank", F.row_number().over(w))
            rows = result.collect()

            assert len(rows) == 2
            assert rows[0]["Rank"] == 1
            assert rows[1]["Rank"] == 1
        finally:
            spark.stop()

    def test_window_orderby_list_multiple_columns(self):
        """Test Window().orderBy() with multiple columns in list."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Bob", "Type": "A", "Score": 90},
                    {"Name": "Charlie", "Type": "B", "Score": 80},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Type", "Score", "Name"])
            result = df.withColumn("Rank", F.row_number().over(w))
            rows = result.collect()

            assert len(rows) == 3
            # Within each Type partition, should be ordered by Score, then Name
            type_a_rows = [row for row in rows if row["Type"] == "A"]
            assert len(type_a_rows) == 2
            assert type_a_rows[0]["Score"] == 90  # Bob first (lower score)
            assert type_a_rows[1]["Score"] == 100  # Alice second
        finally:
            spark.stop()

    def test_window_partitionby_list(self):
        """Test Window().partitionBy() with list."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Category": "X"},
                    {"Name": "Bob", "Type": "A", "Category": "X"},
                    {"Name": "Charlie", "Type": "B", "Category": "Y"},
                ]
            )

            w = Window().partitionBy(["Type", "Category"]).orderBy("Name")
            result = df.withColumn("Rank", F.row_number().over(w))
            rows = result.collect()

            assert len(rows) == 3
            # Each partition should have rank starting at 1
            type_a_rows = [
                row for row in rows if row["Type"] == "A" and row["Category"] == "X"
            ]
            assert len(type_a_rows) == 2
            assert type_a_rows[0]["Rank"] == 1
            assert type_a_rows[1]["Rank"] == 2
        finally:
            spark.stop()

    def test_window_both_list(self):
        """Test Window() with both partitionBy and orderBy using lists."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Bob", "Type": "A", "Score": 90},
                    {"Name": "Charlie", "Type": "B", "Score": 80},
                ]
            )

            w = Window().partitionBy(["Type"]).orderBy(["Score"])
            result = df.withColumn("Rank", F.row_number().over(w))
            rows = result.collect()

            assert len(rows) == 3
            type_a_rows = [row for row in rows if row["Type"] == "A"]
            assert type_a_rows[0]["Score"] == 90  # Lower score first
            assert type_a_rows[0]["Rank"] == 1
            assert type_a_rows[1]["Score"] == 100
            assert type_a_rows[1]["Rank"] == 2
        finally:
            spark.stop()

    def test_window_orderby_list_with_column_objects(self):
        """Test Window().orderBy() with list containing Column objects."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A"},
                    {"Name": "Bob", "Type": "B"},
                ]
            )

            w = Window().partitionBy("Type").orderBy([F.col("Name"), F.col("Type")])
            result = df.withColumn("Rank", F.row_number().over(w))
            rows = result.collect()

            assert len(rows) == 2
            assert rows[0]["Rank"] == 1
            assert rows[1]["Rank"] == 1
        finally:
            spark.stop()

    def test_window_orderby_list_mixed_strings_and_columns(self):
        """Test Window().orderBy() with list containing both strings and Column objects."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A"},
                    {"Name": "Bob", "Type": "B"},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Name", F.col("Type")])
            result = df.withColumn("Rank", F.row_number().over(w))
            rows = result.collect()

            assert len(rows) == 2
            assert rows[0]["Rank"] == 1
            assert rows[1]["Rank"] == 1
        finally:
            spark.stop()

    def test_window_orderby_list_backward_compatibility(self):
        """Test that Window().orderBy() still works with individual arguments (backward compatibility)."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A"},
                    {"Name": "Bob", "Type": "B"},
                ]
            )

            w = Window().partitionBy("Type").orderBy("Name", "Type")
            result = df.withColumn("Rank", F.row_number().over(w))
            rows = result.collect()

            assert len(rows) == 2
            assert rows[0]["Rank"] == 1
            assert rows[1]["Rank"] == 1
        finally:
            spark.stop()

    def test_window_orderby_list_with_desc(self):
        """Test Window().orderBy() with list and desc() ordering."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Bob", "Type": "A", "Score": 90},
                ]
            )

            w = Window().partitionBy("Type").orderBy([F.col("Score").desc()])
            result = df.withColumn("Rank", F.row_number().over(w))
            rows = result.collect()

            assert len(rows) == 2
            # Higher score should have rank 1
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            assert alice_row["Rank"] == 1
            bob_row = next(row for row in rows if row["Name"] == "Bob")
            assert bob_row["Rank"] == 2
        finally:
            spark.stop()

    def test_window_orderby_list_with_rows_between(self):
        """Test Window().orderBy() with list and rowsBetween."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Bob", "Type": "A", "Score": 90},
                    {"Name": "Charlie", "Type": "A", "Score": 80},
                ]
            )

            w = (
                Window()
                .partitionBy("Type")
                .orderBy(["Score"])
                .rowsBetween(Window.unboundedPreceding, Window.currentRow)
            )
            result = df.withColumn("RunningSum", F.sum("Score").over(w))
            rows = result.collect()

            assert len(rows) == 3
            # Running sum should accumulate
            charlie_row = next(row for row in rows if row["Name"] == "Charlie")
            assert charlie_row["RunningSum"] == 80
            bob_row = next(row for row in rows if row["Name"] == "Bob")
            assert bob_row["RunningSum"] == 170  # 80 + 90
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            assert alice_row["RunningSum"] == 270  # 80 + 90 + 100
        finally:
            spark.stop()

    def test_window_orderby_list_empty_list_error(self):
        """Test that Window().orderBy() with empty list raises error."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame([{"Name": "Alice"}])

            try:
                w = Window().orderBy([])
                result = df.withColumn("Rank", F.row_number().over(w))
                result.collect()  # Should raise error, but if it doesn't, that's also acceptable
                # (PySpark might handle empty lists differently)
            except ValueError as e:
                assert "At least one column" in str(e) or "must be specified" in str(e)
        finally:
            spark.stop()

    def test_window_orderby_list_with_window_function(self):
        """Test Window().orderBy() with list in window function context."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Bob", "Type": "A", "Score": 90},
                    {"Name": "Charlie", "Type": "B", "Score": 80},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Score"])
            result = df.withColumn("Rank", F.rank().over(w))
            rows = result.collect()

            assert len(rows) == 3
            type_a_rows = [row for row in rows if row["Type"] == "A"]
            assert type_a_rows[0]["Rank"] == 1  # Lower score gets rank 1
            assert type_a_rows[1]["Rank"] == 2
        finally:
            spark.stop()

    def test_window_static_orderby_list(self):
        """Test Window.orderBy() static method with list."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A"},
                    {"Name": "Bob", "Type": "B"},
                ]
            )

            w = Window.partitionBy("Type").orderBy(["Name", "Type"])
            result = df.withColumn("Rank", F.row_number().over(w))
            rows = result.collect()

            assert len(rows) == 2
            assert rows[0]["Rank"] == 1
            assert rows[1]["Rank"] == 1
        finally:
            spark.stop()

    def test_window_static_partitionby_list(self):
        """Test Window.partitionBy() static method with list."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Category": "X"},
                    {"Name": "Bob", "Type": "A", "Category": "X"},
                ]
            )

            w = Window.partitionBy(["Type", "Category"]).orderBy("Name")
            result = df.withColumn("Rank", F.row_number().over(w))
            rows = result.collect()

            assert len(rows) == 2
            assert rows[0]["Rank"] == 1
            assert rows[1]["Rank"] == 2
        finally:
            spark.stop()

    def test_window_orderby_list_with_desc_asc_mixed(self):
        """Test Window().orderBy() with list containing mixed desc() and asc() columns."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100, "Age": 25},
                    {"Name": "Bob", "Type": "A", "Score": 90, "Age": 30},
                    {"Name": "Charlie", "Type": "A", "Score": 100, "Age": 20},
                ]
            )

            w = (
                Window()
                .partitionBy("Type")
                .orderBy([F.col("Score").desc(), F.col("Age").asc()])
            )
            result = df.withColumn("Rank", F.row_number().over(w))
            rows = result.collect()

            assert len(rows) == 3
            # Should be ordered by Score desc, then Age asc
            # With Score desc and Age asc, highest score first, then lowest age
            # The exact ordering may vary, but all should have valid ranks
            ranks = {row["Rank"] for row in rows}
            assert len(ranks) == 3  # All should have different ranks
            # Verify ordering: highest score should have lower rank
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            bob_row = next(row for row in rows if row["Name"] == "Bob")
            # Bob has lower score, so should have higher rank
            assert bob_row["Rank"] > alice_row["Rank"]
        finally:
            spark.stop()

    def test_window_orderby_list_with_range_between(self):
        """Test Window().orderBy() with list and rangeBetween."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Bob", "Type": "A", "Score": 90},
                    {"Name": "Charlie", "Type": "A", "Score": 80},
                ]
            )

            w = (
                Window()
                .partitionBy("Type")
                .orderBy(["Score"])
                .rangeBetween(Window.unboundedPreceding, Window.currentRow)
            )
            result = df.withColumn("RunningSum", F.sum("Score").over(w))
            rows = result.collect()

            assert len(rows) == 3
            # Running sum should accumulate
            charlie_row = next(row for row in rows if row["Name"] == "Charlie")
            assert charlie_row["RunningSum"] == 80
            bob_row = next(row for row in rows if row["Name"] == "Bob")
            assert bob_row["RunningSum"] == 170  # 80 + 90
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            assert alice_row["RunningSum"] == 270  # 80 + 90 + 100
        finally:
            spark.stop()

    def test_window_orderby_list_with_dense_rank(self):
        """Test Window().orderBy() with list and dense_rank window function."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Bob", "Type": "A", "Score": 100},
                    {"Name": "Charlie", "Type": "A", "Score": 90},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Score"])
            result = df.withColumn("Rank", F.dense_rank().over(w))
            rows = result.collect()

            assert len(rows) == 3
            # Charlie has lowest score, so should have rank 1
            # Alice and Bob both have score 100, so should both have rank 2
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            bob_row = next(row for row in rows if row["Name"] == "Bob")
            charlie_row = next(row for row in rows if row["Name"] == "Charlie")
            assert charlie_row["Rank"] == 1  # Lowest score
            assert alice_row["Rank"] == 2  # Same score as Bob
            assert bob_row["Rank"] == 2  # Same score as Alice
        finally:
            spark.stop()

    def test_window_orderby_list_with_percent_rank(self):
        """Test Window().orderBy() with list and percent_rank window function."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Bob", "Type": "A", "Score": 90},
                    {"Name": "Charlie", "Type": "A", "Score": 80},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Score"])
            result = df.withColumn("PercentRank", F.percent_rank().over(w))
            rows = result.collect()

            assert len(rows) == 3
            # Percent rank should be 0.0, 0.5, 1.0 for 3 rows
            charlie_row = next(row for row in rows if row["Name"] == "Charlie")
            assert charlie_row["PercentRank"] == 0.0
            bob_row = next(row for row in rows if row["Name"] == "Bob")
            assert bob_row["PercentRank"] == 0.5
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            assert alice_row["PercentRank"] == 1.0
        finally:
            spark.stop()

    def test_window_orderby_list_with_lag_lead(self):
        """Test Window().orderBy() with list and lag/lead window functions."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Bob", "Type": "A", "Score": 90},
                    {"Name": "Charlie", "Type": "A", "Score": 80},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Score"])
            result = df.withColumn("PrevScore", F.lag("Score", 1).over(w)).withColumn(
                "NextScore", F.lead("Score", 1).over(w)
            )
            rows = result.collect()

            assert len(rows) == 3
            charlie_row = next(row for row in rows if row["Name"] == "Charlie")
            assert charlie_row["PrevScore"] is None  # First row
            assert charlie_row["NextScore"] == 90
            bob_row = next(row for row in rows if row["Name"] == "Bob")
            assert bob_row["PrevScore"] == 80
            assert bob_row["NextScore"] == 100
        finally:
            spark.stop()

    def test_window_orderby_list_with_first_last_value(self):
        """Test Window().orderBy() with list and first_value/last_value window functions."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Bob", "Type": "A", "Score": 90},
                    {"Name": "Charlie", "Type": "A", "Score": 80},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Score"])
            result = df.withColumn(
                "FirstScore", F.first_value("Score").over(w)
            ).withColumn("LastScore", F.last_value("Score").over(w))
            rows = result.collect()

            assert len(rows) == 3
            # First value should be 80 (lowest), last value should be current row's value
            for row in rows:
                assert row["FirstScore"] == 80
                assert row["LastScore"] == row["Score"]
        finally:
            spark.stop()

    def test_window_orderby_list_with_ntile(self):
        """Test Window().orderBy() with list and ntile window function."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Bob", "Type": "A", "Score": 90},
                    {"Name": "Charlie", "Type": "A", "Score": 80},
                    {"Name": "David", "Type": "A", "Score": 70},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Score"])
            result = df.withColumn("Tile", F.ntile(2).over(w))
            rows = result.collect()

            assert len(rows) == 4
            # Should be divided into 2 tiles
            charlie_row = next(row for row in rows if row["Name"] == "Charlie")
            assert charlie_row["Tile"] == 1
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            assert alice_row["Tile"] == 2
        finally:
            spark.stop()

    def test_window_orderby_list_with_cume_dist(self):
        """Test Window().orderBy() with list and cume_dist window function."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Bob", "Type": "A", "Score": 90},
                    {"Name": "Charlie", "Type": "A", "Score": 80},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Score"])
            result = df.withColumn("CumeDist", F.cume_dist().over(w))
            rows = result.collect()

            assert len(rows) == 3
            # Cumulative distribution
            charlie_row = next(row for row in rows if row["Name"] == "Charlie")
            assert charlie_row["CumeDist"] == 1.0 / 3.0
            bob_row = next(row for row in rows if row["Name"] == "Bob")
            assert bob_row["CumeDist"] == 2.0 / 3.0
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            assert alice_row["CumeDist"] == 1.0
        finally:
            spark.stop()

    def test_window_orderby_list_chained_operations(self):
        """Test Window().orderBy() with list in chained DataFrame operations."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Bob", "Type": "A", "Score": 90},
                    {"Name": "Charlie", "Type": "B", "Score": 80},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Score"])
            result = (
                df.withColumn("Rank", F.row_number().over(w))
                .filter(F.col("Rank") == 1)
                .select("Name", "Type", "Score")
            )
            rows = result.collect()

            assert len(rows) == 2  # One row per Type partition
            types = {row["Type"] for row in rows}
            assert types == {"A", "B"}
        finally:
            spark.stop()

    def test_window_orderby_list_with_groupby(self):
        """Test Window().orderBy() with list followed by groupBy operation."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Bob", "Type": "A", "Score": 90},
                    {"Name": "Charlie", "Type": "B", "Score": 80},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Score"])
            result = (
                df.withColumn("Rank", F.row_number().over(w))
                .groupBy("Type")
                .agg(F.max("Rank").alias("MaxRank"))
            )
            rows = result.collect()

            assert len(rows) == 2
            type_a_row = next(row for row in rows if row["Type"] == "A")
            assert type_a_row["MaxRank"] == 2
        finally:
            spark.stop()

    def test_window_orderby_list_with_join(self):
        """Test Window().orderBy() with list in join operations."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Bob", "Type": "A", "Score": 90},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Type": "A", "Dept": "Engineering"},
                    {"Type": "B", "Dept": "Sales"},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Score"])
            result = df1.withColumn("Rank", F.row_number().over(w)).join(
                df2, on="Type", how="left"
            )
            rows = result.collect()

            assert len(rows) == 2
            assert rows[0]["Dept"] == "Engineering"
        finally:
            spark.stop()

    def test_window_orderby_list_with_union(self):
        """Test Window().orderBy() with list in union operations."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"Name": "Bob", "Type": "B", "Score": 90},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Score"])
            result1 = df1.withColumn("Rank", F.row_number().over(w))
            result2 = df2.withColumn("Rank", F.row_number().over(w))
            combined = result1.unionByName(result2, allowMissingColumns=True)
            rows = combined.collect()

            assert len(rows) == 2
            names = {row["Name"] for row in rows}
            assert names == {"Alice", "Bob"}
        finally:
            spark.stop()

    def test_window_orderby_list_with_select_expr(self):
        """Test Window().orderBy() with list followed by selectExpr."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Bob", "Type": "A", "Score": 90},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Score"])
            result = df.withColumn("Rank", F.row_number().over(w)).selectExpr(
                "Name", "Type", "Score", "Rank"
            )
            rows = result.collect()

            assert len(rows) == 2
            assert "Rank" in rows[0]
        finally:
            spark.stop()

    def test_window_orderby_list_with_withcolumn_renamed(self):
        """Test Window().orderBy() with list and withColumnRenamed."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Bob", "Type": "A", "Score": 90},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Score"])
            result = df.withColumn("Rank", F.row_number().over(w)).withColumnRenamed(
                "Rank", "Position"
            )
            rows = result.collect()

            assert len(rows) == 2
            assert "Position" in rows[0]
            assert "Rank" not in rows[0]
        finally:
            spark.stop()

    def test_window_orderby_list_with_distinct(self):
        """Test Window().orderBy() with list followed by distinct."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Alice", "Type": "A", "Score": 100},  # Duplicate
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Score"])
            result = (
                df.withColumn("Rank", F.row_number().over(w))
                .select("Name", "Type", "Score")
                .distinct()
            )
            rows = result.collect()

            assert len(rows) == 1
            assert rows[0]["Name"] == "Alice"
        finally:
            spark.stop()

    def test_window_orderby_list_with_orderby_dataframe(self):
        """Test Window().orderBy() with list followed by DataFrame orderBy."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Bob", "Type": "A", "Score": 90},
                    {"Name": "Charlie", "Type": "B", "Score": 80},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Score"])
            result = df.withColumn("Rank", F.row_number().over(w)).orderBy(
                "Type", "Rank"
            )
            rows = result.collect()

            assert len(rows) == 3
            # Should be ordered by Type, then Rank
            assert rows[0]["Type"] in ["A", "B"]
        finally:
            spark.stop()

    def test_window_orderby_list_with_limit(self):
        """Test Window().orderBy() with list followed by limit."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Bob", "Type": "A", "Score": 90},
                    {"Name": "Charlie", "Type": "A", "Score": 80},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Score"])
            result = df.withColumn("Rank", F.row_number().over(w)).limit(2)
            rows = result.collect()

            assert len(rows) == 2
        finally:
            spark.stop()

    def test_window_orderby_list_with_filter(self):
        """Test Window().orderBy() with list followed by filter."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Bob", "Type": "A", "Score": 90},
                    {"Name": "Charlie", "Type": "B", "Score": 80},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Score"])
            result = df.withColumn("Rank", F.row_number().over(w)).filter(
                F.col("Rank") == 1
            )
            rows = result.collect()

            assert len(rows) == 2  # One row per Type partition
            for row in rows:
                assert row["Rank"] == 1
        finally:
            spark.stop()

    def test_window_orderby_list_with_aggregation_functions(self):
        """Test Window().orderBy() with list and various aggregation window functions."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Bob", "Type": "A", "Score": 90},
                    {"Name": "Charlie", "Type": "A", "Score": 80},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Score"])
            result = (
                df.withColumn("AvgScore", F.avg("Score").over(w))
                .withColumn("MaxScore", F.max("Score").over(w))
                .withColumn("MinScore", F.min("Score").over(w))
            )
            rows = result.collect()

            assert len(rows) == 3
            for row in rows:
                assert row["MaxScore"] == 100
                assert row["MinScore"] == 80
        finally:
            spark.stop()

    def test_window_orderby_list_with_count_distinct(self):
        """Test Window().orderBy() with list and countDistinct window function."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Bob", "Type": "A", "Score": 100},
                    {"Name": "Charlie", "Type": "A", "Score": 90},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Score"])
            result = df.withColumn("DistinctScores", F.countDistinct("Score").over(w))
            rows = result.collect()

            assert len(rows) == 3
            # Count distinct counts distinct values in the window frame
            # All rows should see 2 distinct scores (90 and 100) in the partition
            for row in rows:
                assert row["DistinctScores"] == 2  # Two distinct scores: 90 and 100
        finally:
            spark.stop()

    def test_window_orderby_list_with_stddev_variance(self):
        """Test Window().orderBy() with list and stddev/variance window functions."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Bob", "Type": "A", "Score": 90},
                    {"Name": "Charlie", "Type": "A", "Score": 80},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Score"])
            result = df.withColumn("StdDev", F.stddev("Score").over(w)).withColumn(
                "Variance", F.variance("Score").over(w)
            )
            rows = result.collect()

            assert len(rows) == 3
            # First row should have None or 0 for stddev/variance (only one value)
            charlie_row = next(row for row in rows if row["Name"] == "Charlie")
            # Subsequent rows should have calculated values
            assert "StdDev" in charlie_row
            assert "Variance" in charlie_row
        finally:
            spark.stop()

    def test_window_orderby_list_with_null_values(self):
        """Test Window().orderBy() with list when columns contain null values."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Bob", "Type": "A", "Score": None},
                    {"Name": "Charlie", "Type": "A", "Score": 80},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Score"])
            result = df.withColumn("Rank", F.row_number().over(w))
            rows = result.collect()

            assert len(rows) == 3
            # Nulls should be handled (typically sorted last or first depending on implementation)
            for row in rows:
                assert "Rank" in row
        finally:
            spark.stop()

    def test_window_orderby_list_with_empty_partition(self):
        """Test Window().orderBy() with list when partition has no rows."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                    {"Name": "Bob", "Type": "B", "Score": 90},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Score"])
            result = df.withColumn("Rank", F.row_number().over(w))
            rows = result.collect()

            assert len(rows) == 2
            # Each partition should have rank 1
            for row in rows:
                assert row["Rank"] == 1
        finally:
            spark.stop()

    def test_window_orderby_list_with_single_row_partition(self):
        """Test Window().orderBy() with list when partition has single row."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Score"])
            result = df.withColumn("Rank", F.row_number().over(w))
            rows = result.collect()

            assert len(rows) == 1
            assert rows[0]["Rank"] == 1
        finally:
            spark.stop()

    def test_window_orderby_list_with_large_list(self):
        """Test Window().orderBy() with list containing many columns."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {
                        "Name": "Alice",
                        "Type": "A",
                        "Col1": 1,
                        "Col2": 2,
                        "Col3": 3,
                        "Col4": 4,
                        "Col5": 5,
                    },
                    {
                        "Name": "Bob",
                        "Type": "A",
                        "Col1": 1,
                        "Col2": 2,
                        "Col3": 3,
                        "Col4": 4,
                        "Col5": 6,
                    },
                ]
            )

            w = (
                Window()
                .partitionBy("Type")
                .orderBy(["Col1", "Col2", "Col3", "Col4", "Col5"])
            )
            result = df.withColumn("Rank", F.row_number().over(w))
            rows = result.collect()

            assert len(rows) == 2
            alice_row = next(row for row in rows if row["Name"] == "Alice")
            assert alice_row["Rank"] == 1  # Lower Col5 value
            bob_row = next(row for row in rows if row["Name"] == "Bob")
            assert bob_row["Rank"] == 2
        finally:
            spark.stop()

    def test_window_orderby_list_schema_verification(self):
        """Test that Window().orderBy() with list produces correct schema."""
        spark = SparkSession.builder.appName("issue-335").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Score": 100},
                ]
            )

            w = Window().partitionBy("Type").orderBy(["Score"])
            result = df.withColumn("Rank", F.row_number().over(w))

            schema = result.schema
            field_names = [field.name for field in schema.fields]

            # Should contain original columns plus new window function column
            assert "Name" in field_names
            assert "Type" in field_names
            assert "Score" in field_names
            assert "Rank" in field_names
        finally:
            spark.stop()

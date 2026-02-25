"""
PySpark parity tests for Issue #335: Window().orderBy() should accept list of column names.

These tests verify that Sparkless behavior matches PySpark behavior.
"""

from tests.fixtures.spark_imports import get_spark_imports


class TestWindowOrderByListParity:
    """PySpark parity tests for Window().orderBy() with list arguments."""

    def test_window_orderby_list_basic_parity(self):
        """Test basic Window().orderBy() with list matches PySpark."""
        spark_imports = get_spark_imports()
        SparkSession = spark_imports.SparkSession
        F = spark_imports.F
        Window = spark_imports.Window

        spark = SparkSession.builder.appName("window-orderby-list-parity").getOrCreate()
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

    def test_window_orderby_list_multiple_columns_parity(self):
        """Test Window().orderBy() with multiple columns in list matches PySpark."""
        spark_imports = get_spark_imports()
        SparkSession = spark_imports.SparkSession
        F = spark_imports.F
        Window = spark_imports.Window

        spark = SparkSession.builder.appName("window-orderby-list-parity").getOrCreate()
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

    def test_window_partitionby_list_parity(self):
        """Test Window().partitionBy() with list matches PySpark."""
        spark_imports = get_spark_imports()
        SparkSession = spark_imports.SparkSession
        F = spark_imports.F
        Window = spark_imports.Window

        spark = SparkSession.builder.appName("window-orderby-list-parity").getOrCreate()
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

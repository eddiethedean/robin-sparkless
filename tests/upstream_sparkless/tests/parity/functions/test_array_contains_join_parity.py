"""
PySpark parity tests for Issue #331: Join with array_contains() condition.

These tests verify that Sparkless behavior matches PySpark behavior.
"""

from tests.fixtures.spark_imports import get_spark_imports


class TestArrayContainsJoinParity:
    """PySpark parity tests for array_contains() as join condition."""

    def test_array_contains_join_basic_parity(self):
        """Test basic array_contains join matches PySpark."""
        spark_imports = get_spark_imports()
        SparkSession = spark_imports.SparkSession
        F = spark_imports.F

        spark = SparkSession.builder.appName("array-contains-join-parity").getOrCreate()
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

    def test_array_contains_join_multiple_matches_parity(self):
        """Test array_contains join with multiple matches matches PySpark."""
        spark_imports = get_spark_imports()
        SparkSession = spark_imports.SparkSession
        F = spark_imports.F

        spark = SparkSession.builder.appName("array-contains-join-parity").getOrCreate()
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

    def test_array_contains_join_left_parity(self):
        """Test array_contains join with left join type matches PySpark."""
        spark_imports = get_spark_imports()
        SparkSession = spark_imports.SparkSession
        F = spark_imports.F

        spark = SparkSession.builder.appName("array-contains-join-parity").getOrCreate()
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

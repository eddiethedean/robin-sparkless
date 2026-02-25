"""
PySpark parity tests for Issue #327: orderBy() with ascending parameter.

These tests verify that Sparkless behavior matches PySpark behavior.
"""

from tests.fixtures.spark_imports import get_spark_imports


class TestOrderByAscendingParity:
    """PySpark parity tests for orderBy() with ascending parameter."""

    def test_orderby_ascending_true_parity(self):
        """Test orderBy with ascending=True matches PySpark."""
        spark_imports = get_spark_imports()
        SparkSession = spark_imports.SparkSession

        spark = SparkSession.builder.appName("orderby-ascending-parity").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "StringValue": "AAA"},
                    {"Name": "Bob", "StringValue": "ZZZ"},
                    {"Name": "Charlie", "StringValue": "MMM"},
                ]
            )

            result = df.orderBy("StringValue", ascending=True)
            rows = result.collect()

            assert len(rows) == 3
            assert rows[0]["StringValue"] == "AAA"
            assert rows[1]["StringValue"] == "MMM"
            assert rows[2]["StringValue"] == "ZZZ"
        finally:
            spark.stop()

    def test_orderby_ascending_false_parity(self):
        """Test orderBy with ascending=False matches PySpark."""
        spark_imports = get_spark_imports()
        SparkSession = spark_imports.SparkSession

        spark = SparkSession.builder.appName("orderby-ascending-parity").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "StringValue": "AAA"},
                    {"Name": "Bob", "StringValue": "ZZZ"},
                    {"Name": "Charlie", "StringValue": "MMM"},
                ]
            )

            result = df.orderBy("StringValue", ascending=False)
            rows = result.collect()

            assert len(rows) == 3
            assert rows[0]["StringValue"] == "ZZZ"
            assert rows[1]["StringValue"] == "MMM"
            assert rows[2]["StringValue"] == "AAA"
        finally:
            spark.stop()

    def test_sort_with_ascending_parity(self):
        """Test sort() with ascending parameter matches PySpark."""
        spark_imports = get_spark_imports()
        SparkSession = spark_imports.SparkSession

        spark = SparkSession.builder.appName("orderby-ascending-parity").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                    {"Name": "Charlie", "Value": 20},
                ]
            )

            result = df.sort("Value", ascending=False)
            rows = result.collect()

            assert len(rows) == 3
            assert rows[0]["Value"] == 20
            assert rows[1]["Value"] == 10
            assert rows[2]["Value"] == 5
        finally:
            spark.stop()

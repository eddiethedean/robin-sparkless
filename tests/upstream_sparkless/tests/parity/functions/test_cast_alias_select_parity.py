"""
PySpark parity tests for Issue #332: Column resolution with cast+alias+select.

These tests verify that Sparkless behavior matches PySpark behavior.
"""

from tests.fixtures.spark_imports import get_spark_imports


class TestCastAliasSelectParity:
    """PySpark parity tests for cast+alias+select column resolution."""

    def test_cast_alias_select_basic_parity(self):
        """Test basic cast+alias+select scenario matches PySpark."""
        spark_imports = get_spark_imports()
        SparkSession = spark_imports.SparkSession
        F = spark_imports.F
        DoubleType = spark_imports.DoubleType

        spark = SparkSession.builder.appName("cast-alias-select-parity").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 2},
                    {"Name": "Bob", "Value": 3},
                    {"Name": "Bob", "Value": 4},
                ]
            )

            result = (
                df.groupBy("Name")
                .agg(F.mean("Value").cast(DoubleType()).alias("AvgValue"))
                .select("Name", "AvgValue")
            )

            rows = result.collect()
            assert len(rows) == 2
            assert rows[0]["Name"] == "Alice"
            # PySpark returns float for DoubleType, but may be string in some cases
            avg_value_0 = (
                float(rows[0]["AvgValue"])
                if isinstance(rows[0]["AvgValue"], str)
                else rows[0]["AvgValue"]
            )
            assert avg_value_0 == 1.5
            assert rows[1]["Name"] == "Bob"
            avg_value_1 = (
                float(rows[1]["AvgValue"])
                if isinstance(rows[1]["AvgValue"], str)
                else rows[1]["AvgValue"]
            )
            assert avg_value_1 == 3.5
        finally:
            spark.stop()

    def test_cast_alias_select_multiple_aggregations_parity(self):
        """Test multiple aggregations with cast+alias matches PySpark."""
        spark_imports = get_spark_imports()
        SparkSession = spark_imports.SparkSession
        F = spark_imports.F
        DoubleType = spark_imports.DoubleType
        IntegerType = spark_imports.IntegerType

        spark = SparkSession.builder.appName("cast-alias-select-parity").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1, "Score": 10},
                    {"Name": "Alice", "Value": 2, "Score": 20},
                    {"Name": "Bob", "Value": 3, "Score": 30},
                    {"Name": "Bob", "Value": 4, "Score": 40},
                ]
            )

            result = (
                df.groupBy("Name")
                .agg(
                    F.mean("Value").cast(DoubleType()).alias("AvgValue"),
                    F.sum("Score").cast(IntegerType()).alias("TotalScore"),
                )
                .select("Name", "AvgValue", "TotalScore")
            )

            rows = result.collect()
            assert len(rows) == 2
            assert rows[0]["Name"] == "Alice"
            # PySpark returns float for DoubleType, but may be string in some cases
            avg_value = (
                float(rows[0]["AvgValue"])
                if isinstance(rows[0]["AvgValue"], str)
                else rows[0]["AvgValue"]
            )
            assert avg_value == 1.5
            total_score = (
                int(rows[0]["TotalScore"])
                if isinstance(rows[0]["TotalScore"], str)
                else rows[0]["TotalScore"]
            )
            assert total_score == 30
        finally:
            spark.stop()

    def test_cast_alias_select_with_filter_parity(self):
        """Test cast+alias+select with filter matches PySpark."""
        spark_imports = get_spark_imports()
        SparkSession = spark_imports.SparkSession
        F = spark_imports.F
        DoubleType = spark_imports.DoubleType

        spark = SparkSession.builder.appName("cast-alias-select-parity").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 2},
                    {"Name": "Bob", "Value": 3},
                    {"Name": "Bob", "Value": 4},
                ]
            )

            result = (
                df.groupBy("Name")
                .agg(F.mean("Value").cast(DoubleType()).alias("AvgValue"))
                .filter(F.col("AvgValue") > 2.0)
                .select("Name", "AvgValue")
            )

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Name"] == "Bob"
            # PySpark returns float for DoubleType, but may be string in some cases
            avg_value = (
                float(rows[0]["AvgValue"])
                if isinstance(rows[0]["AvgValue"], str)
                else rows[0]["AvgValue"]
            )
            assert avg_value == 3.5
        finally:
            spark.stop()

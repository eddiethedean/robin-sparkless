import sparkless.sql.functions as F
import sparkless.sql.types as T
from sparkless.sql import SparkSession


def test_join_with_unmaterialized_withcolumn_on_right_regression_281():
    spark = SparkSession.builder.appName("issue-281").getOrCreate()
    try:
        df1 = spark.createDataFrame(
            [
                {"Name": "Alice", "Period": "1", "Value1": "A"},
                {"Name": "Bob", "Period": "2", "Value1": "B"},
            ]
        )

        df2 = spark.createDataFrame(
            [
                {"Name": "Alice", "Period": "1", "Value2": "C"},
                {"Name": "Bob", "Period": "2", "Value2": "D"},
            ]
        )

        # Unmaterialized op on right side should not break join materialization.
        df2 = df2.withColumn("ExtraColumn", df2["Value2"])

        df = df1.join(df2, on=["Name", "Period"], how="left")
        rows = df.orderBy("Name", "Period").collect()

        assert [
            (r["Name"], r["Period"], r["Value1"], r["Value2"], r["ExtraColumn"])
            for r in rows
        ] == [
            ("Alice", "1", "A", "C", "C"),
            ("Bob", "2", "B", "D", "D"),
        ]
    finally:
        spark.stop()


def test_join_with_multiple_unmaterialized_ops_on_right():
    spark = SparkSession.builder.appName("issue-281-multi-ops").getOrCreate()
    try:
        df1 = spark.createDataFrame(
            [
                {"Name": "Alice", "Period": "1", "Value1": "A"},
                {"Name": "Bob", "Period": "2", "Value1": "B"},
            ]
        )

        df2 = spark.createDataFrame(
            [
                {"Name": "Alice", "Period": "1", "Value2": "C"},
                {"Name": "Bob", "Period": "2", "Value2": "D"},
            ]
        )

        # Multiple pending ops (the original failure mode was row -> dict conversion).
        df2 = (
            df2.withColumn("ExtraColumn", df2["Value2"])
            .withColumnRenamed("Value2", "Value2Renamed")
            .drop("ExtraColumn")
        )

        df = df1.join(df2, on=["Name", "Period"], how="left")
        rows = df.orderBy("Name", "Period").collect()

        assert [
            (r["Name"], r["Period"], r["Value1"], r["Value2Renamed"]) for r in rows
        ] == [
            ("Alice", "1", "A", "C"),
            ("Bob", "2", "B", "D"),
        ]
    finally:
        spark.stop()


def test_join_with_unmaterialized_select_filter_on_right():
    spark = SparkSession.builder.appName("issue-281-select-filter").getOrCreate()
    try:
        df1 = spark.createDataFrame(
            [
                {"Name": "Alice", "Period": "1", "Value1": "A"},
                {"Name": "Bob", "Period": "2", "Value1": "B"},
            ]
        )

        df2 = spark.createDataFrame(
            [
                {"Name": "Alice", "Period": "1", "Value2": "C"},
                {"Name": "Bob", "Period": "2", "Value2": "D"},
                {"Name": "Clare", "Period": "3", "Value2": "E"},
            ]
        )

        df2 = (
            df2.withColumn("ExtraColumn", df2["Value2"])
            .filter(F.col("Name") != F.lit("Clare"))
            .select("Name", "Period", "Value2", "ExtraColumn")
        )

        df = df1.join(df2, on=["Name", "Period"], how="left")
        rows = df.orderBy("Name", "Period").collect()

        assert [
            (r["Name"], r["Period"], r["Value1"], r["Value2"], r["ExtraColumn"])
            for r in rows
        ] == [
            ("Alice", "1", "A", "C", "C"),
            ("Bob", "2", "B", "D", "D"),
        ]
    finally:
        spark.stop()


def test_join_with_unmaterialized_ops_on_both_sides():
    spark = SparkSession.builder.appName("issue-281-both-sides").getOrCreate()
    try:
        df1 = spark.createDataFrame(
            [
                {"Name": "Alice", "Period": "1", "Value1": "A"},
                {"Name": "Bob", "Period": "2", "Value1": "B"},
            ]
        ).withColumn("LeftCopy", F.col("Value1"))

        df2 = spark.createDataFrame(
            [
                {"Name": "Alice", "Period": "1", "Value2": "C"},
                {"Name": "Bob", "Period": "2", "Value2": "D"},
            ]
        ).withColumn("RightCopy", F.col("Value2"))

        df = df1.join(df2, on=["Name", "Period"], how="left")
        rows = df.orderBy("Name", "Period").collect()

        assert [
            (
                r["Name"],
                r["Period"],
                r["Value1"],
                r["LeftCopy"],
                r["Value2"],
                r["RightCopy"],
            )
            for r in rows
        ] == [
            ("Alice", "1", "A", "A", "C", "C"),
            ("Bob", "2", "B", "B", "D", "D"),
        ]
    finally:
        spark.stop()


def test_join_with_unmaterialized_ops_on_right_and_empty_right_dataframe():
    spark = SparkSession.builder.appName("issue-281-empty-right").getOrCreate()
    try:
        df1 = spark.createDataFrame(
            [
                {"Name": "Alice", "Period": "1", "Value1": "A"},
                {"Name": "Bob", "Period": "2", "Value1": "B"},
            ]
        )

        schema = T.StructType(
            [
                T.StructField("Name", T.StringType()),
                T.StructField("Period", T.StringType()),
                T.StructField("Value2", T.StringType()),
            ]
        )
        df2 = spark.createDataFrame([], schema)

        # Pending op on an empty DF should still materialize cleanly for join.
        df2 = df2.withColumn("ExtraColumn", df2["Value2"])

        df = df1.join(df2, on=["Name", "Period"], how="left")
        rows = df.orderBy("Name", "Period").collect()

        assert [
            (r["Name"], r["Period"], r["Value1"], r["Value2"], r["ExtraColumn"])
            for r in rows
        ] == [
            ("Alice", "1", "A", None, None),
            ("Bob", "2", "B", None, None),
        ]
    finally:
        spark.stop()

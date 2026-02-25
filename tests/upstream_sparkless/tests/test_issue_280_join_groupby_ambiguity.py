"""
Comprehensive tests for issue #280: join then groupBy ambiguous column fix.

Tests verify that joins on column names (single or multiple) followed by
operations on those join keys work without ambiguous column errors.
"""

from sparkless.sql import SparkSession
import sparkless.sql.functions as F


class TestJoinThenGroupByNoAmbiguity:
    """Test join followed by groupBy on join keys - no ambiguity errors."""

    def test_basic_left_join_then_groupby(self):
        """Basic regression test: left join on multiple keys then groupBy."""
        spark = SparkSession.builder.appName("issue-280").getOrCreate()
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

            df = df1.join(df2, on=["Name", "Period"], how="left")
            out = df.groupBy(["Name", "Period"]).count().collect()

            got = {(r["Name"], r["Period"]): r["count"] for r in out}
            assert got == {("Alice", "1"): 1, ("Bob", "2"): 1}
        finally:
            spark.stop()

    def test_inner_join_then_groupby(self):
        """Test inner join followed by groupBy."""
        spark = SparkSession.builder.appName("issue-280").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"id": 1, "category": "A", "value": 10},
                    {"id": 2, "category": "B", "value": 20},
                    {"id": 3, "category": "A", "value": 30},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"id": 1, "category": "A", "extra": "X"},
                    {"id": 2, "category": "B", "extra": "Y"},
                ]
            )

            df = df1.join(df2, on=["id", "category"], how="inner")
            result = (
                df.groupBy(["id", "category"])
                .agg(F.sum("value").alias("total"))
                .collect()
            )

            result_dict = {(r["id"], r["category"]): r["total"] for r in result}
            assert result_dict == {(1, "A"): 10, (2, "B"): 20}
        finally:
            spark.stop()

    def test_right_join_then_groupby(self):
        """Test right join followed by groupBy."""
        spark = SparkSession.builder.appName("issue-280").getOrCreate()
        try:
            df1 = spark.createDataFrame([{"key": 1, "left_val": "L1"}])
            df2 = spark.createDataFrame(
                [
                    {"key": 1, "right_val": "R1"},
                    {"key": 2, "right_val": "R2"},
                ]
            )

            df = df1.join(df2, on="key", how="right")
            result = df.groupBy("key").count().collect()

            result_dict = {r["key"]: r["count"] for r in result}
            assert result_dict == {1: 1, 2: 1}
        finally:
            spark.stop()

    def test_outer_join_then_groupby(self):
        """Test outer join followed by groupBy."""
        spark = SparkSession.builder.appName("issue-280").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [{"key": 1, "left_val": "L1"}, {"key": 3, "left_val": "L3"}]
            )
            df2 = spark.createDataFrame(
                [{"key": 1, "right_val": "R1"}, {"key": 2, "right_val": "R2"}]
            )

            df = df1.join(df2, on="key", how="outer")
            result = df.groupBy("key").count().collect()

            result_dict = {r["key"]: r["count"] for r in result}
            # Outer join: matched keys (1, 3) and unmatched right rows get None for join key
            # (PySpark behavior: join key column comes from left side)
            assert result_dict == {1: 1, None: 1, 3: 1}
        finally:
            spark.stop()

    def test_single_column_join_then_groupby(self):
        """Test single column join (string) followed by groupBy."""
        spark = SparkSession.builder.appName("issue-280").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
            )
            df2 = spark.createDataFrame(
                [{"id": 1, "score": 100}, {"id": 2, "score": 200}]
            )

            df = df1.join(df2, on="id", how="left")
            result = df.groupBy("id").agg(F.avg("score").alias("avg_score")).collect()

            result_dict = {r["id"]: r["avg_score"] for r in result}
            assert result_dict == {1: 100.0, 2: 200.0}
        finally:
            spark.stop()

    def test_three_column_join_then_groupby(self):
        """Test join on three columns followed by groupBy."""
        spark = SparkSession.builder.appName("issue-280").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"year": 2023, "month": 1, "day": 1, "value": 10},
                    {"year": 2023, "month": 1, "day": 2, "value": 20},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"year": 2023, "month": 1, "day": 1, "extra": "X"},
                    {"year": 2023, "month": 1, "day": 2, "extra": "Y"},
                ]
            )

            df = df1.join(df2, on=["year", "month", "day"], how="inner")
            result = (
                df.groupBy(["year", "month", "day"])
                .agg(F.sum("value").alias("total"))
                .collect()
            )

            result_dict = {
                (r["year"], r["month"], r["day"]): r["total"] for r in result
            }
            assert result_dict == {(2023, 1, 1): 10, (2023, 1, 2): 20}
        finally:
            spark.stop()

    def test_join_then_select_join_keys(self):
        """Test join followed by select on join keys."""
        spark = SparkSession.builder.appName("issue-280").getOrCreate()
        try:
            df1 = spark.createDataFrame([{"id": 1, "name": "Alice"}])
            df2 = spark.createDataFrame([{"id": 1, "score": 100}])

            df = df1.join(df2, on="id", how="left")
            result = df.select("id", "name", "score").collect()

            assert len(result) == 1
            assert result[0]["id"] == 1
            assert result[0]["name"] == "Alice"
            assert result[0]["score"] == 100
        finally:
            spark.stop()

    def test_join_then_filter_on_join_keys(self):
        """Test join followed by filter on join keys."""
        spark = SparkSession.builder.appName("issue-280").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"id": 1, "name": "Alice"},
                    {"id": 2, "name": "Bob"},
                    {"id": 3, "name": "Charlie"},
                ]
            )
            df2 = spark.createDataFrame(
                [{"id": 1, "score": 100}, {"id": 2, "score": 200}]
            )

            df = df1.join(df2, on="id", how="left")
            result = df.filter(F.col("id") > 1).collect()

            assert len(result) == 2
            ids = {r["id"] for r in result}
            assert ids == {2, 3}
        finally:
            spark.stop()

    def test_join_then_orderby_on_join_keys(self):
        """Test join followed by orderBy on join keys."""
        spark = SparkSession.builder.appName("issue-280").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [{"id": 3, "name": "C"}, {"id": 1, "name": "A"}, {"id": 2, "name": "B"}]
            )
            df2 = spark.createDataFrame(
                [{"id": 1, "val": 10}, {"id": 2, "val": 20}, {"id": 3, "val": 30}]
            )

            df = df1.join(df2, on="id", how="inner")
            result = df.orderBy("id").collect()

            assert len(result) == 3
            assert [r["id"] for r in result] == [1, 2, 3]
        finally:
            spark.stop()

    def test_join_with_non_join_key_duplicates(self):
        """Test join where non-join-key columns have duplicates (should still work)."""
        spark = SparkSession.builder.appName("issue-280").getOrCreate()
        try:
            df1 = spark.createDataFrame([{"id": 1, "name": "Alice", "common": "X"}])
            df2 = spark.createDataFrame([{"id": 1, "score": 100, "common": "Y"}])

            df = df1.join(df2, on="id", how="left")
            # "common" exists in both but is not a join key - should still work
            result = df.groupBy("id").count().collect()

            assert len(result) == 1
            assert result[0]["id"] == 1
            assert result[0]["count"] == 1
        finally:
            spark.stop()

    def test_chained_joins_then_groupby(self):
        """Test multiple joins chained together then groupBy."""
        spark = SparkSession.builder.appName("issue-280").getOrCreate()
        try:
            df1 = spark.createDataFrame([{"id": 1, "name": "Alice"}])
            df2 = spark.createDataFrame([{"id": 1, "dept": "IT"}])
            df3 = spark.createDataFrame([{"id": 1, "location": "NYC"}])

            df = df1.join(df2, on="id", how="left").join(df3, on="id", how="left")
            result = df.groupBy("id").count().collect()

            assert len(result) == 1
            assert result[0]["id"] == 1
            assert result[0]["count"] == 1
        finally:
            spark.stop()

    def test_join_then_aggregate_with_join_keys(self):
        """Test join followed by aggregation using join keys."""
        spark = SparkSession.builder.appName("issue-280").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"category": "A", "subcat": "X", "value": 10},
                    {"category": "A", "subcat": "Y", "value": 20},
                    {"category": "B", "subcat": "X", "value": 30},
                ]
            )
            df2 = spark.createDataFrame(
                [
                    {"category": "A", "subcat": "X", "extra": 1},
                    {"category": "A", "subcat": "Y", "extra": 2},
                    {"category": "B", "subcat": "X", "extra": 3},
                ]
            )

            df = df1.join(df2, on=["category", "subcat"], how="inner")
            result = df.groupBy("category").agg(F.sum("value").alias("total")).collect()

            result_dict = {r["category"]: r["total"] for r in result}
            assert result_dict == {"A": 30, "B": 30}
        finally:
            spark.stop()

    def test_join_with_nulls_then_groupby(self):
        """Test join with null values in join keys then groupBy."""
        spark = SparkSession.builder.appName("issue-280").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [{"id": 1, "name": "Alice"}, {"id": None, "name": "Bob"}]
            )
            df2 = spark.createDataFrame([{"id": 1, "score": 100}])

            df = df1.join(df2, on="id", how="left")
            result = df.groupBy("id").count().collect()

            # Should have two groups: id=1 and id=None
            result_dict = {r["id"]: r["count"] for r in result}
            assert result_dict[1] == 1
            assert result_dict[None] == 1
        finally:
            spark.stop()

    def test_join_different_data_types_then_groupby(self):
        """Test join on columns with different data types then groupBy."""
        spark = SparkSession.builder.appName("issue-280").getOrCreate()
        try:
            df1 = spark.createDataFrame([{"id": 1, "name": "Alice", "age": 25}])
            df2 = spark.createDataFrame([{"id": 1, "name": "Alice", "score": 100.5}])

            df = df1.join(df2, on=["id", "name"], how="inner")
            result = (
                df.groupBy(["id", "name"]).agg(F.avg("age").alias("avg_age")).collect()
            )

            assert len(result) == 1
            assert result[0]["id"] == 1
            assert result[0]["name"] == "Alice"
            assert result[0]["avg_age"] == 25.0
        finally:
            spark.stop()

    def test_join_then_withcolumn_on_join_key(self):
        """Test join followed by withColumn that references join key."""
        spark = SparkSession.builder.appName("issue-280").getOrCreate()
        try:
            df1 = spark.createDataFrame([{"id": 1, "name": "Alice"}])
            df2 = spark.createDataFrame([{"id": 1, "score": 100}])

            df = df1.join(df2, on="id", how="left")
            result = df.withColumn("id_doubled", F.col("id") * 2).collect()

            assert len(result) == 1
            assert result[0]["id"] == 1
            assert result[0]["id_doubled"] == 2
        finally:
            spark.stop()

    def test_join_then_drop_other_columns(self):
        """Test join followed by dropping non-join-key columns then groupBy."""
        spark = SparkSession.builder.appName("issue-280").getOrCreate()
        try:
            df1 = spark.createDataFrame([{"id": 1, "name": "Alice", "temp": "X"}])
            df2 = spark.createDataFrame([{"id": 1, "score": 100, "temp": "Y"}])

            df = df1.join(df2, on="id", how="left")
            df = df.drop("name", "score", "temp")
            result = df.groupBy("id").count().collect()

            assert len(result) == 1
            assert result[0]["id"] == 1
            assert result[0]["count"] == 1
        finally:
            spark.stop()

    def test_join_with_mixed_case_column_names(self):
        """Test join where left and right have same-case join keys (no ambiguity)."""
        spark = SparkSession.builder.appName("issue-280").getOrCreate()
        try:
            # Both DataFrames use same case for join key
            df1 = spark.createDataFrame([{"id": 1, "Name": "Alice"}])
            df2 = spark.createDataFrame([{"id": 1, "name": "Bob"}])

            df = df1.join(df2, on="id", how="left")
            result = df.groupBy("id").count().collect()

            assert len(result) == 1
            assert result[0]["id"] == 1
        finally:
            spark.stop()

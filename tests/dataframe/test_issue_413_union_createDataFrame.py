"""Tests for issue #413: union() with createDataFrame(data, column_names).

Issue #413 reports that createDataFrame(data, ["id", "age", "name"]) with tuples
then union(other) fails with AnalysisException due to column name/position mismatch.
PySpark's union() matches by position, not by name.

These tests verify that:
- createDataFrame(data, column_names) produces columns in the given order
- union() matches by position and succeeds when schemas are compatible by position
"""

from tests.fixtures.spark_imports import get_spark_imports

imports = get_spark_imports()
StructType = imports.StructType
StructField = imports.StructField
IntegerType = imports.IntegerType
StringType = imports.StringType


class TestIssue413UnionCreateDataFrame:
    """Regression tests for union() with createDataFrame(data, cols) (issue #413)."""

    def test_union_createDataFrame_tuple_column_names(self, spark) -> None:
        """Exact reproduction: union of DataFrames from createDataFrame(data, cols)."""
        cols = ["id", "age", "name"]
        data_a = [(1, 25, "alice"), (2, 30, "bob")]
        data_b = [(3, 22, "carol"), (4, 28, "dave")]

        df_a = spark.createDataFrame(data_a, cols)
        df_b = spark.createDataFrame(data_b, cols)
        result = df_a.union(df_b)

        rows = result.collect()
        assert len(rows) == 4
        assert (
            rows[0]["id"] == 1 and rows[0]["age"] == 25 and rows[0]["name"] == "alice"
        )
        assert rows[1]["id"] == 2 and rows[1]["age"] == 30 and rows[1]["name"] == "bob"
        assert (
            rows[2]["id"] == 3 and rows[2]["age"] == 22 and rows[2]["name"] == "carol"
        )
        assert rows[3]["id"] == 4 and rows[3]["age"] == 28 and rows[3]["name"] == "dave"

    def test_createDataFrame_preserves_column_order(self, spark) -> None:
        """createDataFrame(data, cols) should produce columns in the given order."""
        cols = ["id", "age", "name"]
        data = [(1, 25, "alice")]
        df = spark.createDataFrame(data, cols)
        assert df.columns == ["id", "age", "name"]

    def test_union_different_column_order_by_position(self, spark) -> None:
        """union() matches by position; different column names at same position should still work."""
        df_a = spark.createDataFrame([(1, 25, "alice")], ["id", "age", "name"])
        df_b = spark.createDataFrame([(2, 30, "bob")], ["x", "y", "z"])

        result = df_a.union(df_b)
        rows = result.collect()
        assert len(rows) == 2
        assert (
            rows[0]["id"] == 1 and rows[0]["age"] == 25 and rows[0]["name"] == "alice"
        )
        assert rows[1]["id"] == 2 and rows[1]["age"] == 30 and rows[1]["name"] == "bob"

    def test_union_chained_three_dataframes(self, spark) -> None:
        """Chained union of 3+ DataFrames with createDataFrame(data, cols)."""
        cols = ["a", "b"]
        df1 = spark.createDataFrame([(1, "x")], cols)
        df2 = spark.createDataFrame([(2, "y")], ["col1", "col2"])  # different names
        df3 = spark.createDataFrame([(3, "z")], cols)

        result = df1.union(df2).union(df3)
        rows = result.collect()
        assert len(rows) == 3
        assert rows[0]["a"] == 1 and rows[0]["b"] == "x"
        assert rows[1]["a"] == 2 and rows[1]["b"] == "y"
        assert rows[2]["a"] == 3 and rows[2]["b"] == "z"

    def test_union_empty_dataframe_left(self, spark) -> None:
        """Union when left DataFrame is empty (requires StructType for empty data)."""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        )
        df_empty = spark.createDataFrame([], schema)
        df_right = spark.createDataFrame([(1, "alice")], ["x", "y"])

        result = df_empty.union(df_right)
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["id"] == 1 and rows[0]["name"] == "alice"

    def test_union_empty_dataframe_right(self, spark) -> None:
        """Union when right DataFrame is empty (requires StructType for empty data)."""
        cols = ["id", "name"]
        df_left = spark.createDataFrame([(1, "alice")], cols)
        schema = StructType(
            [
                StructField("a", IntegerType(), True),
                StructField("b", StringType(), True),
            ]
        )
        df_empty = spark.createDataFrame([], schema)

        result = df_left.union(df_empty)
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["id"] == 1 and rows[0]["name"] == "alice"

    def test_union_both_empty(self, spark) -> None:
        """Union of two empty DataFrames with different column names (StructType required)."""
        schema1 = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        )
        schema2 = StructType(
            [
                StructField("x", IntegerType(), True),
                StructField("y", StringType(), True),
            ]
        )
        df_empty1 = spark.createDataFrame([], schema1)
        df_empty2 = spark.createDataFrame([], schema2)

        result = df_empty1.union(df_empty2)
        rows = result.collect()
        assert len(rows) == 0
        assert result.columns == ["id", "name"]

    def test_union_with_nulls(self, spark) -> None:
        """Union with null values in rows."""
        df_a = spark.createDataFrame(
            [(1, None, "alice"), (2, 30, None)],
            ["id", "age", "name"],
        )
        df_b = spark.createDataFrame(
            [(3, 22, "carol"), (None, 28, "dave")],
            ["x", "y", "z"],
        )

        result = df_a.union(df_b)
        rows = result.collect()
        assert len(rows) == 4
        assert (
            rows[0]["id"] == 1 and rows[0]["age"] is None and rows[0]["name"] == "alice"
        )
        assert rows[1]["id"] == 2 and rows[1]["age"] == 30 and rows[1]["name"] is None
        assert (
            rows[2]["id"] == 3 and rows[2]["age"] == 22 and rows[2]["name"] == "carol"
        )
        assert (
            rows[3]["id"] is None and rows[3]["age"] == 28 and rows[3]["name"] == "dave"
        )

    def test_unionAll_alias(self, spark) -> None:
        """unionAll is alias for union; position-based matching applies."""
        df_a = spark.createDataFrame([(1, "a")], ["id", "val"])
        df_b = spark.createDataFrame([(2, "b")], ["x", "y"])

        result = df_a.unionAll(df_b)
        rows = result.collect()
        assert len(rows) == 2
        assert rows[0]["id"] == 1 and rows[0]["val"] == "a"
        assert rows[1]["id"] == 2 and rows[1]["val"] == "b"

    def test_union_single_column(self, spark) -> None:
        """Union with single-column schema."""
        df_a = spark.createDataFrame([(1,), (2,)], ["id"])
        df_b = spark.createDataFrame([(3,)], ["other"])

        result = df_a.union(df_b)
        rows = result.collect()
        assert len(rows) == 3
        assert [r["id"] for r in rows] == [1, 2, 3]

    def test_union_many_columns_different_names(self, spark) -> None:
        """Union with many columns and different names by position."""
        cols_left = ["c1", "c2", "c3", "c4", "c5"]
        cols_right = ["a", "b", "c", "d", "e"]
        df_a = spark.createDataFrame([(1, 2, 3, 4, 5)], cols_left)
        df_b = spark.createDataFrame([(10, 20, 30, 40, 50)], cols_right)

        result = df_a.union(df_b)
        rows = result.collect()
        assert len(rows) == 2
        assert rows[0]["c1"] == 1 and rows[0]["c5"] == 5
        assert rows[1]["c1"] == 10 and rows[1]["c5"] == 50
        assert result.columns == cols_left

    def test_union_then_select(self, spark) -> None:
        """Chain select after union."""
        df_a = spark.createDataFrame([(1, "alice")], ["id", "name"])
        df_b = spark.createDataFrame([(2, "bob")], ["x", "y"])

        result = df_a.union(df_b).select("id", "name")
        rows = result.collect()
        assert len(rows) == 2
        assert rows[0]["id"] == 1 and rows[0]["name"] == "alice"
        assert rows[1]["id"] == 2 and rows[1]["name"] == "bob"

    def test_union_then_order_by(self, spark) -> None:
        """Chain orderBy after union."""
        df_a = spark.createDataFrame([(2, "b")], ["id", "val"])
        df_b = spark.createDataFrame([(1, "a")], ["x", "y"])

        result = df_a.union(df_b).orderBy("id")
        rows = result.collect()
        assert len(rows) == 2
        assert rows[0]["id"] == 1 and rows[0]["val"] == "a"
        assert rows[1]["id"] == 2 and rows[1]["val"] == "b"

    def test_union_then_filter(self, spark) -> None:
        """Chain filter after union."""
        df_a = spark.createDataFrame([(1, 10), (2, 20)], ["id", "score"])
        df_b = spark.createDataFrame([(3, 30), (4, 40)], ["a", "b"])

        result = df_a.union(df_b).filter("id > 2")
        rows = result.collect()
        assert len(rows) == 2
        assert rows[0]["id"] == 3 and rows[0]["score"] == 30
        assert rows[1]["id"] == 4 and rows[1]["score"] == 40

    def test_union_count(self, spark) -> None:
        """count() after union returns correct total."""
        df_a = spark.createDataFrame([(1,), (2,)], ["id"])
        df_b = spark.createDataFrame([(3,)], ["x"])

        result = df_a.union(df_b)
        assert result.count() == 3

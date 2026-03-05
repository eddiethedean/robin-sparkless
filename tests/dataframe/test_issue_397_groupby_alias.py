"""
Tests for issue #397: groupBy with aliased columns.

PySpark accepts df.groupBy(F.col('Name').alias('Key')).agg(...).
Sparkless previously raised SparkColumnNotFoundError: cannot resolve 'Key'.
"""

from tests.fixtures.spark_imports import get_spark_imports


class TestIssue397GroupByAlias:
    """Test groupBy with aliased columns (issue #397)."""

    def test_groupby_alias_exact_issue(self, spark):
        """Exact scenario from issue #397."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1},
                {"Name": "Alice", "Value": 2},
                {"Name": "Bob", "Value": 3},
                {"Name": "Bob", "Value": 4},
            ]
        )
        df = df.groupBy(F.col("Name").alias("Key")).agg(F.sum("Value"))
        rows = df.collect()
        assert len(rows) == 2
        alice = next(r for r in rows if r["Key"] == "Alice")
        bob = next(r for r in rows if r["Key"] == "Bob")
        assert alice["sum(Value)"] == 3
        assert bob["sum(Value)"] == 7

    def test_groupby_alias_with_show(self, spark):
        """groupBy alias + show() as in issue example."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1},
                {"Name": "Alice", "Value": 2},
                {"Name": "Bob", "Value": 3},
            ]
        )
        df = df.groupBy(F.col("Name").alias("Key")).agg(F.sum("Value"))
        df.show()
        rows = df.collect()
        assert len(rows) == 2

    def test_groupby_multiple_cols_one_aliased(self, spark):
        """groupBy with mix of plain and aliased columns."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"A": "x", "B": 1, "Val": 10},
                {"A": "x", "B": 1, "Val": 20},
                {"A": "y", "B": 2, "Val": 30},
            ]
        )
        df = df.groupBy(F.col("A").alias("Key"), "B").agg(F.sum("Val"))
        rows = df.collect()
        assert len(rows) == 2
        key_b_pairs = [(r["Key"], r["B"]) for r in rows]
        assert ("x", 1) in key_b_pairs
        assert ("y", 2) in key_b_pairs

    def test_groupby_all_cols_aliased(self, spark):
        """groupBy with all columns aliased."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"A": 1, "B": 10, "Val": 100},
                {"A": 1, "B": 10, "Val": 200},
                {"A": 2, "B": 20, "Val": 300},
            ]
        )
        df = df.groupBy(F.col("A").alias("X"), F.col("B").alias("Y")).agg(F.sum("Val"))
        rows = df.collect()
        assert len(rows) == 2
        cols = list(rows[0].asDict().keys())
        assert "X" in cols and "Y" in cols
        sums = {(r["X"], r["Y"]): r["sum(Val)"] for r in rows}
        assert sums[(1, 10)] == 300
        assert sums[(2, 20)] == 300

    def test_groupby_alias_avg_max_min(self, spark):
        """groupBy alias with multiple agg functions."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"k": "x", "v": 10},
                {"k": "x", "v": 20},
                {"k": "y", "v": 5},
            ]
        )
        df = df.groupBy(F.col("k").alias("key")).agg(F.avg("v"), F.max("v"), F.min("v"))
        rows = df.collect()
        assert len(rows) == 2
        by_key = {r["key"]: r for r in rows}
        assert by_key["x"]["avg(v)"] == 15.0
        assert by_key["x"]["max(v)"] == 20
        assert by_key["x"]["min(v)"] == 10
        assert by_key["y"]["avg(v)"] == 5.0

    def test_groupby_alias_count(self, spark):
        """groupBy alias with count(*)."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"cat": "A", "x": 1},
                {"cat": "A", "x": 2},
                {"cat": "B", "x": 3},
            ]
        )
        df = df.groupBy(F.col("cat").alias("category")).agg(F.count("*"))
        rows = df.collect()
        assert len(rows) == 2
        # PySpark and sparkless both use count(1) for agg(F.count("*"))
        count_col = "count(1)"
        by_cat = {r["category"]: r[count_col] for r in rows}
        assert by_cat["A"] == 2
        assert by_cat["B"] == 1

    def test_groupby_alias_with_nulls(self, spark):
        """groupBy alias when group column has nulls."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"n": "a", "v": 1},
                {"n": None, "v": 2},
                {"n": "a", "v": 3},
            ]
        )
        df = df.groupBy(F.col("n").alias("name")).agg(F.sum("v"))
        rows = df.collect()
        assert len(rows) == 2
        by_name = {r["name"]: r["sum(v)"] for r in rows}
        assert by_name["a"] == 4
        assert by_name[None] == 2

    def test_groupby_alias_select_after(self, spark):
        """Select after groupBy with alias."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"x": "a", "y": 1},
                {"x": "a", "y": 2},
                {"x": "b", "y": 5},
            ]
        )
        df = (
            df.groupBy(F.col("x").alias("grp"))
            .agg(F.sum("y").alias("total"))
            .select("grp", "total")
        )
        rows = df.collect()
        assert len(rows) == 2
        by_grp = {r["grp"]: r["total"] for r in rows}
        assert by_grp["a"] == 3
        assert by_grp["b"] == 5

    def test_groupby_alias_agg_alias_matches_issue_expected(self, spark):
        """groupBy alias + agg alias to match issue expected output."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1},
                {"Name": "Alice", "Value": 2},
                {"Name": "Bob", "Value": 3},
                {"Name": "Bob", "Value": 4},
            ]
        )
        df = df.groupBy(F.col("Name").alias("Key")).agg(
            F.sum("Value").alias("SumValue")
        )
        rows = df.collect()
        assert len(rows) == 2
        assert {r["Key"] for r in rows} == {"Alice", "Bob"}
        by_key = {r["Key"]: r["SumValue"] for r in rows}
        assert by_key["Alice"] == 3
        assert by_key["Bob"] == 7

    def test_groupby_list_syntax_with_alias(self, spark):
        """groupBy([col.alias(...)]) list syntax."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1},
                {"Name": "Alice", "Value": 2},
                {"Name": "Bob", "Value": 3},
            ]
        )
        df = df.groupBy([F.col("Name").alias("Key")]).agg(F.sum("Value"))
        rows = df.collect()
        assert len(rows) == 2
        assert "Key" in rows[0].asDict()
        by_key = {r["Key"]: r["sum(Value)"] for r in rows}
        assert by_key["Alice"] == 3
        assert by_key["Bob"] == 3

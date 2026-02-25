"""
Tests for issue #394: LIKE and NOT LIKE in F.expr() / filter string expressions.

PySpark supports df.filter("Name like '%TEST%'") and df.filter("Name not like '%TEST%'").
Sparkless SQLExprParser previously raised ParseException for these expressions.
Now parses LIKE and NOT LIKE in F.expr() for PySpark parity.
"""

from tests.fixtures.spark_imports import get_spark_imports


class TestIssue394LikeInExpr:
    """Test LIKE and NOT LIKE in filter string expressions (issue #394)."""

    def test_filter_like_exact_issue(self, spark):
        """Exact scenario from issue #394."""
        df = spark.createDataFrame(
            [
                {"Name": "Al-TEST-ice"},
                {"Name": "Bob"},
            ]
        )
        df1 = df.filter("Name like '%TEST%'")
        rows = df1.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "Al-TEST-ice"

    def test_filter_not_like_exact_issue(self, spark):
        """Exact scenario from issue #394 - NOT LIKE."""
        df = spark.createDataFrame(
            [
                {"Name": "Al-TEST-ice"},
                {"Name": "Bob"},
            ]
        )
        df2 = df.filter("Name not like '%TEST%'")
        rows = df2.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "Bob"

    def test_filter_like_with_show(self, spark):
        """filter + show() as in issue example."""
        df = spark.createDataFrame(
            [
                {"Name": "Al-TEST-ice"},
                {"Name": "Bob"},
            ]
        )
        df1 = df.filter("Name like '%TEST%'")
        df1.show()
        rows = df1.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "Al-TEST-ice"

    def test_filter_like_prefix_pattern(self, spark):
        """LIKE with prefix pattern (starts with)."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice"},
                {"Name": "Bob"},
                {"Name": "Charlie"},
            ]
        )
        df1 = df.filter("Name like 'A%'")
        rows = df1.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "Alice"

    def test_filter_like_underscore_wildcard(self, spark):
        """LIKE with _ wildcard (single char)."""
        df = spark.createDataFrame(
            [
                {"Name": "A"},
                {"Name": "AB"},
                {"Name": "ABC"},
            ]
        )
        df1 = df.filter("Name like 'A_'")
        rows = df1.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "AB"

    def test_filter_not_like_multiple_rows(self, spark):
        """NOT LIKE returns multiple rows."""
        df = spark.createDataFrame(
            [
                {"Name": "Al-TEST-ice"},
                {"Name": "Bob"},
                {"Name": "Charlie"},
            ]
        )
        df2 = df.filter("Name not like '%TEST%'")
        rows = df2.collect()
        assert len(rows) == 2
        names = {r["Name"] for r in rows}
        assert names == {"Bob", "Charlie"}

    def test_expr_like_standalone(self, spark):
        """F.expr with LIKE used in select/withColumn."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"Name": "Al-TEST-ice"},
                {"Name": "Bob"},
            ]
        )
        df = df.withColumn("has_test", F.expr("Name like '%TEST%'"))
        rows = df.collect()
        alice = next(r for r in rows if r["Name"] == "Al-TEST-ice")
        bob = next(r for r in rows if r["Name"] == "Bob")
        assert alice["has_test"] is True
        assert bob["has_test"] is False

    def test_filter_like_suffix_pattern(self, spark):
        """LIKE with suffix pattern (ends with)."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice"},
                {"Name": "Bob"},
                {"Name": "Charlie"},
            ]
        )
        df1 = df.filter("Name like '%e'")
        rows = df1.collect()
        assert len(rows) == 2
        names = {r["Name"] for r in rows}
        assert names == {"Alice", "Charlie"}

    def test_filter_like_and_combined(self, spark):
        """LIKE combined with AND."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Code": "A1"},
                {"Name": "Bob", "Code": "B1"},
                {"Name": "Anna", "Code": "A2"},
            ]
        )
        df1 = df.filter("Name like 'A%' AND Code like '%1'")
        rows = df1.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "Alice"

    def test_filter_like_or_combined(self, spark):
        """LIKE combined with OR."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice"},
                {"Name": "Bob"},
                {"Name": "Charlie"},
            ]
        )
        df1 = df.filter("Name like 'A%' OR Name like 'C%'")
        rows = df1.collect()
        assert len(rows) == 2
        names = {r["Name"] for r in rows}
        assert names == {"Alice", "Charlie"}

    def test_filter_like_empty_result(self, spark):
        """LIKE with no matches returns empty."""
        df = spark.createDataFrame([{"Name": "Alice"}, {"Name": "Bob"}])
        df1 = df.filter("Name like '%XYZ%'")
        rows = df1.collect()
        assert len(rows) == 0

    def test_filter_like_multiple_underscores(self, spark):
        """LIKE with multiple _ wildcards (exact length)."""
        df = spark.createDataFrame(
            [
                {"Name": "A"},
                {"Name": "AB"},
                {"Name": "ABC"},
                {"Name": "ABCD"},
            ]
        )
        df1 = df.filter("Name like 'A__'")
        rows = df1.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "ABC"

    def test_expr_not_like_in_with_column(self, spark):
        """F.expr NOT LIKE in withColumn."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"Name": "Al-TEST-ice"},
                {"Name": "Bob"},
            ]
        )
        df = df.withColumn("no_test", F.expr("Name not like '%TEST%'"))
        rows = df.collect()
        alice = next(r for r in rows if r["Name"] == "Al-TEST-ice")
        bob = next(r for r in rows if r["Name"] == "Bob")
        assert alice["no_test"] is False
        assert bob["no_test"] is True

    def test_filter_like_with_nulls(self, spark):
        """LIKE excludes nulls from match (null not like pattern)."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice"},
                {"Name": None},
                {"Name": "Bob"},
            ]
        )
        df1 = df.filter("Name like 'A%'")
        rows = df1.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "Alice"

    def test_filter_like_middle_wildcard(self, spark):
        """LIKE with % in middle (A%B: A, any chars, B). % matches zero or more."""
        df = spark.createDataFrame(
            [
                {"Name": "AxB"},
                {"Name": "AxxB"},
                {"Name": "AB"},
                {"Name": "XAB"},
            ]
        )
        df1 = df.filter("Name like 'A%B'")
        rows = df1.collect()
        assert len(rows) == 3
        names = {r["Name"] for r in rows}
        assert names == {"AB", "AxB", "AxxB"}

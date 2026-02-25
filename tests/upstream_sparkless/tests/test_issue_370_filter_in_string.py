"""
Tests for issue #370: df.filter("col in ('val')") and df.filter("col in (val)").

PySpark supports literal list values inside string conditions; Sparkless now parses
"col IN (literal, ...)" in F.expr() and applies the filter (with type coercion when needed).
"""


class TestIssue370FilterInString:
    """Test filter with string condition containing IN (literal list)."""

    def test_filter_values_in_string_literal(self, spark):
        """Exact scenario from issue #370: df.filter(\"Values in ('20')\")."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Values": "10"},
                {"Name": "Bob", "Values": "20"},
            ]
        )
        df1 = df.filter("Values in ('20')")
        rows = df1.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "Bob" and rows[0]["Values"] == "20"

    def test_filter_values_in_numeric_literal(self, spark):
        """df.filter('Values in (20)') - numeric literal (PySpark coerces to string column)."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Values": "10"},
                {"Name": "Bob", "Values": "20"},
            ]
        )
        df1 = df.filter("Values in (20)")
        rows = df1.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "Bob" and rows[0]["Values"] == "20"

    def test_filter_in_string_show(self, spark):
        """Exact issue scenario: filter + show()."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Values": "10"},
                {"Name": "Bob", "Values": "20"},
            ]
        )
        df1 = df.filter("Values in ('20')")
        df1.show()
        rows = df1.collect()
        assert len(rows) == 1 and rows[0]["Name"] == "Bob"

    def test_filter_equality_string_works(self, spark):
        """Sanity: df.filter(\"Values == '20'\") still works (from issue)."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Values": "10"},
                {"Name": "Bob", "Values": "20"},
            ]
        )
        df1 = df.filter("Values == '20'")
        rows = df1.collect()
        assert len(rows) == 1 and rows[0]["Name"] == "Bob"

    def test_filter_in_multiple_literals(self, spark):
        """df.filter(\"Values in ('10', '20')\") returns both rows."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Values": "10"},
                {"Name": "Bob", "Values": "20"},
                {"Name": "Carol", "Values": "30"},
            ]
        )
        df1 = df.filter("Values in ('10', '20')")
        rows = df1.collect()
        assert len(rows) == 2
        names = {r["Name"] for r in rows}
        assert names == {"Alice", "Bob"}

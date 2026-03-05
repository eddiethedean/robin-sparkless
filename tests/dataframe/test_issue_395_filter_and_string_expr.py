"""
Tests for issue #395: filter with AND and string literals - operator precedence.

"status == 'Y' and Name is not null" was parsed incorrectly (AND joined to 'Y')
because IS NULL was matched before AND split. Fix: parse AND/OR before IS NULL,
and use "==" for equality split so "a == 'Y'" splits correctly.
"""


class TestIssue395FilterAndStringExpr:
    """Test filter with AND and string literals (issue #395)."""

    def test_filter_and_string_equality_exact_issue(self, spark):
        """Exact scenario from issue #395."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Status": "Y"},
                {"Name": None, "Status": "Y"},
            ]
        )
        df = df.filter("status == 'Y' and Name is not null")
        rows = df.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "Alice"

    def test_filter_and_string_equality_with_show(self, spark):
        """filter + show() as in issue example."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Status": "Y"},
                {"Name": None, "Status": "Y"},
            ]
        )
        df = df.filter("status == 'Y' and Name is not null")
        df.show()
        rows = df.collect()
        assert len(rows) == 1

    def test_filter_and_is_null_workaround_still_works(self, spark):
        """Parentheses workaround still works."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Status": "Y"},
                {"Name": None, "Status": "Y"},
            ]
        )
        df = df.filter("(status == 'Y') and (Name is not null)")
        rows = df.collect()
        assert len(rows) == 1

    def test_filter_or_string_equality(self, spark):
        """OR with string equality."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Status": "Y"},
                {"Name": "Bob", "Status": "N"},
                {"Name": "Charlie", "Status": "Y"},
            ]
        )
        df = df.filter("Status == 'Y' or Status == 'N'")
        rows = df.collect()
        assert len(rows) == 3

    def test_filter_and_string_and_is_null(self, spark):
        """Multiple AND with string and is null."""
        df = spark.createDataFrame(
            [
                {"A": "x", "B": "y", "C": 1},
                {"A": "x", "B": None, "C": 2},
                {"A": "x", "B": "y", "C": None},
            ]
        )
        df = df.filter("A == 'x' and B is not null and C is not null")
        rows = df.collect()
        assert len(rows) == 1
        assert rows[0]["C"] == 1

    def test_filter_and_literal_contains_and(self, spark):
        """AND inside string literal is not split: col == 'yes and no'."""
        df = spark.createDataFrame(
            [
                {"Msg": "yes and no"},
                {"Msg": "yes"},
                {"Msg": "no"},
            ]
        )
        df = df.filter("Msg == 'yes and no'")
        rows = df.collect()
        assert len(rows) == 1
        assert rows[0]["Msg"] == "yes and no"

    def test_filter_or_with_is_null(self, spark):
        """OR combining string equality and is null."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Code": "A"},
                {"Name": "Bob", "Code": None},
                {"Name": None, "Code": "B"},
            ]
        )
        df = df.filter("Name == 'Alice' or Code is null")
        rows = df.collect()
        assert len(rows) == 2
        names = {r["Name"] for r in rows if r["Name"] is not None}
        assert "Alice" in names
        assert "Bob" in names

    def test_filter_string_equality_single_condition(self, spark):
        """Single condition with string equality (no AND)."""
        df = spark.createDataFrame(
            [
                {"Status": "Y"},
                {"Status": "N"},
            ]
        )
        df = df.filter("Status == 'Y'")
        rows = df.collect()
        assert len(rows) == 1
        assert rows[0]["Status"] == "Y"

    def test_filter_and_string_equality_empty_result(self, spark):
        """AND with string equality - no matches."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Status": "N"},
                {"Name": "Bob", "Status": "N"},
            ]
        )
        df = df.filter("Status == 'Y' and Name is not null")
        rows = df.collect()
        assert len(rows) == 0

    def test_filter_and_numeric_and_string(self, spark):
        """AND with numeric comparison and string equality."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Score": 10, "Grade": "A"},
                {"Name": "Bob", "Score": 5, "Grade": "A"},
                {"Name": "Charlie", "Score": 15, "Grade": "B"},
            ]
        )
        df = df.filter("Score >= 10 and Grade == 'A'")
        rows = df.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "Alice"

    def test_expr_and_string_with_column(self, spark):
        """F.expr with AND and string in withColumn."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Status": "Y"},
                {"Name": "Bob", "Status": "N"},
            ]
        )
        df = df.withColumn("active", F.expr("Status == 'Y' and Name is not null"))
        rows = df.collect()
        alice = next(r for r in rows if r["Name"] == "Alice")
        bob = next(r for r in rows if r["Name"] == "Bob")
        assert alice["active"] is True
        assert bob["active"] is False

    def test_filter_and_select_after(self, spark):
        """filter then select."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Status": "Y"},
                {"Name": None, "Status": "Y"},
            ]
        )
        df = df.filter("status == 'Y' and Name is not null").select("Name")
        rows = df.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "Alice"

    def test_filter_is_null_and_string_equality(self, spark):
        """Order: is null first, then string equality."""
        df = spark.createDataFrame(
            [
                {"A": "x", "B": "y"},
                {"A": None, "B": "y"},
                {"A": "x", "B": None},
            ]
        )
        df = df.filter("A is not null and B == 'y'")
        rows = df.collect()
        assert len(rows) == 1
        assert rows[0]["A"] == "x"

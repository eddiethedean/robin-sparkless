"""
Tests for issue #419: ParseException when IN literal list is combined with OR.

Sparkless raised ParseException for expressions like:
    df.filter("Value in ('1234') or (Name == 'Alice')")

Root cause: _split_logical_operator was adding parentheses twice (in the paren
branch and again at the end of the loop), causing malformed OR parts.
"""


class TestIssue419FilterInOrParseException:
    """Test filter with IN + OR no longer raises ParseException."""

    def test_filter_in_or_with_integer_literal_exact_issue(self, spark):
        """Exact scenario from issue #419 - IN + OR (using int literal for type match)."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1234},
                {"Name": "Bob", "Value": 5678},
            ]
        )
        df = df.filter("Value in (1234) or (Name == 'Alice')")
        rows = df.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "Alice"
        assert rows[0]["Value"] == 1234

    def test_filter_in_or_string_literal_no_parse_exception(self, spark, spark_backend):
        """F.expr parses 'Value in ('1234') or (Name == 'Alice')' without ParseException."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports(spark_backend)
        F = imports.F
        # This previously raised ParseException (Sparkless) - parses successfully in both backends
        expr = F.expr("Value in ('1234') or (Name == 'Alice')")
        assert expr is not None

    def test_filter_in_or_string_literal_type_coercion_exact_issue(self, spark):
        """Exact issue scenario: int column + string literal in IN + OR (type coercion)."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1234},
                {"Name": "Bob", "Value": 5678},
            ]
        )
        df = df.filter("Value in ('1234') or (Name == 'Alice')")
        rows = df.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "Alice"
        assert rows[0]["Value"] == 1234

    def test_filter_in_or_workaround_still_works(self, spark):
        """Workaround from issue (IN only) still works."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1234},
                {"Name": "Bob", "Value": 5678},
            ]
        )
        df = df.filter("Value in ('1234')")
        rows = df.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "Alice"

    def test_filter_in_or_with_string_column(self, spark):
        """IN + OR with string column (no type coercion needed)."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Code": "A"},
                {"Name": "Bob", "Code": "B"},
                {"Name": "Charlie", "Code": "C"},
            ]
        )
        df = df.filter("Code in ('A') or (Name == 'Charlie')")
        rows = df.collect()
        assert len(rows) == 2
        names = {r["Name"] for r in rows}
        assert names == {"Alice", "Charlie"}

    def test_filter_in_multiple_values_or(self, spark):
        """IN with multiple values + OR - all rows match."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1234},
                {"Name": "Bob", "Value": 5678},
                {"Name": "Charlie", "Value": 9999},
            ]
        )
        df = df.filter("Value in ('1234', '5678') or Name == 'Charlie'")
        rows = df.collect()
        assert len(rows) == 3
        names = {r["Name"] for r in rows}
        assert names == {"Alice", "Bob", "Charlie"}

    def test_filter_in_and_condition(self, spark):
        """IN + AND - both conditions must hold."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1234},
                {"Name": "Bob", "Value": 5678},
            ]
        )
        df = df.filter("Value in ('1234') and Name == 'Alice'")
        rows = df.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "Alice"
        assert rows[0]["Value"] == 1234

    def test_filter_multiple_in_or(self, spark):
        """Multiple IN clauses combined with OR."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1234},
                {"Name": "Bob", "Value": 5678},
                {"Name": "Charlie", "Value": 9999},
            ]
        )
        df = df.filter("(Value in (1234)) or (Value in (5678)) or Name == 'Charlie'")
        rows = df.collect()
        assert len(rows) == 3
        names = {r["Name"] for r in rows}
        assert names == {"Alice", "Bob", "Charlie"}

    def test_filter_in_float_column_or(self, spark):
        """Float column IN with multiple values - exercises IN + float type."""
        df = spark.createDataFrame(
            [
                {"Name": "A", "Price": 10.5},
                {"Name": "B", "Price": 20.0},
                {"Name": "C", "Price": 30.0},
            ]
        )
        df = df.filter("Price in (10.5, 20.0)")
        rows = df.collect()
        assert len(rows) == 2
        names = {r["Name"] for r in rows}
        assert names == {"A", "B"}

    def test_filter_in_or_empty_result(self, spark):
        """IN + OR that yields no rows."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1234},
                {"Name": "Bob", "Value": 5678},
            ]
        )
        df = df.filter("Value in ('999') and Name == 'X'")
        rows = df.collect()
        assert len(rows) == 0

    def test_filter_in_multiple_strings_or(self, spark):
        """IN with multiple string values + OR."""
        df = spark.createDataFrame(
            [
                {"Code": "A", "X": 1},
                {"Code": "B", "X": 2},
                {"Code": "C", "X": 3},
            ]
        )
        df = df.filter("Code in ('A', 'C') or X == 2")
        rows = df.collect()
        assert len(rows) == 3
        codes = {r["Code"] for r in rows}
        assert codes == {"A", "B", "C"}

    def test_filter_in_or_with_show(self, spark):
        """IN + OR with show() - regression for full pipeline."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1234},
                {"Name": "Bob", "Value": 5678},
            ]
        )
        df = df.filter("Value in ('1234') or (Name == 'Alice')")
        df.show()
        rows = df.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "Alice"

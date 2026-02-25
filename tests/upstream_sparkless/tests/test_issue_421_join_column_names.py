"""
Tests for issue #421: join with different column names using F.col().

Sparkless raised ValueError when joining with Column-based comparisons
where column names differ (e.g. F.col("Key") == F.col("Name")).
PySpark resolves column names by which DataFrame contains them.

Run with PySpark first to establish baseline:
  MOCK_SPARK_TEST_BACKEND=pyspark pytest tests/test_issue_421_join_column_names.py -v

Then run with Sparkless to verify parity:
  MOCK_SPARK_TEST_BACKEND=mock pytest tests/test_issue_421_join_column_names.py -v
"""


def _get_functions(spark):
    """Return functions module for the active backend (PySpark or Sparkless)."""
    if "pyspark" in type(spark).__module__:
        from pyspark.sql import functions as F

        return F
    import sparkless.sql.functions as F

    return F


def _val(row, *keys):
    """Get value from row by trying multiple column names (backend-agnostic)."""
    for k in keys:
        try:
            if hasattr(row, "asDict"):
                d = row.asDict()
                if k in d:
                    return d[k]
            if hasattr(row, "__getitem__"):
                return row[k]
        except (KeyError, TypeError, AttributeError):
            continue
    return None


class TestIssue421JoinColumnNames:
    """Test join with different column names using F.col() notation."""

    def test_join_different_column_names_exact_issue(self, spark):
        """Exact scenario from issue #421 - F.col('Key') == F.col('Name')."""
        F = _get_functions(spark)
        df1 = spark.createDataFrame(
            [{"Name": "Alice", "Value1": 5}, {"Name": "Bob", "Value1": 7}]
        )
        df2 = spark.createDataFrame(
            [{"Key": "Alice", "Value2": "A"}, {"Key": "Bob", "Value2": "B"}]
        )
        df = df1.join(df2, F.col("Key") == F.col("Name"), "left")
        rows = df.collect()
        assert len(rows) == 2
        by_name = {_val(r, "Name"): r for r in rows}
        assert _val(by_name["Alice"], "Key") == "Alice"
        assert _val(by_name["Alice"], "Value2") == "A"
        assert _val(by_name["Bob"], "Key") == "Bob"
        assert _val(by_name["Bob"], "Value2") == "B"

    def test_join_different_column_names_reverse_order(self, spark):
        """F.col('Name') == F.col('Key') - reverse order of columns."""
        F = _get_functions(spark)
        df1 = spark.createDataFrame([{"Name": "Alice"}, {"Name": "Bob"}])
        df2 = spark.createDataFrame([{"Key": "Alice"}, {"Key": "Bob"}])
        df = df1.join(df2, F.col("Name") == F.col("Key"), "inner")
        rows = df.collect()
        assert len(rows) == 2
        names = {_val(r, "Name") for r in rows}
        assert names == {"Alice", "Bob"}
        alice_row = next(r for r in rows if _val(r, "Name") == "Alice")
        assert _val(alice_row, "Key") == "Alice"

    def test_join_different_column_names_left_no_match(self, spark):
        """Left join: left row with no right match yields nulls in right columns."""
        F = _get_functions(spark)
        df1 = spark.createDataFrame(
            [{"Name": "Alice", "V1": 1}, {"Name": "Charlie", "V1": 3}]
        )
        df2 = spark.createDataFrame([{"Key": "Alice", "V2": "A"}])  # no Charlie
        df = df1.join(df2, F.col("Key") == F.col("Name"), "left")
        rows = sorted(df.collect(), key=lambda r: _val(r, "Name") or "")
        assert len(rows) == 2
        alice = next(r for r in rows if _val(r, "Name") == "Alice")
        charlie = next(r for r in rows if _val(r, "Name") == "Charlie")
        assert _val(alice, "Key") == "Alice"
        assert _val(alice, "V2") == "A"
        assert _val(charlie, "Key") is None
        assert _val(charlie, "V2") is None

    def test_join_different_column_names_inner(self, spark):
        """Inner join with F.col() on different column names."""
        F = _get_functions(spark)
        df1 = spark.createDataFrame(
            [{"id_l": 1, "x": 10}, {"id_l": 2, "x": 20}, {"id_l": 3, "x": 30}]
        )
        df2 = spark.createDataFrame([{"id_r": 1, "y": 100}, {"id_r": 2, "y": 200}])
        df = df1.join(df2, F.col("id_r") == F.col("id_l"), "inner")
        rows = sorted(df.collect(), key=lambda r: _val(r, "id_l"))
        assert len(rows) == 2
        assert _val(rows[0], "id_l") == 1 and _val(rows[0], "y") == 100
        assert _val(rows[1], "id_l") == 2 and _val(rows[1], "y") == 200

    def test_join_different_column_names_right(self, spark):
        """Right join with F.col() on different column names."""
        F = _get_functions(spark)
        df1 = spark.createDataFrame([{"a": 1, "x": 10}])  # only id 1
        df2 = spark.createDataFrame([{"b": 1, "y": 100}, {"b": 2, "y": 200}])
        df = df1.join(df2, F.col("b") == F.col("a"), "right")
        rows = sorted(df.collect(), key=lambda r: _val(r, "b") or 0)
        assert len(rows) == 2
        r1 = next(r for r in rows if _val(r, "b") == 1)
        r2 = next(r for r in rows if _val(r, "b") == 2)
        assert _val(r1, "a") == 1
        assert _val(r1, "x") == 10
        assert _val(r1, "y") == 100
        assert _val(r2, "a") is None
        assert _val(r2, "x") is None
        assert _val(r2, "y") == 200

    def test_join_different_column_names_outer(self, spark):
        """Full outer join with F.col() on different column names."""
        F = _get_functions(spark)
        df1 = spark.createDataFrame(
            [{"left_id": 1, "lval": "L1"}, {"left_id": 2, "lval": "L2"}]
        )
        df2 = spark.createDataFrame(
            [{"right_id": 2, "rval": "R2"}, {"right_id": 3, "rval": "R3"}]
        )
        df = df1.join(df2, F.col("right_id") == F.col("left_id"), "outer")
        rows = df.collect()
        # Outer: (1,L1,null,null), (2,L2,2,R2), (null,null,3,R3)
        assert len(rows) == 3
        by_left = {
            _val(r, "left_id"): r for r in rows if _val(r, "left_id") is not None
        }
        by_right = {
            _val(r, "right_id"): r for r in rows if _val(r, "right_id") is not None
        }
        assert _val(by_left[1], "right_id") is None
        assert _val(by_left[2], "right_id") == 2 and _val(by_left[2], "rval") == "R2"
        assert _val(by_right[3], "left_id") is None
        assert _val(by_right[3], "rval") == "R3"

    def test_join_different_column_names_with_show(self, spark):
        """Join then show() - exercises full pipeline (issue stack trace used show)."""
        F = _get_functions(spark)
        df1 = spark.createDataFrame(
            [{"Name": "Alice", "Value1": 5}, {"Name": "Bob", "Value1": 7}]
        )
        df2 = spark.createDataFrame(
            [{"Key": "Alice", "Value2": "A"}, {"Key": "Bob", "Value2": "B"}]
        )
        df = df1.join(df2, F.col("Key") == F.col("Name"), "left")
        df.show()  # No exception
        rows = df.collect()
        assert len(rows) == 2

    def test_join_different_column_names_with_select(self, spark):
        """Join then select - verifies column resolution in downstream ops."""
        F = _get_functions(spark)
        df1 = spark.createDataFrame(
            [{"Name": "Alice", "Value1": 5}, {"Name": "Bob", "Value1": 7}]
        )
        df2 = spark.createDataFrame(
            [{"Key": "Alice", "Value2": "A"}, {"Key": "Bob", "Value2": "B"}]
        )
        df = df1.join(df2, F.col("Key") == F.col("Name"), "inner").select(
            "Name", "Value1", "Value2"
        )
        rows = df.collect()
        assert len(rows) == 2
        by_name = {_val(r, "Name"): r for r in rows}
        assert _val(by_name["Alice"], "Value1") == 5
        assert _val(by_name["Alice"], "Value2") == "A"

    def test_join_dot_notation_still_works(self, spark):
        """Dot notation df1.Name == df2.Key still works (workaround from issue)."""
        df1 = spark.createDataFrame(
            [{"Name": "Alice", "Value1": 5}, {"Name": "Bob", "Value1": 7}]
        )
        df2 = spark.createDataFrame(
            [{"Key": "Alice", "Value2": "A"}, {"Key": "Bob", "Value2": "B"}]
        )
        df = df1.join(df2, df1["Name"] == df2["Key"], "left")
        rows = df.collect()
        assert len(rows) == 2
        by_name = {_val(r, "Name"): r for r in rows}
        assert _val(by_name["Alice"], "Key") == "Alice"
        assert _val(by_name["Alice"], "Value2") == "A"

    def test_join_same_column_name_string_key_still_works(self, spark):
        """Join on same column name via string key - regression that we didn't break it."""
        df1 = spark.createDataFrame([{"id": 1, "x": 10}])
        df2 = spark.createDataFrame([{"id": 1, "y": 20}])
        df = df1.join(df2, "id", "inner")
        rows = df.collect()
        assert len(rows) == 1
        assert rows[0]["id"] == 1 and rows[0]["x"] == 10 and rows[0]["y"] == 20

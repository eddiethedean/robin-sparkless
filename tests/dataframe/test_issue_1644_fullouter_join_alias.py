"""Regression test for issue #1644: accept 'fullouter' as a full outer join alias."""


def _row_val(row, key):
    if hasattr(row, "asDict"):
        return row.asDict().get(key)
    return row[key]


class TestIssue1644FullouterJoinAlias:
    """PySpark accepts fullouter, full_outer, and outer for full outer joins."""

    def test_join_fullouter_string_key(self, spark):
        """Exact scenario from issue #1644."""
        df_a = spark.createDataFrame([("A", 1), ("B", 2)], ["col1", "col2"])
        df_b = spark.createDataFrame([("A", 100), ("C", 300)], ["col1", "col3"])

        result = df_a.join(df_b, "col1", "fullouter")
        rows = {_row_val(r, "col1"): r for r in result.collect()}

        assert set(rows) == {"A", "B", "C"}
        assert _row_val(rows["A"], "col2") == 1
        assert _row_val(rows["A"], "col3") == 100
        assert _row_val(rows["B"], "col2") == 2
        assert _row_val(rows["B"], "col3") is None
        assert _row_val(rows["C"], "col2") is None
        assert _row_val(rows["C"], "col3") == 300

    def test_join_fullouter_equals_outer(self, spark):
        """fullouter should match outer on the same data."""
        df_a = spark.createDataFrame([("A", 1), ("B", 2)], ["col1", "col2"])
        df_b = spark.createDataFrame([("A", 100), ("C", 300)], ["col1", "col3"])

        fullouter_rows = sorted(
            df_a.join(df_b, "col1", "fullouter").collect(),
            key=lambda r: (_row_val(r, "col1") or "",),
        )
        outer_rows = sorted(
            df_a.join(df_b, "col1", "outer").collect(),
            key=lambda r: (_row_val(r, "col1") or "",),
        )
        assert len(fullouter_rows) == len(outer_rows) == 3
        for a, b in zip(fullouter_rows, outer_rows):
            assert _row_val(a, "col1") == _row_val(b, "col1")
            assert _row_val(a, "col2") == _row_val(b, "col2")
            assert _row_val(a, "col3") == _row_val(b, "col3")

    def test_join_full_outer_still_accepted(self, spark):
        """Existing full_outer alias remains supported."""
        df_a = spark.createDataFrame([("A", 1)], ["col1", "col2"])
        df_b = spark.createDataFrame([("A", 100)], ["col1", "col3"])
        row = df_a.join(df_b, "col1", "full_outer").collect()[0]
        assert _row_val(row, "col2") == 1
        assert _row_val(row, "col3") == 100

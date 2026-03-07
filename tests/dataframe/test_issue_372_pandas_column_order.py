"""
Tests for issue #372: Column order when creating DataFrame from Pandas vs list of dicts.

PySpark: createDataFrame(pandas_df) preserves column order as-given; createDataFrame(list_of_dicts)
sorts columns alphabetically. Sparkless now matches both behaviors.
"""

import pytest


class TestIssue372PandasColumnOrder:
    """Test column ordering: Pandas preserves order, list-of-dicts alphabetical."""

    def test_create_dataframe_from_pandas_preserves_column_order(self, spark):
        """Exact scenario from issue #372: Pandas input preserves order (ZZZ, AAA, PPP)."""
        pd = pytest.importorskip("pandas")
        if getattr(pd, "__version__", "") == "0.0.0-mock":
            pytest.skip(
                "PySpark createDataFrame(pandas_df) requires real pandas >= 1.0.5"
            )

        pdf = pd.DataFrame(
            {
                "ZZZ-FirstColumn": ["12", "34", "56", "78"],
                "AAA-SecondColumn": [10, 20, 30, 40],
                "PPP-ThirdColumn": ["ab", "cd", "ef", "gh"],
            }
        )
        df = spark.createDataFrame(pdf)
        cols = df.columns
        assert cols == ["ZZZ-FirstColumn", "AAA-SecondColumn", "PPP-ThirdColumn"], (
            f"Expected Pandas column order, got {cols}"
        )
        rows = df.collect()
        assert len(rows) == 4
        assert rows[0]["ZZZ-FirstColumn"] == "12"
        assert rows[0]["AAA-SecondColumn"] == 10
        assert rows[0]["PPP-ThirdColumn"] == "ab"

    def test_create_dataframe_from_list_of_dicts_alphabetical(self, spark):
        """List of dicts: columns sorted alphabetically (PySpark behavior)."""
        data = [
            {"ZZZ-FirstColumn": "12", "AAA-SecondColumn": 10, "PPP-ThirdColumn": "ab"},
            {"ZZZ-FirstColumn": "34", "AAA-SecondColumn": 20, "PPP-ThirdColumn": "cd"},
        ]
        df = spark.createDataFrame(data)
        cols = df.columns
        assert cols == ["AAA-SecondColumn", "PPP-ThirdColumn", "ZZZ-FirstColumn"], (
            f"Expected alphabetical order, got {cols}"
        )
        rows = df.collect()
        assert len(rows) == 2
        assert rows[0]["AAA-SecondColumn"] == 10
        assert rows[0]["ZZZ-FirstColumn"] == "12"

    def test_pandas_column_order_show(self, spark):
        """Pandas DataFrame: show() displays columns in Pandas order."""
        pd = pytest.importorskip("pandas")
        if getattr(pd, "__version__", "") == "0.0.0-mock":
            pytest.skip(
                "PySpark createDataFrame(pandas_df) requires real pandas >= 1.0.5"
            )

        pdf = pd.DataFrame({"B-second": [1, 2], "A-first": [10, 20]})
        df = spark.createDataFrame(pdf)
        df.show()
        assert df.columns == ["B-second", "A-first"]

    def test_list_of_dicts_single_column(self, spark):
        """List of dicts with one column: order is that single column (alphabetically trivial)."""
        data = [{"only_col": 1}, {"only_col": 2}]
        df = spark.createDataFrame(data)
        assert df.columns == ["only_col"]
        rows = df.collect()
        assert len(rows) == 2 and rows[0]["only_col"] == 1

    def test_list_of_dicts_many_columns_alphabetical(self, spark):
        """List of dicts with many columns: always alphabetical regardless of dict key order."""
        data = [
            {"z": 1, "a": 10, "m": 100},
            {"z": 2, "a": 20, "m": 200},
        ]
        df = spark.createDataFrame(data)
        assert df.columns == ["a", "m", "z"]
        rows = df.collect()
        assert rows[0]["a"] == 10 and rows[0]["z"] == 1

    def test_list_of_dicts_schema_field_order_alphabetical(self, spark):
        """Schema field order from list-of-dicts matches alphabetical column order."""
        data = [{"C": 1, "A": 2, "B": 3}]
        df = spark.createDataFrame(data)
        field_names = [f.name for f in df.schema.fields]
        assert field_names == ["A", "B", "C"]

    def test_list_of_dicts_collect_preserves_alphabetical_order(self, spark):
        """Columns from list-of-dicts are alphabetical; collect returns correct values."""
        data = [
            {"x": "a", "y": 1},
            {"x": "b", "y": 2},
        ]
        df = spark.createDataFrame(data)
        assert df.columns == ["x", "y"]
        rows = df.collect()
        assert len(rows) == 2
        assert rows[0]["x"] == "a" and rows[0]["y"] == 1
        assert rows[1]["x"] == "b" and rows[1]["y"] == 2

    def test_list_of_dicts_then_select_preserves_selected_order(self, spark):
        """Select column order is determined by select(), not source order."""
        data = [{"a": 1, "b": 2, "c": 3}]
        df = spark.createDataFrame(data)
        # Source columns are alphabetical a,b,c; select in custom order
        result = df.select("c", "a", "b")
        assert result.columns == ["c", "a", "b"]
        row = result.collect()[0]
        assert row["c"] == 3 and row["a"] == 1 and row["b"] == 2

    def test_list_of_dicts_sparse_keys_alphabetical(self, spark):
        """Sparse rows (different keys): all keys collected and ordered alphabetically."""
        data = [
            {"a": 1, "b": 2},
            {"a": 3, "c": 4},
        ]
        df = spark.createDataFrame(data)
        assert df.columns == ["a", "b", "c"]
        rows = df.collect()
        assert rows[0]["a"] == 1 and rows[0]["b"] == 2 and rows[0]["c"] is None
        assert rows[1]["a"] == 3 and rows[1]["b"] is None and rows[1]["c"] == 4

    def test_pandas_single_column_order_preserved(self, spark):
        """Pandas DataFrame with one column: order preserved."""
        pd = pytest.importorskip("pandas")
        if getattr(pd, "__version__", "") == "0.0.0-mock":
            pytest.skip(
                "PySpark createDataFrame(pandas_df) requires real pandas >= 1.0.5"
            )

        pdf = pd.DataFrame({"only": [1, 2, 3]})
        df = spark.createDataFrame(pdf)
        assert df.columns == ["only"]
        assert [r["only"] for r in df.collect()] == [1, 2, 3]

    def test_pandas_schema_field_order_matches_columns(self, spark):
        """When created from Pandas, schema field order matches Pandas column order."""
        pd = pytest.importorskip("pandas")
        if getattr(pd, "__version__", "") == "0.0.0-mock":
            pytest.skip(
                "PySpark createDataFrame(pandas_df) requires real pandas >= 1.0.5"
            )

        pdf = pd.DataFrame({"first": [1], "second": [2], "third": [3]})
        df = spark.createDataFrame(pdf)
        field_names = [f.name for f in df.schema.fields]
        assert field_names == ["first", "second", "third"]

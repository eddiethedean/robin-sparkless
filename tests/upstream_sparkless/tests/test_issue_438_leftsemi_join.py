"""
Tests for issue #438: leftsemi join incorrectly returns columns from right DataFrame.

PySpark leftsemi/left_semi join returns only columns from the left DataFrame.
Sparkless was incorrectly including right-side columns in the result.
"""


class TestIssue438LeftsemiJoin:
    """Test leftsemi join returns only left DataFrame columns (issue #438)."""

    def test_leftsemi_join_excludes_right_columns(self, spark):
        """Exact scenario from issue #438: leftsemi should not include V2."""
        df1 = spark.createDataFrame(
            [
                {"Name": "Alice", "V1": 1},
                {"Name": "Bob", "V1": 2},
            ]
        )
        df2 = spark.createDataFrame(
            [
                {"Name": "Alice", "V2": 3},
            ]
        )
        result = df1.join(df2, on="Name", how="leftsemi")
        rows = result.collect()

        assert len(rows) == 1
        row = rows[0]
        row_dict = row.asDict() if hasattr(row, "asDict") else dict(row)
        assert row_dict["Name"] == "Alice"
        assert row_dict["V1"] == 1
        assert "V2" not in row_dict

    def test_left_semi_join_excludes_right_columns(self, spark):
        """left_semi (underscore) alias also excludes right columns."""
        df1 = spark.createDataFrame(
            [{"A": 1, "B": 10}, {"A": 2, "B": 20}, {"A": 3, "B": 30}]
        )
        df2 = spark.createDataFrame([{"A": 1, "C": 100}, {"A": 3, "C": 300}])
        result = df1.join(df2, on="A", how="left_semi")
        rows = result.collect()

        assert len(rows) == 2
        for row in rows:
            row_dict = row.asDict() if hasattr(row, "asDict") else dict(row)
            assert "A" in row_dict
            assert "B" in row_dict
            assert "C" not in row_dict

    def test_leftsemi_join_with_column_expression(self, spark):
        """leftsemi with Column expression (left.x == right.x) excludes right columns."""
        df1 = spark.createDataFrame(
            [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
                {"id": 3, "name": "Charlie"},
            ]
        )
        df2 = spark.createDataFrame([{"id": 1, "dept": "IT"}, {"id": 3, "dept": "HR"}])
        result = df1.join(df2, df1.id == df2.id, "leftsemi")
        rows = result.collect()

        assert len(rows) == 2  # id 1 and 3 match
        col_names = result.columns
        assert "id" in col_names
        assert "name" in col_names
        assert "dept" not in col_names

    def test_leftanti_join_excludes_right_columns(self, spark):
        """leftanti/left_anti join returns only left columns."""
        df1 = spark.createDataFrame(
            [{"K": 1, "V": 10}, {"K": 2, "V": 20}, {"K": 3, "V": 30}]
        )
        df2 = spark.createDataFrame([{"K": 2, "X": 999}])  # Only K=2 in right
        result = df1.join(df2, on="K", how="leftanti")
        rows = result.collect()

        assert len(rows) == 2  # K=1 and K=3 not in right
        for row in rows:
            row_dict = row.asDict() if hasattr(row, "asDict") else dict(row)
            assert "K" in row_dict
            assert "V" in row_dict
            assert "X" not in row_dict

    def test_leftsemi_join_multiple_keys(self, spark):
        """leftsemi with multiple join keys excludes right columns."""
        df1 = spark.createDataFrame(
            [
                {"A": 1, "B": 10, "C": 100},
                {"A": 1, "B": 20, "C": 200},
                {"A": 2, "B": 10, "C": 300},
            ]
        )
        df2 = spark.createDataFrame([{"A": 1, "B": 10, "D": 999}])
        result = df1.join(df2, on=["A", "B"], how="leftsemi")
        rows = result.collect()

        assert len(rows) == 1
        row_dict = rows[0].asDict() if hasattr(rows[0], "asDict") else dict(rows[0])
        assert row_dict["A"] == 1 and row_dict["B"] == 10 and row_dict["C"] == 100
        assert "D" not in row_dict

    def test_leftsemi_join_no_matches(self, spark):
        """leftsemi with no matches returns empty, schema has only left columns."""
        df1 = spark.createDataFrame([{"X": 1, "Y": 2}])
        df2 = spark.createDataFrame([{"X": 99, "Z": 3}])  # No overlap on X
        result = df1.join(df2, on="X", how="leftsemi")
        rows = result.collect()

        assert len(rows) == 0
        assert result.columns == ["X", "Y"]
        assert "Z" not in result.columns

    def test_leftsemi_join_all_match(self, spark):
        """leftsemi when all left rows match returns only left columns."""
        df1 = spark.createDataFrame([{"K": 1}, {"K": 2}])
        df2 = spark.createDataFrame([{"K": 1}, {"K": 2}, {"K": 3}])
        result = df1.join(df2, on="K", how="leftsemi")
        rows = result.collect()

        assert len(rows) == 2
        assert result.columns == ["K"]

    def test_leftsemi_join_schema_only_left_columns(self, spark):
        """Verify schema contains only left DataFrame fields."""
        df1 = spark.createDataFrame([{"a": 1, "b": 2}])
        df2 = spark.createDataFrame([{"a": 1, "c": 3, "d": 4}])
        result = df1.join(df2, on="a", how="leftsemi")

        field_names = [f.name for f in result.schema.fields]
        assert field_names == ["a", "b"]
        assert "c" not in field_names
        assert "d" not in field_names

    def test_leftsemi_join_then_select(self, spark):
        """leftsemi + select preserves only left columns in pipeline."""
        df1 = spark.createDataFrame([{"A": 1, "B": 2}, {"A": 2, "B": 4}])
        df2 = spark.createDataFrame([{"A": 1}])
        result = df1.join(df2, on="A", how="leftsemi").select("A", "B")
        rows = result.collect()

        assert len(rows) == 1
        row_dict = rows[0].asDict() if hasattr(rows[0], "asDict") else dict(rows[0])
        assert row_dict == {"A": 1, "B": 2}

"""
Tests for issue #406: ValueError when combining aggregate + cast to DecimalType + drop.

Sparkless raised Unsupported Polars dtype: Decimal(precision=38, scale=6) because
polars_dtype_to_mock_type did not handle pl.Decimal. Fixed by mapping Polars Decimal
to Sparkless DecimalType and using pl.Decimal in mock_type_to_polars_dtype for DecimalType.
"""


def _decimal_value(val):
    """Normalize collected value to float for assertion (PySpark/Sparkless may return Decimal)."""
    if val is None:
        return None
    return float(val)


def _row_dict(row):
    """Backend-agnostic row to dict (Row.asDict() or dict(row))."""
    if hasattr(row, "asDict"):
        return row.asDict()
    return dict(row)


class TestIssue406AggregateCastDecimalDrop:
    """Test aggregate + cast(DecimalType) + drop (issue #406)."""

    def test_groupby_agg_cast_decimal_drop(self, spark):
        """Exact scenario from issue #406: groupBy, agg with sum().cast(DecimalType(38,6)), drop."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        F = imports.F
        DecimalType = imports.DecimalType
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1.234},
                {"Name": "Alice", "Value": 1.234},
                {"Name": "Bob", "Value": 2.345},
            ]
        )
        df = df.groupBy("Name").agg(F.sum("Value").cast(DecimalType(38, 6)))
        df = df.drop("Name")
        df.show()
        rows = df.collect()
        assert len(rows) == 2
        col_name = list(_row_dict(rows[0]).keys())[0]
        values = [_decimal_value(_row_dict(r)[col_name]) for r in rows]
        assert sorted(values) == [2.345, 2.468]

    def test_agg_cast_decimal_drop_different_precision_scale(self, spark):
        """DecimalType(10, 2) and DecimalType(5, 1) with sum + drop."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        F = imports.F
        DecimalType = imports.DecimalType
        df = spark.createDataFrame(
            [{"k": "a", "v": 10.5}, {"k": "a", "v": 20.5}, {"k": "b", "v": 3.0}]
        )
        df = df.groupBy("k").agg(
            F.sum("v").cast(DecimalType(10, 2)),
            F.count("v").cast(DecimalType(5, 0)),
        )
        df = df.drop("k")
        rows = df.collect()
        assert len(rows) == 2
        # One row sum=31.0 count=2, other sum=3.0 count=1
        row_dicts = [_row_dict(r) for r in rows]
        cols = list(row_dicts[0].keys())
        assert len(cols) == 2
        sums = sorted(_decimal_value(d[cols[0]]) for d in row_dicts)
        counts = sorted(_decimal_value(d[cols[1]]) for d in row_dicts)
        assert sums == [3.0, 31.0]
        assert counts == [1.0, 2.0]

    def test_agg_cast_decimal_drop_with_nulls(self, spark):
        """Group with nulls in value; sum cast to Decimal + drop key."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        F = imports.F
        DecimalType = imports.DecimalType
        df = spark.createDataFrame(
            [
                {"name": "x", "val": 1.0},
                {"name": "x", "val": None},
                {"name": "y", "val": 2.0},
            ]
        )
        df = df.groupBy("name").agg(F.sum("val").cast(DecimalType(38, 2)))
        df = df.drop("name")
        rows = df.collect()
        assert len(rows) == 2
        col_name = list(_row_dict(rows[0]).keys())[0]
        values = [_decimal_value(_row_dict(r)[col_name]) for r in rows]
        # x: 1.0 + null = 1.0; y: 2.0
        assert sorted(values) == [1.0, 2.0]

    def test_avg_cast_decimal_drop(self, spark):
        """avg() cast to Decimal + drop (not just sum)."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        F = imports.F
        DecimalType = imports.DecimalType
        df = spark.createDataFrame(
            [
                {"g": "A", "x": 10.0},
                {"g": "A", "x": 20.0},
                {"g": "B", "x": 5.0},
            ]
        )
        df = df.groupBy("g").agg(F.avg("x").cast(DecimalType(10, 2)))
        df = df.drop("g")
        rows = df.collect()
        assert len(rows) == 2
        col_name = list(_row_dict(rows[0]).keys())[0]
        values = sorted(_decimal_value(_row_dict(r)[col_name]) for r in rows)
        assert values == [5.0, 15.0]

    def test_min_max_cast_decimal_drop(self, spark):
        """min() and max() cast to Decimal + drop key."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        F = imports.F
        DecimalType = imports.DecimalType
        df = spark.createDataFrame(
            [{"k": "a", "v": 5}, {"k": "a", "v": 15}, {"k": "b", "v": 10}]
        )
        df = df.groupBy("k").agg(
            F.min("v").cast(DecimalType(5, 0)),
            F.max("v").cast(DecimalType(5, 0)),
        )
        df = df.drop("k")
        rows = df.collect()
        assert len(rows) == 2
        row_dicts = [_row_dict(r) for r in rows]
        cols = sorted(row_dicts[0].keys())
        # Column order may be min then max or max then min; collect pairs
        pairs = [tuple(sorted(_decimal_value(d[c]) for c in cols)) for d in row_dicts]
        # (5, 15) for group a, (10, 10) for group b
        assert set(pairs) == {(5.0, 15.0), (10.0, 10.0)}

    def test_agg_cast_decimal_drop_then_show_and_collect(self, spark):
        """show() then collect() after aggregate + cast Decimal + drop."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        F = imports.F
        DecimalType = imports.DecimalType
        df = spark.createDataFrame(
            [{"id": 1, "amt": 1.5}, {"id": 1, "amt": 2.5}, {"id": 2, "amt": 3.0}]
        )
        df = df.groupBy("id").agg(F.sum("amt").cast(DecimalType(38, 2)))
        df = df.drop("id")
        df.show()
        rows = df.collect()
        assert len(rows) == 2
        col_name = list(_row_dict(rows[0]).keys())[0]
        values = sorted(_decimal_value(_row_dict(r)[col_name]) for r in rows)
        assert values == [3.0, 4.0]

    def test_single_group_agg_cast_decimal_drop(self, spark):
        """Single group: one row after agg; drop key leaves one decimal column."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        F = imports.F
        DecimalType = imports.DecimalType
        df = spark.createDataFrame([{"x": "only", "y": 1.0}, {"x": "only", "y": 2.0}])
        df = df.groupBy("x").agg(F.sum("y").cast(DecimalType(10, 0)))
        df = df.drop("x")
        rows = df.collect()
        assert len(rows) == 1
        col_name = list(_row_dict(rows[0]).keys())[0]
        assert _decimal_value(_row_dict(rows[0])[col_name]) == 3.0

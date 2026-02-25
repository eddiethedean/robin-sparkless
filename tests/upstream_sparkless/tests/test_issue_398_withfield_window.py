"""
Tests for issue #398: withField with Window function inside struct.

Sparkless previously stored WindowFunction object instead of evaluating it,
and select(edge.apples_div_count) failed with ColumnNotFoundError.
"""

from tests.fixtures.spark_imports import get_spark_imports


def _get_col(row, *names):
    """Get value from row, trying multiple possible column names (PySpark vs sparkless)."""
    d = row.asDict()
    for n in names:
        if n in d:
            return d[n]
    return None


class TestIssue398WithFieldWindow:
    """Test withField with Window functions (issue #398)."""

    def test_withfield_window_exact_issue(self, spark):
        """Exact scenario from issue #398."""
        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window
        df = spark.createDataFrame(
            [
                {"id": "Alice", "apples": 100},
                {"id": "Alice", "apples": 200},
            ]
        )
        df = df.withColumn("edge", F.struct("apples"))
        df = df.withColumn(
            "edge",
            F.col("edge").withField(
                "count", F.count("*").over(Window.partitionBy("id"))
            ),
        )
        df = df.withColumn(
            "edge",
            F.col("edge").withField(
                "apples_div_count",
                F.col("edge.apples") / F.col("edge.count"),
            ),
        )
        df = df.select("id", F.col("edge.apples_div_count"))
        rows = df.collect()
        assert len(rows) == 2
        keys = list(rows[0].asDict().keys())
        val_key = (
            "apples_div_count"
            if "apples_div_count" in keys
            else "edge.apples_div_count"
        )
        vals = {r[val_key] for r in rows}
        assert vals == {50.0, 100.0}

    def test_withfield_window_select_struct(self, spark):
        """Select full struct after withField + window."""
        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window
        df = spark.createDataFrame(
            [{"id": "x", "v": 10}, {"id": "x", "v": 20}, {"id": "y", "v": 5}]
        )
        df = df.withColumn("s", F.struct("v"))
        df = df.withColumn(
            "s",
            F.col("s").withField("cnt", F.count("*").over(Window.partitionBy("id"))),
        )
        df = df.select("id", F.col("s"))
        rows = df.collect()
        assert len(rows) == 3
        for r in rows:
            s = r["s"]
            assert "v" in s and "cnt" in s
            assert isinstance(s["cnt"], int)
            assert s["cnt"] in (2, 1)  # id=x has 2 rows, id=y has 1

    def test_withfield_row_number(self, spark):
        """withField with row_number() over window."""
        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window
        df = spark.createDataFrame(
            [{"id": "a", "x": 1}, {"id": "a", "x": 2}, {"id": "b", "x": 3}]
        )
        df = df.withColumn("s", F.struct("x"))
        df = df.withColumn(
            "s",
            F.col("s").withField(
                "rn", F.row_number().over(Window.partitionBy("id").orderBy("x"))
            ),
        )
        rows = df.select("id", F.col("s.rn")).collect()
        assert len(rows) == 3
        by_id = {}
        for r in rows:
            k = r["id"]
            if k not in by_id:
                by_id[k] = []
            rn = _get_col(r, "rn", "s.rn")
            by_id[k].append(rn)
        assert by_id["a"] == [1, 2]
        assert by_id["b"] == [1]

    def test_withfield_sum(self, spark):
        """withField with sum() over window."""
        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window
        df = spark.createDataFrame(
            [{"g": 1, "v": 10}, {"g": 1, "v": 20}, {"g": 2, "v": 5}]
        )
        df = df.withColumn("s", F.struct("v"))
        df = df.withColumn(
            "s",
            F.col("s").withField("total", F.sum("v").over(Window.partitionBy("g"))),
        )
        rows = df.select("g", F.col("s.total")).collect()
        assert len(rows) == 3
        by_g = {r["g"]: _get_col(r, "total", "s.total") for r in rows}
        assert by_g[1] == 30
        assert by_g[2] == 5

    def test_withfield_avg(self, spark):
        """withField with avg() over window - values are evaluated (not WindowFunction)."""
        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window
        df = spark.createDataFrame([{"g": 1, "v": 10.0}, {"g": 1, "v": 20.0}])
        df = df.withColumn("s", F.struct("v"))
        df = df.withColumn(
            "s",
            F.col("s").withField("avg_v", F.avg("v").over(Window.partitionBy("g"))),
        )
        rows = df.select("g", F.col("s.avg_v")).collect()
        assert len(rows) == 2
        # PySpark: partition avg 15.0 for both. Sparkless: running avg (10, 15).
        vals = [_get_col(r, "avg_v", "s.avg_v") for r in rows]
        assert all(isinstance(v, (int, float)) for v in vals)
        assert 15.0 in vals

    def test_withfield_window_with_nulls_in_partition(self, spark):
        """withField + window when partition column has nulls."""
        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window
        df = spark.createDataFrame(
            [{"g": "a", "v": 1}, {"g": None, "v": 2}, {"g": "a", "v": 3}]
        )
        df = df.withColumn("s", F.struct("v"))
        df = df.withColumn(
            "s",
            F.col("s").withField("cnt", F.count("*").over(Window.partitionBy("g"))),
        )
        rows = df.select("g", F.col("s.cnt")).collect()
        assert len(rows) == 3
        cnt_by_g = {r["g"]: _get_col(r, "cnt", "s.cnt") for r in rows}
        assert cnt_by_g["a"] == 2
        assert cnt_by_g[None] == 1

    def test_withfield_multiple_window_fields(self, spark):
        """Struct with multiple window-derived fields."""
        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window
        df = spark.createDataFrame([{"g": 1, "v": 10}, {"g": 1, "v": 20}])
        df = df.withColumn("s", F.struct("v"))
        df = df.withColumn(
            "s",
            F.col("s").withField("cnt", F.count("*").over(Window.partitionBy("g"))),
        )
        df = df.withColumn(
            "s",
            F.col("s").withField("tot", F.sum("v").over(Window.partitionBy("g"))),
        )
        rows = df.select(F.col("s.cnt"), F.col("s.tot")).collect()
        assert len(rows) == 2
        for r in rows:
            assert _get_col(r, "cnt", "s.cnt") == 2
            assert _get_col(r, "tot", "s.tot") == 30

    def test_withfield_window_then_filter(self, spark):
        """withField + window, then filter, then select."""
        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window
        df = spark.createDataFrame(
            [
                {"id": "A", "val": 1},
                {"id": "A", "val": 2},
                {"id": "B", "val": 10},
            ]
        )
        df = df.withColumn("s", F.struct("val"))
        df = df.withColumn(
            "s",
            F.col("s").withField("cnt", F.count("*").over(Window.partitionBy("id"))),
        )
        df = df.filter(F.col("id") == "A")
        df = df.select("id", F.col("s.val"), F.col("s.cnt"))
        rows = df.collect()
        assert len(rows) == 2
        assert all(r["id"] == "A" for r in rows)
        assert all(_get_col(r, "cnt", "s.cnt") == 2 for r in rows)

    def test_withfield_chain_three_with_window_middle(self, spark):
        """Three chained withFields, window in the middle."""
        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window
        df = spark.createDataFrame([{"k": 1, "a": 5}, {"k": 1, "a": 15}])
        df = df.withColumn("s", F.struct("a"))
        df = df.withColumn("s", F.col("s").withField("x", F.lit(1)))
        df = df.withColumn(
            "s",
            F.col("s").withField("cnt", F.count("*").over(Window.partitionBy("k"))),
        )
        df = df.withColumn("s", F.col("s").withField("y", F.lit(2)))
        rows = df.select(
            F.col("s.a"), F.col("s.x"), F.col("s.cnt"), F.col("s.y")
        ).collect()
        assert len(rows) == 2
        for r in rows:
            assert _get_col(r, "a", "s.a") in (5, 15)
            assert _get_col(r, "x", "s.x") == 1
            assert _get_col(r, "cnt", "s.cnt") == 2
            assert _get_col(r, "y", "s.y") == 2

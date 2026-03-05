"""
Tests for issue #414: row_number().over(Window.partitionBy(...).orderBy(F.desc(...)))
must not raise TypeError: over() got an unexpected keyword argument 'descending'.

Polars over() expects descending: bool and supports it only from 1.22+.
Sparkless supports polars>=0.20.0, so we must handle older Polars and avoid
passing List[bool] where a single bool is expected.

Uses get_spark_imports() for backend-aware F/Window - runs with both
sparkless and PySpark (MOCK_SPARK_TEST_BACKEND=pyspark).
"""

from tests.fixtures.spark_imports import get_spark_imports


def _norm(val):
    """Normalize for backend-agnostic assertion (PySpark may return int/long/float)."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val) if isinstance(val, float) else int(val)
    return val


class TestIssue414RowNumberOverDescending:
    """Regression tests for row_number().over() with orderBy(F.desc(...))."""

    def test_row_number_over_partition_order_desc(self, spark):
        """row_number().over(partitionBy, orderBy(F.desc)) must not raise."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame(
            [("a", 1), ("a", 2), ("a", 3), ("b", 1), ("b", 2)],
            ["id", "value"],
        )
        window = Window.partitionBy("id").orderBy(F.desc("value"))
        result = df.select(
            F.row_number().over(window).alias("rn"), "id", "value"
        ).collect()
        assert len(result) == 5
        # id='a': value 3,2,1 -> rn 1,2,3 (descending order)
        a_rows = {r["value"]: r["rn"] for r in result if r["id"] == "a"}
        assert a_rows[3] == 1
        assert a_rows[2] == 2
        assert a_rows[1] == 3
        # id='b': value 2,1 -> rn 1,2
        b_rows = {r["value"]: r["rn"] for r in result if r["id"] == "b"}
        assert b_rows[2] == 1
        assert b_rows[1] == 2

    def test_row_number_over_order_desc_no_partition(self, spark):
        """row_number().over(orderBy(F.desc)) without partition must not raise."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame(
            [(1,), (2,), (3,)],
            ["value"],
        )
        window = Window.orderBy(F.desc("value"))
        result = df.select(F.row_number().over(window).alias("rn"), "value").collect()
        assert len(result) == 3
        assert _norm(result[0]["rn"]) == 1 and _norm(result[0]["value"]) == 3
        assert _norm(result[1]["rn"]) == 2 and _norm(result[1]["value"]) == 2
        assert _norm(result[2]["rn"]) == 3 and _norm(result[2]["value"]) == 1

    def test_row_number_over_partition_order_asc(self, spark):
        """row_number().over(partitionBy, orderBy(F.asc)) must still work."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame(
            [("a", 1), ("a", 2), ("b", 1)],
            ["id", "value"],
        )
        window = Window.partitionBy("id").orderBy(F.asc("value"))
        result = df.select(
            F.row_number().over(window).alias("rn"), "id", "value"
        ).collect()
        assert len(result) == 3
        rn_id_val = [(_norm(r["rn"]), r["id"], _norm(r["value"])) for r in result]
        assert (1, "a", 1) in rn_id_val
        assert (2, "a", 2) in rn_id_val
        assert (1, "b", 1) in rn_id_val

    def test_row_number_with_column_pattern(self, spark):
        """withColumn + row_number().over(partitionBy.orderBy(desc)) - common pattern."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame(
            [("x", 10), ("x", 20), ("y", 5)],
            ["grp", "val"],
        )
        w = Window.partitionBy("grp").orderBy(F.desc("val"))
        df = df.withColumn("rn", F.row_number().over(w))
        rows = df.collect()
        assert len(rows) == 3
        by_grp = {(r["grp"], r["val"]): _norm(r["rn"]) for r in rows}
        assert by_grp[("x", 20)] == 1
        assert by_grp[("x", 10)] == 2
        assert by_grp[("y", 5)] == 1

    def test_sum_over_partition_order_desc(self, spark):
        """sum().over(partitionBy, orderBy(F.desc)) - running sum descending."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame(
            [("a", 1), ("a", 2), ("a", 3), ("b", 10)],
            ["id", "value"],
        )
        w = Window.partitionBy("id").orderBy(F.desc("value"))
        df = df.withColumn("running_sum", F.sum("value").over(w))
        rows = df.collect()
        assert len(rows) == 4
        # id='a' desc: value 3,2,1 -> running_sum 3, 5, 6
        a_rows = {r["value"]: _norm(r["running_sum"]) for r in rows if r["id"] == "a"}
        assert a_rows[3] == 3
        assert a_rows[2] == 5
        assert a_rows[1] == 6
        b_row = next(r for r in rows if r["id"] == "b" and r["value"] == 10)
        assert _norm(b_row["running_sum"]) == 10

    def test_lag_over_partition_order_desc(self, spark):
        """lag().over(partitionBy, orderBy(F.desc)) - previous row in desc order."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame(
            [("a", 3), ("a", 2), ("a", 1), ("b", 2), ("b", 1)],
            ["id", "value"],
        )
        w = Window.partitionBy("id").orderBy(F.desc("value"))
        df = df.withColumn("prev", F.lag("value", 1).over(w))
        rows = df.collect()
        assert len(rows) == 5
        by_id_val = {(r["id"], r["value"]): r["prev"] for r in rows}
        assert by_id_val[("a", 3)] is None
        assert _norm(by_id_val[("a", 2)]) == 3
        assert _norm(by_id_val[("a", 1)]) == 2
        assert by_id_val[("b", 2)] is None
        assert _norm(by_id_val[("b", 1)]) == 2

    def test_first_over_partition_order_desc(self, spark):
        """first().over(partitionBy, orderBy(F.desc)) - first value in desc order."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame(
            [("a", 1), ("a", 2), ("a", 3), ("b", 10)],
            ["id", "value"],
        )
        w = Window.partitionBy("id").orderBy(F.desc("value"))
        df = df.withColumn("first_val", F.first("value").over(w))
        rows = df.collect()
        assert len(rows) == 4
        # In desc order, first is max value
        for r in rows:
            if r["id"] == "a":
                assert _norm(r["first_val"]) == 3
            else:
                assert _norm(r["first_val"]) == 10

    def test_mixed_order_asc_desc(self, spark):
        """orderBy(F.asc(a), F.desc(b)) - mixed directions must not raise."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame(
            [("a", 1), ("a", 2), ("a", 3), ("b", 1), ("b", 2)],
            ["grp", "val"],
        )
        w = Window.partitionBy("grp").orderBy(F.asc("grp"), F.desc("val"))
        result = df.select(F.row_number().over(w).alias("rn"), "grp", "val").collect()
        assert len(result) == 5
        # grp asc, val desc within partition
        grp_val_rn = {
            (_norm(r["grp"]), _norm(r["val"])): _norm(r["rn"]) for r in result
        }
        assert grp_val_rn[("a", 3)] == 1
        assert grp_val_rn[("a", 2)] == 2
        assert grp_val_rn[("a", 1)] == 3
        assert grp_val_rn[("b", 2)] == 1
        assert grp_val_rn[("b", 1)] == 2

    def test_single_row_per_partition(self, spark):
        """Partition with single row - edge case."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame(
            [("a", 1), ("b", 2), ("c", 3)],
            ["id", "value"],
        )
        w = Window.partitionBy("id").orderBy(F.desc("value"))
        result = df.select(F.row_number().over(w).alias("rn"), "id", "value").collect()
        assert len(result) == 3
        for r in result:
            assert _norm(r["rn"]) == 1

    def test_avg_over_partition_order_desc(self, spark):
        """avg().over(partitionBy, orderBy(F.desc)) - running average descending."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame(
            [("a", 2.0), ("a", 4.0), ("a", 6.0)],
            ["id", "value"],
        )
        w = Window.partitionBy("id").orderBy(F.desc("value"))
        df = df.withColumn("running_avg", F.avg("value").over(w))
        rows = df.collect()
        assert len(rows) == 3
        # Desc order: 6, 4, 2 -> running avg 6, 5, 4
        by_val = {_norm(r["value"]): _norm(r["running_avg"]) for r in rows}
        assert by_val[6] == 6.0
        assert by_val[4] == 5.0
        assert by_val[2] == 4.0

"""
Tests for issue #392: sum() over window returns wrong value when orderBy cols
are subset of partitionBy cols (all rows are peers in RANGE frame semantics).

PySpark uses RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW by default.
Rows with the same ORDER BY value are peers and share the same frame.
When orderBy("Type") and partitionBy("Type"), all rows in partition have same
order key -> both should get partition sum (30), not cumulative (10, 30).

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


class TestIssue392WindowSumPeers:
    """Test sum()/avg() window with orderBy cols subset of partitionBy (issue #392)."""

    def test_sum_order_by_same_as_partition_by(self, spark):
        """Exact scenario from issue #392: partitionBy(Type).orderBy(Type)."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Type": "A", "Value": 10},
                {"Name": "Bob", "Type": "A", "Value": 20},
            ]
        )
        w = Window().partitionBy("Type").orderBy("Type")
        df = df.withColumn("SumValue", F.sum(df.Value).over(w))
        rows = df.collect()
        assert len(rows) == 2
        alice = next(r for r in rows if r["Name"] == "Alice")
        bob = next(r for r in rows if r["Name"] == "Bob")
        assert _norm(alice["SumValue"]) == 30
        assert _norm(bob["SumValue"]) == 30

    def test_sum_order_by_subset_of_partition_by(self, spark):
        """orderBy is subset of partitionBy -> all rows in partition are peers."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame(
            [
                {"Dept": "IT", "Type": "A", "Value": 5},
                {"Dept": "IT", "Type": "A", "Value": 15},
                {"Dept": "HR", "Type": "B", "Value": 10},
            ]
        )
        w = Window.partitionBy("Dept", "Type").orderBy("Type")
        df = df.withColumn("SumValue", F.sum("Value").over(w))
        rows = df.collect()
        it_rows = [r for r in rows if r["Dept"] == "IT"]
        assert len(it_rows) == 2
        assert _norm(it_rows[0]["SumValue"]) == 20
        assert _norm(it_rows[1]["SumValue"]) == 20
        hr_rows = [r for r in rows if r["Dept"] == "HR"]
        assert len(hr_rows) == 1
        assert _norm(hr_rows[0]["SumValue"]) == 10

    def test_sum_order_by_differs_from_partition_running_sum(self, spark):
        """When orderBy is NOT subset of partitionBy, use running sum (cumulative)."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame(
            [
                {"Type": "A", "Name": "Alice", "Value": 10},
                {"Type": "A", "Name": "Bob", "Value": 20},
            ]
        )
        w = Window.partitionBy("Type").orderBy("Name")
        df = df.withColumn("SumValue", F.sum("Value").over(w))
        rows = df.collect()
        alice = next(r for r in rows if r["Name"] == "Alice")
        bob = next(r for r in rows if r["Name"] == "Bob")
        assert _norm(alice["SumValue"]) == 10
        assert _norm(bob["SumValue"]) == 30

    def test_avg_order_by_subset_of_partition_by(self, spark):
        """AVG with orderBy subset of partitionBy -> peers get partition avg."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame(
            [
                {"Type": "A", "Value": 10},
                {"Type": "A", "Value": 20},
                {"Type": "A", "Value": 30},
            ]
        )
        w = Window.partitionBy("Type").orderBy("Type")
        df = df.withColumn("AvgValue", F.avg("Value").over(w))
        rows = df.collect()
        # All peers -> partition avg = 20.0
        for r in rows:
            assert _norm(r["AvgValue"]) == 20.0

    def test_sum_order_by_col_desc_still_subset(self, spark):
        """orderBy(F.col('Type').desc()) - order col still subset of partitionBy."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame(
            [
                {"Type": "A", "Value": 10},
                {"Type": "A", "Value": 20},
            ]
        )
        w = Window.partitionBy("Type").orderBy(F.col("Type").desc())
        df = df.withColumn("SumValue", F.sum("Value").over(w))
        rows = df.collect()
        for r in rows:
            assert _norm(r["SumValue"]) == 30

    def test_sum_single_row_partition(self, spark):
        """Single row in partition -> sum is that row's value."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame([{"Type": "A", "Value": 42}])
        w = Window.partitionBy("Type").orderBy("Type")
        df = df.withColumn("SumValue", F.sum("Value").over(w))
        rows = df.collect()
        assert len(rows) == 1
        assert _norm(rows[0]["SumValue"]) == 42

    def test_sum_three_rows_all_peers(self, spark):
        """Three rows, all same partition+order key -> all get partition sum."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame(
            [
                {"Type": "X", "Value": 1},
                {"Type": "X", "Value": 2},
                {"Type": "X", "Value": 3},
            ]
        )
        w = Window.partitionBy("Type").orderBy("Type")
        df = df.withColumn("SumValue", F.sum("Value").over(w))
        rows = df.collect()
        assert len(rows) == 3
        for r in rows:
            assert _norm(r["SumValue"]) == 6

    def test_sum_with_nulls_excluded(self, spark):
        """Sum over window excludes nulls (standard SQL behavior)."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame(
            [
                {"Type": "A", "Value": 10},
                {"Type": "A", "Value": None},
                {"Type": "A", "Value": 20},
            ]
        )
        w = Window.partitionBy("Type").orderBy("Type")
        df = df.withColumn("SumValue", F.sum("Value").over(w))
        rows = df.collect()
        # All peers -> sum of non-null = 30
        for r in rows:
            assert _norm(r["SumValue"]) == 30

    def test_sum_order_by_multiple_cols_subset_of_partition_by(self, spark):
        """orderBy(A,B) with partitionBy(A,B) -> all peers."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame(
            [
                {"A": 1, "B": 1, "Value": 5},
                {"A": 1, "B": 1, "Value": 15},
                {"A": 2, "B": 2, "Value": 10},
            ]
        )
        w = Window.partitionBy("A", "B").orderBy("A", "B")
        df = df.withColumn("SumValue", F.sum("Value").over(w))
        rows = df.collect()
        ab11 = [r for r in rows if r["A"] == 1 and r["B"] == 1]
        assert len(ab11) == 2
        for r in ab11:
            assert _norm(r["SumValue"]) == 20
        ab22 = [r for r in rows if r["A"] == 2 and r["B"] == 2]
        assert len(ab22) == 1
        assert _norm(ab22[0]["SumValue"]) == 10

    def test_sum_no_order_by_partition_total(self, spark):
        """No orderBy -> partition sum (baseline, no fix needed)."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame(
            [
                {"Type": "A", "Value": 10},
                {"Type": "A", "Value": 20},
            ]
        )
        w = Window.partitionBy("Type")
        df = df.withColumn("SumValue", F.sum("Value").over(w))
        rows = df.collect()
        for r in rows:
            assert _norm(r["SumValue"]) == 30

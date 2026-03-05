"""
Tests for issue #393: sum() over window on string columns with numeric data.

PySpark coerces string columns to double when using sum(); Polars raises
InvalidOperationError: cum_sum operation not supported for dtype str.
Sparkless now casts string/Utf8 columns to Float64 before sum/avg (PySpark parity).
"""

from tests.fixtures.spark_imports import get_spark_imports


def _norm(val):
    """Normalize for backend-agnostic assertion."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val) if isinstance(val, float) else int(val)
    return val


class TestIssue393SumStringColumn:
    """Test sum()/avg() on string columns with numeric content (issue #393)."""

    def test_sum_string_column_partition_by_order_by(self, spark):
        """Exact scenario from issue #393: Value as string '10','20'."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Type": "A", "Value": "10"},
                {"Name": "Bob", "Type": "A", "Value": "20"},
            ]
        )
        w = Window().partitionBy("Type").orderBy("Type")
        df = df.withColumn("SumValue", F.sum(df.Value).over(w))
        rows = df.collect()
        assert len(rows) == 2
        for r in rows:
            assert _norm(r["SumValue"]) == 30.0

    def test_avg_string_column(self, spark):
        """avg() on string column with numeric content."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame(
            [
                {"Type": "A", "Value": "10"},
                {"Type": "A", "Value": "20"},
                {"Type": "A", "Value": "30"},
            ]
        )
        w = Window().partitionBy("Type").orderBy("Type")
        df = df.withColumn("AvgValue", F.avg("Value").over(w))
        rows = df.collect()
        for r in rows:
            assert _norm(r["AvgValue"]) == 20.0

    def test_sum_string_column_running_sum(self, spark):
        """sum() on string column with orderBy different from partitionBy -> running sum."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame(
            [
                {"Type": "A", "Name": "Alice", "Value": "10"},
                {"Type": "A", "Name": "Bob", "Value": "20"},
            ]
        )
        w = Window.partitionBy("Type").orderBy("Name")
        df = df.withColumn("SumValue", F.sum("Value").over(w))
        rows = df.collect()
        alice = next(r for r in rows if r["Name"] == "Alice")
        bob = next(r for r in rows if r["Name"] == "Bob")
        assert _norm(alice["SumValue"]) == 10.0
        assert _norm(bob["SumValue"]) == 30.0

    def test_sum_string_column_with_show(self, spark):
        """Exact issue scenario: withColumn + show() (issue #393)."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Type": "A", "Value": "10"},
                {"Name": "Bob", "Type": "A", "Value": "20"},
            ]
        )
        w = Window().partitionBy("Type").orderBy("Type")
        df = df.withColumn("SumValue", F.sum(df.Value).over(w))
        df.show()
        rows = df.collect()
        for r in rows:
            assert _norm(r["SumValue"]) == 30.0

    def test_sum_string_column_with_nulls(self, spark):
        """sum() on string column with nulls - nulls excluded from sum."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame(
            [
                {"Type": "A", "Value": "10"},
                {"Type": "A", "Value": None},
                {"Type": "A", "Value": "20"},
            ]
        )
        w = Window().partitionBy("Type").orderBy("Type")
        df = df.withColumn("SumValue", F.sum("Value").over(w))
        rows = df.collect()
        for r in rows:
            assert _norm(r["SumValue"]) == 30.0

    def test_sum_string_column_no_partition_running_sum(self, spark):
        """sum() on string column with orderBy only -> running sum (no partition = whole table)."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame(
            [
                {"Value": "5"},
                {"Value": "15"},
                {"Value": "10"},
            ]
        )
        # orderBy only -> RANGE frame -> running sum; lexicographic sort: "10","15","5"
        w = Window.orderBy("Value")
        df = df.withColumn("SumValue", F.sum("Value").over(w))
        rows = df.collect()
        # PySpark: cumsum in sort order -> 10, 25, 30
        sums = sorted([_norm(r["SumValue"]) for r in rows])
        assert sums == [10.0, 25.0, 30.0]

    def test_avg_string_column_multiple_partitions(self, spark):
        """avg() on string column with multiple partitions."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame(
            [
                {"Type": "A", "Value": "10"},
                {"Type": "A", "Value": "20"},
                {"Type": "B", "Value": "30"},
                {"Type": "B", "Value": "50"},
            ]
        )
        w = Window().partitionBy("Type").orderBy("Type")
        df = df.withColumn("AvgValue", F.avg("Value").over(w))
        rows = df.collect()
        a_rows = [r for r in rows if r["Type"] == "A"]
        b_rows = [r for r in rows if r["Type"] == "B"]
        for r in a_rows:
            assert _norm(r["AvgValue"]) == 15.0
        for r in b_rows:
            assert _norm(r["AvgValue"]) == 40.0

    def test_sum_string_column_decimal_like(self, spark):
        """sum() on string column with decimal-like values."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame(
            [
                {"Type": "A", "Value": "1.5"},
                {"Type": "A", "Value": "2.5"},
            ]
        )
        w = Window().partitionBy("Type").orderBy("Type")
        df = df.withColumn("SumValue", F.sum("Value").over(w))
        rows = df.collect()
        for r in rows:
            assert _norm(r["SumValue"]) == 4.0

    def test_sum_string_column_single_row_partition(self, spark):
        """sum() on string column with single row in partition."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame([{"Type": "A", "Value": "42"}])
        w = Window().partitionBy("Type").orderBy("Type")
        df = df.withColumn("SumValue", F.sum("Value").over(w))
        rows = df.collect()
        assert len(rows) == 1
        assert _norm(rows[0]["SumValue"]) == 42.0

    def test_sum_string_column_select_after(self, spark):
        """sum() on string column, then select and filter."""
        imports = get_spark_imports()
        F, Window = imports.F, imports.Window
        df = spark.createDataFrame(
            [
                {"Type": "A", "Value": "10"},
                {"Type": "A", "Value": "20"},
                {"Type": "B", "Value": "5"},
            ]
        )
        w = Window().partitionBy("Type").orderBy("Type")
        df = df.withColumn("SumValue", F.sum("Value").over(w)).select(
            "Type", "Value", "SumValue"
        )
        rows = df.collect()
        assert len(rows) == 3
        a_rows = [r for r in rows if r["Type"] == "A"]
        assert _norm(a_rows[0]["SumValue"]) == 30.0
        assert _norm(a_rows[1]["SumValue"]) == 30.0
        b_rows = [r for r in rows if r["Type"] == "B"]
        assert _norm(b_rows[0]["SumValue"]) == 5.0

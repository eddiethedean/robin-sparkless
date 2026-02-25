"""
Tests for issue #437: F.mean() on string columns with numeric content.

PySpark interprets string columns as numeric for mean/avg; sparkless previously
raised TypeError: can't convert type 'str' to numerator/denominator when using
F.mean() on string columns. Sparkless now coerces string columns to numeric
(PySpark parity).
"""

import math

from tests.fixtures.spark_imports import get_spark_imports


def _norm(val):
    """Normalize for backend-agnostic assertion."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val) if isinstance(val, float) else int(val)
    return val


class TestIssue437MeanStringColumn:
    """Test F.mean() on string columns with numeric content (issue #437)."""

    def test_mean_string_column_exact_issue_scenario(self, spark):
        """Exact scenario from issue #437: F.mean() on string X and int Y."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "X": "1", "Y": 1},
                {"Name": "Alice", "X": "2", "Y": 2},
                {"Name": "Alice", "X": "3", "Y": 3},
                {"Name": "Bob", "X": "4", "Y": 4},
            ]
        )
        df = df.groupBy("Name").agg(
            F.mean(F.col("X")).alias("avg(X)"),
            F.mean(F.col("Y")).alias("avg(Y)"),
        )
        rows = df.collect()
        assert len(rows) == 2
        alice = next(r for r in rows if r["Name"] == "Alice")
        bob = next(r for r in rows if r["Name"] == "Bob")
        assert _norm(alice["avg(X)"]) == 2.0
        assert _norm(alice["avg(Y)"]) == 2.0
        assert _norm(bob["avg(X)"]) == 4.0
        assert _norm(bob["avg(Y)"]) == 4.0

    def test_mean_string_column_groupby(self, spark):
        """F.mean() on string column with groupBy."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"Type": "A", "Value": "10"},
                {"Type": "A", "Value": "20"},
                {"Type": "A", "Value": "30"},
                {"Type": "B", "Value": "5"},
                {"Type": "B", "Value": "15"},
            ]
        )
        df = df.groupBy("Type").agg(F.mean("Value"))
        rows = df.collect()
        a_row = next(r for r in rows if r["Type"] == "A")
        b_row = next(r for r in rows if r["Type"] == "B")
        assert _norm(a_row["avg(Value)"]) == 20.0
        assert _norm(b_row["avg(Value)"]) == 10.0

    def test_mean_string_column_decimal_values(self, spark):
        """F.mean() on string column with decimal-like values."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"Type": "A", "Value": "1.5"},
                {"Type": "A", "Value": "2.5"},
                {"Type": "A", "Value": "3.5"},
            ]
        )
        df = df.groupBy("Type").agg(F.mean("Value"))
        rows = df.collect()
        assert len(rows) == 1
        assert abs(_norm(rows[0]["avg(Value)"]) - 2.5) < 1e-9

    def test_mean_string_column_with_nulls(self, spark):
        """F.mean() on string column with nulls - nulls excluded from mean."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"Type": "A", "Value": "10"},
                {"Type": "A", "Value": None},
                {"Type": "A", "Value": "20"},
                {"Type": "A", "Value": "30"},
            ]
        )
        df = df.groupBy("Type").agg(F.mean("Value"))
        rows = df.collect()
        assert len(rows) == 1
        # (10 + 20 + 30) / 3 = 20.0
        assert _norm(rows[0]["avg(Value)"]) == 20.0

    def test_mean_string_column_single_row_per_group(self, spark):
        """F.mean() on string column with single row per group."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"Type": "A", "Value": "42"},
                {"Type": "B", "Value": "100"},
            ]
        )
        df = df.groupBy("Type").agg(F.mean("Value"))
        rows = df.collect()
        assert len(rows) == 2
        a_row = next(r for r in rows if r["Type"] == "A")
        b_row = next(r for r in rows if r["Type"] == "B")
        assert _norm(a_row["avg(Value)"]) == 42.0
        assert _norm(b_row["avg(Value)"]) == 100.0

    def test_avg_string_column_same_as_mean(self, spark):
        """F.avg() on string column - same behavior as F.mean()."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"Type": "A", "Value": "1"},
                {"Type": "A", "Value": "2"},
                {"Type": "A", "Value": "3"},
            ]
        )
        df = df.groupBy("Type").agg(F.avg("Value"))
        rows = df.collect()
        assert len(rows) == 1
        assert _norm(rows[0]["avg(Value)"]) == 2.0

    def test_mean_string_column_multiple_aggregations(self, spark):
        """F.mean() + F.sum() + F.count() on string column."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"Type": "A", "Value": "10"},
                {"Type": "A", "Value": "20"},
                {"Type": "A", "Value": "30"},
                {"Type": "B", "Value": "5"},
            ]
        )
        df = df.groupBy("Type").agg(
            F.mean("Value").alias("avg_val"),
            F.sum("Value").alias("sum_val"),
            F.count("Value").alias("cnt_val"),
        )
        rows = df.collect()
        a_row = next(r for r in rows if r["Type"] == "A")
        b_row = next(r for r in rows if r["Type"] == "B")
        assert _norm(a_row["avg_val"]) == 20.0
        assert _norm(a_row["sum_val"]) == 60.0
        assert a_row["cnt_val"] == 3
        assert _norm(b_row["avg_val"]) == 5.0
        assert _norm(b_row["sum_val"]) == 5.0
        assert b_row["cnt_val"] == 1

    def test_mean_string_column_select_after(self, spark):
        """F.mean() on string column, then select."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"Type": "A", "Value": "10"},
                {"Type": "A", "Value": "20"},
                {"Type": "B", "Value": "5"},
            ]
        )
        result = df.groupBy("Type").agg(F.mean("Value")).select("Type", "avg(Value)")
        rows = result.collect()
        assert len(rows) == 2
        a_row = next(r for r in rows if r["Type"] == "A")
        b_row = next(r for r in rows if r["Type"] == "B")
        assert _norm(a_row["avg(Value)"]) == 15.0
        assert _norm(b_row["avg(Value)"]) == 5.0

    def test_mean_string_column_scientific_notation(self, spark):
        """F.mean() on string column with scientific notation (PySpark parity)."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"Type": "A", "Value": "1e2"},
                {"Type": "A", "Value": "2e2"},
                {"Type": "A", "Value": "3e2"},
            ]
        )
        df = df.groupBy("Type").agg(F.mean("Value"))
        rows = df.collect()
        assert len(rows) == 1
        # 100 + 200 + 300 = 600, mean = 200
        assert abs(_norm(rows[0]["avg(Value)"]) - 200.0) < 1e-6

    def test_mean_string_column_mixed_int_float_strings(self, spark):
        """F.mean() on string column with mixed '1' and '1.0' style values."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"Type": "A", "Value": "1"},
                {"Type": "A", "Value": "2.0"},
                {"Type": "A", "Value": "3"},
            ]
        )
        df = df.groupBy("Type").agg(F.mean("Value"))
        rows = df.collect()
        assert len(rows) == 1
        assert abs(_norm(rows[0]["avg(Value)"]) - 2.0) < 1e-9

    def test_mean_string_column_all_nulls_returns_none(self, spark):
        """F.mean() on string column with all nulls in group returns None/NaN."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"Type": "A", "Value": "10"},
                {"Type": "B", "Value": None},
                {"Type": "B", "Value": None},
            ]
        )
        df = df.groupBy("Type").agg(F.mean("Value"))
        rows = df.collect()
        a_row = next(r for r in rows if r["Type"] == "A")
        b_row = next(r for r in rows if r["Type"] == "B")
        assert _norm(a_row["avg(Value)"]) == 10.0
        # PySpark returns null for empty aggregate; sparkless may return None
        assert b_row["avg(Value)"] is None or (
            isinstance(b_row["avg(Value)"], float) and math.isnan(b_row["avg(Value)"])
        )

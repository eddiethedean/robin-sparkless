"""
Tests for issue #422: fillna(0.0) fails to replace None in calculated numeric columns and integer columns.

- fillna(0.0, subset=["V3"]) did not fill None in column V3 (V1/V2) when V3 was a calculated float column.
- fillna(0.0) did not fill None in integer column V1 when using float fill value; only float columns were filled.

PySpark allows fillna(0.0) to fill both integer and float columns (coerces as needed).

Run with PySpark first: MOCK_SPARK_TEST_BACKEND=pyspark pytest tests/test_issue_422_fillna_float.py -v
Then mock: pytest tests/test_issue_422_fillna_float.py -v
"""

from tests.fixtures.spark_imports import get_spark_imports


def _row_val(row: object, key: str) -> object:
    """Get value from Row or dict (PySpark returns Row, mock may return dict)."""
    if hasattr(row, "__getitem__"):
        return row[key]
    return getattr(row, key, None)


class TestIssue422FillnaFloat:
    """Test fillna(0.0) with calculated columns and integer columns."""

    def test_fillna_float_subset_calculated_column(self, spark, spark_backend):
        """fillna(0.0, subset=['V3']) fills None in calculated float column V3 (V1/V2)."""
        F = get_spark_imports(spark_backend).F

        df = spark.createDataFrame(
            [
                {"Name": "Alice", "V1": 1, "V2": 0},
                {"Name": "Bob", "V1": 0, "V2": 2},
                {"Name": "Charlie", "V1": None, "V2": 0},
                {"Name": "Delta", "V1": 0, "V2": None},
            ]
        )
        df = df.withColumn("V3", F.col("V1") / F.col("V2"))
        df = df.fillna(0.0, subset=["V3"])

        rows = df.collect()
        assert len(rows) == 4
        assert _row_val(rows[0], "Name") == "Alice" and _row_val(rows[0], "V3") == 0.0
        assert _row_val(rows[1], "Name") == "Bob" and _row_val(rows[1], "V3") == 0.0
        assert _row_val(rows[2], "Name") == "Charlie" and _row_val(rows[2], "V3") == 0.0
        assert _row_val(rows[3], "Name") == "Delta" and _row_val(rows[3], "V3") == 0.0

    def test_fillna_float_fills_integer_column(self, spark):
        """fillna(0.0) fills None in both integer (V1) and float (V2) columns (PySpark behavior)."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "V1": 1, "V2": 0.1},
                {"Name": "Bob", "V1": 0, "V2": 2.0},
                {"Name": "Charlie", "V1": None, "V2": 3.5},
                {"Name": "Delta", "V1": 0, "V2": None},
            ]
        )
        df = df.fillna(0.0)

        rows = df.collect()
        assert len(rows) == 4
        assert _row_val(rows[0], "V1") == 1 and _row_val(rows[0], "V2") == 0.1
        assert _row_val(rows[1], "V1") == 0 and _row_val(rows[1], "V2") == 2.0
        assert _row_val(rows[2], "V1") == 0.0 and _row_val(rows[2], "V2") == 3.5
        assert _row_val(rows[3], "V1") == 0 and _row_val(rows[3], "V2") == 0.0

    def test_fillna_float_subset_single_int_column(self, spark):
        """fillna(0.0, subset=['V1']) fills None in integer column V1."""
        df = spark.createDataFrame(
            [
                {"V1": 1, "V2": 10},
                {"V1": None, "V2": 20},
            ]
        )
        result = df.fillna(0.0, subset=["V1"])
        rows = result.collect()
        assert _row_val(rows[0], "V1") == 1 and _row_val(rows[0], "V2") == 10
        assert _row_val(rows[1], "V1") == 0.0 and _row_val(rows[1], "V2") == 20

    # -------------------------------------------------------------------------
    # Robust / edge-case tests (backend-agnostic where no F used)
    # -------------------------------------------------------------------------

    def test_fillna_float_dict_int_column(self, spark):
        """fillna with dict mapping int column to 0.0 fills nulls (PySpark allows float for int col)."""
        df = spark.createDataFrame(
            [
                {"id": 1, "count": 10},
                {"id": 2, "count": None},
                {"id": 3, "count": None},
            ]
        )
        result = df.fillna({"count": 0.0})
        rows = result.collect()
        assert len(rows) == 3
        assert _row_val(rows[0], "count") == 10
        assert _row_val(rows[1], "count") == 0.0
        assert _row_val(rows[2], "count") == 0.0

    def test_fillna_float_subset_tuple_int_columns(self, spark):
        """fillna(0.0, subset=(...)) with tuple fills int columns."""
        df = spark.createDataFrame(
            [
                {"A": 1, "B": 2, "C": 3.0},
                {"A": None, "B": None, "C": 1.5},
            ]
        )
        result = df.fillna(0.0, subset=("A", "B"))
        rows = result.collect()
        assert (
            _row_val(rows[0], "A") == 1
            and _row_val(rows[0], "B") == 2
            and _row_val(rows[0], "C") == 3.0
        )
        assert (
            _row_val(rows[1], "A") == 0.0
            and _row_val(rows[1], "B") == 0.0
            and _row_val(rows[1], "C") == 1.5
        )

    def test_fillna_float_all_nulls_integer_column(self, spark, spark_backend):
        """fillna(0.0) when every row has None in integer column."""
        imports = get_spark_imports(spark_backend)
        schema = imports.StructType(
            [
                imports.StructField("name", imports.StringType()),
                imports.StructField("x", imports.IntegerType()),
            ]
        )
        df = spark.createDataFrame(
            [
                {"name": "a", "x": None},
                {"name": "b", "x": None},
            ],
            schema=schema,
        )
        result = df.fillna(0.0, subset=["x"])
        rows = result.collect()
        assert _row_val(rows[0], "x") == 0.0 and _row_val(rows[1], "x") == 0.0

    def test_fillna_float_empty_dataframe(self, spark):
        """fillna(0.0) on empty DataFrame returns empty (no error)."""
        df = spark.createDataFrame([], "name string, value int")
        result = df.fillna(0.0)
        rows = result.collect()
        assert len(rows) == 0

    def test_fillna_float_single_row_with_null_int(self, spark, spark_backend):
        """Single row, int column None, fillna(0.0) fills it."""
        imports = get_spark_imports(spark_backend)
        schema = imports.StructType(
            [
                imports.StructField("a", imports.IntegerType()),
                imports.StructField("b", imports.IntegerType()),
            ]
        )
        df = spark.createDataFrame([{"a": None, "b": 1}], schema=schema)
        result = df.fillna(0.0)
        rows = result.collect()
        assert len(rows) == 1
        assert _row_val(rows[0], "a") == 0.0 and _row_val(rows[0], "b") == 1

    def test_fillna_float_multiple_calculated_columns(self, spark, spark_backend):
        """Two calculated columns; fillna(0.0, subset=[...]) fills both."""
        F = get_spark_imports(spark_backend).F

        df = spark.createDataFrame(
            [
                {"V1": 1, "V2": 2, "V3": 1},
                {"V1": 0, "V2": 0, "V3": 0},
                {"V1": None, "V2": 1, "V3": 1},
            ]
        )
        df = df.withColumn("ratio", F.col("V1") / F.col("V2"))
        df = df.withColumn("sum", F.col("V1") + F.col("V3"))
        df = df.fillna(0.0, subset=["ratio", "sum"])

        rows = df.collect()
        assert len(rows) == 3
        # Row 0: 1/2=0.5, 1+1=2
        assert _row_val(rows[0], "ratio") == 0.5 and _row_val(rows[0], "sum") == 2
        # Row 1: 0/0 -> None -> 0.0, 0+0=0
        assert _row_val(rows[1], "ratio") == 0.0 and _row_val(rows[1], "sum") == 0
        # Row 2: None/1 -> None -> 0.0, None+1 -> None -> 0.0
        assert _row_val(rows[2], "ratio") == 0.0 and _row_val(rows[2], "sum") == 0.0

    def test_fillna_float_after_filter(self, spark, spark_backend):
        """withColumn -> filter -> fillna(0.0, subset=[calculated]) preserves fill."""
        F = get_spark_imports(spark_backend).F

        df = spark.createDataFrame(
            [
                {"id": 1, "a": 10, "b": 0},
                {"id": 2, "a": 0, "b": 5},
                {"id": 3, "a": None, "b": 2},
            ]
        )
        df = df.withColumn("div", F.col("a") / F.col("b"))
        df = df.filter(F.col("id") >= 2)
        df = df.fillna(0.0, subset=["div"])

        rows = df.collect()
        assert len(rows) == 2
        assert _row_val(rows[0], "id") == 2 and _row_val(rows[0], "div") == 0.0
        assert _row_val(rows[1], "id") == 3 and _row_val(rows[1], "div") == 0.0

    def test_fillna_float_subset_string_single_column(self, spark):
        """fillna(0.0, subset='col') with string (single column name) fills int column."""
        df = spark.createDataFrame(
            [
                {"x": 1, "y": 2.0},
                {"x": None, "y": 3.0},
            ]
        )
        result = df.fillna(0.0, subset="x")
        rows = result.collect()
        assert _row_val(rows[0], "x") == 1 and _row_val(rows[1], "x") == 0.0

    def test_fillna_float_mixed_int_and_float_columns_no_subset(self, spark):
        """fillna(0.0) with no subset fills all numeric nulls (int and float)."""
        df = spark.createDataFrame(
            [
                {"i": 1, "f": 1.0},
                {"i": None, "f": 2.0},
                {"i": 3, "f": None},
                {"i": None, "f": None},
            ]
        )
        result = df.fillna(0.0)
        rows = result.collect()
        assert _row_val(rows[0], "i") == 1 and _row_val(rows[0], "f") == 1.0
        assert _row_val(rows[1], "i") == 0.0 and _row_val(rows[1], "f") == 2.0
        assert _row_val(rows[2], "i") == 3 and _row_val(rows[2], "f") == 0.0
        assert _row_val(rows[3], "i") == 0.0 and _row_val(rows[3], "f") == 0.0

    def test_fillna_float_chained_fillna(self, spark):
        """Chained fillna(0.0) then fillna('') leaves numeric nulls filled."""
        df = spark.createDataFrame(
            [
                {"n": None, "s": "x"},
                {"n": 1, "s": None},
            ]
        )
        result = df.fillna(0.0).fillna("")
        rows = result.collect()
        assert _row_val(rows[0], "n") == 0.0 and _row_val(rows[0], "s") == "x"
        assert _row_val(rows[1], "n") == 1 and _row_val(rows[1], "s") == ""

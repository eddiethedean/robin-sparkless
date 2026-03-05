"""
Tests for issue #439: array_distinct returns non-unique values as wrong data type.

Sparkless was returning:
- ['1', '1', '2', '2', '3'] (strings, not deduplicated)

Instead of PySpark behavior:
- [1, 2, 3] (integers, deduplicated, original element type preserved)

Run with PySpark first: SPARKLESS_TEST_BACKEND=pyspark pytest tests/test_issue_439_array_distinct.py -v
Then mock: pytest tests/test_issue_439_array_distinct.py -v
"""

from tests.fixtures.spark_imports import get_spark_imports


def _row_val(row: object, key: str) -> object:
    """Get value from Row or dict (PySpark returns Row, mock may return dict)."""
    if hasattr(row, "__getitem__"):
        return row[key]
    return getattr(row, key, None)


class TestIssue439ArrayDistinct:
    """Test array_distinct preserves element type and deduplicates correctly."""

    def test_array_distinct_integer_arrays_preserves_type(self, spark, spark_backend):
        """array_distinct on int arrays returns int array, not string array."""
        F = get_spark_imports(spark_backend).F

        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": [1, 1, 2, 2, 3]},
                {"Name": "Bob", "Value": [4, 4, 5, 5, 6]},
            ]
        )
        df = df.withColumn("ValueSet", F.array_distinct("Value"))

        rows = df.collect()
        assert len(rows) == 2

        # Alice: [1,1,2,2,3] -> [1,2,3] (order may vary in PySpark)
        alice_valset = _row_val(rows[0], "ValueSet")
        assert alice_valset is not None
        assert set(alice_valset) == {1, 2, 3}
        assert len(alice_valset) == 3
        # Element type must be int, not str
        for v in alice_valset:
            assert isinstance(v, int), f"Expected int, got {type(v).__name__}: {v!r}"

        # Bob: [4,4,5,5,6] -> [4,5,6]
        bob_valset = _row_val(rows[1], "ValueSet")
        assert bob_valset is not None
        assert set(bob_valset) == {4, 5, 6}
        assert len(bob_valset) == 3
        for v in bob_valset:
            assert isinstance(v, int), f"Expected int, got {type(v).__name__}: {v!r}"

    def test_array_distinct_string_arrays(self, spark, spark_backend):
        """array_distinct on string arrays returns deduplicated string array."""
        F = get_spark_imports(spark_backend).F

        df = spark.createDataFrame(
            [
                {"tags": ["a", "a", "b", "b", "c"]},
                {"tags": ["x", "x", "x"]},
            ]
        )
        df = df.withColumn("unique_tags", F.array_distinct("tags"))

        rows = df.collect()
        assert len(rows) == 2

        row0 = _row_val(rows[0], "unique_tags")
        assert row0 is not None
        assert set(row0) == {"a", "b", "c"}
        assert len(row0) == 3
        for v in row0:
            assert isinstance(v, str), f"Expected str, got {type(v).__name__}: {v!r}"

        row1 = _row_val(rows[1], "unique_tags")
        assert row1 is not None
        assert list(row1) == ["x"]
        assert isinstance(row1[0], str)

    def test_array_distinct_with_column_expr(self, spark, spark_backend):
        """array_distinct(F.col('Value')) works same as string column name."""
        F = get_spark_imports(spark_backend).F

        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": [10, 10, 20]},
            ]
        )
        result = df.select(F.array_distinct(F.col("Value")).alias("distinct_values"))

        rows = result.collect()
        assert len(rows) == 1
        vals = _row_val(rows[0], "distinct_values")
        assert vals is not None
        assert set(vals) == {10, 20}
        assert len(vals) == 2
        for v in vals:
            assert isinstance(v, int)

    def test_array_distinct_empty_array(self, spark, spark_backend):
        """array_distinct on empty array returns empty array."""
        F = get_spark_imports(spark_backend).F
        StructType = get_spark_imports(spark_backend).StructType
        StructField = get_spark_imports(spark_backend).StructField
        ArrayType = get_spark_imports(spark_backend).ArrayType
        IntegerType = get_spark_imports(spark_backend).IntegerType

        schema = StructType([StructField("arr", ArrayType(IntegerType()))])
        df = spark.createDataFrame([{"arr": []}], schema=schema)
        result = df.withColumn("unique_arr", F.array_distinct("arr"))

        rows = result.collect()
        assert len(rows) == 1
        vals = _row_val(rows[0], "unique_arr")
        assert vals is not None
        assert vals == []

    def test_array_distinct_single_element(self, spark, spark_backend):
        """array_distinct on single-element array returns same."""
        F = get_spark_imports(spark_backend).F

        df = spark.createDataFrame(
            [
                {"arr": [42]},
            ]
        )
        result = df.withColumn("unique_arr", F.array_distinct("arr"))

        rows = result.collect()
        assert len(rows) == 1
        vals = _row_val(rows[0], "unique_arr")
        assert vals == [42]
        assert isinstance(vals[0], int)

    # -------------------------------------------------------------------------
    # Robust / edge-case tests
    # -------------------------------------------------------------------------

    def test_array_distinct_with_nulls_in_array(self, spark, spark_backend):
        """array_distinct deduplicates nulls: [1, None, 1, None] -> [1, None]."""
        F = get_spark_imports(spark_backend).F

        df = spark.createDataFrame(
            [
                {"a": [1, None, 1, None]},
                {"a": [None, None]},
            ]
        )
        result = df.withColumn("d", F.array_distinct("a"))
        rows = result.collect()

        row0 = _row_val(rows[0], "d")
        assert row0 is not None
        assert len(row0) == 2
        assert row0[0] == 1 and row0[1] is None

        row1 = _row_val(rows[1], "d")
        assert row1 is not None
        assert len(row1) == 1 and row1[0] is None

    def test_array_distinct_null_array_returns_null(self, spark, spark_backend):
        """array_distinct on null array (whole column null) returns null."""
        F = get_spark_imports(spark_backend).F
        StructType = get_spark_imports(spark_backend).StructType
        StructField = get_spark_imports(spark_backend).StructField
        ArrayType = get_spark_imports(spark_backend).ArrayType
        IntegerType = get_spark_imports(spark_backend).IntegerType

        schema = StructType([StructField("a", ArrayType(IntegerType()), True)])
        df = spark.createDataFrame([{"a": None}], schema=schema)
        result = df.withColumn("d", F.array_distinct("a"))
        rows = result.collect()
        assert _row_val(rows[0], "d") is None

    def test_array_distinct_float_arrays_preserves_type(self, spark, spark_backend):
        """array_distinct on float arrays preserves float type."""
        F = get_spark_imports(spark_backend).F

        df = spark.createDataFrame([{"a": [1.5, 1.5, 2.0, 3.14]}])
        result = df.withColumn("d", F.array_distinct("a"))
        rows = result.collect()
        vals = _row_val(rows[0], "d")
        assert vals is not None
        assert set(vals) == {1.5, 2.0, 3.14}
        assert len(vals) == 3
        for v in vals:
            assert isinstance(v, float), (
                f"Expected float, got {type(v).__name__}: {v!r}"
            )

    def test_array_distinct_boolean_arrays(self, spark, spark_backend):
        """array_distinct on boolean arrays preserves type."""
        F = get_spark_imports(spark_backend).F

        df = spark.createDataFrame([{"a": [True, False, True, False]}])
        result = df.withColumn("d", F.array_distinct("a"))
        rows = result.collect()
        vals = _row_val(rows[0], "d")
        assert vals is not None
        assert set(vals) == {True, False}
        assert len(vals) == 2
        for v in vals:
            assert isinstance(v, bool), f"Expected bool, got {type(v).__name__}: {v!r}"

    def test_array_distinct_all_duplicates_int(self, spark, spark_backend):
        """array_distinct when all elements are same returns single int."""
        F = get_spark_imports(spark_backend).F

        df = spark.createDataFrame([{"a": [7, 7, 7]}, {"a": [99, 99, 99]}])
        result = df.withColumn("d", F.array_distinct("a"))
        rows = result.collect()

        assert list(_row_val(rows[0], "d")) == [7]
        assert isinstance(_row_val(rows[0], "d")[0], int)
        assert list(_row_val(rows[1], "d")) == [99]
        assert isinstance(_row_val(rows[1], "d")[0], int)

    def test_array_distinct_all_duplicates_string(self, spark, spark_backend):
        """array_distinct when all elements are same returns single string."""
        F = get_spark_imports(spark_backend).F

        df = spark.createDataFrame([{"a": ["x", "x"]}, {"a": ["y", "y", "y"]}])
        result = df.withColumn("d", F.array_distinct("a"))
        rows = result.collect()

        assert list(_row_val(rows[0], "d")) == ["x"]
        assert isinstance(_row_val(rows[0], "d")[0], str)
        assert list(_row_val(rows[1], "d")) == ["y"]
        assert isinstance(_row_val(rows[1], "d")[0], str)

    def test_array_distinct_zero_values(self, spark, spark_backend):
        """array_distinct with zeros: [0, 0, 1] -> [0, 1]."""
        F = get_spark_imports(spark_backend).F

        df = spark.createDataFrame([{"a": [0, 0, 1, 1, 0]}])
        result = df.withColumn("d", F.array_distinct("a"))
        rows = result.collect()
        vals = _row_val(rows[0], "d")
        assert vals is not None
        assert set(vals) == {0, 1}
        assert len(vals) == 2
        for v in vals:
            assert isinstance(v, int)

    def test_array_distinct_chained_with_filter(self, spark, spark_backend):
        """array_distinct after filter/select preserves type."""
        F = get_spark_imports(spark_backend).F

        df = spark.createDataFrame(
            [
                {"id": 1, "vals": [10, 10, 20]},
                {"id": 2, "vals": [30, 30, 40]},
            ]
        )
        result = (
            df.filter(F.col("id") >= 1)
            .select("id", "vals")
            .withColumn("unique_vals", F.array_distinct("vals"))
        )
        rows = result.collect()
        assert len(rows) == 2
        assert set(_row_val(rows[0], "unique_vals")) == {10, 20}
        assert set(_row_val(rows[1], "unique_vals")) == {30, 40}
        for row in rows:
            for v in _row_val(row, "unique_vals"):
                assert isinstance(v, int)

    def test_array_distinct_unicode_strings(self, spark, spark_backend):
        """array_distinct on unicode string arrays preserves type."""
        F = get_spark_imports(spark_backend).F

        df = spark.createDataFrame(
            [
                {"a": ["café", "café", "naïve", "naïve"]},
            ]
        )
        result = df.withColumn("d", F.array_distinct("a"))
        rows = result.collect()
        vals = _row_val(rows[0], "d")
        assert vals is not None
        assert set(vals) == {"café", "naïve"}
        assert len(vals) == 2
        for v in vals:
            assert isinstance(v, str)

    def test_array_distinct_multiple_rows_mixed(self, spark, spark_backend):
        """array_distinct across multiple rows with different array content."""
        F = get_spark_imports(spark_backend).F
        StructType = get_spark_imports(spark_backend).StructType
        StructField = get_spark_imports(spark_backend).StructField
        ArrayType = get_spark_imports(spark_backend).ArrayType
        IntegerType = get_spark_imports(spark_backend).IntegerType

        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("arr", ArrayType(IntegerType())),
            ]
        )
        df = spark.createDataFrame(
            [
                {"id": 1, "arr": [1, 2, 1]},
                {"id": 2, "arr": [3, 3, 3]},
                {"id": 3, "arr": []},
            ],
            schema=schema,
        )
        result = df.withColumn("d", F.array_distinct("arr"))
        rows = result.collect()
        assert len(rows) == 3
        assert set(_row_val(rows[0], "d")) == {1, 2}
        assert list(_row_val(rows[1], "d")) == [3]
        assert _row_val(rows[2], "d") == []

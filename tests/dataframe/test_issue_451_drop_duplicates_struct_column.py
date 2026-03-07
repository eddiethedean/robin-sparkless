"""Test issue #451: dropDuplicates() with struct column raises TypeError when materialized.

Sparkless previously raised:
  TypeError: unhashable type: 'dict'
when calling dropDuplicates() after materialization on a DataFrame with a struct column.
The fix was applied in PR #462 (issue #448): _make_hashable handles both list and dict.

Tests are written PySpark-first: run with PySpark, then mock:
  SPARKLESS_TEST_BACKEND=pyspark pytest tests/test_issue_451_drop_duplicates_struct_column.py -v
  pytest tests/test_issue_451_drop_duplicates_struct_column.py -v

https://github.com/eddiethedean/sparkless/issues/451
"""

from tests.fixtures.spark_imports import get_spark_imports


def _row_val(row, key):
    """Get value from Row or dict (PySpark returns Row, mock may return dict)."""
    if hasattr(row, "__getitem__"):
        return row[key]
    return getattr(row, key, None)


def test_drop_duplicates_struct_column_after_materialization_exact_issue_451(
    spark, spark_backend
):
    """Exact scenario from #451: struct column + materialize + dropDuplicates.

    Previously raised TypeError: unhashable type: 'dict' because struct columns
    materialize as dict and were not hashable for set membership.
    """
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"id": "1", "value": "A"},
            {"id": "1", "value": "A"},
            {"id": "2", "value": "b"},
        ]
    )
    df = df.withColumn("structInfo", F.struct("id", "value"))

    df.count()  # force materialization - required to trigger the bug

    result = df.dropDuplicates()
    rows = result.collect()

    assert len(rows) == 2
    ids = {_row_val(r, "id") for r in rows}
    assert ids == {"1", "2"}
    # structInfo should be present and deduplication should have worked
    for r in rows:
        struct_val = _row_val(r, "structInfo")
        assert struct_val is not None


def test_drop_duplicates_struct_column_before_materialization(spark, spark_backend):
    """dropDuplicates before materialization (workaround from issue - should still work)."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"id": "1", "value": "A"},
            {"id": "1", "value": "A"},
            {"id": "2", "value": "b"},
        ]
    )
    df = df.withColumn("structInfo", F.struct("id", "value"))

    # No materialization - dropDuplicates first
    result = df.dropDuplicates()
    rows = result.collect()

    assert len(rows) == 2
    ids = {_row_val(r, "id") for r in rows}
    assert ids == {"1", "2"}


def test_distinct_struct_column_after_materialization(spark, spark_backend):
    """distinct() with struct column after materialization."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"id": 1, "value": "x"},
            {"id": 1, "value": "x"},
            {"id": 2, "value": "y"},
        ]
    )
    df = df.withColumn("info", F.struct("id", "value"))
    df.count()  # materialize

    result = df.distinct()
    rows = result.collect()
    assert len(rows) == 2
    assert {_row_val(r, "id") for r in rows} == {1, 2}


def test_drop_duplicates_subset_with_struct_column(spark, spark_backend):
    """dropDuplicates(subset) when struct column exists but subset excludes it."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"id": "1", "value": "A"},
            {"id": "1", "value": "A"},
            {"id": "2", "value": "b"},
        ]
    )
    df = df.withColumn("structInfo", F.struct("id", "value"))
    df.count()

    result = df.dropDuplicates(subset=["id"])
    rows = result.collect()
    assert len(rows) == 2
    assert {_row_val(r, "id") for r in rows} == {"1", "2"}

"""Test issue #445: between() on string column with numeric bounds.

PySpark implicitly casts string column to numeric when bounds are numeric:
  df.filter(F.col("Value").between(1, 6))  # "5" -> 5, "10" -> 10

Sparkless previously raised:
  polars.exceptions.InvalidOperationError: got invalid or ambiguous dtypes:
  '[str, dyn int, dyn int]' in expression 'is_between'

Run in PySpark mode first, then mock mode:
  SPARKLESS_TEST_BACKEND=pyspark pytest tests/test_issue_445_between_string_column_numeric_bounds.py -v
  pytest tests/test_issue_445_between_string_column_numeric_bounds.py -v

https://github.com/eddiethedean/sparkless/issues/445
"""

from tests.fixtures.spark_imports import get_spark_imports


def test_between_string_column_numeric_bounds_exact_issue_445(spark, spark_backend):
    """Exact scenario from #445 - string column with numeric bounds."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"Name": "Alice", "Value": "10"},
            {"Name": "Bob", "Value": "5"},
        ]
    )
    result = df.filter(F.col("Value").between(1, 6))
    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["Name"] == "Bob"
    assert rows[0]["Value"] == "5"


def test_between_string_column_numeric_bounds_both_in_range(spark, spark_backend):
    """Both rows in range when bounds are wide."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"id": 1, "val": "3"},
            {"id": 2, "val": "7"},
        ]
    )
    result = df.filter(F.col("val").between(1, 10))
    rows = result.collect()

    assert len(rows) == 2


def test_between_string_column_numeric_bounds_none_in_range(spark, spark_backend):
    """No rows in range."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"id": 1, "val": "100"},
            {"id": 2, "val": "200"},
        ]
    )
    result = df.filter(F.col("val").between(1, 10))
    rows = result.collect()

    assert len(rows) == 0


def test_between_string_column_float_bounds(spark, spark_backend):
    """String column with float bounds."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"id": 1, "val": "3.5"},
            {"id": 2, "val": "7.2"},
        ]
    )
    result = df.filter(F.col("val").between(1.0, 5.0))
    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["val"] == "3.5"


def test_between_string_column_invalid_numeric_returns_null(spark, spark_backend):
    """String that can't parse as number -> null -> excluded from filter."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"id": 1, "val": "abc"},
            {"id": 2, "val": "5"},
        ]
    )
    result = df.filter(F.col("val").between(1, 10))
    rows = result.collect()

    # "abc" can't parse as number -> null -> filter excludes it
    assert len(rows) == 1
    assert rows[0]["val"] == "5"


def test_between_integer_column_numeric_bounds_unchanged(spark, spark_backend):
    """Integer column with numeric bounds - existing behavior unchanged."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"id": 1, "val": 5},
            {"id": 2, "val": 15},
        ]
    )
    result = df.filter(F.col("val").between(1, 10))
    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["val"] == 5


def test_between_string_column_with_lit_bounds(spark, spark_backend):
    """between with F.lit() for bounds."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"id": 1, "val": "4"},
            {"id": 2, "val": "20"},
        ]
    )
    result = df.filter(F.col("val").between(F.lit(1), F.lit(10)))
    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["val"] == "4"


def test_between_string_column_in_select_expression(spark, spark_backend):
    """between in select creates boolean column."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"id": 1, "val": "5"},
            {"id": 2, "val": "15"},
        ]
    )
    result = df.select(
        F.col("id"),
        F.col("val"),
        F.col("val").between(1, 10).alias("in_range"),
    )
    rows = result.collect()

    assert rows[0]["in_range"] is True
    assert rows[1]["in_range"] is False


def test_between_string_column_inclusive_boundaries(spark, spark_backend):
    """String values equal to lower/upper bound are included."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"id": 1, "val": "1"},
            {"id": 2, "val": "5"},
            {"id": 3, "val": "10"},
            {"id": 4, "val": "11"},
        ]
    )
    result = df.filter(F.col("val").between(1, 10))
    rows = result.collect()

    assert len(rows) == 3
    vals = {r["val"] for r in rows}
    assert vals == {"1", "5", "10"}


def test_between_string_column_null_excluded(spark, spark_backend):
    """Null string column value is excluded from filter (null between -> null -> excluded)."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"id": 1, "val": "5"},
            {"id": 2, "val": None},
        ]
    )
    result = df.filter(F.col("val").between(1, 10))
    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["id"] == 1


def test_between_string_column_negative_numbers(spark, spark_backend):
    """String column with negative numbers."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"id": 1, "val": "-5"},
            {"id": 2, "val": "5"},
            {"id": 3, "val": "-15"},
        ]
    )
    result = df.filter(F.col("val").between(-10, 0))
    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["val"] == "-5"


def test_between_string_column_then_orderby(spark, spark_backend):
    """Filter with between then orderBy."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"id": 1, "val": "8"},
            {"id": 2, "val": "3"},
            {"id": 3, "val": "5"},
        ]
    )
    result = df.filter(F.col("val").between(1, 10)).orderBy(F.col("val"))
    rows = result.collect()

    assert len(rows) == 3
    assert [r["val"] for r in rows] == ["3", "5", "8"]


def test_between_string_column_in_when_otherwise(spark, spark_backend):
    """between inside when/otherwise expression."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"id": 1, "val": "3"},
            {"id": 2, "val": "7"},
            {"id": 3, "val": "15"},
        ]
    )
    result = df.withColumn(
        "tier",
        F.when(F.col("val").between(1, 5), "low")
        .when(F.col("val").between(6, 10), "mid")
        .otherwise("high"),
    )
    rows = result.collect()

    tier_map = {r["id"]: r["tier"] for r in rows}
    assert tier_map[1] == "low"
    assert tier_map[2] == "mid"
    assert tier_map[3] == "high"


def test_between_string_column_not_between(spark, spark_backend):
    """~between (NOT between) filters inverse."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"id": 1, "val": "5"},
            {"id": 2, "val": "15"},
        ]
    )
    result = df.filter(~F.col("val").between(1, 10))
    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["val"] == "15"


def test_between_string_column_chained_with_select(spark, spark_backend):
    """filter + select + filter chain with string column between."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"id": 1, "val": "3", "name": "a"},
            {"id": 2, "val": "7", "name": "b"},
            {"id": 3, "val": "15", "name": "c"},
        ]
    )
    result = (
        df.filter(F.col("val").between(1, 10))
        .select(F.col("id"), F.col("val"))
        .filter(F.col("val").between(5, 10))
    )
    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["val"] == "7"


def test_between_string_column_zero_bounds(spark, spark_backend):
    """between with zero in bounds."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"id": 1, "val": "0"},
            {"id": 2, "val": "5"},
            {"id": 3, "val": "-1"},
        ]
    )
    result = df.filter(F.col("val").between(0, 10))
    rows = result.collect()

    assert len(rows) == 2
    vals = {r["val"] for r in rows}
    assert vals == {"0", "5"}

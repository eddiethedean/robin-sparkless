"""Test issue #441: F.col('MapValue')[F.col('Value')] - map column subscript with Column key.

PySpark supports using a Column as the lookup key for a map column:
  F.col("MapValue")[F.col("Value")]  # lookup MapValue using key from Value column

Fixed by #440 - Column.__getitem__ now delegates to getItem for Column keys.

Run in PySpark mode first, then mock mode:
  SPARKLESS_TEST_BACKEND=pyspark pytest tests/test_issue_441_map_column_subscript.py -v
  pytest tests/test_issue_441_map_column_subscript.py -v

https://github.com/eddiethedean/sparkless/issues/441
"""

import pytest

from tests.fixtures.spark_backend import BackendType
from tests.fixtures.spark_imports import get_spark_imports


def test_map_column_subscript_with_column_key_exact_issue_441(spark, spark_backend):
    """Exact scenario from #441 - map in column, lookup key from another column."""
    F = get_spark_imports(spark_backend).F

    # Use string keys for cross-backend compatibility (Polars createDataFrame + int map keys)
    df = spark.createDataFrame(
        [
            {"Name": "Alice", "Value": "1", "MapValue": {"1": "Small", "2": "Medium"}},
            {"Name": "Bob", "Value": "2", "MapValue": {"2": "Medium", "3": "Large"}},
        ]
    )
    df = df.withColumn("Size", F.col("MapValue")[F.col("Value")])
    rows = df.collect()

    assert rows[0]["Name"] == "Alice"
    assert rows[0]["Size"] == "Small"
    assert rows[1]["Name"] == "Bob"
    assert rows[1]["Size"] == "Medium"


def test_map_column_subscript_key_not_found(spark, spark_backend):
    """map_col[key_col] returns None when key not in map."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"k": "a", "m": {"a": 1, "b": 2}},
            {"k": "z", "m": {"a": 1, "b": 2}},
        ]
    )
    df = df.withColumn("v", F.col("m")[F.col("k")])
    rows = df.collect()

    assert rows[0]["v"] == 1
    assert rows[1]["v"] is None


def test_map_column_subscript_in_select(spark, spark_backend):
    """map_col[key_col] works in select."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame([{"id": 1, "key": "x", "data": {"x": 10, "y": 20}}])
    result = df.select(F.col("data")[F.col("key")].alias("val"))
    rows = result.collect()

    assert rows[0]["val"] == 10


def test_map_column_subscript_exact_issue_441_with_int_keys_pyspark(
    spark, spark_backend
):
    """Exact issue #441 scenario: createDataFrame with int-keyed map (PySpark only).

    Polars createDataFrame may not support int keys in map columns.
    """
    if spark_backend != BackendType.PYSPARK:
        pytest.skip("Int-keyed map in createDataFrame only tested with PySpark")

    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"Name": "Alice", "Value": 1, "MapValue": {1: "Small", 2: "Medium"}},
            {"Name": "Bob", "Value": 2, "MapValue": {2: "Medium", 3: "Large"}},
        ]
    )
    df = df.withColumn("Size", F.col("MapValue")[F.col("Value")])
    rows = df.collect()

    assert rows[0]["Name"] == "Alice"
    assert rows[0]["Size"] == "Small"
    assert rows[1]["Name"] == "Bob"
    assert rows[1]["Size"] == "Medium"


def test_map_column_subscript_then_filter(spark, spark_backend):
    """map_col[key_col] then filter on result."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"id": 1, "k": "a", "m": {"a": 1, "b": 2, "c": 3}},
            {"id": 2, "k": "b", "m": {"a": 1, "b": 2, "c": 3}},
            {"id": 3, "k": "z", "m": {"a": 1, "b": 2, "c": 3}},
        ]
    )
    df = df.withColumn("v", F.col("m")[F.col("k")])
    result = df.filter(F.col("v").isNotNull())
    rows = result.collect()

    assert len(rows) == 2
    assert {r["id"] for r in rows} == {1, 2}
    assert rows[0]["v"] == 1
    assert rows[1]["v"] == 2


def test_map_column_subscript_null_key_returns_null(spark, spark_backend):
    """map_col[key_col] returns null when key column is null."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"k": "a", "m": {"a": 1, "b": 2}},
            {"k": None, "m": {"a": 1, "b": 2}},
        ]
    )
    df = df.withColumn("v", F.col("m")[F.col("k")])
    rows = df.collect()

    assert rows[0]["v"] == 1
    assert rows[1]["v"] is None


def test_map_column_subscript_coalesce_default(spark, spark_backend):
    """coalesce(map_col[key_col], lit(default)) for missing keys."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"k": "a", "m": {"a": 1, "b": 2}},
            {"k": "z", "m": {"a": 1, "b": 2}},
        ]
    )
    df = df.withColumn("v", F.coalesce(F.col("m")[F.col("k")], F.lit(-1)))
    rows = df.collect()

    assert rows[0]["v"] == 1
    assert rows[1]["v"] == -1


def test_map_column_subscript_multiple_in_select(spark, spark_backend):
    """Multiple map lookups in single select."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"k1": "x", "k2": "y", "m": {"x": 10, "y": 20, "z": 30}},
        ]
    )
    result = df.select(
        F.col("m")[F.col("k1")].alias("v1"),
        F.col("m")[F.col("k2")].alias("v2"),
    )
    rows = result.collect()

    assert rows[0]["v1"] == 10
    assert rows[0]["v2"] == 20


def test_map_column_subscript_orderby_result(spark, spark_backend):
    """orderBy on map lookup result."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"id": 1, "k": "c", "m": {"a": 1, "b": 2, "c": 3}},
            {"id": 2, "k": "a", "m": {"a": 1, "b": 2, "c": 3}},
            {"id": 3, "k": "b", "m": {"a": 1, "b": 2, "c": 3}},
        ]
    )
    df = df.withColumn("v", F.col("m")[F.col("k")])
    result = df.orderBy(F.col("v"))
    rows = result.collect()

    assert [r["id"] for r in rows] == [2, 3, 1]
    assert [r["v"] for r in rows] == [1, 2, 3]


def test_map_column_subscript_when_otherwise(spark, spark_backend):
    """map lookup in when/otherwise expression."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"k": "a", "m": {"a": "low", "b": "mid", "c": "high"}},
            {"k": "z", "m": {"a": "low", "b": "mid", "c": "high"}},
        ]
    )
    df = df.withColumn(
        "tier",
        F.when(F.col("m")[F.col("k")] == "high", "H")
        .when(F.col("m")[F.col("k")] == "mid", "M")
        .otherwise("L"),
    )
    rows = df.collect()

    assert rows[0]["tier"] == "L"
    assert rows[1]["tier"] == "L"


def test_map_column_subscript_chained_with_columns(spark, spark_backend):
    """Chained withColumn calls using map lookups."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame([{"k": "b", "m": {"a": 1, "b": 2, "c": 3}}])
    df = df.withColumn("v", F.col("m")[F.col("k")]).withColumn(
        "doubled", F.col("v") * 2
    )
    rows = df.collect()

    assert rows[0]["v"] == 2
    assert rows[0]["doubled"] == 4


def test_map_column_subscript_create_map_with_column_key(spark, spark_backend):
    """create_map expression with map[col] lookup - overlaps with #440."""
    from itertools import chain

    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [{"Name": "Alice", "Value": 1}, {"Name": "Bob", "Value": 3}]
    )
    mapping = F.create_map(
        [F.lit(x) for x in chain(*{1: "Small", 2: "Medium", 3: "Large"}.items())]
    )
    df = df.withColumn("Size", mapping[F.col("Value")])
    rows = df.collect()

    assert rows[0]["Size"] == "Small"
    assert rows[1]["Size"] == "Large"

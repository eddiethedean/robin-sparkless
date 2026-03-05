"""Test issue #440: F.create_map() accepts list with alternating key-value pairs.

PySpark's create_map accepts both:
  - create_map(k1, v1, k2, v2, ...)  # variadic
  - create_map([k1, v1, k2, v2, ...])  # single list

Run in PySpark mode first, then mock mode:
  SPARKLESS_TEST_BACKEND=pyspark pytest tests/test_issue_440_create_map_list.py -v
  pytest tests/test_issue_440_create_map_list.py -v

https://github.com/eddiethedean/sparkless/issues/440
"""

from itertools import chain

from tests.fixtures.spark_imports import get_spark_imports


def test_create_map_list_exact_issue_440(spark, spark_backend):
    """Exact scenario from issue #440 - create_map with list from chain(*dict.items())."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"Name": "Alice", "Value": 1},
            {"Name": "Bob", "Value": 3},
        ]
    )

    translation_dict = {1: "Small", 2: "Medium", 3: "Large"}
    mapping = F.create_map([F.lit(x) for x in chain(*translation_dict.items())])

    df = df.withColumn("Size", mapping[F.col("Value")])
    rows = df.collect()

    assert len(rows) == 2
    assert rows[0]["Name"] == "Alice"
    assert rows[0]["Value"] == 1
    assert rows[0]["Size"] == "Small"
    assert rows[1]["Name"] == "Bob"
    assert rows[1]["Value"] == 3
    assert rows[1]["Size"] == "Large"


def test_create_map_list_literals_only(spark, spark_backend):
    """create_map([lit(k1), lit(v1), lit(k2), lit(v2)]) - all literals."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame([{"x": 1}])
    m = F.create_map([F.lit("a"), F.lit(10), F.lit("b"), F.lit(20)])
    result = df.select(m.alias("map_col"))
    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["map_col"] == {"a": 10, "b": 20}


def test_create_map_list_mixed_literals_columns(spark, spark_backend):
    """create_map([lit(k1), col(v1), lit(k2), col(v2)]) - mixed."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame([{"v1": "x", "v2": "y"}])
    m = F.create_map([F.lit("k1"), F.col("v1"), F.lit("k2"), F.col("v2")])
    result = df.select(m.alias("map_col"))
    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["map_col"] == {"k1": "x", "k2": "y"}


def test_create_map_list_map_lookup_key_not_found(spark, spark_backend):
    """map[col] returns None when key is not in map."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"Name": "Alice", "Value": 1},
            {"Name": "Bob", "Value": 99},  # 99 not in mapping
        ]
    )
    mapping = F.create_map([F.lit(1), F.lit("A"), F.lit(2), F.lit("B")])
    df = df.withColumn("Size", mapping[F.col("Value")])
    rows = df.collect()

    assert rows[0]["Size"] == "A"
    assert rows[1]["Size"] is None


def test_create_map_list_in_select(spark, spark_backend):
    """create_map([...]) works in select."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame([{"a": 1, "b": 2}])
    result = df.select(
        F.create_map([F.lit("x"), F.col("a"), F.lit("y"), F.col("b")]).alias("m")
    )
    rows = result.collect()
    assert rows[0]["m"] == {"x": 1, "y": 2}


def test_create_map_list_then_filter(spark, spark_backend):
    """create_map from list, lookup, then filter on result."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [{"id": 1, "v": 10}, {"id": 2, "v": 20}, {"id": 3, "v": 30}]
    )
    m = F.create_map([F.lit(10), F.lit("ten"), F.lit(20), F.lit("twenty")])
    df = df.withColumn("label", m[F.col("v")])
    result = df.filter(F.col("label").isNotNull())
    rows = result.collect()

    assert len(rows) == 2
    labels = {r["label"] for r in rows}
    assert labels == {"ten", "twenty"}


def test_create_map_list_with_null_value_in_map(spark, spark_backend):
    """create_map list with column that has null - map lookup with null key."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"k": 1, "v": "a"},
            {"k": None, "v": "b"},
        ]
    )
    mapping = F.create_map([F.lit(1), F.lit("one"), F.lit(2), F.lit("two")])
    df = df.withColumn("lookup", mapping[F.col("k")])
    rows = df.collect()

    assert rows[0]["lookup"] == "one"
    assert rows[1]["lookup"] is None


def test_create_map_list_numeric_like_keys(spark, spark_backend):
    """create_map list with keys that may be int or string - both backends agree on result."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame([{"k": 1}])
    # Use string keys for cross-backend consistency (Polars/Sparkless may coerce int->str in maps)
    m = F.create_map([F.lit("1"), F.lit("a"), F.lit("2"), F.lit("b")])
    result = df.select(m.alias("map_col"))
    rows = result.collect()

    assert rows[0]["map_col"] == {"1": "a", "2": "b"}


def test_create_map_list_single_pair(spark, spark_backend):
    """create_map([lit('k'), lit('v')]) - single pair in list."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame([{"x": 1}])
    m = F.create_map([F.lit("k"), F.lit("v")])
    result = df.select(m.alias("map_col"))
    rows = result.collect()

    assert rows[0]["map_col"] == {"k": "v"}


def test_create_map_list_six_pairs(spark, spark_backend):
    """create_map list with 6 pairs - larger list."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame([{"v": 3}])
    pairs = [
        (F.lit(1), F.lit("a")),
        (F.lit(2), F.lit("b")),
        (F.lit(3), F.lit("c")),
        (F.lit(4), F.lit("d")),
        (F.lit(5), F.lit("e")),
        (F.lit(6), F.lit("f")),
    ]
    flat = [x for p in pairs for x in p]
    m = F.create_map(flat)
    df = df.withColumn("label", m[F.col("v")])
    rows = df.collect()

    assert rows[0]["label"] == "c"

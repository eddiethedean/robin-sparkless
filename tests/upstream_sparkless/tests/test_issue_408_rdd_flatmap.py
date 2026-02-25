"""Tests for Issue #408: MockRDD.flatMap support."""

from __future__ import annotations


def test_rdd_flatmap_words(spark) -> None:
    """Reproduce issue #408: df.rdd.flatMap splits lines into words."""
    df = spark.createDataFrame(
        [("hello world",), ("hello pyspark",), ("flatMap is useful",)],
        ["line"],
    )
    rdd = df.rdd.flatMap(lambda row: row["line"].split())
    assert rdd.collect() == [
        "hello",
        "world",
        "hello",
        "pyspark",
        "flatMap",
        "is",
        "useful",
    ]


def test_rdd_flatmap_empty_iterable(spark) -> None:
    """flatMap with func that sometimes returns empty iterable."""
    df = spark.createDataFrame([(1,), (0,), (2,)], ["x"])
    rdd = df.rdd.flatMap(lambda row: range(row["x"]) if row["x"] > 0 else [])
    assert rdd.collect() == [0, 0, 1]


def test_rdd_flatmap_then_map(spark) -> None:
    """flatMap then map (chaining)."""
    df = spark.createDataFrame([("a b",), ("c",)], ["line"])
    rdd = df.rdd.flatMap(lambda row: row["line"].split()).map(lambda s: s.upper())
    assert rdd.collect() == ["A", "B", "C"]


def test_rdd_flatmap_empty_rdd(spark) -> None:
    """flatMap on empty DataFrame yields empty RDD."""
    # PySpark cannot infer schema from empty list; use schema explicitly.
    from tests.fixtures.spark_imports import get_spark_imports

    imports = get_spark_imports()
    schema = imports.StructType([imports.StructField("line", imports.StringType())])
    df = spark.createDataFrame([], schema)
    rdd = df.rdd.flatMap(lambda row: row["line"].split())
    assert rdd.collect() == []
    assert rdd.count() == 0


def test_rdd_flatmap_one_element_per_row(spark) -> None:
    """flatMap with each row mapping to a single-element iterable."""
    df = spark.createDataFrame([(1,), (2,), (3,)], ["x"])
    rdd = df.rdd.flatMap(lambda row: (row["x"] * 10,))
    assert rdd.collect() == [10, 20, 30]


def test_rdd_flatmap_tuples_for_pair_ops(spark) -> None:
    """flatMap to (key, value) pairs (e.g. for word count style)."""
    df = spark.createDataFrame([("a b",), ("a c",)], ["line"])
    rdd = df.rdd.flatMap(lambda row: ((word, 1) for word in row["line"].split()))
    collected = sorted(rdd.collect())
    assert collected == [("a", 1), ("a", 1), ("b", 1), ("c", 1)]


def test_rdd_flatmap_then_filter(spark) -> None:
    """flatMap then filter."""
    df = spark.createDataFrame([("one",), ("two",), ("three",)], ["word"])
    rdd = df.rdd.flatMap(lambda row: [row["word"], row["word"].upper()]).filter(
        lambda x: x.isupper() or len(x) > 3
    )
    # isupper(): ONE, TWO, THREE; len > 3: "three" (lowercase)
    assert sorted(rdd.collect()) == ["ONE", "THREE", "TWO", "three"]


def test_rdd_flatmap_then_count_take_first(spark) -> None:
    """flatMap then count(), take(), first()."""
    df = spark.createDataFrame([("x y",), ("z",)], ["line"])
    rdd = df.rdd.flatMap(lambda row: row["line"].split())
    assert rdd.count() == 3
    assert len(rdd.take(2)) == 2
    assert rdd.take(2) == ["x", "y"]
    assert rdd.first() == "x"


def test_rdd_flatmap_empty_string_split(spark) -> None:
    """flatMap with empty string split yields no elements for that row."""
    df = spark.createDataFrame([("a b",), ("",), ("c",)], ["line"])
    rdd = df.rdd.flatMap(lambda row: row["line"].split())
    assert rdd.collect() == ["a", "b", "c"]


def test_rdd_flatmap_then_reduce(spark) -> None:
    """flatMap then reduce (e.g. sum of all numbers)."""
    df = spark.createDataFrame([(1,), (2,), (3,)], ["x"])
    rdd = df.rdd.flatMap(lambda row: (row["x"], row["x"] + 1))
    total = rdd.reduce(lambda a, b: a + b)
    assert total == (1 + 2 + 2 + 3 + 3 + 4)  # 1,2,2,3,3,4


def test_rdd_flatmap_chain_double_flatmap(spark) -> None:
    """Chained flatMaps: split then each word -> [word, word.upper()]."""
    df = spark.createDataFrame([("ab cd",)], ["line"])
    rdd = df.rdd.flatMap(lambda row: row["line"].split()).flatMap(
        lambda w: [w, w.upper()]
    )
    # Default sort: uppercase before lowercase
    assert sorted(rdd.collect()) == sorted(["ab", "AB", "cd", "CD"])


def test_rdd_flatmap_preserves_order(spark) -> None:
    """flatMap preserves order: row order then element order within each row."""
    df = spark.createDataFrame([("1",), ("2",), ("3",)], ["x"])
    rdd = df.rdd.flatMap(lambda row: [row["x"], row["x"] + "x"])
    assert rdd.collect() == ["1", "1x", "2", "2x", "3", "3x"]

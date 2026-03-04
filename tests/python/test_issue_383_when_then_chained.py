"""Tests for issue #383: WhenThen chained .when() and .otherwise()."""

from __future__ import annotations

from tests.fixtures.spark_imports import get_spark_imports


_imports = get_spark_imports()
SparkSession = _imports.SparkSession
F = _imports.F


def test_when_then_when_then_otherwise() -> None:
    """when(a).then(x).when(b).then(y).otherwise(z) evaluates first match."""
    spark = SparkSession.builder.appName("issue_383").getOrCreate()
    df = spark.createDataFrame(
        [(1, "a"), (2, "b"), (3, "c"), (4, "d")], ["id", "label"]
    )
    out = df.with_column(
        "tier",
        F.when(F.col("id") == F.lit(1))
        .then(F.lit("first"))
        .when(F.col("id") == F.lit(2))
        .then(F.lit("second"))
        .when(F.col("id") == F.lit(3))
        .then(F.lit("third"))
        .otherwise(F.lit("other")),
    )
    rows = out.collect()
    assert len(rows) == 4
    by_id = {r["id"]: r["tier"] for r in rows}
    assert by_id[1] == "first"
    assert by_id[2] == "second"
    assert by_id[3] == "third"
    assert by_id[4] == "other"


def test_when_then_otherwise_single_branch_unchanged() -> None:
    """Single when().then().otherwise() still works as before."""
    spark = SparkSession.builder.appName("issue_383").getOrCreate()
    df = spark.createDataFrame([(10,), (25,)], ["age"])
    out = df.with_column(
        "group",
        F.when(F.col("age") > F.lit(18))
        .then(F.lit("adult"))
        .otherwise(F.lit("minor")),
    )
    rows = out.collect()
    assert len(rows) == 2
    assert rows[0]["group"] == "minor"
    assert rows[1]["group"] == "adult"


def test_chained_when_in_select() -> None:
    """Chained when/then in select()."""
    spark = SparkSession.builder.appName("issue_383").getOrCreate()
    df = spark.createDataFrame([(1,), (2,), (3,)], ["x"])
    out = df.select(
        F.col("x"),
        F.when(F.col("x") == F.lit(1))
        .then(F.lit("one"))
        .when(F.col("x") == F.lit(2))
        .then(F.lit("two"))
        .otherwise(F.lit("many"))
        .alias("word"),
    )
    rows = out.collect()
    assert [r["word"] for r in rows] == ["one", "two", "many"]

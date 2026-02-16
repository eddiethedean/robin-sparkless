"""Tests for issue #383: WhenThen chained .when() and .otherwise()."""

from __future__ import annotations

import robin_sparkless as rs


def test_when_then_when_then_otherwise() -> None:
    """when(a).then(x).when(b).then(y).otherwise(z) evaluates first match."""
    spark = rs.SparkSession.builder().app_name("issue_383").get_or_create()
    df = spark.createDataFrame(
        [(1, "a"), (2, "b"), (3, "c"), (4, "d")], ["id", "label"]
    )
    out = df.with_column(
        "tier",
        rs.when(rs.col("id").eq(rs.lit(1)))
        .then(rs.lit("first"))
        .when(rs.col("id").eq(rs.lit(2)))
        .then(rs.lit("second"))
        .when(rs.col("id").eq(rs.lit(3)))
        .then(rs.lit("third"))
        .otherwise(rs.lit("other")),
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
    spark = rs.SparkSession.builder().app_name("issue_383").get_or_create()
    df = spark.createDataFrame([(10,), (25,)], ["age"])
    out = df.with_column(
        "group",
        rs.when(rs.col("age").gt(rs.lit(18)))
        .then(rs.lit("adult"))
        .otherwise(rs.lit("minor")),
    )
    rows = out.collect()
    assert len(rows) == 2
    assert rows[0]["group"] == "minor"
    assert rows[1]["group"] == "adult"


def test_chained_when_in_select() -> None:
    """Chained when/then in select()."""
    spark = rs.SparkSession.builder().app_name("issue_383").get_or_create()
    df = spark.createDataFrame([(1,), (2,), (3,)], ["x"])
    out = df.select(
        rs.col("x"),
        rs.when(rs.col("x").eq(rs.lit(1)))
        .then(rs.lit("one"))
        .when(rs.col("x").eq(rs.lit(2)))
        .then(rs.lit("two"))
        .otherwise(rs.lit("many"))
        .alias("word"),
    )
    rows = out.collect()
    assert [r["word"] for r in rows] == ["one", "two", "many"]

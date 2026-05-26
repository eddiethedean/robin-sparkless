"""Tests for issue #1556: chained F.when() without .otherwise() in withColumn/select."""

from __future__ import annotations

from sparkless.testing import get_imports

_imports = get_imports()
SparkSession = _imports.SparkSession
F = _imports.F


def test_chained_when_without_otherwise_in_with_column() -> None:
    """when(a, x).when(b, y).when(c, z) without otherwise() — unmatched rows are null."""
    spark = SparkSession.builder.appName("issue_1556").get_or_create()
    df = spark.createDataFrame([("A",), ("B",), ("C",), ("D",)], ["type"])
    out = df.withColumn(
        "label",
        F.when(F.col("type") == "A", F.lit("alpha"))
        .when(F.col("type") == "B", F.lit("beta"))
        .when(F.col("type") == "C", F.lit("gamma")),
    )
    rows = {r["type"]: r["label"] for r in out.collect()}
    assert rows["A"] == "alpha"
    assert rows["B"] == "beta"
    assert rows["C"] == "gamma"
    assert rows["D"] is None


def test_chained_when_without_otherwise_in_select() -> None:
    """select() accepts chained when without otherwise (via .alias() or bare expression)."""
    spark = SparkSession.builder.appName("issue_1556_select").get_or_create()
    df = spark.createDataFrame([("A",), ("D",)], ["type"])
    out = df.select(
        F.when(F.col("type") == "A", F.lit("alpha"))
        .when(F.col("type") == "B", F.lit("beta"))
        .alias("label"),
    )
    rows = out.collect()
    assert rows[0]["label"] == "alpha"
    assert rows[1]["label"] is None

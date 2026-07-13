"""High-value regression tests from independent verification pass.

These tests validate documented behavior and would fail if common AI-generated
implementation shortcuts (wrong join default, skipped UDF eval, weak stubs)
were reintroduced.
"""

from __future__ import annotations

import pytest

from sparkless.testing import get_imports

_imports = get_imports()
SparkSession = _imports.SparkSession
F = _imports.F
T = _imports
udf = F.udf


def test_invalid_join_type_raises_instead_of_defaulting(spark) -> None:
    """Invalid how must error; must not silently default to inner join."""
    left = spark.createDataFrame([{"k": 1}, {"k": 2}])
    right = spark.createDataFrame([{"k": 1, "v": "a"}])

    with pytest.raises((RuntimeError, ValueError), match="join type"):
        left.join(right, on="k", how="lef").collect()


def test_sql_bare_union_deduplicates_rows(spark) -> None:
    """Bare SQL UNION deduplicates (UNION DISTINCT semantics)."""
    df = spark.createDataFrame(
        [("Alice", 1), ("Bob", 2), ("Alice", 1)],
        ["name", "age"],
    )
    df.createOrReplaceTempView("u1")
    df.createOrReplaceTempView("u2")

    distinct_rows = spark.sql(
        "SELECT name, age FROM u1 UNION SELECT name, age FROM u2"
    ).collect()
    all_rows = spark.sql(
        "SELECT name, age FROM u1 UNION ALL SELECT name, age FROM u2"
    ).collect()

    assert len(distinct_rows) == 2
    assert len(all_rows) == 6
    assert sorted((r["name"], r["age"]) for r in distinct_rows) == [
        ("Alice", 1),
        ("Bob", 2),
    ]


def test_udf_filter_eq_would_fail_if_udf_not_evaluated(spark) -> None:
    """Regression #1664: filter must evaluate UDF before == comparison.

    If the engine compared a placeholder to 10 without evaluating double(x),
    x=10 would incorrectly match (double(10)==10) instead of only x=5.
    """
    df = spark.createDataFrame([{"x": 1}, {"x": 5}, {"x": 10}])

    @udf(T.IntegerType())
    def double(x):
        return x * 2

    rows = df.filter(double(F.col("x")) == 10).collect()
    assert sorted(r["x"] for r in rows) == [5]
    assert 10 not in {r["x"] for r in rows}


def test_range_limit_enforced_with_clear_error(monkeypatch) -> None:
    monkeypatch.setenv("SPARKLESS_MAX_RANGE_ROWS", "1000")
    spark = SparkSession.builder.appName("range-limit-regression").getOrCreate()
    try:
        with pytest.raises((RuntimeError, ValueError), match="exceeding limit"):
            spark.range(0, 10_000).collect()
    finally:
        spark.stop()


def test_cross_join_guard_enforced_with_clear_error(monkeypatch) -> None:
    monkeypatch.setenv("SPARKLESS_MAX_CROSS_JOIN_ROWS", "10000")
    spark = SparkSession.builder.appName("cross-guard-regression").getOrCreate()
    try:
        left = spark.range(0, 200)
        right = spark.range(0, 200)
        with pytest.raises((RuntimeError, ValueError), match="materialize"):
            left.join(right, left.id > right.id, how="inner").collect()
    finally:
        spark.stop()

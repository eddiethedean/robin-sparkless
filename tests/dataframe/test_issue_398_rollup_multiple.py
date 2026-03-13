"""
Tests for #398: rollup() multiple columns (PySpark parity).

PySpark df.rollup("a", "b") and df.rollup(*cols) accept multiple column names.
"""

from __future__ import annotations

from sparkless.testing import get_imports

_imports = get_imports()
F = _imports.F


def test_rollup_two_columns_variadic(spark) -> None:
    """df.rollup(\"a\", \"b\").count() works with variadic args."""
    df = spark.createDataFrame(
        [{"a": 1, "b": 2, "v": 10}, {"a": 1, "b": 2, "v": 20}],
        ["a", "b", "v"],
    )
    out = df.rollup("a", "b").agg(F.count(F.col("v")).alias("count")).collect()
    assert len(out) >= 1
    assert all("count" in r for r in out)


def test_rollup_single_list(spark) -> None:
    """df.rollup([\"a\", \"b\"]) still works."""
    df = spark.createDataFrame(
        [{"a": 1, "b": 2, "v": 10}],
        ["a", "b", "v"],
    )
    out = df.rollup(["a", "b"]).agg(F.count(F.col("v")).alias("count")).collect()
    assert len(out) >= 1

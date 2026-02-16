"""
Tests for #419: createDataFrame single-column schema (PySpark parity).

When schema is a single type (e.g. "bigint", "string"), PySpark uses column name "value" and wraps each record.
"""

from __future__ import annotations

import robin_sparkless as rs


def _spark() -> rs.SparkSession:
    return rs.SparkSession.builder().app_name("issue_419").get_or_create()


def test_single_column_schema_bigint() -> None:
    """createDataFrame([1, 2, 3], "bigint") -> one column "value", three rows."""
    spark = _spark()
    df = spark.createDataFrame([1, 2, 3], "bigint")
    out = df.collect()
    assert len(out) == 3
    assert list(out[0].keys()) == ["value"]
    assert [r["value"] for r in out] == [1, 2, 3]


def test_single_column_schema_string() -> None:
    """createDataFrame(["a", "b"], "string") -> column "value"."""
    spark = _spark()
    df = spark.createDataFrame(["a", "b"], "string")
    out = df.collect()
    assert len(out) == 2
    assert [r["value"] for r in out] == ["a", "b"]

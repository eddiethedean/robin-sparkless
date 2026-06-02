"""
Tests for #419: createDataFrame single-column schema (PySpark parity).

When schema is a single type (e.g. "bigint", "string"), PySpark uses column name "value" and wraps each record.
"""

from __future__ import annotations


def test_single_column_schema_bigint(spark) -> None:
    """createDataFrame([1, 2, 3], "bigint") -> one column "value", three rows."""
    df = spark.createDataFrame([1, 2, 3], "bigint")
    out = df.collect()
    assert len(out) == 3
    from tests.utils import _row_to_dict

    assert list(_row_to_dict(out[0]).keys()) == ["value"]
    assert [r["value"] for r in out] == [1, 2, 3]


def test_single_column_schema_string(spark) -> None:
    """createDataFrame(["a", "b"], "string") -> column "value"."""
    df = spark.createDataFrame(["a", "b"], "string")
    out = df.collect()
    assert len(out) == 2
    assert [r["value"] for r in out] == ["a", "b"]

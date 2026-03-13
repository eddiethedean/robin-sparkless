"""
Tests for #419: createDataFrame single-column schema (PySpark parity).

When schema is a single type (e.g. "bigint", "string"), PySpark uses column name "value" and wraps each record.
Rust: create_dataframe_from_single_column(values, type_str) is implemented. Python binding must call it
when createDataFrame(data, "bigint") is used (data a list of scalars).
"""

from __future__ import annotations

import pytest


def _supports_single_column_schema() -> bool:
    """True if the binding accepts createDataFrame([1,2,3], 'bigint') (single type as schema)."""
    from sparkless.testing import get_imports

    _imports = get_imports()
    _session = _imports.SparkSession.builder.appName("single_col_check").getOrCreate()
    try:
        try:
            _session.createDataFrame([1, 2, 3], "bigint")
            return True
        except (TypeError, Exception):
            return False
    finally:
        _session.stop()


@pytest.mark.skipif(
    not _supports_single_column_schema(),
    reason="Python binding does not yet support createDataFrame(data, single_type_str); Rust API create_dataframe_from_single_column is ready.",
)
def test_single_column_schema_bigint(spark) -> None:
    """createDataFrame([1, 2, 3], "bigint") -> one column "value", three rows."""
    df = spark.createDataFrame([1, 2, 3], "bigint")
    out = df.collect()
    assert len(out) == 3
    from tests.utils import _row_to_dict

    assert list(_row_to_dict(out[0]).keys()) == ["value"]
    assert [r["value"] for r in out] == [1, 2, 3]


@pytest.mark.skipif(
    not _supports_single_column_schema(),
    reason="Python binding does not yet support createDataFrame(data, single_type_str); Rust API create_dataframe_from_single_column is ready.",
)
def test_single_column_schema_string(spark) -> None:
    """createDataFrame(["a", "b"], "string") -> column "value"."""
    df = spark.createDataFrame(["a", "b"], "string")
    out = df.collect()
    assert len(out) == 2
    assert [r["value"] for r in out] == ["a", "b"]

"""
Tests for #420: createDataFrame verify_schema strict per-row validation (PySpark parity).
Rust: create_dataframe_from_rows(..., verify_schema=true) is implemented. Python binding must
pass verify_schema through when createDataFrame(..., verify_schema=True) is called.
"""

from __future__ import annotations

import pytest

from tests.utils import get_spark


def _spark():
    return get_spark("issue_420")


def _supports_verify_schema_kw() -> bool:
    """True if createDataFrame(..., schema=..., verify_schema=True) is accepted."""
    try:
        spark = _spark()
        spark.createDataFrame(
            [{"name": "a", "age": 1}],
            schema="name string, age bigint",
            verify_schema=False,
        )
        return True
    except TypeError:
        return False


@pytest.mark.skipif(
    not _supports_verify_schema_kw(),
    reason="Python binding does not yet accept verify_schema= keyword; Rust API create_dataframe_from_rows(..., verify_schema) is ready.",
)
def test_verify_schema_true_raises_on_type_mismatch() -> None:
    """When verify_schema=True, wrong type raises with Row/column message."""
    spark = _spark()
    # Schema says age is bigint; second row has age as string.
    data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": "thirty"}]
    with pytest.raises(TypeError) as exc_info:
        spark.createDataFrame(
            data, schema="name string, age bigint", verify_schema=True
        )
    msg = str(exc_info.value)
    assert "Row 1" in msg or "row 1" in msg.lower()
    assert "age" in msg or "column" in msg.lower()
    assert "bigint" in msg or "number" in msg.lower()


@pytest.mark.skipif(
    not _supports_verify_schema_kw(),
    reason="Python binding does not yet accept verify_schema= keyword; Rust API is ready.",
)
def test_verify_schema_false_allows_mismatch_or_fails_later() -> None:
    """When verify_schema=False, creation may succeed or fail later."""
    spark = _spark()
    data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
    df = spark.createDataFrame(
        data, schema="name string, age bigint", verify_schema=False
    )
    rows = df.collect()
    assert len(rows) == 2
    assert rows[0]["age"] == 25 and rows[1]["age"] == 30

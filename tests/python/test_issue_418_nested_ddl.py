"""Tests for #418: createDataFrame with full nested DDL schema."""

from __future__ import annotations

import robin_sparkless as rs


def _spark():
    return rs.SparkSession.builder().app_name("issue_418").get_or_create()


def test_nested_ddl_struct_and_array() -> None:
    """createDataFrame(..., schema='addr struct<city:string>, tags array<string>') parses and creates schema."""
    spark = _spark()
    # DDL with nested types: comma inside struct<> and array<> must not split top-level fields.
    schema_ddl = "addr struct<city:string>, tags array<string>"
    df = spark.createDataFrame(
        [
            {"addr": {"city": "NYC"}, "tags": ["a", "b"]},
            {"addr": {"city": "LA"}, "tags": ["c"]},
        ],
        schema=schema_ddl,
    )
    names = df.columns()
    assert "addr" in names
    assert "tags" in names
    rows = df.collect()
    assert len(rows) == 2
    # Row content: addr as struct, tags as list
    r0 = rows[0]
    assert r0["addr"] is not None
    assert r0["tags"] is not None

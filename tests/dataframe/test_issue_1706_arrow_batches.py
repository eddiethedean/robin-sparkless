"""Regression coverage for bounded Arrow batch export (issue #1706)."""

from __future__ import annotations

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")


def test_to_arrow_batches_respects_limits_and_closes(spark) -> None:
    frame = spark.range(7)
    batches = frame.toArrowBatches(max_rows=3, max_bytes=1024)

    exported = list(batches)
    assert sum(batch.num_rows for batch in exported) == 7
    assert [batch.num_rows for batch in exported] == [3, 3, 1]
    assert all(0 < batch.num_rows <= 3 for batch in exported)
    assert all(batch.nbytes <= 1024 for batch in exported)
    batches.close()
    assert list(batches) == []


def test_to_arrow_batches_splits_at_byte_limit(spark) -> None:
    exported = list(spark.range(7).toArrowBatches(max_rows=5, max_bytes=16))

    assert [batch.num_rows for batch in exported] == [2, 2, 2, 1]
    assert all(batch.nbytes <= 16 for batch in exported)


def test_to_arrow_batches_preserves_arrow_schema_details(spark) -> None:
    nested_type = pa.list_(pa.field("item", pa.int64(), nullable=False))
    schema = pa.schema(
        [
            pa.field(
                "values",
                nested_type,
                nullable=True,
                metadata={b"field": b"preserved"},
            )
        ],
        metadata={b"schema": b"preserved"},
    )
    source = pa.Table.from_pylist([{"values": [1, 2]}, {"values": None}], schema=schema)
    exported = list(
        spark.createDataFrame(source).toArrowBatches(max_rows=2, max_bytes=1024)
    )

    assert pa.schema(exported[0].schema, metadata=schema.metadata) == schema
    assert exported[0].schema.field("values").metadata == {b"field": b"preserved"}


def test_replaced_arrow_column_does_not_reuse_non_nullable_field(spark) -> None:
    from sparkless.testing import get_imports

    F = get_imports().functions
    schema = pa.schema([pa.field("id", pa.int64(), nullable=False)])
    source = pa.Table.from_pylist([{"id": 1}, {"id": 2}], schema=schema)
    frame = spark.createDataFrame(source).withColumn("id", F.lit(None).cast("long"))

    exported = frame.toArrow()
    assert exported.schema.field("id").nullable
    sink = pa.BufferOutputStream()
    pq.write_table(exported, sink)


def test_with_columns_replaced_arrow_column_drops_stale_field_metadata(spark) -> None:
    from sparkless.testing import get_imports

    F = get_imports().functions
    schema = pa.schema([pa.field("id", pa.int64(), metadata={b"field": b"stale"})])
    source = pa.Table.from_pylist([{"id": 1}], schema=schema)
    frame = spark.createDataFrame(source).withColumns({"id": F.lit(9)})

    exported = frame.toArrow()
    assert exported.schema.field("id").metadata is None

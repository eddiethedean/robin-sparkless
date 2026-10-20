"""Regression tests for collecting valid structs with null fields."""

import pytest


@pytest.mark.delta
def test_delta_collect_preserves_valid_all_null_structs_and_null_parents(spark, tmp_path):
    pa = pytest.importorskip("pyarrow")
    deltalake = pytest.importorskip("deltalake")

    schema = pa.schema(
        [
            ("details", pa.struct([("label", pa.string())])),
            ("items", pa.list_(pa.struct([("label", pa.string())]))),
        ]
    )
    expected = [
        {"details": {"label": "present"}, "items": [{"label": None}, None]},
        {"details": None, "items": [{"label": "nested"}]},
        {"details": {"label": None}, "items": None},
    ]
    arrow = pa.Table.from_pylist(expected, schema=schema)
    path = tmp_path / "null-structs"
    deltalake.write_deltalake(str(path), arrow)

    frame = spark.read.format("delta").load(str(path))
    actual = [
        {"details": row["details"], "items": row["items"]}
        for row in frame.collect()
    ]
    assert actual == expected

"""Regression tests for collecting valid structs with null fields."""

import pytest

from sparkless.testing import get_imports

F = get_imports().F


@pytest.mark.delta
def test_delta_collect_preserves_valid_all_null_structs_and_null_parents(
    spark, tmp_path
):
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
        {"details": row["details"], "items": row["items"]} for row in frame.collect()
    ]
    assert actual == expected


@pytest.mark.delta
def test_withfield_preserves_nested_validity_after_union(spark, tmp_path):
    pa = pytest.importorskip("pyarrow")
    deltalake = pytest.importorskip("deltalake")

    schema = pa.schema(
        [
            ("replacement", pa.list_(pa.int32())),
            (
                "details",
                pa.struct(
                    [
                        ("count", pa.int32()),
                        ("nested", pa.struct([("label", pa.string())])),
                        ("items", pa.list_(pa.int32())),
                    ]
                ),
            ),
        ]
    )
    value = {
        "replacement": [2],
        "details": {"count": 1, "nested": {"label": None}, "items": [1]},
    }
    source = tmp_path / "withfield-source"
    deltalake.write_deltalake(str(source), pa.Table.from_pylist([value], schema=schema))

    frame = spark.read.format("delta").load(str(source))
    unioned = frame.union(frame)
    updated = unioned.withColumn(
        "details", F.col("details").withField("items", F.col("replacement"))
    )
    expected_details = {
        "count": 1,
        "nested": {"label": None},
        "items": [2],
    }
    assert [row["details"] for row in updated.collect()] == [
        expected_details,
        expected_details,
    ]

    destination = tmp_path / "withfield-output"
    updated.write.format("delta").mode("overwrite").save(str(destination))
    assert deltalake.DeltaTable(str(destination)).to_pyarrow_table().to_pylist() == [
        {"replacement": [2], "details": expected_details},
        {"replacement": [2], "details": expected_details},
    ]

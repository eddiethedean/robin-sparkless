"""Regression tests for logical Delta MAP writes and reads."""

import json
import time
import uuid

import pytest

from sparkless.testing import get_imports

_imports = get_imports()
ArrayType = _imports.ArrayType
LongType = _imports.LongType
MapType = _imports.MapType
StringType = _imports.StringType
StructField = _imports.StructField
StructType = _imports.StructType


def _map_schema():
    return StructType(
        [
            StructField("id", LongType()),
            StructField("m", MapType(StringType(), LongType())),
        ]
    )


def _write_map_of_struct_delta_table(path, field_name, value):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    path.mkdir()
    value_type = {
        "type": "struct",
        "fields": [
            {"name": field_name, "type": "long", "nullable": True, "metadata": {}}
        ],
    }
    schema = {
        "type": "struct",
        "fields": [
            {"name": "id", "type": "long", "nullable": True, "metadata": {}},
            {
                "name": "m",
                "type": {
                    "type": "map",
                    "keyType": "string",
                    "valueType": value_type,
                    "valueContainsNull": True,
                },
                "nullable": True,
                "metadata": {},
            },
        ],
    }
    table = pa.table(
        {
            "id": [value],
            "m": pa.array(
                [[("k", {field_name: value})]],
                type=pa.map_(pa.string(), pa.struct([(field_name, pa.int64())])),
            ),
        }
    )
    parquet_path = path / "part.parquet"
    pq.write_table(table, parquet_path)
    now = int(time.time() * 1000)
    actions = [
        {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
        {
            "metaData": {
                "id": str(uuid.uuid4()),
                "format": {"provider": "parquet", "options": {}},
                "schemaString": json.dumps(schema),
                "partitionColumns": [],
                "configuration": {},
                "createdTime": now,
            }
        },
        {
            "add": {
                "path": parquet_path.name,
                "partitionValues": {},
                "size": parquet_path.stat().st_size,
                "modificationTime": now,
                "dataChange": True,
            }
        },
    ]
    log_path = path / "_delta_log"
    log_path.mkdir()
    (log_path / "00000000000000000000.json").write_text(
        "\n".join(json.dumps(action) for action in actions) + "\n"
    )


@pytest.mark.delta
def test_partitioned_map_delta_write_round_trips(spark, tmp_path):
    path = tmp_path / "partitioned-map"
    schema = StructType(
        [
            StructField("id", LongType()),
            StructField("p", LongType()),
            StructField("m", MapType(StringType(), LongType())),
        ]
    )
    df = spark.createDataFrame([(1, 10, {"a": 1}), (2, 20, {"b": 2})], schema)

    df.write.format("delta").partitionBy("p").mode("overwrite").save(str(path))

    rows = spark.read.format("delta").load(str(path)).collect()
    assert {row["p"]: row["m"] for row in rows} == {10: {"a": 1}, 20: {"b": 2}}


@pytest.mark.delta
def test_append_missing_map_column_keeps_parquet_map_encoding(spark, tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = tmp_path / "map-append"
    spark.createDataFrame([(1, {"a": 1})], _map_schema()).write.format("delta").mode(
        "overwrite"
    ).save(str(path))
    spark.createDataFrame([(2,)], ["id"]).write.format("delta").mode("append").save(
        str(path)
    )

    appended_file = max(path.glob("*.parquet"), key=lambda item: item.stat().st_mtime_ns)
    map_type = pq.read_schema(appended_file).field("m").type
    assert pa.types.is_map(map_type)


@pytest.mark.delta
def test_nested_case_insensitive_append_preserves_struct_value(spark, tmp_path):
    path = tmp_path / "nested-case"
    lower_schema = StructType([
        StructField("id", LongType()),
        StructField("s", StructType([StructField("a", LongType())])),
    ])
    upper_schema = StructType([
        StructField("id", LongType()),
        StructField("s", StructType([StructField("A", LongType())])),
    ])
    spark.createDataFrame([(1, {"a": 1})], lower_schema).write.format("delta").mode(
        "overwrite"
    ).save(str(path))
    spark.createDataFrame([(2, {"A": 2})], upper_schema).write.format("delta").mode(
        "append"
    ).save(str(path))

    rows = spark.read.format("delta").load(str(path)).collect()
    assert {row["id"]: row["s"]["a"] for row in rows} == {1: 1, 2: 2}


@pytest.mark.delta
def test_nested_case_insensitive_append_preserves_map_value(spark, tmp_path):
    target = tmp_path / "map-value-case-target"
    source = tmp_path / "map-value-case-source"
    _write_map_of_struct_delta_table(target, "a", 1)
    _write_map_of_struct_delta_table(source, "A", 2)

    incoming = spark.read.format("delta").load(str(source))
    incoming.write.format("delta").mode("append").save(str(target))

    rows = spark.read.format("delta").load(str(target)).collect()
    assert {row["id"]: row["m"]["k"]["a"] for row in rows} == {1: 1, 2: 2}


@pytest.mark.delta
def test_catalog_table_keeps_map_type_when_copied(spark, tmp_path):
    table_name = f"issue_1684_map_{tmp_path.name.replace('-', '_')}"
    spark.createDataFrame([(1, {"a": 1})], _map_schema()).write.format("delta").mode(
        "overwrite"
    ).saveAsTable(table_name)

    table = spark.table(table_name)
    assert isinstance(table.schema["m"].dataType, MapType)
    copy_path = tmp_path / "catalog-copy"
    table.write.format("delta").mode("overwrite").save(str(copy_path))
    copied_schema = spark.read.format("delta").load(str(copy_path)).schema
    assert isinstance(copied_schema["m"].dataType, MapType)


@pytest.mark.delta
def test_sql_catalog_read_keeps_map_type_when_copied(spark, tmp_path):
    table_name = f"issue_1684_sql_map_{tmp_path.name.replace('-', '_')}"
    spark.createDataFrame([(1, {"a": 1})], _map_schema()).write.format("delta").mode(
        "overwrite"
    ).saveAsTable(table_name)

    for query, output_column in (
        (f"SELECT * FROM {table_name}", "m"),
        (f"SELECT t.m AS copied FROM {table_name} t", "copied"),
    ):
        table = spark.sql(query)
        assert isinstance(table.schema[output_column].dataType, MapType)
        copy_path = tmp_path / f"sql-catalog-copy-{output_column}"
        table.write.format("delta").mode("overwrite").save(str(copy_path))
        copied_schema = spark.read.format("delta").load(str(copy_path)).schema
        assert isinstance(copied_schema[output_column].dataType, MapType)


@pytest.mark.delta
def test_temp_view_shadowing_delta_table_keeps_array_semantics(spark, tmp_path):
    table_name = f"issue_1684_view_map_{tmp_path.name.replace('-', '_')}"
    spark.createDataFrame([(1, {"a": 1})], _map_schema()).write.format("delta").mode(
        "overwrite"
    ).saveAsTable(table_name)

    array_schema = StructType(
        [
            StructField("id", LongType()),
            StructField(
                "m",
                ArrayType(
                    StructType(
                        [StructField("key", StringType()), StructField("value", LongType())]
                    )
                ),
            ),
        ]
    )
    view = spark.createDataFrame(
        [(2, [{"key": "x", "value": 2}, {"key": "x", "value": 3}])], array_schema
    )
    view.createOrReplaceTempView(table_name)

    for index, resolved in enumerate((spark.table(table_name), spark.read.table(table_name))):
        assert isinstance(resolved.schema["m"].dataType, ArrayType)
        copy_path = tmp_path / f"view-copy-{index}"
        resolved.write.format("delta").mode("overwrite").save(str(copy_path))
        copied_schema = spark.read.format("delta").load(str(copy_path)).schema
        assert isinstance(copied_schema["m"].dataType, ArrayType)


@pytest.mark.delta
def test_delta_reader_keeps_plain_parquet_fallback(spark, tmp_path):
    path = tmp_path / "plain-parquet"
    path.mkdir()
    spark.createDataFrame([(1,)], ["id"]).write.parquet(str(path / "part.parquet"))

    rows = spark.read.format("delta").load(str(path)).collect()
    assert [row["id"] for row in rows] == [1]

"""Regression tests for logical Delta MAP writes and reads."""

import json
import time
import uuid

import pytest

from sparkless.testing import create_session, get_imports

_imports = get_imports()
ArrayType = _imports.ArrayType
LongType = _imports.LongType
MapType = _imports.MapType
StringType = _imports.StringType
StructField = _imports.StructField
StructType = _imports.StructType
F = _imports.F


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


def _write_column_mapped_map_table(path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    path.mkdir()
    mapped_value = {
        "type": "struct",
        "fields": [
            {
                "name": "a",
                "type": "long",
                "nullable": True,
                "metadata": {
                    "delta.columnMapping.id": 2,
                    "delta.columnMapping.physicalName": "col_a",
                },
            }
        ],
    }
    schema = {
        "type": "struct",
        "fields": [
            {
                "name": "id",
                "type": "long",
                "nullable": True,
                "metadata": {
                    "delta.columnMapping.id": 3,
                    "delta.columnMapping.physicalName": "col_id",
                },
            },
            {
                "name": "m",
                "type": {
                    "type": "map",
                    "keyType": "string",
                    "valueType": mapped_value,
                    "valueContainsNull": True,
                },
                "nullable": True,
                "metadata": {
                    "delta.columnMapping.id": 1,
                    "delta.columnMapping.physicalName": "col_m",
                },
            },
        ],
    }
    pq.write_table(
        pa.table(
            {
                "col_id": [1],
                "col_m": pa.array(
                    [[("k", {"col_a": 42})]],
                    type=pa.map_(pa.string(), pa.struct([("col_a", pa.int64())])),
                ),
            }
        ),
        path / "part.parquet",
    )
    now = int(time.time() * 1000)
    actions = [
        {"protocol": {"minReaderVersion": 2, "minWriterVersion": 5}},
        {
            "metaData": {
                "id": str(uuid.uuid4()),
                "format": {"provider": "parquet", "options": {}},
                "schemaString": json.dumps(schema),
                "partitionColumns": [],
                "configuration": {
                    "delta.columnMapping.mode": "name",
                    "delta.columnMapping.maxColumnId": "3",
                },
                "createdTime": now,
            }
        },
        {
            "add": {
                "path": "part.parquet",
                "partitionValues": {},
                "size": (path / "part.parquet").stat().st_size,
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


def _write_non_nullable_id_table(path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    path.mkdir()
    pq.write_table(pa.table({"id": [1]}), path / "part.parquet")
    now = int(time.time() * 1000)
    schema = {
        "type": "struct",
        "fields": [{"name": "id", "type": "long", "nullable": False, "metadata": {}}],
    }
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
                "path": "part.parquet",
                "partitionValues": {},
                "size": (path / "part.parquet").stat().st_size,
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


def _write_nested_non_nullable_table(path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    path.mkdir()
    pq.write_table(
        pa.table(
            {
                "id": [1],
                "s": pa.array(
                    [{"x": 5, "y": 6}],
                    type=pa.struct([("x", pa.int64()), ("y", pa.int64())]),
                ),
            }
        ),
        path / "part.parquet",
    )
    now = int(time.time() * 1000)
    schema = {
        "type": "struct",
        "fields": [
            {"name": "id", "type": "long", "nullable": True, "metadata": {}},
            {
                "name": "s",
                "type": {
                    "type": "struct",
                    "fields": [
                        {
                            "name": "x",
                            "type": "long",
                            "nullable": False,
                            "metadata": {},
                        },
                        {"name": "y", "type": "long", "nullable": True, "metadata": {}},
                    ],
                },
                "nullable": True,
                "metadata": {},
            },
        ],
    }
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
                "path": "part.parquet",
                "partitionValues": {},
                "size": (path / "part.parquet").stat().st_size,
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
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    path = tmp_path / "map-append"
    spark.createDataFrame([(1, {"a": 1})], _map_schema()).write.format("delta").mode(
        "overwrite"
    ).save(str(path))
    spark.createDataFrame([(2,)], ["id"]).write.format("delta").mode("append").save(
        str(path)
    )

    appended_file = max(
        path.glob("*.parquet"), key=lambda item: item.stat().st_mtime_ns
    )
    map_type = pq.read_schema(appended_file).field("m").type
    assert pa.types.is_map(map_type)


@pytest.mark.delta
def test_nested_case_insensitive_append_preserves_struct_value(spark, tmp_path):
    path = tmp_path / "nested-case"
    lower_schema = StructType(
        [
            StructField("id", LongType()),
            StructField("s", StructType([StructField("a", LongType())])),
        ]
    )
    upper_schema = StructType(
        [
            StructField("id", LongType()),
            StructField("s", StructType([StructField("A", LongType())])),
        ]
    )
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
                        [
                            StructField("key", StringType()),
                            StructField("value", LongType()),
                        ]
                    )
                ),
            ),
        ]
    )
    view = spark.createDataFrame(
        [(2, [{"key": "x", "value": 2}, {"key": "x", "value": 3}])], array_schema
    )
    view.createOrReplaceTempView(table_name)

    for index, resolved in enumerate(
        (spark.table(table_name), spark.read.table(table_name))
    ):
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


@pytest.mark.delta
def test_delta_null_map_column_retains_existing_map_type(spark, tmp_path):
    path = tmp_path / "null-map-append"
    spark.createDataFrame([(1, {"a": 1})], _map_schema()).write.format("delta").mode(
        "overwrite"
    ).save(str(path))
    incoming = spark.createDataFrame([(2,)], ["id"]).withColumn("m", F.lit(None))
    incoming.write.format("delta").mode("append").save(str(path))

    result = spark.read.format("delta").load(str(path))
    assert isinstance(result.schema["m"].dataType, MapType)
    assert {row["id"]: row["m"] for row in result.collect()} == {1: {"a": 1}, 2: None}


@pytest.mark.delta
def test_append_case_only_partition_rename(spark, tmp_path):
    path = tmp_path / "case-partition"
    spark.createDataFrame([(1, 1)], ["id", "p"]).write.format("delta").partitionBy(
        "p"
    ).mode("overwrite").save(str(path))
    spark.createDataFrame([(2, 2)], ["id", "P"]).write.format("delta").partitionBy(
        "P"
    ).mode("append").save(str(path))

    assert {
        row["id"]: row["p"]
        for row in spark.read.format("delta").load(str(path)).collect()
    } == {
        1: 1,
        2: 2,
    }


@pytest.mark.delta
def test_append_rejects_null_in_non_nullable_column(spark, tmp_path):
    path = tmp_path / "not-null"
    _write_non_nullable_id_table(path)
    incoming = spark.createDataFrame([(2,)], ["id"]).withColumn("id", F.lit(None))
    with pytest.raises(Exception, match="non-nullable field 'id' contains NULL"):
        incoming.write.format("delta").mode("append").save(str(path))


@pytest.mark.delta
def test_file_url_write_keeps_data_under_table_root(spark, tmp_path):
    path = tmp_path / "file-url-table"
    path.mkdir()
    spark.createDataFrame([(1,)], ["id"]).write.format("delta").mode("overwrite").save(
        path.as_uri()
    )

    assert [
        row["id"] for row in spark.read.format("delta").load(path.as_uri()).collect()
    ] == [1]


@pytest.mark.delta
def test_read_delta_method_preserves_map_type(spark, tmp_path):
    path = tmp_path / "read-delta-method"
    spark.createDataFrame([(1, {"a": 1})], _map_schema()).write.format("delta").mode(
        "overwrite"
    ).save(str(path))

    result = spark.read.delta(str(path))
    assert isinstance(result.schema["m"].dataType, MapType)


@pytest.mark.delta
def test_map_metadata_survives_temp_view_and_join(spark, tmp_path):
    path = tmp_path / "view-map"
    spark.createDataFrame([(1, {"a": 1})], _map_schema()).write.format("delta").mode(
        "overwrite"
    ).save(str(path))
    source = spark.read.delta(str(path))
    source.createOrReplaceTempView("issue_1684_map_view")
    spark.createDataFrame([(1, "x")], ["id", "label"]).createOrReplaceTempView(
        "issue_1684_map_lookup"
    )

    view = spark.table("issue_1684_map_view")
    joined = spark.sql(
        "SELECT m.m AS m FROM issue_1684_map_view m "
        "JOIN issue_1684_map_lookup l ON m.id = l.id"
    )
    assert isinstance(view.schema["m"].dataType, MapType)
    assert isinstance(joined.schema["m"].dataType, MapType)
    for index, frame in enumerate((view, joined)):
        copy_path = tmp_path / f"view-map-copy-{index}"
        frame.write.format("delta").mode("overwrite").save(str(copy_path))
        assert isinstance(
            spark.read.format("delta").load(str(copy_path)).schema["m"].dataType,
            MapType,
        )


@pytest.mark.delta
def test_global_temp_map_metadata_is_shared_with_new_session(spark, tmp_path):
    path = tmp_path / "global-view-map"
    view_name = f"issue_1684_global_map_{tmp_path.name.replace('-', '_')}"
    spark.createDataFrame([(1, {"a": 1})], _map_schema()).write.format("delta").mode(
        "overwrite"
    ).save(str(path))
    spark.read.delta(str(path)).createOrReplaceGlobalTempView(view_name)

    new_session = spark.newSession()
    independent_session = _imports.SparkSession(app_name=f"global-map-{view_name}")
    try:
        for session in (new_session, independent_session):
            result = session.table(f"global_temp.{view_name}")
            assert isinstance(result.schema["m"].dataType, MapType)
    finally:
        spark.catalog.dropGlobalTempView(view_name)


@pytest.mark.delta
def test_nested_non_nullable_field_rejects_null_value(spark, tmp_path):
    path = tmp_path / "nested-not-null"
    _write_nested_non_nullable_table(path)
    incoming_schema = StructType(
        [
            StructField("id", LongType()),
            StructField(
                "s",
                StructType(
                    [StructField("x", LongType()), StructField("y", LongType())]
                ),
            ),
        ]
    )
    incoming = spark.createDataFrame([(2, {"x": None, "y": 7})], incoming_schema)

    with pytest.raises(Exception, match="non-nullable field 's.x' contains NULL"):
        incoming.write.format("delta").mode("append").save(str(path))


@pytest.mark.delta
def test_column_mapping_inside_map_values_is_respected(spark, tmp_path):
    path = tmp_path / "mapped-map-value"
    _write_column_mapped_map_table(path)

    rows = spark.read.format("delta").load(str(path)).collect()
    assert rows[0]["m"]["k"]["a"] == 42


@pytest.mark.delta
@pytest.mark.xdist_group(name="delta_serial")
def test_qualified_delta_save_as_table_appends_to_existing_nested_warehouse_path(
    tmp_path,
):
    warehouse = tmp_path / "warehouse"
    spark = create_session(
        app_name="qualified_delta_warehouse_append",
        enable_delta=True,
        **{"spark.sql.warehouse.dir": str(warehouse)},
    )
    table_name = "legacy_schema.legacy_table"
    nested_path = warehouse / "legacy_schema" / "legacy_table"

    try:
        spark.createDataFrame([(1,)], ["id"]).write.format("delta").mode(
            "overwrite"
        ).save(str(nested_path))

        spark.createDataFrame([(2,)], ["id"]).write.format("delta").mode(
            "append"
        ).saveAsTable(table_name)

        assert sorted(row["id"] for row in spark.table(table_name).collect()) == [1, 2]
        assert (nested_path / "_delta_log" / "00000000000000000001.json").is_file()
        assert not (warehouse / table_name / "_delta_log").exists()
    finally:
        spark.stop()

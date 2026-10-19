"""Regression tests for logical Delta MAP writes and reads."""

import pytest

from sparkless.testing import get_imports

_imports = get_imports()
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
def test_delta_reader_keeps_plain_parquet_fallback(spark, tmp_path):
    path = tmp_path / "plain-parquet"
    path.mkdir()
    spark.createDataFrame([(1,)], ["id"]).write.parquet(str(path / "part.parquet"))

    rows = spark.read.format("delta").load(str(path)).collect()
    assert [row["id"] for row in rows] == [1]

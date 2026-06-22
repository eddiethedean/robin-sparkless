"""H-1: Delta parquet URIs must stay under the table root."""

from __future__ import annotations

import json
import tempfile
import uuid
from pathlib import Path

import pytest

from sparkless.testing import get_imports

_imports = get_imports()
SparkSession = _imports.SparkSession


def _write_minimal_delta_log(table: Path, parquet_path: str) -> None:
    schema = (
        '{"type":"struct","fields":[{"name":"x","type":"long",'
        '"nullable":true,"metadata":{}}]}'
    )
    log_dir = table / "_delta_log"
    log_dir.mkdir(parents=True, exist_ok=True)
    commit = [
        {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
        {
            "metaData": {
                "id": str(uuid.uuid4()),
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema,
                "partitionColumns": [],
                "configuration": {},
                "createdTime": 1,
            }
        },
        {"commitInfo": {"timestamp": 1, "operation": "WRITE", "isBlindAppend": True}},
        {
            "add": {
                "path": parquet_path,
                "partitionValues": {},
                "size": 1000,
                "modificationTime": 1,
                "dataChange": True,
            }
        },
    ]
    (log_dir / "00000000000000000000.json").write_text(
        "\n".join(json.dumps(o) for o in commit)
    )


@pytest.fixture
def delta_escape_fixture(tmp_path):
    table = tmp_path / "table"
    table.mkdir()
    inside = table / "part-00000.parquet"
    outside = tmp_path / "outside.parquet"
    spark = SparkSession.builder.appName("delta-uri").getOrCreate()
    spark.createDataFrame([{"x": 1}]).write.mode("overwrite").parquet(str(inside))
    spark.createDataFrame([{"x": 999}]).write.mode("overwrite").parquet(str(outside))
    yield spark, table, outside
    spark.stop()


def test_delta_rejects_log_entry_outside_table(delta_escape_fixture) -> None:
    spark, table, outside = delta_escape_fixture
    _write_minimal_delta_log(table, "../outside.parquet")
    with pytest.raises(Exception, match="escapes table root"):
        spark.read.format("delta").load(str(table)).collect()


def test_delta_rejects_absolute_uri_outside_table(delta_escape_fixture) -> None:
    spark, table, outside = delta_escape_fixture
    outside_uri = f"file://{outside.resolve()}"
    _write_minimal_delta_log(table, outside_uri)
    with pytest.raises(Exception, match="escapes table root"):
        spark.read.format("delta").load(str(table)).collect()


def test_delta_reads_in_table_parquet(delta_escape_fixture) -> None:
    spark, table, _outside = delta_escape_fixture
    _write_minimal_delta_log(table, "part-00000.parquet")
    rows = spark.read.format("delta").load(str(table)).collect()
    assert len(rows) == 1
    assert rows[0]["x"] == 1

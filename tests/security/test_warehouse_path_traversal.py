"""C-1: warehouse table() must not read outside spark.sql.warehouse.dir."""

from __future__ import annotations

import pytest

from sparkless.testing import get_imports

from tests.security.conftest import SECURITY_ERROR, assert_security_error

_imports = get_imports()
SparkSession = _imports.SparkSession


def _spark_with_warehouse(warehouse) -> object:
    return (
        SparkSession.builder.app_name("path-security")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .getOrCreate()
    )


def test_table_rejects_path_traversal_outside_warehouse(tmp_path) -> None:
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    secret = outside / "secret_table"
    secret.mkdir()

    spark = _spark_with_warehouse(warehouse)
    try:
        with pytest.raises(SECURITY_ERROR) as exc_info:
            spark.table("../../outside/secret_table").collect()
        assert_security_error(exc_info.value, "..")
    finally:
        spark.stop()


def test_table_rejects_absolute_path_outside_warehouse(tmp_path) -> None:
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()

    spark = _spark_with_warehouse(warehouse)
    try:
        with pytest.raises(SECURITY_ERROR) as exc_info:
            spark.table(str(outside / "secret")).collect()
        assert_security_error(exc_info.value, "relative")
    finally:
        spark.stop()


def test_table_rejects_null_byte_in_name(tmp_path) -> None:
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()

    spark = _spark_with_warehouse(warehouse)
    try:
        with pytest.raises(SECURITY_ERROR) as exc_info:
            spark.table("people\x00evil").collect()
        assert_security_error(exc_info.value, "nul")
    finally:
        spark.stop()


def test_traversal_does_not_leak_outside_data(tmp_path) -> None:
    """Reject traversal before reading secret parquet outside warehouse."""
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    secret_dir = outside / "secret_table"
    secret_dir.mkdir()

    spark = _spark_with_warehouse(warehouse)
    try:
        spark.createDataFrame([{"secret": 999}], schema="secret bigint").write.mode(
            "overwrite"
        ).parquet(str(secret_dir / "data.parquet"))

        with pytest.raises(SECURITY_ERROR):
            rows = spark.table("../../outside/secret_table").collect()
            assert not any(r.get("secret") == 999 for r in rows), (
                "path traversal leaked data outside warehouse"
            )
    finally:
        spark.stop()


def test_valid_warehouse_table_reads(tmp_path) -> None:
    warehouse = tmp_path / "warehouse"
    table_dir = warehouse / "people"
    table_dir.mkdir(parents=True)

    spark = _spark_with_warehouse(warehouse)
    try:
        df = spark.createDataFrame([{"id": 1}], schema="id bigint")
        df.write.mode("overwrite").parquet(str(table_dir / "data.parquet"))
        out = spark.table("people").collect()
        assert len(out) == 1
        assert out[0]["id"] == 1
    finally:
        spark.stop()

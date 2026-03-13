"""JDBC read/write tests against MariaDB.

These tests use testcontainers to automatically start a MariaDB container,
or fall back to SPARKLESS_TEST_JDBC_MARIADB_URL environment variable if set.

Required tables (created automatically by testcontainers fixture):
  - sparkless_jdbc_test (id BIGINT PRIMARY KEY, name TEXT) — for read/append
  - sparkless_jdbc_writeread_test (id BIGINT, name TEXT) — for overwrite/read-back
"""

from __future__ import annotations

import os

import pytest

_HAS_ENV_URL = "SPARKLESS_TEST_JDBC_MARIADB_URL" in os.environ


def _use_env_url() -> bool:
    return _HAS_ENV_URL


@pytest.fixture
def jdbc_url(request, spark):
    """Get JDBC URL - from env var or testcontainers fixture."""
    if _use_env_url():
        return os.environ["SPARKLESS_TEST_JDBC_MARIADB_URL"]
    try:
        conn = request.getfixturevalue("mariadb_jdbc")
        return conn.url
    except pytest.FixtureLookupError:
        pytest.skip(
            "MariaDB container not available and SPARKLESS_TEST_JDBC_MARIADB_URL not set"
        )


@pytest.fixture
def jdbc_props(request):
    """Get JDBC properties - from env var or testcontainers fixture."""
    if _use_env_url():
        return {
            "user": os.getenv("SPARKLESS_TEST_JDBC_MARIADB_USER", ""),
            "password": os.getenv("SPARKLESS_TEST_JDBC_MARIADB_PASSWORD", ""),
            "driver": "org.mariadb.jdbc.Driver",
        }
    try:
        conn = request.getfixturevalue("mariadb_jdbc")
        return conn.properties
    except pytest.FixtureLookupError:
        return {"driver": "org.mariadb.jdbc.Driver"}


def test_read_jdbc_table_round_trip(spark, jdbc_url, jdbc_props) -> None:
    df = spark.read.jdbc(
        url=jdbc_url, table="sparkless_jdbc_test", properties=jdbc_props
    )
    rows = df.collect()
    assert isinstance(rows, list)
    assert len(rows) >= 2
    schema_names = [f.name for f in df.schema.fields]
    assert "id" in schema_names and "name" in schema_names


def test_read_jdbc_via_format_options_load(spark, jdbc_url, jdbc_props) -> None:
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "sparkless_jdbc_test")
        .options(jdbc_props)
        .load(".")
    )
    rows = df.collect()
    assert isinstance(rows, list)
    assert len(rows) >= 2


def test_read_jdbc_with_query_option(spark, jdbc_url, jdbc_props) -> None:
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("query", "SELECT id, name FROM sparkless_jdbc_test WHERE id = 1")
        .options(jdbc_props)
        .load(".")
    )
    rows = df.collect()
    assert isinstance(rows, list)
    assert len(rows) <= 1
    if len(rows) == 1:
        row = rows[0]
        assert row["id"] == 1 or row["id"] == "1"


def test_write_jdbc_append(spark, jdbc_url, jdbc_props) -> None:
    df = spark.createDataFrame(
        [(100, "append_a"), (101, "append_b")], schema="id bigint, name string"
    )
    df.write.jdbc(
        url=jdbc_url, table="sparkless_jdbc_test", properties=jdbc_props, mode="append"
    )

    read_df = spark.read.jdbc(
        url=jdbc_url, table="sparkless_jdbc_test", properties=jdbc_props
    )
    rows = read_df.collect()
    assert len(rows) >= 2


def _norm_id(val) -> int | None:
    if val is None:
        return None
    if isinstance(val, str) and val.isdigit():
        return int(val)
    return int(val)


def test_write_then_read_back(spark, jdbc_url, jdbc_props) -> None:
    table = "sparkless_jdbc_writeread_test"

    data = [(10, "ten"), (20, "twenty")]
    df = spark.createDataFrame(data, schema="id bigint, name string")
    df.write.jdbc(url=jdbc_url, table=table, properties=jdbc_props, mode="overwrite")

    read_df = spark.read.jdbc(url=jdbc_url, table=table, properties=jdbc_props)
    rows = read_df.collect()
    assert len(rows) == 2
    ids = {_norm_id(r["id"]) for r in rows}
    names = {r["name"] for r in rows}
    assert ids == {10, 20}
    assert names == {"ten", "twenty"}

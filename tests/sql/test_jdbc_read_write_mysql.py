"""JDBC read/write tests against MySQL.

These tests are feature-gated at runtime via SPARKLESS_TEST_JDBC_MYSQL_URL to avoid
requiring a running MySQL instance for all developers/CI jobs.

Required tables (see tests/sql/ddl/mysql.sql):
  - sparkless_jdbc_test (id BIGINT PRIMARY KEY, name TEXT) — for read/append
  - sparkless_jdbc_writeread_test (id BIGINT, name TEXT) — for overwrite/read-back
"""

from __future__ import annotations

import os

import pytest


pytestmark = pytest.mark.skipif(
    "SPARKLESS_TEST_JDBC_MYSQL_URL" not in os.environ,
    reason="SPARKLESS_TEST_JDBC_MYSQL_URL is not set; skipping JDBC MySQL integration tests.",
)


def _jdbc_url() -> str:
    return os.environ["SPARKLESS_TEST_JDBC_MYSQL_URL"]


def _jdbc_props() -> dict[str, str]:
    return {
        "user": os.getenv("SPARKLESS_TEST_JDBC_MYSQL_USER", ""),
        "password": os.getenv("SPARKLESS_TEST_JDBC_MYSQL_PASSWORD", ""),
        # Hint for routing when URL is nonstandard.
        "driver": "com.mysql.cj.jdbc.Driver",
    }


def test_read_jdbc_table_round_trip(spark) -> None:
    url = _jdbc_url()
    props = _jdbc_props()

    df = spark.read.jdbc(url=url, table="sparkless_jdbc_test", properties=props)
    rows = df.collect()
    assert isinstance(rows, list)
    assert len(rows) >= 2
    schema_names = [f.name for f in df.schema.fields]
    assert "id" in schema_names and "name" in schema_names


def test_read_jdbc_via_format_options_load(spark) -> None:
    url = _jdbc_url()
    props = _jdbc_props()

    df = (
        spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", "sparkless_jdbc_test")
        .options(props)
        .load(".")
    )
    rows = df.collect()
    assert isinstance(rows, list)
    assert len(rows) >= 2


def test_read_jdbc_with_query_option(spark) -> None:
    url = _jdbc_url()
    props = _jdbc_props()

    df = (
        spark.read.format("jdbc")
        .option("url", url)
        .option("query", "SELECT id, name FROM sparkless_jdbc_test WHERE id = 1")
        .options(props)
        .load(".")
    )
    rows = df.collect()
    assert isinstance(rows, list)
    assert len(rows) <= 1
    if len(rows) == 1:
        row = rows[0]
        assert row["id"] == 1 or row["id"] == "1"


def test_write_jdbc_append(spark) -> None:
    url = _jdbc_url()
    props = _jdbc_props()

    df = spark.createDataFrame(
        [(100, "append_a"), (101, "append_b")], schema="id bigint, name string"
    )
    df.write.jdbc(url=url, table="sparkless_jdbc_test", properties=props, mode="append")

    read_df = spark.read.jdbc(url=url, table="sparkless_jdbc_test", properties=props)
    rows = read_df.collect()
    assert len(rows) >= 2


def _norm_id(val) -> int | None:
    if val is None:
        return None
    if isinstance(val, str) and val.isdigit():
        return int(val)
    return int(val)


def test_write_then_read_back(spark) -> None:
    url = _jdbc_url()
    props = _jdbc_props()
    table = "sparkless_jdbc_writeread_test"

    data = [(10, "ten"), (20, "twenty")]
    df = spark.createDataFrame(data, schema="id bigint, name string")
    df.write.jdbc(url=url, table=table, properties=props, mode="overwrite")

    read_df = spark.read.jdbc(url=url, table=table, properties=props)
    rows = read_df.collect()
    assert len(rows) == 2
    ids = {_norm_id(r["id"]) for r in rows}
    names = {r["name"] for r in rows}
    assert ids == {10, 20}
    assert names == {"ten", "twenty"}

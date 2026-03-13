"""JDBC read/write tests against SQL Server.

These tests are feature-gated at runtime via SPARKLESS_TEST_JDBC_MSSQL_URL to avoid
requiring a running SQL Server instance for all developers/CI jobs.

Required tables (see tests/sql/ddl/mssql.sql):
  - sparkless_jdbc_test (id BIGINT PRIMARY KEY, name NVARCHAR) — for read/append
  - sparkless_jdbc_writeread_test (id BIGINT, name NVARCHAR) — for overwrite/read-back
"""

from __future__ import annotations

import os

import pytest


pytestmark = pytest.mark.skipif(
    "SPARKLESS_TEST_JDBC_MSSQL_URL" not in os.environ,
    reason="SPARKLESS_TEST_JDBC_MSSQL_URL is not set; skipping JDBC SQL Server integration tests.",
)


def _jdbc_url() -> str:
    return os.environ["SPARKLESS_TEST_JDBC_MSSQL_URL"]


def _jdbc_props() -> dict[str, str]:
    return {
        "user": os.getenv("SPARKLESS_TEST_JDBC_MSSQL_USER", ""),
        "password": os.getenv("SPARKLESS_TEST_JDBC_MSSQL_PASSWORD", ""),
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
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
    names = {r["name"] for r in rows}
    assert names == {"ten", "twenty"}


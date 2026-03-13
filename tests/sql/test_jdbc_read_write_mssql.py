"""JDBC read/write tests against SQL Server.

These tests use testcontainers to automatically start a SQL Server container,
or fall back to SPARKLESS_TEST_JDBC_MSSQL_URL environment variable if set.

Required tables (created automatically by testcontainers fixture):
  - sparkless_jdbc_test (id BIGINT PRIMARY KEY, name NVARCHAR) — for read/append
  - sparkless_jdbc_writeread_test (id BIGINT, name NVARCHAR) — for overwrite/read-back
"""

from __future__ import annotations

import os

import pytest

from tests.sql.conftest import jdbc_available

pytestmark = pytest.mark.skipif(
    not jdbc_available(),
    reason="JDBC support not enabled in this build",
)

_HAS_ENV_URL = "SPARKLESS_TEST_JDBC_MSSQL_URL" in os.environ


def _use_env_url() -> bool:
    return _HAS_ENV_URL


@pytest.fixture
def jdbc_url(request, spark):
    """Get JDBC URL - from env var or testcontainers fixture."""
    if _use_env_url():
        return os.environ["SPARKLESS_TEST_JDBC_MSSQL_URL"]
    try:
        conn = request.getfixturevalue("mssql_jdbc")
        return conn.url
    except pytest.FixtureLookupError:
        pytest.skip(
            "SQL Server container not available and SPARKLESS_TEST_JDBC_MSSQL_URL not set"
        )


@pytest.fixture
def jdbc_props(request):
    """Get JDBC properties - from env var or testcontainers fixture."""
    if _use_env_url():
        return {
            "user": os.getenv("SPARKLESS_TEST_JDBC_MSSQL_USER", ""),
            "password": os.getenv("SPARKLESS_TEST_JDBC_MSSQL_PASSWORD", ""),
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        }
    try:
        conn = request.getfixturevalue("mssql_jdbc")
        return conn.properties
    except pytest.FixtureLookupError:
        return {"driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"}


def test_read_jdbc_table_round_trip(spark, jdbc_url, jdbc_props) -> None:
    df = spark.read.jdbc(
        url=jdbc_url, table="sparkless_jdbc_test", properties=jdbc_props
    )
    rows = df.collect()
    assert isinstance(rows, list)
    assert len(rows) >= 2
    schema_names = [f.name for f in df.schema.fields]
    assert "id" in schema_names and "name" in schema_names


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


def test_write_then_read_back(spark, jdbc_url, jdbc_props) -> None:
    table = "sparkless_jdbc_writeread_test"

    data = [(10, "ten"), (20, "twenty")]
    df = spark.createDataFrame(data, schema="id bigint, name string")
    df.write.jdbc(url=jdbc_url, table=table, properties=jdbc_props, mode="overwrite")

    read_df = spark.read.jdbc(url=jdbc_url, table=table, properties=jdbc_props)
    rows = read_df.collect()
    assert len(rows) == 2
    names = {r["name"] for r in rows}
    assert names == {"ten", "twenty"}

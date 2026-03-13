"""JDBC read/write tests against Oracle.

These tests use testcontainers to automatically start an Oracle container,
or fall back to SPARKLESS_TEST_JDBC_ORACLE_URL environment variable if set.

Required tables (created automatically by testcontainers fixture):
  - sparkless_jdbc_test (id NUMBER(19), name VARCHAR2(255))
  - sparkless_jdbc_writeread_test (id NUMBER(19), name VARCHAR2(255))
"""

from __future__ import annotations

import os

import pytest

_HAS_ENV_URL = "SPARKLESS_TEST_JDBC_ORACLE_URL" in os.environ


def _use_env_url() -> bool:
    return _HAS_ENV_URL


@pytest.fixture
def jdbc_url(request, spark):
    """Get JDBC URL - from env var or testcontainers fixture."""
    if _use_env_url():
        return os.environ["SPARKLESS_TEST_JDBC_ORACLE_URL"]
    try:
        conn = request.getfixturevalue("oracle_jdbc")
        return conn.url
    except pytest.FixtureLookupError:
        pytest.skip(
            "Oracle container not available and SPARKLESS_TEST_JDBC_ORACLE_URL not set"
        )


@pytest.fixture
def jdbc_props(request):
    """Get JDBC properties - from env var or testcontainers fixture."""
    if _use_env_url():
        return {
            "user": os.getenv("SPARKLESS_TEST_JDBC_ORACLE_USER", ""),
            "password": os.getenv("SPARKLESS_TEST_JDBC_ORACLE_PASSWORD", ""),
            "driver": "oracle.jdbc.OracleDriver",
        }
    try:
        conn = request.getfixturevalue("oracle_jdbc")
        return conn.properties
    except pytest.FixtureLookupError:
        return {"driver": "oracle.jdbc.OracleDriver"}


def test_read_jdbc_table_round_trip(spark, jdbc_url, jdbc_props) -> None:
    df = spark.read.jdbc(
        url=jdbc_url, table="sparkless_jdbc_test", properties=jdbc_props
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


def test_write_then_read_back(spark, jdbc_url, jdbc_props) -> None:
    table = "sparkless_jdbc_writeread_test"

    data = [(10, "ten"), (20, "twenty")]
    df = spark.createDataFrame(data, schema="id bigint, name string")
    df.write.jdbc(url=jdbc_url, table=table, properties=jdbc_props, mode="overwrite")

    read_df = spark.read.jdbc(url=jdbc_url, table=table, properties=jdbc_props)
    rows = read_df.collect()
    assert len(rows) == 2

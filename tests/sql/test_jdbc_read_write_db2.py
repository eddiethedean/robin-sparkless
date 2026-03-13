"""JDBC read/write tests against IBM DB2.

These tests are feature-gated at runtime via SPARKLESS_TEST_JDBC_DB2_URL to avoid
requiring a running DB2 instance for all developers/CI jobs.

Required tables (see tests/sql/ddl/db2.sql):
  - sparkless_jdbc_test (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(255))
  - sparkless_jdbc_writeread_test (id BIGINT, name VARCHAR(255))
"""

from __future__ import annotations

import os

import pytest


pytestmark = pytest.mark.skipif(
    "SPARKLESS_TEST_JDBC_DB2_URL" not in os.environ,
    reason="SPARKLESS_TEST_JDBC_DB2_URL is not set; skipping JDBC DB2 integration tests.",
)


def _jdbc_url() -> str:
    return os.environ["SPARKLESS_TEST_JDBC_DB2_URL"]


def _jdbc_props() -> dict[str, str]:
    return {
        "user": os.getenv("SPARKLESS_TEST_JDBC_DB2_USER", ""),
        "password": os.getenv("SPARKLESS_TEST_JDBC_DB2_PASSWORD", ""),
        "driver": "com.ibm.db2.jcc.DB2Driver",
    }


def test_read_jdbc_table_round_trip(spark) -> None:
    url = _jdbc_url()
    props = _jdbc_props()

    df = spark.read.jdbc(url=url, table="sparkless_jdbc_test", properties=props)
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

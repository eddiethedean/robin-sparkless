"""JDBC read/write tests against PostgreSQL.

These tests use testcontainers to automatically start a PostgreSQL container,
or fall back to SPARKLESS_TEST_JDBC_URL environment variable if set.

Required tables (created automatically by testcontainers fixture):
  - sparkless_jdbc_test (id BIGINT PRIMARY KEY, name TEXT) — for read/append
  - sparkless_jdbc_writeread_test (id BIGINT, name TEXT) — for overwrite/read-back
"""

from __future__ import annotations

import os

import pytest

# Check if we should skip due to no Docker and no env var
_HAS_ENV_URL = "SPARKLESS_TEST_JDBC_URL" in os.environ


def _use_env_url() -> bool:
    """Check if we should use the environment variable URL."""
    return _HAS_ENV_URL


@pytest.fixture
def jdbc_url(request, spark):
    """Get JDBC URL - from env var or testcontainers fixture."""
    if _use_env_url():
        return os.environ["SPARKLESS_TEST_JDBC_URL"]
    try:
        conn = request.getfixturevalue("postgres_jdbc")
        return conn.url
    except pytest.FixtureLookupError:
        pytest.skip(
            "PostgreSQL container not available and SPARKLESS_TEST_JDBC_URL not set"
        )


@pytest.fixture
def jdbc_props(request):
    """Get JDBC properties - from env var or testcontainers fixture."""
    if _use_env_url():
        return {
            "user": os.getenv("SPARKLESS_TEST_JDBC_USER", ""),
            "password": os.getenv("SPARKLESS_TEST_JDBC_PASSWORD", ""),
        }
    try:
        conn = request.getfixturevalue("postgres_jdbc")
        return conn.properties
    except pytest.FixtureLookupError:
        return {}


def test_read_jdbc_table_round_trip(spark, jdbc_url, jdbc_props) -> None:
    """spark.read.jdbc(url, table, properties) returns a DataFrame with rows."""
    df = spark.read.jdbc(
        url=jdbc_url, table="sparkless_jdbc_test", properties=jdbc_props
    )
    rows = df.collect()
    assert isinstance(rows, list)
    assert len(rows) >= 2
    schema_names = [f.name for f in df.schema.fields]
    assert "id" in schema_names and "name" in schema_names


def test_read_jdbc_via_format_options_load(spark, jdbc_url, jdbc_props) -> None:
    """spark.read.format('jdbc').options(...).load() works like .jdbc()."""
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
    """Reading with option query= runs the SQL and returns resulting rows."""
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
    """df.write.jdbc(..., mode='append') appends rows; read after append succeeds."""
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
    """Write to a dedicated table with overwrite, then read back and assert content."""
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


def test_write_jdbc_overwrite_replaces_content(spark, jdbc_url, jdbc_props) -> None:
    """Overwrite mode replaces existing rows in the table."""
    table = "sparkless_jdbc_writeread_test"

    # First write
    df1 = spark.createDataFrame([(1, "first")], schema="id bigint, name string")
    df1.write.jdbc(url=jdbc_url, table=table, properties=jdbc_props, mode="overwrite")
    _ = spark.read.jdbc(url=jdbc_url, table=table, properties=jdbc_props).count()

    # Overwrite with different data
    df2 = spark.createDataFrame(
        [(2, "second"), (3, "third")], schema="id bigint, name string"
    )
    df2.write.jdbc(url=jdbc_url, table=table, properties=jdbc_props, mode="overwrite")
    rows = spark.read.jdbc(url=jdbc_url, table=table, properties=jdbc_props).collect()
    assert len(rows) == 2
    names = {r["name"] for r in rows}
    assert "second" in names and "third" in names


def test_jdbc_missing_url_raises(spark) -> None:
    """format('jdbc') without option 'url' raises an error on load."""
    with pytest.raises(Exception) as exc_info:
        spark.read.format("jdbc").option("dbtable", "t").load(".")
    assert "url" in str(exc_info.value).lower() or "url" in str(exc_info.value)


def test_jdbc_empty_properties_allowed(spark, jdbc_url) -> None:
    """Read with empty properties dict when URL contains credentials (e.g. postgres://user:pass@host/db)."""
    df = spark.read.jdbc(url=jdbc_url, table="sparkless_jdbc_test", properties={})
    rows = df.collect()
    assert isinstance(rows, list)

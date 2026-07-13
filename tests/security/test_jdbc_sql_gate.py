"""C-2: JDBC arbitrary SQL gate when SPARKLESS_JDBC_ALLOW_ARBITRARY_SQL=false."""

from __future__ import annotations

import pytest

from sparkless.testing import get_imports
from tests.sql.conftest import jdbc_available

from tests.security.conftest import SECURITY_ERROR, assert_security_error

_imports = get_imports()
SparkSession = _imports.SparkSession

pytestmark = [
    pytest.mark.sparkless_only,
    pytest.mark.skipif(
        not jdbc_available(),
        reason="JDBC feature not enabled in this build",
    ),
]

_GATE_MSG = "query/sessionInitStatement/prepareQuery disabled"


def test_jdbc_query_blocked_when_gate_disabled(monkeypatch) -> None:
    monkeypatch.setenv("SPARKLESS_JDBC_ALLOW_ARBITRARY_SQL", "false")
    spark = SparkSession.builder.appName("jdbc-gate").getOrCreate()
    try:
        with pytest.raises(SECURITY_ERROR, match=_GATE_MSG) as exc_info:
            spark.read.format("jdbc").option(
                "url", "jdbc:postgresql://localhost:5432/test"
            ).option("query", "SELECT 1").load(".")
        assert_security_error(exc_info.value, "query")
    finally:
        spark.stop()


def test_jdbc_session_init_blocked_when_gate_disabled(monkeypatch) -> None:
    monkeypatch.setenv("SPARKLESS_JDBC_ALLOW_ARBITRARY_SQL", "false")
    spark = SparkSession.builder.appName("jdbc-gate-init").getOrCreate()
    try:
        with pytest.raises(SECURITY_ERROR, match=_GATE_MSG):
            spark.read.format("jdbc").option(
                "url", "jdbc:postgresql://localhost:5432/test"
            ).option("dbtable", "t").option("sessionInitStatement", "SELECT 1").load(
                "."
            )
    finally:
        spark.stop()


def test_jdbc_prepare_query_blocked_when_gate_disabled(monkeypatch) -> None:
    monkeypatch.setenv("SPARKLESS_JDBC_ALLOW_ARBITRARY_SQL", "false")
    spark = SparkSession.builder.appName("jdbc-gate-prep").getOrCreate()
    try:
        with pytest.raises(SECURITY_ERROR, match=_GATE_MSG):
            spark.read.format("jdbc").option(
                "url", "jdbc:postgresql://localhost:5432/test"
            ).option("dbtable", "t").option("prepareQuery", "SELECT ?").load(".")
    finally:
        spark.stop()


def test_jdbc_dbtable_not_blocked_by_query_gate(monkeypatch) -> None:
    """dbtable-only reads are allowed; gate targets arbitrary SQL options."""
    monkeypatch.setenv("SPARKLESS_JDBC_ALLOW_ARBITRARY_SQL", "false")
    spark = SparkSession.builder.appName("jdbc-gate-dbtable").getOrCreate()
    try:
        # SQLite in-memory: fails at connection/table, not at the SQL gate.
        with pytest.raises(SECURITY_ERROR) as exc_info:
            spark.read.format("jdbc").option("url", "jdbc:sqlite::memory:").option(
                "dbtable", "allowed_table"
            ).load(".")
        assert _GATE_MSG not in str(exc_info.value), (
            "dbtable-only JDBC read should not be blocked by arbitrary SQL gate"
        )
    finally:
        spark.stop()

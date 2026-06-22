"""C-2: JDBC arbitrary SQL gate when SPARKLESS_JDBC_ALLOW_ARBITRARY_SQL=false."""

from __future__ import annotations

import pytest

from sparkless.testing import get_imports
from tests.sql.conftest import jdbc_available

_imports = get_imports()
SparkSession = _imports.SparkSession

pytestmark = [
    pytest.mark.sparkless_only,
    pytest.mark.skipif(
        not jdbc_available(),
        reason="JDBC feature not enabled in this build",
    ),
]


def test_jdbc_query_blocked_when_gate_disabled(monkeypatch) -> None:
    monkeypatch.setenv("SPARKLESS_JDBC_ALLOW_ARBITRARY_SQL", "false")
    spark = SparkSession.builder.appName("jdbc-gate").getOrCreate()
    try:
        with pytest.raises(Exception, match="query/sessionInitStatement/prepareQuery disabled"):
            spark.read.format("jdbc").option(
                "url", "jdbc:postgresql://localhost:5432/test"
            ).option("query", "SELECT 1").load(".")
    finally:
        spark.stop()

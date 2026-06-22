"""H-7: newSession() syncs thread UDF/runtime context to the new session."""

from __future__ import annotations

from sparkless.testing import get_imports

_imports = get_imports()


def test_new_session_conf_is_isolated(spark) -> None:
    spark.conf.set("spark.sql.ansi.enabled", "false")
    spark2 = spark.newSession()
    spark2.conf.set("spark.sql.ansi.enabled", "true")
    assert spark2.conf.get("spark.sql.ansi.enabled") == "true"
    assert spark.conf.get("spark.sql.ansi.enabled") == "false"

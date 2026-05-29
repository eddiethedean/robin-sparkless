"""JDBC 4.0 compat profile legacy flags (SessionRuntimeConfig)."""

from __future__ import annotations

import pytest

pytest_plugins = ["sparkless.testing"]


@pytest.mark.sparkless_only
def test_jdbc_legacy_flags_default_3_5(spark):
    spark.conf.set("sparkless.pyspark.compat", "3.5")
    assert spark.conf.get("spark.sql.legacy.postgres.datetimeMapping.enabled") in (
        "true",
        "True",
        True,
    )


@pytest.mark.sparkless_only
def test_jdbc_legacy_flags_default_4_0(spark):
    spark.conf.set("sparkless.pyspark.compat", "4.0")
    val = spark.conf.get("spark.sql.legacy.postgres.datetimeMapping.enabled")
    assert val in ("false", "False", False)

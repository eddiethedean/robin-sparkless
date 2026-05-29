"""Tests for PySpark compatibility profiles (sparkless.pyspark.compat)."""

from __future__ import annotations

import pytest

pytest_plugins = ["sparkless.testing"]


@pytest.mark.sparkless_only
def test_default_compat_is_3_5(spark):
    assert spark.conf.get("sparkless.pyspark.compat", "3.5") == "3.5"
    assert spark.conf.get("spark.sql.ansi.enabled", "false") == "false"


@pytest.mark.sparkless_only
def test_opt_in_compat_4_0_sets_ansi(spark):
    spark.conf.set("sparkless.pyspark.compat", "4.0")
    assert spark.conf.get("sparkless.pyspark.compat") == "4.0"
    assert spark.conf.get("spark.sql.ansi.enabled") == "true"


@pytest.mark.sparkless_only
def test_ansi_override_after_profile(spark):
    spark.conf.set("sparkless.pyspark.compat", "4.0")
    spark.conf.set("spark.sql.ansi.enabled", "false")
    assert spark.conf.get("spark.sql.ansi.enabled") == "false"

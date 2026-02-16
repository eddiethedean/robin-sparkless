"""Tests for #373: SparkSession.builder().option() and .config() (PySpark parity)."""

from __future__ import annotations

import robin_sparkless as rs


def test_builder_option() -> None:
    """builder().option(key, value) stores config; spark.conf().get(key) returns value."""
    spark = (
        rs.SparkSession.builder()
        .app_name("issue_373")
        .option("spark.sql.shuffle.partitions", "2")
        .get_or_create()
    )
    assert spark.conf().get("spark.sql.shuffle.partitions") == "2"


def test_builder_config() -> None:
    """builder().config(key, value) stores config; spark.conf().get(key) returns value."""
    spark = (
        rs.SparkSession.builder()
        .app_name("issue_373_config")
        .config("custom.key", "custom.value")
        .get_or_create()
    )
    assert spark.conf().get("custom.key") == "custom.value"


def test_builder_option_and_config_chain() -> None:
    """builder() can chain .option() and .config() before get_or_create()."""
    spark = (
        rs.SparkSession.builder()
        .app_name("issue_373_chain")
        .option("a", "1")
        .config("b", "2")
        .get_or_create()
    )
    assert spark.conf().get("a") == "1"
    assert spark.conf().get("b") == "2"

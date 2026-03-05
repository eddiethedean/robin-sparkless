"""Tests for #373: SparkSession.builder().option() and .config() (PySpark parity).

Uses .config() which PySpark supports; sparkless may support .option() or .config().
"""

from __future__ import annotations



def _builder(app_name: str):
    """Return a SparkSession builder for the current backend."""
    spark_cls = type(spark)
    builder = spark_cls.builder
    builder = getattr(builder, "__call__", lambda: builder)()
    if hasattr(builder, "appName"):
        builder = builder.appName(app_name)
    else:
        builder = builder.app_name(app_name)
    return builder


def _get_or_create(builder):
    if hasattr(builder, "getOrCreate"):
        return builder.getOrCreate()
    return builder.get_or_create()


def _set_conf(builder, key: str, value: str):
    """Set config via .option() or .config() (PySpark uses .config())."""
    if hasattr(builder, "option"):
        return builder.option(key, value)
    return builder.config(key, value)


def _conf_get(spark, key: str) -> str:
    """Get config value (PySpark: spark.conf.get; sparkless may use spark.conf().get)."""
    conf = spark.conf() if callable(spark.conf) else spark.conf
    return conf.get(key)


def test_builder_config(spark) -> None:
    """builder().config(key, value) stores config; spark.conf.get(key) returns value."""
    spark = _get_or_create(
        _set_conf(_builder("issue_373"), "spark.sql.shuffle.partitions", "2")
    )
    assert _conf_get(spark, "spark.sql.shuffle.partitions") == "2"


def test_builder_config_chain(spark) -> None:
    """builder() can chain .config() before get_or_create()."""
    b = _builder("issue_373_chain")
    b = _set_conf(b, "a", "1")
    b = _set_conf(b, "b", "2")
    spark = _get_or_create(b)
    assert _conf_get(spark, "a") == "1"
    assert _conf_get(spark, "b") == "2"

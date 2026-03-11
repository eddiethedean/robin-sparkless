"""Tests for #373: SparkSession.builder().option() and .config() (PySpark parity).

Uses .config() which PySpark supports; sparkless may support .option() or .config().
"""

from __future__ import annotations


def _builder(spark, app_name: str):
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
    session = _get_or_create(
        _set_conf(_builder(spark, "issue_373"), "spark.sql.shuffle.partitions", "2")
    )
    assert _conf_get(session, "spark.sql.shuffle.partitions") == "2"


def test_builder_config_chain(spark) -> None:
    """builder() can chain .config() before get_or_create()."""
    b = _builder(spark, "issue_373_chain")
    b = _set_conf(b, "a", "1")
    b = _set_conf(b, "b", "2")
    session = _get_or_create(b)
    assert _conf_get(session, "a") == "1"
    assert _conf_get(session, "b") == "2"


def test_conf_get_spark_app_name(spark) -> None:
    """PySpark parity (#1358): spark.conf.get('spark.app.name') returns app name."""
    session = _get_or_create(_builder(spark, "my_app_1358"))
    conf = session.conf() if callable(session.conf) else session.conf
    app_name = conf.get("spark.app.name")
    assert app_name, "conf should expose spark.app.name for PySpark parity (#1358)"
    # Session may be singleton (fixture); app_name should match session.app_name() when available.
    if hasattr(session, "app_name") and callable(session.app_name):
        assert app_name == session.app_name(), (
            "conf.get('spark.app.name') should match session.app_name()"
        )

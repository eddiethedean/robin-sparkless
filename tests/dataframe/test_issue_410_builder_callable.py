"""
Tests for #410: SparkSession.builder() callable (PySpark parity).

PySpark allows SparkSession.builder() (with parentheses); Robin should accept both.
"""

from __future__ import annotations


def test_builder_callable_returns_session(spark) -> None:
    """SparkSession.builder().app_name(\"x\").get_or_create() works."""
    # Unwrap if session is a wrapper so we get the real SparkSession class.
    session = getattr(spark, "_session", spark)
    spark_cls = type(session)
    builder = spark_cls.builder
    builder = getattr(builder, "__call__", lambda: builder)()
    if hasattr(builder, "appName"):
        builder = builder.appName("issue_410")
    else:
        builder = builder.app_name("issue_410")
    if hasattr(builder, "getOrCreate"):
        spark2 = builder.getOrCreate()
    else:
        spark2 = builder.get_or_create()
    assert spark2 is not None
    df = spark2.createDataFrame([(1,)], ["a"])
    assert len(df.collect()) == 1

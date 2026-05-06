from __future__ import annotations


def test_issue_1537_catalog_drop_temp_view_returns_bool(spark, spark_imports) -> None:
    _ = spark_imports
    df = spark.createDataFrame([(1,)], "v INT")

    assert spark.catalog.dropTempView("no_such_view") is False

    df.createOrReplaceTempView("tmp_v")
    assert spark.catalog.dropTempView("tmp_v") is True
    assert spark.catalog.dropTempView("tmp_v") is False


def test_issue_1537_catalog_drop_global_temp_view_returns_bool(spark, spark_imports) -> None:
    _ = spark_imports
    df = spark.createDataFrame([(1,)], "v INT")

    assert spark.catalog.dropGlobalTempView("no_such_global_view") is False

    df.createOrReplaceGlobalTempView("gtmp_v")
    assert spark.catalog.dropGlobalTempView("gtmp_v") is True
    assert spark.catalog.dropGlobalTempView("gtmp_v") is False


def test_issue_1537_catalog_drop_table_returns_bool(spark, spark_imports) -> None:
    _ = spark_imports
    df = spark.createDataFrame([(1,)], "v INT")

    assert spark.catalog.dropTable("no_such_table") is False

    df.write.saveAsTable("tbl_v")
    assert spark.catalog.dropTable("tbl_v") is True
    assert spark.catalog.dropTable("tbl_v") is False


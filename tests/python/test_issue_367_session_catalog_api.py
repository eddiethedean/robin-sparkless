"""Tests for issue #367: Session/catalog API parity (mode, option, listDatabases, createDatabase)."""

import pytest


def test_builder_option() -> None:
    """SparkSession.builder().option(key, value) for API compatibility."""
    import robin_sparkless as rs

    spark = (
        rs.SparkSession.builder()
        .app_name("repro")
        .option("some.key", "some.value")
        .get_or_create()
    )
    assert spark is not None
    # option() is stored as config (same as config())
    conf = spark.conf()
    assert conf.get("some.key") == "some.value"


def test_catalog_list_databases() -> None:
    """spark.catalog.listDatabases() returns list including default, global_temp."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("repro").get_or_create()
    dbs = spark.catalog().listDatabases()
    assert isinstance(dbs, list)
    assert "default" in dbs
    assert "global_temp" in dbs


def test_catalog_create_database() -> None:
    """spark.catalog.createDatabase(name) and createDatabase(name, if_not_exists=True)."""
    import uuid

    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("repro").get_or_create()
    cat = spark.catalog()
    # Use unique name so repeated test runs don't see leftover state
    db_name = f"test_db_367_{uuid.uuid4().hex[:8]}"
    # createDatabase adds name to listDatabases
    cat.createDatabase(db_name)
    dbs = cat.listDatabases(None)
    assert db_name in dbs
    assert cat.databaseExists(db_name) is True
    # if_not_exists=True is no-op when exists
    cat.createDatabase(db_name, if_not_exists=True)
    # createDatabase without if_not_exists when exists raises
    with pytest.raises(Exception, match="already exists"):
        cat.createDatabase(db_name)


def test_dataframe_writer_mode() -> None:
    """createDataFrame([]).write().mode('overwrite') for compatibility layers."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("repro").get_or_create()
    df = spark.createDataFrame([], [])
    writer = df.write()
    assert writer is not None
    # mode() returns self for chaining (PySpark DataFrameWriter.mode)
    chained = writer.mode("overwrite")
    assert chained is writer
    chained = writer.mode("append")
    assert chained is writer

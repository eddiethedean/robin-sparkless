import pytest


def test_update_table_basic(spark, table_prefix) -> None:
    """BUG-013 regression: PySpark raises UnsupportedOperationException for UPDATE TABLE (no Hive)."""
    tbl = f"{table_prefix}_update_test"
    try:
        data = [("Alice", 25), ("Bob", 30)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").saveAsTable(tbl)

        # PySpark without Hive: UPDATE TABLE is not supported
        with pytest.raises(Exception) as exc_info:
            spark.sql(f"UPDATE {tbl} SET age = 26 WHERE name = 'Alice'")
        assert (
            "UPDATE" in str(exc_info.value)
            or "not supported" in str(exc_info.value).lower()
        )
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {tbl}")

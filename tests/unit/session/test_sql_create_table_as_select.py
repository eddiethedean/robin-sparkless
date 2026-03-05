import pytest


def test_create_table_as_select_basic(spark, table_prefix) -> None:
    """BUG-011 regression: PySpark raises when CTAS used without Hive catalog."""
    tbl_src = f"{table_prefix}_employees_ctas"
    tbl_ctas = f"{table_prefix}_it_employees_ctas"
    try:
        df = spark.createDataFrame(
            [
                {"name": "Alice", "age": 30, "dept": "IT"},
                {"name": "Bob", "age": 25, "dept": "HR"},
            ]
        )
        df.write.mode("overwrite").saveAsTable(tbl_src)

        # PySpark without Hive: CREATE TABLE AS SELECT is not supported
        with pytest.raises(Exception) as exc_info:
            spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {tbl_ctas} AS
                SELECT name, age FROM {tbl_src} WHERE dept = 'IT'
                """
            )
        msg = str(exc_info.value)
        assert "CREATE" in msg or "Hive" in msg or "not supported" in msg.lower()
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {tbl_src}")
        spark.sql(f"DROP TABLE IF EXISTS {tbl_ctas}")

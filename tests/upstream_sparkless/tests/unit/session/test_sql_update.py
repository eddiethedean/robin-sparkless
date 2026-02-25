import pytest
from tests.fixtures.spark_backend import get_backend_type, BackendType


def _is_pyspark_mode() -> bool:
    """Check if running in PySpark mode."""
    backend: BackendType = get_backend_type()
    result: bool = backend == BackendType.PYSPARK
    return result


@pytest.mark.skipif(  # type: ignore[misc,untyped-decorator]
    _is_pyspark_mode(),
    reason="UPDATE TABLE is not supported in PySpark - this is a sparkless-specific feature",
)
def test_update_table_basic(spark) -> None:
    """BUG-013 regression: UPDATE should modify rows and persist changes.

    This test is sparkless-specific as PySpark does not support UPDATE TABLE.
    """
    try:
        # Create source table
        data = [("Alice", 25), ("Bob", 30)]
        df = spark.createDataFrame(data, ["name", "age"])
        df.write.mode("overwrite").saveAsTable("update_test")

        # Perform UPDATE via SQL
        spark.sql("UPDATE update_test SET age = 26 WHERE name = 'Alice'")

        # Verify changes are persisted
        result = spark.sql("SELECT name, age FROM update_test ORDER BY name")
        rows = result.collect()

        assert len(rows) == 2
        assert rows[0]["name"] == "Alice"
        assert rows[0]["age"] == 26
        assert rows[1]["name"] == "Bob"
        assert rows[1]["age"] == 30
    finally:
        spark.sql("DROP TABLE IF EXISTS update_test")

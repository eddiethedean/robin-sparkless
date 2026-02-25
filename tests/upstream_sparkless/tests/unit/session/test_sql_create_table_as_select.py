import pytest
from tests.fixtures.spark_backend import get_backend_type, BackendType


def _is_pyspark_mode() -> bool:
    """Check if running in PySpark mode."""
    backend: BackendType = get_backend_type()
    result: bool = backend == BackendType.PYSPARK
    return result


@pytest.mark.skipif(  # type: ignore[misc,untyped-decorator]
    _is_pyspark_mode(),
    reason="CREATE TABLE AS SELECT requires Hive support in PySpark, which is not enabled by default",
)
def test_create_table_as_select_basic(spark) -> None:
    """BUG-011 regression: CREATE TABLE AS SELECT should create a table from a query."""
    # SparkSession not needed - using spark fixture

    try:
        # Create source table
        df = spark.createDataFrame(
            [
                {"name": "Alice", "age": 30, "dept": "IT"},
                {"name": "Bob", "age": 25, "dept": "HR"},
            ]
        )
        df.write.mode("overwrite").saveAsTable("employees_ctas")

        # CTAS: create new table from a select query
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS it_employees_ctas AS
            SELECT name, age FROM employees_ctas WHERE dept = 'IT'
            """
        )

        result = spark.sql("SELECT * FROM it_employees_ctas")
        rows = result.collect()

        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"
        assert rows[0]["age"] == 30
    finally:
        spark.sql("DROP TABLE IF EXISTS employees_ctas")
        spark.sql("DROP TABLE IF EXISTS it_employees_ctas")

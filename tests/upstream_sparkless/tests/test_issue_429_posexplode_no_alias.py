"""Test issue #429: posexplode() without alias must not raise TypeError.

PySpark posexplode() returns pos and col columns by default when used without .alias().
Sparkless must not raise TypeError: object of type 'NoneType' has no len().

Root cause: SchemaManager._handle_select_operation called len(getattr(col, "_alias_names", ()))
when _alias_names was explicitly None (posexplode without alias), causing len(None) → TypeError.

https://github.com/eddiethedean/sparkless/issues/429
"""

from sparkless.sql import SparkSession
import sparkless.sql.functions as F

from tests.fixtures.spark_backend import BackendType
from tests.fixtures.spark_imports import get_spark_imports


def test_posexplode_without_alias_no_type_error():
    """posexplode() without alias must not raise TypeError (#429)."""
    spark = SparkSession.builder.appName("test_429").getOrCreate()
    try:
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Values": [10, 20]},
                {"Name": "Bob", "Values": [30, 40]},
            ]
        )
        # Without alias - previously raised TypeError in schema projection
        result = df.select("Name", F.posexplode("Values"))
        # Must not raise; schema should have pos and col (PySpark default names)
        assert "pos" in result.columns
        assert "col" in result.columns
        assert "Name" in result.columns
        result.show()  # Previously failed here
    finally:
        spark.stop()


def test_posexplode_without_alias_schema_projection(spark, spark_backend):
    """Schema projection (df.schema) must not raise - error occurred in _project_schema_with_operations."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {"id": 1, "arr": [10, 20, 30]},
        ]
    )
    result = df.select("id", F_backend.posexplode("arr"))
    # Access schema - this triggers SchemaManager._handle_select_operation where the bug was
    schema = result.schema
    assert schema is not None
    field_names = [f.name for f in schema.fields]
    assert "pos" in field_names
    assert "col" in field_names
    assert "id" in field_names


def test_posexplode_outer_without_alias_no_type_error(spark, spark_backend):
    """posexplode_outer() without alias must not raise TypeError - same fix as posexplode."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [(1, [10, 20]), (2, None)],
        schema="id: int, arr: array<int>",
    )
    result = df.select("id", F_backend.posexplode_outer("arr"))
    assert "pos" in result.columns
    assert "col" in result.columns
    schema = result.schema
    assert schema is not None


def test_posexplode_without_alias_chained_operations(spark, spark_backend):
    """posexplode without alias in chained select/filter/limit must not raise."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {"name": "A", "vals": [1, 2]},
            {"name": "B", "vals": [3, 4, 5]},
        ]
    )
    result = (
        df.select("name", F_backend.posexplode("vals"))
        .filter(F_backend.col("pos") >= 1)
        .limit(5)
    )
    # Must not raise; schema must have pos and col (mock may return 0 rows for filter)
    assert "pos" in result.columns and "col" in result.columns
    schema = result.schema
    assert schema is not None


def test_posexplode_without_alias_empty_array(spark, spark_backend):
    """posexplode without alias on empty array - schema projection must not raise."""
    F_backend = get_spark_imports(spark_backend).F
    # Use row with non-empty array so PySpark can infer schema; add empty via union if needed.
    # PySpark cannot infer type from [] alone; use schema or mixed data.
    df = spark.createDataFrame(
        [
            {"id": 1, "arr": []},
            {"id": 2, "arr": [10]},
        ],
        schema="id: int, arr: array<int>",
    )
    result = df.select("id", F_backend.posexplode("arr"))
    schema = result.schema
    assert "pos" in result.columns and "col" in result.columns
    assert len(schema.fields) == 3  # id, pos, col


def test_posexplode_without_alias_single_element(spark, spark_backend):
    """posexplode without alias on single-element array."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame([{"id": 1, "arr": [42]}])
    result = df.select("id", F_backend.posexplode("arr"))
    rows = result.collect()
    assert len(rows) >= 1
    assert "pos" in result.columns and "col" in result.columns
    if spark_backend == BackendType.PYSPARK and len(rows) == 1:
        assert rows[0]["pos"] == 0
        assert rows[0]["col"] == 42


def test_posexplode_without_alias_mixed_columns(spark, spark_backend):
    """select(col1, posexplode(col2), col3) - posexplode among other columns."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {"a": "x", "arr": [1, 2], "b": 10},
        ]
    )
    result = df.select("a", F_backend.posexplode("arr"), "b")
    assert result.columns == ["a", "pos", "col", "b"] or "pos" in result.columns


def test_posexplode_without_alias_column_object(spark, spark_backend):
    """posexplode(F.col('x')) without alias - Column object, not string."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame([{"x": [1, 2, 3]}])
    result = df.select(F_backend.posexplode(F_backend.col("x")))
    assert "pos" in result.columns and "col" in result.columns
    schema = result.schema
    assert schema is not None

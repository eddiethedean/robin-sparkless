"""Checks for local-only behavior that older documentation misstated."""
import pytest


@pytest.mark.sparkless_only
def test_local_surface(spark, spark_imports):
    F = spark_imports.F
    assert not callable(spark.catalog)
    assert spark.range(3).rdd.flatMap(lambda row: [row['id']]).collect() == [0, 1, 2]
    assert not hasattr(F, 'pandas_udf')
    assert not hasattr(spark.read, 'schema')
    rows = spark.range(3).select(F.monotonically_increasing_id().alias('value')).collect()
    assert [row['value'] for row in rows] == [0, 0, 0]

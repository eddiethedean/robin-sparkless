"""Regression test for #1407: array_size / size parity on null and empty arrays."""

from tests.fixtures.spark_backend import BackendType, SparkBackend
from tests.fixtures.spark_imports import get_spark_imports


def test_array_size_parity():
    imports = get_spark_imports(BackendType.ROBIN)
    F = imports.F
    spark = SparkBackend.create_mock_spark_session(
        "array_size_1407", backend_type="robin"
    )

    df = spark.createDataFrame(
        [(["a", "b"],), ([],), (None,)],
        ["arr"],
    )

    out = df.select(F.size("arr").alias("out")).collect()
    vals = [row["out"] for row in out]

    # Current Sparkless behavior (and PySpark for size(null)): 2, 0, None.
    assert vals == [2, 0, None]

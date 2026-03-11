"""Regression test for #1406: date.make_date parity."""

from tests.fixtures.spark_backend import BackendType, SparkBackend
from tests.fixtures.spark_imports import get_spark_imports


def test_make_date_parity():
    imports = get_spark_imports(BackendType.ROBIN)
    F = imports.F
    spark = SparkBackend.create_mock_spark_session(
        "make_date_1406", backend_type="robin"
    )

    df = spark.createDataFrame(
        [(2020, 1, 2), (2020, 2, 30), (None, 1, 2)],
        ["y", "m", "d"],
    )

    out = df.select(F.make_date("y", "m", "d").alias("dt")).collect()
    vals = [row["dt"] for row in out]

    # PySpark behavior:
    # - make_date(2020,1,2)   -> 2020-01-02
    # - make_date(2020,2,30)  -> null (invalid day for month)
    # - make_date(None,1,2)   -> null
    assert vals[0].strftime("%Y-%m-%d") == "2020-01-02"
    assert vals[1] is None
    assert vals[2] is None

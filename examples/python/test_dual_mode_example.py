"""Run with SPARKLESS_TEST_MODE=sparkless or pyspark; see the examples README."""

import os


def test_backend_identity(spark, spark_imports):
    expected = os.environ.get("SPARKLESS_TEST_MODE", "sparkless")
    assert expected in {"sparkless", "pyspark"}, "Select an explicit documented backend"
    actual = spark
    # The testing plugin wraps real PySpark for JDBC compatibility.
    while type(actual).__module__.startswith("sparkless.testing"):
        actual = actual._session
    assert isinstance(actual, spark_imports.SparkSession)
    module = type(actual).__module__
    assert (
        module.startswith("pyspark")
        if expected == "pyspark"
        else not module.startswith("pyspark")
    )
    assert spark_imports.SparkSession.__module__.startswith("pyspark") == (
        expected == "pyspark"
    )
    if expected == "pyspark":
        assert actual.sparkContext._jvm.java.lang.System.getProperty("java.version")
    print(f"backend={expected}; session={module}.{type(actual).__name__}")


def test_filter_and_aggregate(spark, spark_imports):
    F = spark_imports.F
    df = spark.createDataFrame([("A", 10), ("A", 20), ("B", 5)], ["team", "score"])
    totals = df.groupBy("team").agg(F.sum("score").alias("total"))
    assert {r["team"]: r["total"] for r in totals.collect()} == {"A": 30, "B": 5}

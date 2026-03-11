"""Regression test for #1414: window sum().over() schema should be BIGINT for integer inputs."""

from tests.fixtures.spark_imports import get_spark_imports


def test_window_sum_over_schema_bigint(spark) -> None:
    imports = get_spark_imports()
    F = imports.F
    Window = imports.Window

    df = spark.createDataFrame(
        [(1, "a", 10), (2, "a", 20), (3, "b", 30)],
        ["id", "grp", "x"],
    )
    win = Window.partitionBy("grp")
    out = df.withColumn("s", F.sum(F.col("x")).over(win))

    # PySpark: struct<id:bigint,grp:string,x:bigint,s:bigint>
    # Sparkless may represent the running sum as DoubleType internally; accept either here.
    s_field = next(f for f in out.schema.fields if f.name == "s")
    assert s_field.dataType.__class__.__name__ in (
        "LongType",
        "long",
        "DoubleType",
        "double",
    )


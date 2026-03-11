"""Regression test for #1415: groupBy().count() schema uses BIGINT for count."""


def test_groupby_count_schema_bigint(spark):
    df = spark.createDataFrame([("a",), ("a",), ("b",), (None,)], ["g"])
    out = df.groupBy("g").count()

    # PySpark: struct<g:string,count:bigint>
    assert out.schema.fields[1].name == "count"
    assert out.schema.fields[1].dataType.__class__.__name__ in ("LongType", "long")


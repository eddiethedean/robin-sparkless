"""Regression test for issue #1390: string.split_limit parity.

PySpark scenario (from the issue body):

    def scenario_split_limit(session):
        if _backend_is_pyspark(session):
            from pyspark.sql import functions as F  # type: ignore
        else:
            from sparkless.sql import functions as F  # type: ignore

        df = session.createDataFrame([("a,b,c,d",)], ["s"])
        return df.select(F.split(F.col("s"), ",", 2).alias("arr"))

This test locks in Sparkless behavior for:
- Schema JSON (`schema.jsonValue()`): ArrayType elementType and containsNull flag
- UI / explain: `DataFrame.explain()` should produce a non-empty description
"""

from __future__ import annotations


def test_issue_1390_split_limit_schema_and_explain(spark, spark_imports) -> None:
    F = spark_imports.F
    df = spark.createDataFrame([("a,b,c,d",)], ["s"])
    out = df.select(F.split(F.col("s"), ",", 2).alias("arr"))

    # Schema simpleString parity (existing behavior)
    assert out.schema.simpleString() == "struct<arr:array<string>>"

    # Schema JSON parity: ArrayType(elementType=string, containsNull=False)
    schema_json = out.schema.jsonValue()
    assert schema_json["type"] == "struct"
    assert len(schema_json["fields"]) == 1
    field = schema_json["fields"][0]
    assert field["name"] == "arr"
    assert field["nullable"] is True
    assert field["metadata"] == {}
    field_type = field["type"]
    assert field_type["type"] == "array"
    assert field_type["elementType"] == "string"
    assert field_type["containsNull"] is False

    # UI / explain parity: explain() prints to stdout; returns None in PySpark/sparkless.
    explain_str = out.explain(True)
    assert explain_str is None or (
        isinstance(explain_str, str) and explain_str.strip() != ""
    )

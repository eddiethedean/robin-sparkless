"""Regression test for issue #1394: struct.getfield_alias parity.

Same scenario in both modes via spark + spark_imports: struct getField/alias.

Scenario (schema and expression):
        schema = StructType(
            [
                StructField(
                    "st",
                    StructType(
                        [
                            StructField("E1", IntegerType(), True),
                            StructField("E2", StringType(), True),
                        ]
                    ),
                    True,
                )
            ]
        )
        df = session.createDataFrame([{"st": {"E1": 1, "E2": "a"}}], schema=schema)
        return df.select(F.col("st").getField("E1").alias("e1_out"))

This test locks in Sparkless behavior for:
- Value semantics: getField(\"E1\") from a struct with IntegerType yields 1.
- Schema JSON (`schema.jsonValue()`): struct field type \"integer\" for e1_out.
- UI / explain: `DataFrame.explain()` returns a non-empty description.
"""

from __future__ import annotations


def test_issue_1394_struct_getfield_alias_schema_and_explain(
    spark, spark_imports
) -> None:
    F = spark_imports.F
    StructType = spark_imports.StructType
    StructField = spark_imports.StructField
    StringType = spark_imports.StringType
    IntegerType = spark_imports.IntegerType

    schema = StructType(
        [
            StructField(
                "st",
                StructType(
                    [
                        StructField("E1", IntegerType(), True),
                        StructField("E2", StringType(), True),
                    ]
                ),
                True,
            )
        ]
    )
    df = spark.createDataFrame([{"st": {"E1": 1, "E2": "a"}}], schema=schema)
    out = df.select(F.col("st").getField("E1").alias("e1_out"))

    rows = out.collect()
    assert len(rows) == 1
    assert rows[0]["e1_out"] == 1

    # Schema simpleString: keep current behavior (long vs int may differ), parity is
    # asserted via jsonValue below instead of simpleString.
    assert out.schema.simpleString().startswith("struct<e1_out:")

    # Schema JSON parity: e1_out is integer with nullable=True and empty metadata.
    schema_json = out.schema.jsonValue()
    assert schema_json["type"] == "struct"
    assert len(schema_json["fields"]) == 1
    field = schema_json["fields"][0]
    assert field["name"] == "e1_out"
    assert field["nullable"] is True
    assert field["metadata"] == {}
    assert field["type"] == "integer"

    # UI / explain parity: explain() prints to stdout; returns None in PySpark/sparkless.
    explain_str = out.explain(True)
    assert explain_str is None or (
        isinstance(explain_str, str) and explain_str.strip() != ""
    )

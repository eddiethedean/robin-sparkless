"""Regression test for issue #1392: cast.invalid_string_to_int parity.

Same scenario in both modes via spark + spark_imports: invalid string -> int cast.

This test locks in behavior for:
- Semantics: invalid string -> int cast yields null (no error), like PySpark.
- Schema JSON (`schema.jsonValue()`): struct with integer field type.
- UI / explain: `DataFrame.explain()` returns a non-empty description.
"""

from __future__ import annotations


def test_issue_1392_cast_invalid_string_to_int_schema_and_explain(
    spark, spark_imports
) -> None:
    F = spark_imports.F
    df = spark.createDataFrame([("nope",)], ["s"])
    out = df.select(F.col("s").cast("int").alias("i"))

    rows = out.collect()
    assert len(rows) == 1
    # Invalid string -> int should yield null, not raise.
    assert rows[0]["i"] is None

    # Schema simpleString parity (existing behavior).
    assert out.schema.simpleString() == "struct<i:int>"

    # Schema JSON parity: integer field with nullable=True and empty metadata.
    schema_json = out.schema.jsonValue()
    assert schema_json["type"] == "struct"
    assert len(schema_json["fields"]) == 1
    field = schema_json["fields"][0]
    assert field["name"] == "i"
    assert field["nullable"] is True
    assert field["metadata"] == {}
    assert field["type"] == "integer"

    # UI / explain parity: explain() prints to stdout; returns None in PySpark/sparkless.
    explain_str = out.explain(True)
    assert explain_str is None or (
        isinstance(explain_str, str) and explain_str.strip() != ""
    )

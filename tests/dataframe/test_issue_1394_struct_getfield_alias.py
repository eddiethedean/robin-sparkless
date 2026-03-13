"""Regression test for issue #1394: struct.getfield_alias parity.

Scenario from the issue:

    def scenario_struct_getfield_alias(session):
        if _backend_is_pyspark(session):
            from pyspark.sql import functions as F  # type: ignore
            from pyspark.sql.types import StructType, StructField, StringType, IntegerType  # type: ignore
        else:
            from sparkless.sql import functions as F  # type: ignore
            from sparkless.sql.types import StructType, StructField, StringType, IntegerType  # type: ignore

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

import pytest

from sparkless.sql import functions as F
from sparkless.sql.types import StructType, StructField, StringType, IntegerType


@pytest.mark.sparkless_only
def test_issue_1394_struct_getfield_alias_schema_and_explain(spark) -> None:
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

    # UI / explain parity: explain() should emit a non-empty description.
    explain_str = out.explain(True)
    assert isinstance(explain_str, str)
    assert explain_str.strip() != ""

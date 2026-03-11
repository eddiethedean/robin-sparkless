"""Regression test for issue #1391: session.conf_app_name parity.

PySpark scenario (from the issue body):

    def scenario_conf_app_name(session):
        conf = session.conf() if callable(session.conf) else session.conf
        # Return as single-row DF for uniform capture.
        return session.createDataFrame([(conf.get("spark.app.name"),)], ["spark_app_name"])

This test locks in Sparkless behavior for:
- `spark.app.name` coming through `session.conf.get(...)` into a DataFrame.
- Schema JSON (`schema.jsonValue()`): a struct with a single string field.
- UI / explain: `DataFrame.explain()` should produce a non-empty description.
"""

from __future__ import annotations

from sparkless.sql import SparkSession


def test_issue_1391_session_conf_app_name_schema_and_explain() -> None:
    spark = (
        SparkSession.builder.appName("issue_1391_session_conf_app_name")
        .config("spark.app.name", "issue_1391_session_conf_app_name")
        .getOrCreate()
    )
    try:
        conf = spark.conf() if callable(getattr(spark, "conf", None)) else spark.conf
        df = spark.createDataFrame(
            [(conf.get("spark.app.name"),)],
            ["spark_app_name"],
        )

        rows = df.collect()
        assert len(rows) == 1
        assert rows[0]["spark_app_name"] == "issue_1391_session_conf_app_name"

        # Schema simpleString parity (existing behavior).
        assert df.schema.simpleString() == "struct<spark_app_name:string>"

        # Schema JSON parity: single string field with nullable=True and empty metadata.
        schema_json = df.schema.jsonValue()
        assert schema_json["type"] == "struct"
        assert len(schema_json["fields"]) == 1
        field = schema_json["fields"][0]
        assert field["name"] == "spark_app_name"
        assert field["nullable"] is True
        assert field["metadata"] == {}
        assert field["type"] == "string"

        # UI / explain parity: explain() should emit a non-empty description.
        explain_str = df.explain(True)
        assert isinstance(explain_str, str)
        assert explain_str.strip() != ""
    finally:
        spark.stop()


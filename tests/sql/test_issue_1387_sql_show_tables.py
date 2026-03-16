"""
Regression test for issue #1387: sql.show_tables parity.

Assert PySpark behavior: session.sql("SHOW TABLES") returns a DataFrame with
schema struct<namespace:string,tableName:string,isTemporary:boolean>, and
explain() returns None (plan printed to stdout).
"""



def test_issue_1387_sql_show_tables_schema_data_and_explain(spark) -> None:
    """sql.show_tables: schema and explain match PySpark (issue #1387)."""
    df = spark.sql("SHOW TABLES")

    # PySpark schema: namespace, tableName, isTemporary
    schema_str = df.schema.simpleString()
    assert schema_str == "struct<namespace:string,tableName:string,isTemporary:boolean>"

    # Data: content may vary; just ensure we get a list of rows
    rows = df.collect()
    assert isinstance(rows, list)

    # PySpark: explain() returns None and prints plan to stdout
    plan = df.explain()
    assert plan is None

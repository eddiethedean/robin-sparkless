from __future__ import annotations


def test_issue_1534_double_left_join_shared_non_key_column_does_not_error(
    spark, spark_imports
) -> None:
    df_a = spark.createDataFrame([("A", 1, 100)], "A STRING, B INT, ValA INT")
    df_b = spark.createDataFrame([("A", 1, 200)], "A STRING, B INT, ValB INT")
    df_c = spark.createDataFrame([("A", 1, 300)], "A STRING, B INT, ValC INT")

    out = df_a.join(df_b, "A", "left").join(df_c, "A", "left")

    # Sparkless disambiguates duplicate column names; ensure we don't collide on "B_right"
    # when chaining joins.
    assert out.count() == 1
    assert "B_right" in out.columns
    assert any(c.startswith("B_right_") for c in out.columns)

"""Regression coverage for simultaneous withColumns replacements and generators."""

import pytest

from sparkless.testing import get_imports


def test_with_columns_replaces_case_insensitively(spark):
    F = get_imports().F
    frame = spark.createDataFrame([(1, 2)], ["value", "other"])
    result = frame.withColumns({"VALUE": F.lit(3)})
    assert result.columns == ["VALUE", "other"]
    row = result.collect()[0]
    assert (row["VALUE"], row["other"]) == (3, 2)


@pytest.mark.parametrize("target", ["item", "items"])
def test_with_columns_explode_expands_rows(spark, target):
    F = get_imports().F
    frame = spark.createDataFrame([(1, [10, 20]), (2, [30])], ["id", "items"])
    result = frame.withColumns({target: F.explode("items")})
    assert [(row["id"], row[target]) for row in result.collect()] == [
        (1, 10), (1, 20), (2, 30)
    ]


def test_with_columns_explode_keeps_other_expressions_on_original_rows(spark):
    F = get_imports().F
    frame = spark.createDataFrame([(1, [10, 20]), (2, [30])], ["id", "items"])
    result = frame.withColumns({"id": F.col("id") + 10, "item": F.explode("items"), "old_id": F.col("id")})
    assert [(row["id"], row["item"], row["old_id"]) for row in result.collect()] == [
        (11, 10, 1), (11, 20, 1), (12, 30, 2)
    ]


def test_with_columns_explode_replaces_case_insensitively(spark):
    F = get_imports().F
    frame = spark.createDataFrame([(1, [10, 20])], ["id", "items"])
    result = frame.withColumns({"ITEMS": F.explode("items")})
    assert result.columns == ["id", "ITEMS"]
    assert [(row["id"], row["ITEMS"]) for row in result.collect()] == [(1, 10), (1, 20)]


def test_with_columns_explode_drops_empty_input_rows(spark):
    F = get_imports().F
    frame = spark.createDataFrame([(1, [10]), (2, []), (3, [30, 40])], ["id", "items"])
    result = frame.withColumns({"item": F.explode("items"), "original": F.col("id")})
    assert [(row["item"], row["original"]) for row in result.collect()] == [(10, 1), (30, 3), (40, 3)]

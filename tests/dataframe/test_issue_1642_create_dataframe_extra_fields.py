"""Issue #1642: createDataFrame with extra tuple fields and names-only schema."""

from __future__ import annotations


def test_create_dataframe_extra_tuple_fields_names_only_schema_issue_1642(spark):
    df = spark.createDataFrame([("A", "B", "C", "D")], ["col1", "col2", "col3"])

    assert df.columns == ["col1", "col2", "col3", "_4"]
    rows = df.collect()
    assert len(rows) == 1
    row = rows[0].asDict()
    assert row["col1"] == "A"
    assert row["col2"] == "B"
    assert row["col3"] == "C"
    assert row["_4"] == "D"


def test_create_dataframe_structtype_still_rejects_extra_fields(spark):
    from sparkless.testing import get_imports

    T = get_imports()
    schema = T.StructType(
        [
            T.StructField("col1", T.StringType()),
            T.StructField("col2", T.StringType()),
            T.StructField("col3", T.StringType()),
        ]
    )

    import pytest

    with pytest.raises(Exception) as exc_info:
        spark.createDataFrame([("A", "B", "C", "D")], schema=schema)
    assert (
        "LENGTH_SHOULD_BE_THE_SAME" in str(exc_info.value)
        or "length" in str(exc_info.value).lower()
    )

"""Issue #1641: date_format with time pattern on DateType returns midnight time."""

from __future__ import annotations

from datetime import date

from sparkless.testing import get_imports

_imports = get_imports()
F = _imports.F
DateType = _imports.DateType
StructField = _imports.StructField
StructType = _imports.StructType


def test_date_format_time_pattern_on_date_type_issue_1641(spark):
    schema = StructType([StructField("col1", DateType())])
    df = spark.createDataFrame([(date(2025, 6, 15),)], schema)
    rows = df.withColumn("col2", F.date_format(F.col("col1"), "HH:mm:ss")).collect()

    assert len(rows) == 1
    row = rows[0].asDict()
    assert row["col1"] == date(2025, 6, 15)
    assert row["col2"] == "00:00:00"


def test_date_format_date_pattern_on_date_type_still_works(spark):
    schema = StructType([StructField("col1", DateType())])
    df = spark.createDataFrame([(date(2025, 6, 15),)], schema)
    rows = df.withColumn("col2", F.date_format(F.col("col1"), "yyyy-MM-dd")).collect()

    assert len(rows) == 1
    assert rows[0].asDict()["col2"] == "2025-06-15"

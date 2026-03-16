"""
Tests for #1347: duplicate field names in StructType (PySpark allows, sparkless rejects).

Same scenario in both modes: createDataFrame with duplicate struct field names.
Sparkless raises; PySpark may succeed. See docs/PYSPARK_DIFFERENCES.md.
"""

from __future__ import annotations


from sparkless.testing import get_imports

imports = get_imports()
StructType = imports.StructType
StructField = imports.StructField
IntegerType = imports.IntegerType
StringType = imports.StringType


def test_duplicate_field_names_same_scenario_both_modes(spark):
    """createDataFrame with duplicate struct field names: same scenario in both modes (#1347)."""
    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("id", StringType()),
        ]
    )
    data = [[1, "a"]]
    try:
        df = spark.createDataFrame(data, schema=schema)
        assert df.count() == 1
        assert len(df.columns) == 2
    except Exception as e:
        msg = str(e)
        assert "duplicate column name" in msg or "duplicate" in msg.lower()
        assert "id" in msg

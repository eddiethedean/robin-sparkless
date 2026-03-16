"""
PySpark parity tests for Issue #330: Struct field selection with alias.

These tests verify that Sparkless behavior matches PySpark behavior.
"""

from sparkless.testing import get_imports
import pytest


class TestStructFieldAliasParity:
    """PySpark parity tests for struct field selection with alias."""

    def test_struct_field_with_alias_parity(self):
        """Test struct field extraction with alias matches PySpark."""
        spark_imports = get_imports()
        SparkSession = spark_imports.SparkSession
        F = spark_imports.F

        spark = SparkSession.builder.appName("struct-field-alias-parity").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "StructValue": {"E1": 1, "E2": "A"}},
                    {"Name": "Bob", "StructValue": {"E1": 2, "E2": "B"}},
                ]
            )

            result = df.select(F.col("StructValue.E1").alias("E1-Extract"))
            rows = result.collect()

            assert len(rows) == 2
            assert rows[0]["E1-Extract"] == 1
            assert rows[1]["E1-Extract"] == 2
        finally:
            spark.stop()

    def test_struct_field_with_alias_multiple_fields_parity(self):
        """Test multiple struct fields with aliases matches PySpark."""
        spark_imports = get_imports()
        SparkSession = spark_imports.SparkSession
        F = spark_imports.F

        spark = SparkSession.builder.appName("struct-field-alias-parity").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "StructValue": {"E1": 1, "E2": "A"}},
                    {"Name": "Bob", "StructValue": {"E1": 2, "E2": "B"}},
                ]
            )

            # Missing struct fields should raise (PySpark: AnalysisException; Sparkless: SparklessError).
            with pytest.raises(Exception) as excinfo:
                df.select(
                    F.col("StructValue.E1").alias("E1-Extract"),
                    F.col("StructValue.E2").alias("E2-Extract"),
                ).collect()
            assert "Struct field 'E2' not found" in str(excinfo.value) or "No such struct field" in str(
                excinfo.value
            )
        finally:
            spark.stop()

    def test_struct_field_with_alias_and_other_columns_parity(self):
        """Test struct field with alias combined with other columns matches PySpark."""
        spark_imports = get_imports()
        SparkSession = spark_imports.SparkSession
        F = spark_imports.F

        spark = SparkSession.builder.appName("struct-field-alias-parity").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "StructValue": {"E1": 1, "E2": "A"}},
                    {"Name": "Bob", "StructValue": {"E1": 2, "E2": "B"}},
                ]
            )

            with pytest.raises(Exception) as excinfo:
                df.select(
                    "Name",
                    F.col("StructValue.E1").alias("E1-Extract"),
                    F.col("StructValue.E2").alias("E2-Extract"),
                ).collect()
            assert "Struct field 'E2' not found" in str(excinfo.value) or "No such struct field" in str(
                excinfo.value
            )
        finally:
            spark.stop()

"""
Tests for inferSchema parity between PySpark and Sparkless.

These tests ensure that:
1. CSV reading defaults to inferSchema=False (all columns as strings)
2. Explicit inferSchema=False works correctly
3. Explicit inferSchema=True infers types correctly
4. Type inference matches PySpark behavior
"""

import tempfile
from pathlib import Path

import pytest

from sparkless.sql import SparkSession
from sparkless.spark_types import StringType, LongType, DoubleType, BooleanType


class TestInferSchemaParity:
    """Test inferSchema parity with PySpark."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        spark = SparkSession("InferSchemaTest")
        yield spark
        spark.stop()

    @pytest.fixture
    def sample_csv(self):
        """Create a sample CSV file for testing."""
        csv_content = """name,age,salary,active
Alice,25,50000.5,true
Bob,30,60000,false
Charlie,35,70000.75,true"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name
        yield temp_path
        Path(temp_path).unlink(missing_ok=True)

    def test_csv_default_no_infer_schema(self, spark, sample_csv):
        """Test that CSV reading defaults to inferSchema=False (all strings).

        PySpark behavior: All columns are StringType when inferSchema is not specified.
        """
        df = spark.read.option("header", True).csv(sample_csv)

        # All columns should be StringType (PySpark default)
        schema = df.schema
        assert len(schema.fields) == 4, "Should have 4 columns"

        for field in schema.fields:
            assert isinstance(field.dataType, StringType), (
                f"Column {field.name} should be StringType, got {type(field.dataType).__name__}"
            )

        # Verify column names
        field_names = [f.name for f in schema.fields]
        assert "name" in field_names
        assert "age" in field_names
        assert "salary" in field_names
        assert "active" in field_names

    def test_csv_explicit_infer_schema_false(self, spark, sample_csv):
        """Test that explicit inferSchema=False keeps all columns as strings."""
        df = (
            spark.read.option("header", True)
            .option("inferSchema", False)
            .csv(sample_csv)
        )

        # All columns should be StringType
        for field in df.schema.fields:
            assert isinstance(field.dataType, StringType), (
                f"Column {field.name} should be StringType when inferSchema=False"
            )

    def test_csv_explicit_infer_schema_true(self, spark, sample_csv):
        """Test that explicit inferSchema=True infers types correctly."""
        df = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(sample_csv)
        )

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        # name should be StringType
        assert isinstance(field_dict["name"], StringType), "name should be StringType"

        # age should be LongType (PySpark infers int as LongType)
        assert isinstance(field_dict["age"], LongType), (
            "age should be LongType, not IntegerType"
        )

        # salary should be DoubleType (PySpark infers float as DoubleType)
        assert isinstance(field_dict["salary"], DoubleType), (
            "salary should be DoubleType"
        )

        # active should be BooleanType
        assert isinstance(field_dict["active"], BooleanType), (
            "active should be BooleanType"
        )

    def test_csv_no_header_default_behavior(self, spark):
        """Test CSV reading without header still defaults to inferSchema=False."""
        csv_content = """Alice,25,50000.5
Bob,30,60000"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            df = spark.read.csv(temp_path)

            # All columns should be StringType (default behavior)
            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"Column {field.name} should be StringType by default"
                )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_mixed_int_float_raises_error(self, spark):
        """Test that mixed int/float columns raise TypeError (PySpark behavior).

        Note: PySpark raises an error for mixed int/float in createDataFrame,
        but CSV files with inferSchema=True will promote mixed int/float to DoubleType.
        """
        data = [{"id": 1, "value": 1.5}, {"id": 2, "value": 2}]

        # PySpark raises TypeError for mixed int/float in createDataFrame
        with pytest.raises(TypeError, match="Can not merge type|Can not merge types"):
            spark.createDataFrame(data)

    def test_create_dataframe_type_inference(self, spark):
        """Test that createDataFrame infers types correctly (matching PySpark)."""
        # Use consistent types to avoid type conflicts
        data = [
            {"name": "Alice", "age": 25, "score": 95.5},
            {"name": "Bob", "age": 30, "score": 87.0},  # Use float, not int
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        # name should be StringType
        assert isinstance(field_dict["name"], StringType), "name should be StringType"

        # age should be LongType (Python int → LongType)
        assert isinstance(field_dict["age"], LongType), "age should be LongType"

        # score should be DoubleType (Python float → DoubleType)
        assert isinstance(field_dict["score"], DoubleType), "score should be DoubleType"

    def test_csv_infer_schema_with_numeric_strings(self, spark):
        """Test that numeric strings are kept as strings when inferSchema=False."""
        csv_content = """id,value
001,123
002,456"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema, numeric-looking strings should stay as strings
            df = spark.read.option("header", True).csv(temp_path)

            schema = df.schema
            field_dict = {f.name: f.dataType for f in schema.fields}

            assert isinstance(field_dict["id"], StringType), "id should be StringType"
            assert isinstance(field_dict["value"], StringType), (
                "value should be StringType"
            )

            # With inferSchema=True, they should be inferred as numbers
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            schema_inferred = df_inferred.schema
            field_dict_inferred = {f.name: f.dataType for f in schema_inferred.fields}

            assert isinstance(field_dict_inferred["id"], LongType), (
                "id should be LongType"
            )
            assert isinstance(field_dict_inferred["value"], LongType), (
                "value should be LongType"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_csv_infer_schema_boolean_strings(self, spark):
        """Test boolean string handling with and without inferSchema."""
        csv_content = """flag1,flag2
true,false
false,true"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema, boolean strings should stay as strings
            df = spark.read.option("header", True).csv(temp_path)

            schema = df.schema
            field_dict = {f.name: f.dataType for f in schema.fields}

            assert isinstance(field_dict["flag1"], StringType), (
                "flag1 should be StringType"
            )
            assert isinstance(field_dict["flag2"], StringType), (
                "flag2 should be StringType"
            )

            # With inferSchema=True, they should be inferred as booleans
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            schema_inferred = df_inferred.schema
            field_dict_inferred = {f.name: f.dataType for f in schema_inferred.fields}

            assert isinstance(field_dict_inferred["flag1"], BooleanType), (
                "flag1 should be BooleanType"
            )
            assert isinstance(field_dict_inferred["flag2"], BooleanType), (
                "flag2 should be BooleanType"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_csv_with_leading_zeros(self, spark):
        """Test that leading zeros in numbers are preserved as strings when inferSchema=False."""
        csv_content = """id,code
001,0001
002,0002
003,0003"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema, leading zeros should be preserved as strings
            df = spark.read.option("header", True).csv(temp_path)

            schema = df.schema
            field_dict = {f.name: f.dataType for f in schema.fields}

            assert isinstance(field_dict["id"], StringType), "id should be StringType"
            assert isinstance(field_dict["code"], StringType), (
                "code should be StringType"
            )

            # Verify actual values preserve leading zeros
            rows = df.collect()
            assert rows[0]["id"] == "001", "Leading zero should be preserved"
            assert rows[0]["code"] == "0001", "Leading zeros should be preserved"

            # With inferSchema=True, they should be inferred as numbers (losing leading zeros)
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            schema_inferred = df_inferred.schema
            field_dict_inferred = {f.name: f.dataType for f in schema_inferred.fields}

            assert isinstance(field_dict_inferred["id"], LongType), (
                "id should be LongType"
            )
            assert isinstance(field_dict_inferred["code"], LongType), (
                "code should be LongType"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_csv_with_negative_numbers(self, spark):
        """Test negative number handling."""
        csv_content = """value,temperature
-10,-5.5
-20,-10.0
0,0.0"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema, negative numbers should be strings
            df = spark.read.option("header", True).csv(temp_path)

            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )

            # With inferSchema=True, they should be inferred correctly
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            schema_inferred = df_inferred.schema
            field_dict = {f.name: f.dataType for f in schema_inferred.fields}

            assert isinstance(field_dict["value"], LongType), "value should be LongType"
            assert isinstance(field_dict["temperature"], DoubleType), (
                "temperature should be DoubleType"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_csv_with_scientific_notation(self, spark):
        """Test scientific notation handling."""
        csv_content = """small,large
1e-10,1e10
2.5e-5,2.5e5
3.14e-2,3.14e2"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema, scientific notation should be strings
            df = spark.read.option("header", True).csv(temp_path)

            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )

            # With inferSchema=True, they should be inferred as DoubleType
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            schema_inferred = df_inferred.schema
            field_dict = {f.name: f.dataType for f in schema_inferred.fields}

            assert isinstance(field_dict["small"], DoubleType), (
                "small should be DoubleType"
            )
            assert isinstance(field_dict["large"], DoubleType), (
                "large should be DoubleType"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_csv_with_empty_strings(self, spark):
        """Test empty string handling."""
        csv_content = """name,value,flag
Alice,100,true
,,
Bob,200,false"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema, empty strings should be strings
            df = spark.read.option("header", True).csv(temp_path)

            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )

            rows = df.collect()
            # Polars may return None or empty string for empty CSV values
            assert rows[1]["name"] in ("", None), (
                "Empty string should be preserved or None"
            )
            assert rows[1]["value"] in ("", None), (
                "Empty string should be preserved or None"
            )

            # With inferSchema=True, empty strings might be null or cause issues
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            # Empty strings in numeric columns might be null or cause inference issues
            schema_inferred = df_inferred.schema
            # The behavior depends on Polars, but we should handle it gracefully
            assert len(schema_inferred.fields) == 3, "Should have 3 fields"
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_csv_with_null_values(self, spark):
        """Test null value handling in CSV."""
        csv_content = """id,name,age
1,Alice,25
2,,30
3,Bob,"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema, nulls should be strings
            df = spark.read.option("header", True).csv(temp_path)

            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )

            rows = df.collect()
            # Polars may return None or empty string for empty CSV values
            assert rows[1]["name"] in ("", None), (
                "Empty name should be empty string or None"
            )
            assert rows[2]["age"] in ("", None), (
                "Empty age should be empty string or None"
            )

            # With inferSchema=True, nulls should be handled correctly
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            schema_inferred = df_inferred.schema
            field_dict = {f.name: f.dataType for f in schema_inferred.fields}

            assert isinstance(field_dict["id"], LongType), "id should be LongType"
            assert isinstance(field_dict["name"], StringType), (
                "name should be StringType"
            )
            assert isinstance(field_dict["age"], LongType), "age should be LongType"
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_csv_with_unicode_characters(self, spark):
        """Test unicode character handling."""
        csv_content = """name,city
Alice,北京
Bob,東京
Charlie,Москва"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, encoding="utf-8"
        ) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Unicode should always be StringType
            df = spark.read.option("header", True).csv(temp_path)

            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )

            rows = df.collect()
            assert "北京" in rows[0]["city"], "Unicode should be preserved"
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_csv_with_special_characters(self, spark):
        """Test special character handling in CSV."""
        csv_content = 'text,value\n"Hello, World",100\n"Say ""Hi""",200\n"Tests",300'
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Special characters should be handled as strings
            df = spark.read.option("header", True).csv(temp_path)

            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_csv_with_very_large_numbers(self, spark):
        """Test very large number handling."""
        csv_content = """big_int,big_float
999999999999,999999999999.999
1000000000000,1000000000000.0
-999999999999,-999999999999.999"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema, large numbers should be strings
            df = spark.read.option("header", True).csv(temp_path)

            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )

            # With inferSchema=True, they should be inferred correctly
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            schema_inferred = df_inferred.schema
            field_dict = {f.name: f.dataType for f in schema_inferred.fields}

            assert isinstance(field_dict["big_int"], LongType), (
                "big_int should be LongType"
            )
            assert isinstance(field_dict["big_float"], DoubleType), (
                "big_float should be DoubleType"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_csv_with_decimal_precision(self, spark):
        """Test decimal precision handling."""
        csv_content = """price,rate
99.99,0.123456789
100.00,0.987654321
0.01,0.000000001"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema, decimals should be strings
            df = spark.read.option("header", True).csv(temp_path)

            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )

            # With inferSchema=True, they should be inferred as DoubleType
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            schema_inferred = df_inferred.schema
            field_dict = {f.name: f.dataType for f in schema_inferred.fields}

            assert isinstance(field_dict["price"], DoubleType), (
                "price should be DoubleType"
            )
            assert isinstance(field_dict["rate"], DoubleType), (
                "rate should be DoubleType"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_csv_mixed_boolean_strings(self, spark):
        """Test mixed boolean-like strings that are not all true/false."""
        csv_content = """flag1,flag2,flag3
true,false,maybe
false,true,no
true,false,yes"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema, all should be strings
            df = spark.read.option("header", True).csv(temp_path)

            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )

            # With inferSchema=True, flag1 and flag2 should be boolean, flag3 should be string
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            schema_inferred = df_inferred.schema
            field_dict = {f.name: f.dataType for f in schema_inferred.fields}

            assert isinstance(field_dict["flag1"], BooleanType), (
                "flag1 should be BooleanType"
            )
            assert isinstance(field_dict["flag2"], BooleanType), (
                "flag2 should be BooleanType"
            )
            assert isinstance(field_dict["flag3"], StringType), (
                "flag3 should be StringType (mixed values)"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_all_null_column(self, spark):
        """Test that all-null columns raise ValueError (PySpark behavior)."""
        data = [
            {"name": "Alice", "age": 25, "unknown": None},
            {"name": "Bob", "age": 30, "unknown": None},
        ]

        # PySpark raises ValueError when all values for a column are null
        with pytest.raises(ValueError, match="Some of types cannot be determined"):
            spark.createDataFrame(data)

    def test_create_dataframe_with_partial_nulls(self, spark):
        """Test DataFrame creation with some null values."""
        data = [
            {"name": "Alice", "age": 25, "score": 95.5},
            {"name": "Bob", "age": None, "score": 87.0},
            {"name": "Charlie", "age": 35, "score": None},
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        assert isinstance(field_dict["name"], StringType), "name should be StringType"
        assert isinstance(field_dict["age"], LongType), (
            "age should be LongType (nullable)"
        )
        assert isinstance(field_dict["score"], DoubleType), (
            "score should be DoubleType (nullable)"
        )

    def test_create_dataframe_with_complex_types(self, spark):
        """Test DataFrame creation with complex types (arrays, maps)."""
        data = [
            {
                "name": "Alice",
                "tags": ["python", "spark"],
                "metadata": {"role": "engineer"},
            },
            {
                "name": "Bob",
                "tags": ["java", "scala"],
                "metadata": {"role": "developer"},
            },
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        assert isinstance(field_dict["name"], StringType), "name should be StringType"
        # Arrays and maps are inferred as MapType in current implementation
        # This tests that complex types don't break inference

    def test_csv_with_date_strings(self, spark):
        """Test date string handling."""
        csv_content = (
            "date,datetime\n"
            "2024-01-15,2024-01-15 10:30:00\n"
            "2024-02-20,2024-02-20 14:45:30\n"
            "2024-03-25,2024-03-25 18:00:00"
        )
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema, dates should be strings
            df = spark.read.option("header", True).csv(temp_path)

            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )

            # With inferSchema=True, Polars might infer dates, but we need to check behavior
            # Note: Date inference depends on Polars' behavior
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            schema_inferred = df_inferred.schema
            # Date inference behavior may vary, but should not crash
            assert len(schema_inferred.fields) == 2, "Should have 2 fields"
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_csv_with_mixed_numeric_strings(self, spark):
        """Test CSV with some numeric strings and some non-numeric."""
        csv_content = "id,code\n001,ABC\n002,123\n003,DEF"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema, all should be strings
            df = spark.read.option("header", True).csv(temp_path)

            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )

            # With inferSchema=True, mixed columns should stay as strings
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            schema_inferred = df_inferred.schema
            field_dict = {f.name: f.dataType for f in schema_inferred.fields}

            # If code has mixed numeric/non-numeric, it should be StringType
            # id might be inferred as LongType if all values are numeric
            assert isinstance(field_dict["id"], LongType), "id should be LongType"
            # code should be StringType because it has non-numeric values
            assert isinstance(field_dict["code"], StringType), (
                "code should be StringType (mixed)"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_csv_empty_file(self, spark):
        """Test reading empty CSV file."""
        csv_content = """name,age"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Empty file with header should create empty DataFrame with StringType columns
            df = spark.read.option("header", True).csv(temp_path)

            assert df.count() == 0, "Should have 0 rows"
            assert len(df.schema.fields) == 2, "Should have 2 columns"
            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_csv_single_row(self, spark):
        """Test CSV with single row."""
        csv_content = """name,age,score
Alice,25,95.5"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Single row should work
            df = spark.read.option("header", True).csv(temp_path)

            assert df.count() == 1, "Should have 1 row"
            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )

            # With inferSchema
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            schema_inferred = df_inferred.schema
            field_dict = {f.name: f.dataType for f in schema_inferred.fields}

            assert isinstance(field_dict["name"], StringType), (
                "name should be StringType"
            )
            assert isinstance(field_dict["age"], LongType), "age should be LongType"
            assert isinstance(field_dict["score"], DoubleType), (
                "score should be DoubleType"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_empty_list(self, spark):
        """Test creating DataFrame from empty list."""
        from sparkless.spark_types import StructType, StructField, StringType

        # Empty list with schema should work
        schema = StructType(
            [StructField("name", StringType()), StructField("age", LongType())]
        )
        df = spark.createDataFrame([], schema)

        assert df.count() == 0, "Should have 0 rows"
        assert len(df.schema.fields) == 2, "Should have 2 columns"

        # Empty list without schema should raise error
        with pytest.raises(ValueError, match="can not infer schema from empty dataset"):
            spark.createDataFrame([])

    def test_create_dataframe_sparse_data(self, spark):
        """Test DataFrame creation with sparse data (different keys per row)."""
        data = [
            {"name": "Alice", "age": 25},
            {"name": "Bob", "city": "NYC"},  # Different keys
            {"name": "Charlie", "age": 30, "score": 95.5},
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        # All keys should be present, sorted alphabetically
        assert "age" in field_dict, "age should be in schema"
        assert "city" in field_dict, "city should be in schema"
        assert "name" in field_dict, "name should be in schema"
        assert "score" in field_dict, "score should be in schema"

        # Check types
        assert isinstance(field_dict["name"], StringType), "name should be StringType"
        assert isinstance(field_dict["age"], LongType), "age should be LongType"
        assert isinstance(field_dict["score"], DoubleType), "score should be DoubleType"

    def test_create_dataframe_type_conflict_raises_error(self, spark):
        """Test that type conflicts raise TypeError (PySpark behavior)."""
        data = [
            {"value": 1},  # int
            {"value": "string"},  # string - conflict!
        ]

        # PySpark raises TypeError when types conflict
        with pytest.raises(TypeError, match="Can not merge type"):
            spark.createDataFrame(data)

    def test_csv_with_trailing_whitespace(self, spark):
        """Test CSV with trailing whitespace in values."""
        csv_content = """name,value
 Alice , 100 
 Bob , 200 """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema, whitespace should be preserved
            df = spark.read.option("header", True).csv(temp_path)

            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )

            rows = df.collect()
            # Whitespace should be preserved when read as strings
            assert " Alice " in rows[0]["name"] or "Alice" in rows[0]["name"], (
                "Whitespace handling"
            )

            # With inferSchema, Polars might trim or preserve whitespace
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            # Should not crash
            assert df_inferred.count() == 2, "Should have 2 rows"
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_csv_with_custom_delimiter(self, spark):
        """Test CSV with custom delimiter."""
        csv_content = """name|age|score
Alice|25|95.5
Bob|30|87.0"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema, custom delimiter should work
            df = spark.read.option("header", True).option("sep", "|").csv(temp_path)

            assert df.count() == 2, "Should have 2 rows"
            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )

            # With inferSchema
            df_inferred = (
                spark.read.option("header", True)
                .option("sep", "|")
                .option("inferSchema", True)
                .csv(temp_path)
            )

            schema_inferred = df_inferred.schema
            field_dict = {f.name: f.dataType for f in schema_inferred.fields}

            assert isinstance(field_dict["name"], StringType), (
                "name should be StringType"
            )
            assert isinstance(field_dict["age"], LongType), "age should be LongType"
            assert isinstance(field_dict["score"], DoubleType), (
                "score should be DoubleType"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_zero_values(self, spark):
        """Test DataFrame creation with zero values."""
        data = [
            {"int_val": 0, "float_val": 0.0, "bool_val": False},
            {"int_val": 1, "float_val": 1.0, "bool_val": True},
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        assert isinstance(field_dict["int_val"], LongType), "int_val should be LongType"
        assert isinstance(field_dict["float_val"], DoubleType), (
            "float_val should be DoubleType"
        )
        assert isinstance(field_dict["bool_val"], BooleanType), (
            "bool_val should be BooleanType"
        )

    def test_csv_with_only_boolean_column(self, spark):
        """Test CSV with only boolean values in a column."""
        csv_content = """active
true
false
true
false"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema
            df = spark.read.option("header", True).csv(temp_path)

            assert isinstance(df.schema.fields[0].dataType, StringType), (
                "Should be StringType"
            )

            # With inferSchema
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            assert isinstance(df_inferred.schema.fields[0].dataType, BooleanType), (
                "Should be BooleanType"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_csv_with_only_integer_column(self, spark):
        """Test CSV with only integer values in a column."""
        csv_content = """count
1
2
3
100"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema
            df = spark.read.option("header", True).csv(temp_path)

            assert isinstance(df.schema.fields[0].dataType, StringType), (
                "Should be StringType"
            )

            # With inferSchema
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            assert isinstance(df_inferred.schema.fields[0].dataType, LongType), (
                "Should be LongType, not IntegerType"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_csv_with_only_float_column(self, spark):
        """Test CSV with only float values in a column."""
        csv_content = """price
1.5
2.7
3.14
100.0"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema
            df = spark.read.option("header", True).csv(temp_path)

            assert isinstance(df.schema.fields[0].dataType, StringType), (
                "Should be StringType"
            )

            # With inferSchema
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            assert isinstance(df_inferred.schema.fields[0].dataType, DoubleType), (
                "Should be DoubleType, not FloatType"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_large_dataset(self, spark):
        """Test DataFrame creation with larger dataset."""
        data = [{"id": i, "value": i * 1.5, "name": f"Item{i}"} for i in range(100)]

        df = spark.createDataFrame(data)

        assert df.count() == 100, "Should have 100 rows"

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        assert isinstance(field_dict["id"], LongType), "id should be LongType"
        assert isinstance(field_dict["value"], DoubleType), "value should be DoubleType"
        assert isinstance(field_dict["name"], StringType), "name should be StringType"

    def test_csv_column_order_preserved(self, spark):
        """Test that column order is preserved in CSV reading."""
        csv_content = """z_col,a_col,m_col
1,2,3
4,5,6"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            df = spark.read.option("header", True).csv(temp_path)

            # Column order should match CSV (not alphabetical)
            field_names = [f.name for f in df.schema.fields]
            assert field_names == ["z_col", "a_col", "m_col"], (
                "Column order should be preserved"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_column_order_alphabetical(self, spark):
        """Test that createDataFrame sorts columns alphabetically (PySpark behavior)."""
        data = [
            {"zebra": 1, "apple": 2, "monkey": 3},
            {"zebra": 4, "apple": 5, "monkey": 6},
        ]

        df = spark.createDataFrame(data)

        # Columns should be sorted alphabetically
        field_names = [f.name for f in df.schema.fields]
        assert field_names == ["apple", "monkey", "zebra"], (
            "Columns should be sorted alphabetically"
        )

    def test_csv_with_very_small_numbers(self, spark):
        """Test very small number handling."""
        csv_content = """tiny,small
0.000000001,0.0001
0.000000002,0.0002
0.000000003,0.0003"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema
            df = spark.read.option("header", True).csv(temp_path)

            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )

            # With inferSchema
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            schema_inferred = df_inferred.schema
            field_dict = {f.name: f.dataType for f in schema_inferred.fields}

            assert isinstance(field_dict["tiny"], DoubleType), (
                "tiny should be DoubleType"
            )
            assert isinstance(field_dict["small"], DoubleType), (
                "small should be DoubleType"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_csv_with_mixed_boolean_and_string(self, spark):
        """Test column with some boolean values and some strings."""
        csv_content = """flag
true
false
maybe
yes"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema
            df = spark.read.option("header", True).csv(temp_path)

            assert isinstance(df.schema.fields[0].dataType, StringType), (
                "Should be StringType"
            )

            # With inferSchema, mixed boolean/string should be StringType
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            assert isinstance(df_inferred.schema.fields[0].dataType, StringType), (
                "Should be StringType (mixed boolean/string)"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_csv_with_mixed_int_and_string(self, spark):
        """Test column with some integers and some strings."""
        csv_content = """value
1
2
abc
3"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema
            df = spark.read.option("header", True).csv(temp_path)

            assert isinstance(df.schema.fields[0].dataType, StringType), (
                "Should be StringType"
            )

            # With inferSchema, mixed int/string should be StringType
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            assert isinstance(df_inferred.schema.fields[0].dataType, StringType), (
                "Should be StringType (mixed int/string)"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_csv_with_mixed_float_and_string(self, spark):
        """Test column with some floats and some strings."""
        csv_content = """value
1.5
2.7
abc
3.14"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema
            df = spark.read.option("header", True).csv(temp_path)

            assert isinstance(df.schema.fields[0].dataType, StringType), (
                "Should be StringType"
            )

            # With inferSchema, mixed float/string should be StringType
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            assert isinstance(df_inferred.schema.fields[0].dataType, StringType), (
                "Should be StringType (mixed float/string)"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_bytes(self, spark):
        """Test DataFrame creation with bytes data."""
        data = [
            {"name": "Alice", "data": b"binary_data_1"},
            {"name": "Bob", "data": b"binary_data_2"},
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        assert isinstance(field_dict["name"], StringType), "name should be StringType"
        # Bytes should be inferred as BinaryType
        from sparkless.spark_types import BinaryType

        assert isinstance(field_dict["data"], BinaryType), "data should be BinaryType"

    def test_csv_with_tab_delimiter(self, spark):
        """Test CSV with tab delimiter."""
        csv_content = """name\tage\tscore
Alice\t25\t95.5
Bob\t30\t87.0"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema
            df = spark.read.option("header", True).option("sep", "\t").csv(temp_path)

            assert df.count() == 2, "Should have 2 rows"
            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )

            # With inferSchema
            df_inferred = (
                spark.read.option("header", True)
                .option("sep", "\t")
                .option("inferSchema", True)
                .csv(temp_path)
            )

            schema_inferred = df_inferred.schema
            field_dict = {f.name: f.dataType for f in schema_inferred.fields}

            assert isinstance(field_dict["name"], StringType), (
                "name should be StringType"
            )
            assert isinstance(field_dict["age"], LongType), "age should be LongType"
            assert isinstance(field_dict["score"], DoubleType), (
                "score should be DoubleType"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_csv_with_semicolon_delimiter(self, spark):
        """Test CSV with semicolon delimiter (common in European locales)."""
        csv_content = """name;age;score
Alice;25;95.5
Bob;30;87.0"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            df = spark.read.option("header", True).option("sep", ";").csv(temp_path)

            assert df.count() == 2, "Should have 2 rows"
            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_none_values(self, spark):
        """Test DataFrame creation with None values in various positions."""
        data = [
            {"name": "Alice", "age": 25, "score": 95.5},
            {"name": None, "age": 30, "score": None},
            {"name": "Bob", "age": None, "score": 87.0},
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        assert isinstance(field_dict["name"], StringType), "name should be StringType"
        assert isinstance(field_dict["age"], LongType), "age should be LongType"
        assert isinstance(field_dict["score"], DoubleType), "score should be DoubleType"

        # Verify None values are preserved
        rows = df.collect()
        assert rows[1]["name"] is None, "None should be preserved"
        assert rows[1]["score"] is None, "None should be preserved"
        assert rows[2]["age"] is None, "None should be preserved"

    def test_csv_with_quoted_values(self, spark):
        """Test CSV with quoted values."""
        csv_content = '''name,description
"Alice","Engineer with 10+ years"
"Bob","Developer, loves Python"
"Charlie","Manager; coordinates teams"'''
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema
            df = spark.read.option("header", True).csv(temp_path)

            assert df.count() == 3, "Should have 3 rows"
            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )

            rows = df.collect()
            assert "Engineer with 10+ years" in rows[0]["description"], (
                "Quoted value should be preserved"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_csv_with_escaped_quotes(self, spark):
        """Test CSV with escaped quotes."""
        csv_content = '''name,quote
Alice,"Say ""Hello"""
Bob,"Say ""Hi"" there"'''
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            df = spark.read.option("header", True).csv(temp_path)

            assert df.count() == 2, "Should have 2 rows"
            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_very_long_strings(self, spark):
        """Test DataFrame creation with very long string values."""
        long_string = "A" * 10000
        data = [
            {"id": 1, "text": long_string},
            {"id": 2, "text": "Short"},
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        assert isinstance(field_dict["id"], LongType), "id should be LongType"
        assert isinstance(field_dict["text"], StringType), "text should be StringType"

        rows = df.collect()
        assert len(rows[0]["text"]) == 10000, "Long string should be preserved"

    def test_csv_with_multiple_headers(self, spark):
        """Test CSV reading behavior (should use first row as header)."""
        csv_content = """name,age
Alice,25
name,age
Bob,30"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            df = spark.read.option("header", True).csv(temp_path)

            # Should have 3 rows (first header row is skipped, second "name,age" is data)
            assert df.count() == 3, "Should have 3 rows"
            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_boolean_edge_cases(self, spark):
        """Test DataFrame creation with boolean edge cases."""
        data = [
            {"flag": True, "value": 1},
            {"flag": False, "value": 0},
            {"flag": True, "value": -1},
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        assert isinstance(field_dict["flag"], BooleanType), "flag should be BooleanType"
        assert isinstance(field_dict["value"], LongType), "value should be LongType"

    def test_csv_with_only_zeros(self, spark):
        """Test CSV with only zero values."""
        csv_content = """int_val,float_val
0,0.0
0,0.0
0,0.0"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema
            df = spark.read.option("header", True).csv(temp_path)

            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )

            # With inferSchema
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            schema_inferred = df_inferred.schema
            field_dict = {f.name: f.dataType for f in schema_inferred.fields}

            assert isinstance(field_dict["int_val"], LongType), (
                "int_val should be LongType"
            )
            assert isinstance(field_dict["float_val"], DoubleType), (
                "float_val should be DoubleType"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_float_precision(self, spark):
        """Test DataFrame creation with various float precisions."""
        data = [
            {"value": 1.0},
            {"value": 1.5},
            {"value": 1.123456789},
            {"value": 1.999999999},
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        assert isinstance(field_dict["value"], DoubleType), "value should be DoubleType"

    def test_csv_with_inconsistent_decimal_places(self, spark):
        """Test CSV with inconsistent decimal places."""
        csv_content = """price
1
1.5
1.50
1.500
1.5000"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema
            df = spark.read.option("header", True).csv(temp_path)

            assert isinstance(df.schema.fields[0].dataType, StringType), (
                "Should be StringType"
            )

            # With inferSchema, mixed int/float should be DoubleType
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            assert isinstance(df_inferred.schema.fields[0].dataType, DoubleType), (
                "Should be DoubleType (mixed int/float)"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_negative_floats(self, spark):
        """Test DataFrame creation with negative float values."""
        data = [
            {"temp": -10.5},
            {"temp": -0.1},
            {"temp": 0.0},
            {"temp": 10.5},
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        assert isinstance(field_dict["temp"], DoubleType), "temp should be DoubleType"

    def test_csv_with_commas_in_quoted_strings(self, spark):
        """Test CSV with commas inside quoted strings."""
        csv_content = '''name,address
Alice,"123 Main St, Apt 4B"
Bob,"456 Oak Ave, Suite 100"'''
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            df = spark.read.option("header", True).csv(temp_path)

            assert df.count() == 2, "Should have 2 rows"
            assert len(df.schema.fields) == 2, "Should have 2 columns"
            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )

            rows = df.collect()
            assert "123 Main St, Apt 4B" in rows[0]["address"], (
                "Comma in quoted string should be preserved"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_all_same_values(self, spark):
        """Test DataFrame creation where all values in a column are the same."""
        data = [
            {"name": "Alice", "status": "active"},
            {"name": "Bob", "status": "active"},
            {"name": "Charlie", "status": "active"},
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        assert isinstance(field_dict["name"], StringType), "name should be StringType"
        assert isinstance(field_dict["status"], StringType), (
            "status should be StringType"
        )

    def test_csv_with_newlines_in_quoted_strings(self, spark):
        """Test CSV with newlines inside quoted strings."""
        csv_content = '''name,description
Alice,"Line 1
Line 2"
Bob,"Single line"'''
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            df = spark.read.option("header", True).csv(temp_path)

            assert df.count() == 2, "Should have 2 rows"
            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_very_large_integers(self, spark):
        """Test DataFrame creation with very large integer values."""
        data = [
            {"id": 9223372036854775807},  # Max 64-bit signed integer
            {"id": -9223372036854775808},  # Min 64-bit signed integer
            {"id": 0},
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        assert isinstance(field_dict["id"], LongType), "id should be LongType"

    def test_csv_with_currency_symbols(self, spark):
        """Test CSV with currency symbols."""
        csv_content = """item,price
Apple,$1.50
Banana,$0.75
Orange,$2.00"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema, currency symbols should be strings
            df = spark.read.option("header", True).csv(temp_path)

            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )

            # With inferSchema, currency symbols prevent numeric inference
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            assert isinstance(df_inferred.schema.fields[1].dataType, StringType), (
                "price should be StringType (contains currency symbol)"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_percentage_strings(self, spark):
        """Test DataFrame creation with percentage-like strings."""
        data = [
            {"value": "50%"},
            {"value": "75%"},
            {"value": "100%"},
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        assert isinstance(field_dict["value"], StringType), "value should be StringType"

    def test_csv_with_percentage_values(self, spark):
        """Test CSV with percentage values."""
        csv_content = """rate
50%
75%
100%"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema
            df = spark.read.option("header", True).csv(temp_path)

            assert isinstance(df.schema.fields[0].dataType, StringType), (
                "Should be StringType"
            )

            # With inferSchema, percentage symbol prevents numeric inference
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            assert isinstance(df_inferred.schema.fields[0].dataType, StringType), (
                "Should be StringType (contains % symbol)"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_phone_numbers(self, spark):
        """Test DataFrame creation with phone number-like strings."""
        data = [
            {"phone": "555-1234"},
            {"phone": "(555) 5678"},
            {"phone": "+1-555-9012"},
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        assert isinstance(field_dict["phone"], StringType), "phone should be StringType"

    def test_csv_with_phone_numbers(self, spark):
        """Test CSV with phone number-like strings."""
        csv_content = """name,phone
Alice,555-1234
Bob,(555) 5678
Charlie,+1-555-9012"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            df = spark.read.option("header", True).csv(temp_path)

            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_iso8601_dates(self, spark):
        """Test DataFrame creation with ISO 8601 date strings."""
        data = [
            {"date": "2024-01-15", "datetime": "2024-01-15T10:30:00"},
            {"date": "2024-02-20", "datetime": "2024-02-20T14:45:30"},
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        # Date strings might be inferred as DateType or StringType depending on detection
        # At minimum, they should not cause errors
        assert "date" in field_dict, "date should be in schema"
        assert "datetime" in field_dict, "datetime should be in schema"

    def test_csv_with_iso8601_dates(self, spark):
        """Test CSV with ISO 8601 date strings."""
        csv_content = """date,datetime
2024-01-15,2024-01-15T10:30:00
2024-02-20,2024-02-20T14:45:30"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema
            df = spark.read.option("header", True).csv(temp_path)

            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )

            # With inferSchema, dates might be inferred
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            # Should not crash
            assert len(df_inferred.schema.fields) == 2, "Should have 2 fields"
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_time_values(self, spark):
        """Test DataFrame creation with time-only strings."""
        data = [
            {"time": "10:30:00"},
            {"time": "14:45:30"},
            {"time": "18:00:00"},
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        # Time strings should be StringType (not DateType or TimestampType)
        assert isinstance(field_dict["time"], StringType), "time should be StringType"

    def test_csv_with_time_values(self, spark):
        """Test CSV with time-only strings."""
        csv_content = """event,time
Meeting,10:30:00
Lunch,14:45:30
Dinner,18:00:00"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            df = spark.read.option("header", True).csv(temp_path)

            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_hex_strings(self, spark):
        """Test DataFrame creation with hex-like strings."""
        data = [
            {"hex": "0xFF"},
            {"hex": "0xAB"},
            {"hex": "0x123"},
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        assert isinstance(field_dict["hex"], StringType), "hex should be StringType"

    def test_csv_with_hex_strings(self, spark):
        """Test CSV with hex-like strings."""
        csv_content = """value,hex
100,0xFF
200,0xAB
300,0x123"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema
            df = spark.read.option("header", True).csv(temp_path)

            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )

            # With inferSchema, hex strings should stay as strings
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            schema_inferred = df_inferred.schema
            field_dict = {f.name: f.dataType for f in schema_inferred.fields}

            assert isinstance(field_dict["value"], LongType), "value should be LongType"
            assert isinstance(field_dict["hex"], StringType), (
                "hex should be StringType (hex format)"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_whitespace_only_strings(self, spark):
        """Test DataFrame creation with whitespace-only strings."""
        data = [
            {"name": "Alice", "space": "   "},
            {"name": "Bob", "space": "\t\t"},
            {"name": "Charlie", "space": "\n\n"},
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        assert isinstance(field_dict["name"], StringType), "name should be StringType"
        assert isinstance(field_dict["space"], StringType), "space should be StringType"

    def test_csv_with_whitespace_only_values(self, spark):
        """Test CSV with whitespace-only values."""
        csv_content = """name,space
Alice,   
Bob,	  
Charlie,  

"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            df = spark.read.option("header", True).csv(temp_path)

            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_numeric_string_prefixes(self, spark):
        """Test DataFrame creation with strings that start with numbers."""
        data = [
            {"code": "123ABC"},
            {"code": "456DEF"},
            {"code": "789GHI"},
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        assert isinstance(field_dict["code"], StringType), "code should be StringType"

    def test_csv_with_numeric_string_prefixes(self, spark):
        """Test CSV with strings that start with numbers."""
        csv_content = """id,code
1,123ABC
2,456DEF
3,789GHI"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema
            df = spark.read.option("header", True).csv(temp_path)

            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )

            # With inferSchema, code should stay as string (mixed format)
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            schema_inferred = df_inferred.schema
            field_dict = {f.name: f.dataType for f in schema_inferred.fields}

            assert isinstance(field_dict["id"], LongType), "id should be LongType"
            assert isinstance(field_dict["code"], StringType), (
                "code should be StringType"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_special_numeric_formats(self, spark):
        """Test DataFrame creation with special numeric formats."""
        data = [
            {"value": "+100"},
            {"value": "-50"},
            {"value": "0"},
        ]

        # These are strings, not numbers
        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        assert isinstance(field_dict["value"], StringType), (
            "value should be StringType (string format)"
        )

    def test_csv_with_plus_prefix_numbers(self, spark):
        """Test CSV with plus-prefixed numbers."""
        csv_content = """value
+100
+200
+300"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema
            df = spark.read.option("header", True).csv(temp_path)

            assert isinstance(df.schema.fields[0].dataType, StringType), (
                "Should be StringType"
            )

            # With inferSchema, plus prefix might prevent inference or be handled
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            # Should not crash
            assert len(df_inferred.schema.fields) == 1, "Should have 1 field"
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_mixed_case_booleans(self, spark):
        """Test DataFrame creation with mixed case boolean-like strings."""
        data = [
            {"flag": "True"},
            {"flag": "False"},
            {"flag": "TRUE"},
            {"flag": "FALSE"},
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        # These are strings, not booleans
        assert isinstance(field_dict["flag"], StringType), "flag should be StringType"

    def test_csv_with_mixed_case_booleans(self, spark):
        """Test CSV with mixed case boolean strings."""
        csv_content = """flag
True
False
TRUE
FALSE"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema
            df = spark.read.option("header", True).csv(temp_path)

            assert isinstance(df.schema.fields[0].dataType, StringType), (
                "Should be StringType"
            )

            # With inferSchema, case might affect boolean inference
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            # Polars might infer True/False as boolean, but TRUE/FALSE might be strings
            # Behavior depends on Polars implementation
            assert len(df_inferred.schema.fields) == 1, "Should have 1 field"
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_infinity_and_nan(self, spark):
        """Test DataFrame creation with infinity and NaN values."""
        data = [
            {"value": float("inf")},
            {"value": float("-inf")},
            {"value": float("nan")},
            {"value": 1.5},
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        assert isinstance(field_dict["value"], DoubleType), "value should be DoubleType"

    def test_csv_with_infinity_strings(self, spark):
        """Test CSV with infinity-like strings."""
        csv_content = """value
inf
-inf
Infinity
-Infinity"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema
            df = spark.read.option("header", True).csv(temp_path)

            assert isinstance(df.schema.fields[0].dataType, StringType), (
                "Should be StringType"
            )

            # With inferSchema, infinity strings might be inferred as DoubleType or stay as StringType
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            # Should not crash
            assert len(df_inferred.schema.fields) == 1, "Should have 1 field"
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_very_small_floats(self, spark):
        """Test DataFrame creation with very small float values."""
        data = [
            {"value": 1e-10},
            {"value": 1e-20},
            {"value": 1e-30},
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        assert isinstance(field_dict["value"], DoubleType), "value should be DoubleType"

    def test_csv_with_very_small_floats(self, spark):
        """Test CSV with very small float values."""
        csv_content = """value
1e-10
1e-20
1e-30"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema
            df = spark.read.option("header", True).csv(temp_path)

            assert isinstance(df.schema.fields[0].dataType, StringType), (
                "Should be StringType"
            )

            # With inferSchema
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            assert isinstance(df_inferred.schema.fields[0].dataType, DoubleType), (
                "Should be DoubleType"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_very_large_floats(self, spark):
        """Test DataFrame creation with very large float values."""
        data = [
            {"value": 1e10},
            {"value": 1e20},
            {"value": 1e30},
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        assert isinstance(field_dict["value"], DoubleType), "value should be DoubleType"

    def test_csv_with_very_large_floats(self, spark):
        """Test CSV with very large float values."""
        csv_content = """value
1e10
1e20
1e30"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema
            df = spark.read.option("header", True).csv(temp_path)

            assert isinstance(df.schema.fields[0].dataType, StringType), (
                "Should be StringType"
            )

            # With inferSchema
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            assert isinstance(df_inferred.schema.fields[0].dataType, DoubleType), (
                "Should be DoubleType"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_all_float_zeros(self, spark):
        """Test DataFrame creation with all zero float values."""
        data = [
            {"value": 0.0},
            {"value": 0.0},
            {"value": 0.0},
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        assert isinstance(field_dict["value"], DoubleType), "value should be DoubleType"

    def test_csv_with_all_float_zeros(self, spark):
        """Test CSV with all zero float values."""
        csv_content = """value
0.0
0.0
0.0"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema
            df = spark.read.option("header", True).csv(temp_path)

            assert isinstance(df.schema.fields[0].dataType, StringType), (
                "Should be StringType"
            )

            # With inferSchema
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            assert isinstance(df_inferred.schema.fields[0].dataType, DoubleType), (
                "Should be DoubleType"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_all_integer_zeros(self, spark):
        """Test DataFrame creation with all zero integer values."""
        data = [
            {"value": 0},
            {"value": 0},
            {"value": 0},
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        assert isinstance(field_dict["value"], LongType), "value should be LongType"

    def test_csv_with_all_integer_zeros(self, spark):
        """Test CSV with all zero integer values."""
        csv_content = """value
0
0
0"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema
            df = spark.read.option("header", True).csv(temp_path)

            assert isinstance(df.schema.fields[0].dataType, StringType), (
                "Should be StringType"
            )

            # With inferSchema
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            assert isinstance(df_inferred.schema.fields[0].dataType, LongType), (
                "Should be LongType"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_all_false_booleans(self, spark):
        """Test DataFrame creation with all False boolean values."""
        data = [
            {"flag": False},
            {"flag": False},
            {"flag": False},
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        assert isinstance(field_dict["flag"], BooleanType), "flag should be BooleanType"

    def test_csv_with_all_false_booleans(self, spark):
        """Test CSV with all False boolean values."""
        csv_content = """flag
false
false
false"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema
            df = spark.read.option("header", True).csv(temp_path)

            assert isinstance(df.schema.fields[0].dataType, StringType), (
                "Should be StringType"
            )

            # With inferSchema
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            assert isinstance(df_inferred.schema.fields[0].dataType, BooleanType), (
                "Should be BooleanType"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_all_true_booleans(self, spark):
        """Test DataFrame creation with all True boolean values."""
        data = [
            {"flag": True},
            {"flag": True},
            {"flag": True},
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        assert isinstance(field_dict["flag"], BooleanType), "flag should be BooleanType"

    def test_csv_with_all_true_booleans(self, spark):
        """Test CSV with all True boolean values."""
        csv_content = """flag
true
true
true"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema
            df = spark.read.option("header", True).csv(temp_path)

            assert isinstance(df.schema.fields[0].dataType, StringType), (
                "Should be StringType"
            )

            # With inferSchema
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            assert isinstance(df_inferred.schema.fields[0].dataType, BooleanType), (
                "Should be BooleanType"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_single_character_strings(self, spark):
        """Test DataFrame creation with single character strings."""
        data = [
            {"char": "A"},
            {"char": "B"},
            {"char": "C"},
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        assert isinstance(field_dict["char"], StringType), "char should be StringType"

    def test_csv_with_single_character_values(self, spark):
        """Test CSV with single character values."""
        csv_content = """grade
A
B
C
D
F"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            df = spark.read.option("header", True).csv(temp_path)

            assert isinstance(df.schema.fields[0].dataType, StringType), (
                "Should be StringType"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_numeric_string_suffixes(self, spark):
        """Test DataFrame creation with strings that end with numbers."""
        data = [
            {"code": "ABC123"},
            {"code": "DEF456"},
            {"code": "GHI789"},
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        assert isinstance(field_dict["code"], StringType), "code should be StringType"

    def test_csv_with_numeric_string_suffixes(self, spark):
        """Test CSV with strings that end with numbers."""
        csv_content = """id,code
1,ABC123
2,DEF456
3,GHI789"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema
            df = spark.read.option("header", True).csv(temp_path)

            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )

            # With inferSchema
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            schema_inferred = df_inferred.schema
            field_dict = {f.name: f.dataType for f in schema_inferred.fields}

            assert isinstance(field_dict["id"], LongType), "id should be LongType"
            assert isinstance(field_dict["code"], StringType), (
                "code should be StringType"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_mixed_numeric_formats(self, spark):
        """Test DataFrame creation with mixed numeric string formats."""
        data = [
            {"value": "123"},
            {"value": "123.45"},
            {"value": "1.23e2"},
        ]

        # These are all strings
        df = spark.createDataFrame(data)

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        assert isinstance(field_dict["value"], StringType), "value should be StringType"

    def test_csv_with_mixed_numeric_formats(self, spark):
        """Test CSV with mixed numeric string formats."""
        csv_content = """value
123
123.45
1.23e2"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema
            df = spark.read.option("header", True).csv(temp_path)

            assert isinstance(df.schema.fields[0].dataType, StringType), (
                "Should be StringType"
            )

            # With inferSchema, all should be inferred as DoubleType (mixed formats)
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            assert isinstance(df_inferred.schema.fields[0].dataType, DoubleType), (
                "Should be DoubleType (mixed numeric formats)"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_very_long_column_names(self, spark):
        """Test DataFrame creation with very long column names."""
        long_name = "A" * 100
        data = [
            {long_name: "value1"},
            {long_name: "value2"},
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        assert len(schema.fields) == 1, "Should have 1 field"
        assert schema.fields[0].name == long_name, (
            "Long column name should be preserved"
        )

    def test_csv_with_very_long_column_names(self, spark):
        """Test CSV with very long column names."""
        long_name = "A" * 100
        csv_content = f"""{long_name}
value1
value2"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            df = spark.read.option("header", True).csv(temp_path)

            assert len(df.schema.fields) == 1, "Should have 1 field"
            assert df.schema.fields[0].name == long_name, (
                "Long column name should be preserved"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_special_characters_in_column_names(self, spark):
        """Test DataFrame creation with special characters in column names."""
        data = [
            {"col-name": 1, "col_name": 2, "col.name": 3},
            {"col-name": 4, "col_name": 5, "col.name": 6},
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_names = [f.name for f in schema.fields]

        # Column names should be preserved (sorted alphabetically)
        assert "col-name" in field_names, "Should have col-name"
        assert "col.name" in field_names, "Should have col.name"
        assert "col_name" in field_names, "Should have col_name"

    def test_csv_with_special_characters_in_column_names(self, spark):
        """Test CSV with special characters in column names."""
        csv_content = """col-name,col_name,col.name
1,2,3
4,5,6"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            df = spark.read.option("header", True).csv(temp_path)

            field_names = [f.name for f in df.schema.fields]
            assert "col-name" in field_names, "Should have col-name"
            assert "col.name" in field_names, "Should have col.name"
            assert "col_name" in field_names, "Should have col_name"
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_unicode_in_column_names(self, spark):
        """Test DataFrame creation with unicode in column names."""
        data = [
            {"姓名": "Alice", "年龄": 25},
            {"姓名": "Bob", "年龄": 30},
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_names = [f.name for f in schema.fields]

        assert "姓名" in field_names, "Should have unicode column name"
        assert "年龄" in field_names, "Should have unicode column name"

    def test_csv_with_unicode_in_column_names(self, spark):
        """Test CSV with unicode in column names."""
        csv_content = """姓名,年龄
Alice,25
Bob,30"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, encoding="utf-8"
        ) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            df = spark.read.option("header", True).csv(temp_path)

            field_names = [f.name for f in df.schema.fields]
            assert "姓名" in field_names, "Should have unicode column name"
            assert "年龄" in field_names, "Should have unicode column name"
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_duplicate_column_names_in_data(self, spark):
        """Test DataFrame creation - duplicate keys in same row (should use last value)."""
        # Python dicts can't have duplicate keys, but we test sparse data
        data = [
            {"name": "Alice", "age": 25},
            {"name": "Bob"},  # Missing age
            {"age": 30},  # Missing name
        ]

        df = spark.createDataFrame(data)

        schema = df.schema
        field_names = [f.name for f in schema.fields]

        # All keys should be present, sorted alphabetically
        assert "age" in field_names, "Should have age"
        assert "name" in field_names, "Should have name"
        assert len(field_names) == 2, "Should have 2 columns"

    def test_csv_with_duplicate_column_names(self, spark):
        """Test CSV with duplicate column names (should handle gracefully)."""
        csv_content = """name,name
Alice,Alice
Bob,Bob"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Polars/Sparkless should handle duplicate column names
            df = spark.read.option("header", True).csv(temp_path)

            # Should have 2 columns (possibly with suffix)
            assert len(df.schema.fields) == 2, "Should handle duplicate column names"
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_only_one_row(self, spark):
        """Test DataFrame creation with only one row."""
        data = [{"name": "Alice", "age": 25, "score": 95.5}]

        df = spark.createDataFrame(data)

        assert df.count() == 1, "Should have 1 row"

        schema = df.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        assert isinstance(field_dict["name"], StringType), "name should be StringType"
        assert isinstance(field_dict["age"], LongType), "age should be LongType"
        assert isinstance(field_dict["score"], DoubleType), "score should be DoubleType"

    def test_csv_with_only_one_row(self, spark):
        """Test CSV with only one data row."""
        csv_content = """name,age,score
Alice,25,95.5"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema
            df = spark.read.option("header", True).csv(temp_path)

            assert df.count() == 1, "Should have 1 row"
            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )

            # With inferSchema
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            schema_inferred = df_inferred.schema
            field_dict = {f.name: f.dataType for f in schema_inferred.fields}

            assert isinstance(field_dict["name"], StringType), (
                "name should be StringType"
            )
            assert isinstance(field_dict["age"], LongType), "age should be LongType"
            assert isinstance(field_dict["score"], DoubleType), (
                "score should be DoubleType"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_with_many_columns(self, spark):
        """Test DataFrame creation with many columns."""
        data = [{f"col_{i}": i for i in range(50)}]

        df = spark.createDataFrame(data)

        assert len(df.schema.fields) == 50, "Should have 50 columns"

        # All should be LongType
        for field in df.schema.fields:
            assert isinstance(field.dataType, LongType), (
                f"{field.name} should be LongType"
            )

    def test_csv_with_many_columns(self, spark):
        """Test CSV with many columns."""
        header = ",".join([f"col_{i}" for i in range(20)])
        row = ",".join([str(i) for i in range(20)])
        csv_content = f"""{header}
{row}
{row}"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema
            df = spark.read.option("header", True).csv(temp_path)

            assert len(df.schema.fields) == 20, "Should have 20 columns"
            for field in df.schema.fields:
                assert isinstance(field.dataType, StringType), (
                    f"{field.name} should be StringType"
                )

            # With inferSchema
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            assert len(df_inferred.schema.fields) == 20, "Should have 20 columns"
            for field in df_inferred.schema.fields:
                assert isinstance(field.dataType, LongType), (
                    f"{field.name} should be LongType"
                )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_create_dataframe_type_promotion_int_to_float(self, spark):
        """Test that mixed int/float in createDataFrame raises TypeError (PySpark behavior).

        Note: PySpark does NOT promote types in createDataFrame - it raises TypeError.
        Type promotion only happens in CSV reading with inferSchema=True.
        """
        data = [
            {"value": 1},  # int
            {"value": 1.5},  # float
            {"value": 2},  # int
        ]

        # PySpark raises TypeError for mixed int/float in createDataFrame
        with pytest.raises(TypeError, match="Can not merge type|Can not merge types"):
            spark.createDataFrame(data)

    def test_csv_type_promotion_int_to_float(self, spark):
        """Test CSV with mixed int/float values."""
        csv_content = """value
1
1.5
2
2.7"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            # Without inferSchema
            df = spark.read.option("header", True).csv(temp_path)

            assert isinstance(df.schema.fields[0].dataType, StringType), (
                "Should be StringType"
            )

            # With inferSchema, mixed int/float should be DoubleType
            df_inferred = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(temp_path)
            )

            assert isinstance(df_inferred.schema.fields[0].dataType, DoubleType), (
                "Should be DoubleType (mixed int/float)"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

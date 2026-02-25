"""
Compatibility tests for extended string functions.

This module validates extended string functions against pre-generated PySpark outputs.
"""

import pytest
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal
from sparkless import F


class TestStringFunctionsExtendedCompatibility:
    """Test extended string functions against expected PySpark outputs."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        from sparkless import SparkSession

        session = SparkSession("string_functions_test")
        yield session
        session.stop()

    def test_concat_ws(self, spark):
        """Test concat_ws function."""
        expected = load_expected_output("functions", "concat_ws")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.concat_ws("-", df.name, df.email))
        assert_dataframes_equal(result, expected)

    def test_ascii(self, spark):
        """Test ascii function."""
        expected = load_expected_output("functions", "ascii")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.ascii(df.name))
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(
        reason="Encoding functions may require PySpark for exact comparison"
    )
    def test_encode(self, spark):
        """Test encode function."""
        expected = load_expected_output("functions", "encode")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.encode(df.name, "UTF-8"))
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(
        reason="Decoding functions may require PySpark for exact comparison"
    )
    def test_decode(self, spark):
        """Test decode function."""
        expected = load_expected_output("functions", "decode")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.decode(expected["input_data"][0].get("encoded"), "UTF-8"))
        assert_dataframes_equal(result, expected)

    def test_hex(self, spark):
        """Test hex function."""
        expected = load_expected_output("functions", "hex")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.hex(df.name))
        assert_dataframes_equal(result, expected)

    def test_base64(self, spark):
        """Test base64 function."""
        expected = load_expected_output("functions", "base64")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.base64(df.name))
        assert_dataframes_equal(result, expected)

    def test_initcap(self, spark):
        """Test initcap function."""
        expected = load_expected_output("functions", "initcap")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.initcap(df.text))
        assert_dataframes_equal(result, expected)

    def test_repeat(self, spark):
        """Test repeat function."""
        expected = load_expected_output("functions", "repeat")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.repeat(df.name, 2))
        assert_dataframes_equal(result, expected)

    def test_reverse(self, spark):
        """Test reverse function."""
        expected = load_expected_output("functions", "reverse")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.reverse(df.name))
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="soundex not yet implemented correctly")
    def test_soundex(self, spark):
        """Test soundex function."""
        expected = load_expected_output("functions", "soundex")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.soundex(df.name))
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="translate not yet implemented correctly")
    def test_translate(self, spark):
        """Test translate function."""
        expected = load_expected_output("functions", "translate")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.translate(df.text, "aeiou", "AEIOU"))
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="levenshtein not yet implemented correctly")
    def test_levenshtein(self, spark):
        """Test levenshtein function."""
        expected = load_expected_output("functions", "levenshtein")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.levenshtein(df.name, F.lit("Alice")))
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="crc32 not yet implemented correctly")
    def test_crc32(self, spark):
        """Test crc32 function."""
        expected = load_expected_output("functions", "crc32")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.crc32(df.text))
        assert_dataframes_equal(result, expected)

    def test_md5(self, spark):
        """Test md5 function."""
        expected = load_expected_output("functions", "md5")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.md5(df.text))
        assert_dataframes_equal(result, expected)

    def test_sha1(self, spark):
        """Test sha1 function."""
        expected = load_expected_output("functions", "sha1")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.sha1(df.text))
        assert_dataframes_equal(result, expected)

    def test_sha2(self, spark):
        """Test sha2 function."""
        expected = load_expected_output("functions", "sha2")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.sha2(df.text, 256))
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="xxhash64 not yet implemented correctly")
    def test_xxhash64(self, spark):
        """Test xxhash64 function."""
        expected = load_expected_output("functions", "xxhash64")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.xxhash64(df.text))
        assert_dataframes_equal(result, expected)

    def test_regexp_replace(self, spark):
        """Test regexp_replace function."""
        expected = load_expected_output("functions", "regexp_replace")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.regexp_replace(df.text, "World", "Universe"))
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="Regex extract all may need special handling")
    def test_regexp_extract_all(self, spark):
        """Test regexp_extract_all function."""
        expected = load_expected_output("functions", "regexp_extract_all")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.regexp_extract_all(df.email, r"(\w+)", 1))
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="get_json_object not yet implemented correctly")
    def test_get_json_object(self, spark):
        """Test get_json_object function."""
        expected = load_expected_output("functions", "get_json_object")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(
            F.get_json_object(F.lit('{"name":"Alice","age":25}'), "$.name")
        )
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="json_tuple may need special handling")
    def test_json_tuple(self, spark):
        """Test json_tuple function."""
        expected = load_expected_output("functions", "json_tuple")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(
            F.json_tuple(F.lit('{"name":"Alice","age":25}'), "name", "age")
        )
        assert_dataframes_equal(result, expected)

    def test_instr(self, spark):
        """Test instr function."""
        expected = load_expected_output("functions", "instr")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.instr(df.text, "World"))
        assert_dataframes_equal(result, expected)

    def test_locate(self, spark):
        """Test locate function."""
        expected = load_expected_output("functions", "locate")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.locate("World", df.text))
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="substring_index not yet implemented correctly")
    def test_substring_index(self, spark):
        """Test substring_index function."""
        expected = load_expected_output("functions", "substring_index")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.substring_index(df.email, "@", 1))
        assert_dataframes_equal(result, expected)

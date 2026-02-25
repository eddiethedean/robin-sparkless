"""
Robust tests for issue #189 string/JSON functions.

These tests validate edge cases and common PySpark behaviors. They are designed
to run against both sparkless (mock) and the real PySpark backend.

Set MOCK_SPARK_TEST_BACKEND=pyspark to run with real PySpark.
"""

import pytest

from tests.fixtures.spark_backend import BackendType, get_backend_type
from tests.fixtures.spark_imports import get_spark_imports

imports = get_spark_imports()
F = imports.F


class TestIssue189StringFunctionsRobust:
    def test_translate_edge_cases(self, spark):
        df = spark.createDataFrame(
            [
                {"s": "abc", "t": None},
                {"s": "banana", "t": "aeiou"},
                {"s": "", "t": ""},
            ]
        )

        rows = df.select(
            F.translate("s", "abc", "ABC").alias("m1"),
            # replace_string shorter than matching_string: extra matched chars removed
            F.translate("s", "ab", "Z").alias("m2"),
            # None input stays None
            F.translate("t", "aeiou", "AEIOU").alias("m3"),
            # empty matching string -> no changes
            F.translate("s", "", "X").alias("m4"),
        ).collect()

        assert rows[0]["m1"] == "ABC"
        assert rows[0]["m2"] == "Zc"
        assert rows[0]["m3"] is None
        assert rows[0]["m4"] == "abc"

        assert rows[1]["m1"] == "BAnAnA"  # b -> B, a -> A
        assert rows[1]["m2"] == "ZnZnZ"
        assert rows[1]["m3"] == "AEIOU"
        assert rows[1]["m4"] == "banana"

        assert rows[2]["m1"] == ""
        assert rows[2]["m2"] == ""
        assert rows[2]["m3"] == ""
        assert rows[2]["m4"] == ""

    def test_substring_index_edge_cases(self, spark):
        df = spark.createDataFrame(
            [
                {"s": "a/b/c", "n": None},
                {"s": "no_delim", "n": "x"},
                {"s": "", "n": ""},
            ]
        )

        rows = df.select(
            F.substring_index("s", "/", 2).alias("p2"),
            F.substring_index("s", "/", -1).alias("last"),
            F.substring_index("s", "/", 0).alias("zero"),
            # delimiter not found -> original string
            F.substring_index("s", "|", 1).alias("missing"),
            # empty delimiter -> empty string
            F.substring_index("s", "", 1).alias("empty_delim"),
            # None input -> None
            F.substring_index("n", "/", 1).alias("none_input"),
        ).collect()

        assert rows[0]["p2"] == "a/b"
        assert rows[0]["last"] == "c"
        assert rows[0]["zero"] == ""
        assert rows[0]["missing"] == "a/b/c"
        assert rows[0]["empty_delim"] == ""
        assert rows[0]["none_input"] is None

        assert rows[1]["p2"] == "no_delim"
        assert rows[1]["last"] == "no_delim"
        assert rows[1]["zero"] == ""
        assert rows[1]["missing"] == "no_delim"
        assert rows[1]["empty_delim"] == ""
        assert rows[1]["none_input"] == "x"

        assert rows[2]["p2"] == ""
        assert rows[2]["last"] == ""
        assert rows[2]["zero"] == ""
        assert rows[2]["missing"] == ""
        assert rows[2]["empty_delim"] == ""
        assert rows[2]["none_input"] == ""

    def test_levenshtein_nulls_and_empty_strings(self, spark):
        df = spark.createDataFrame(
            [
                {"a": "kitten", "b": "sitting"},
                {"a": "", "b": ""},
                {"a": "", "b": "a"},
                {"a": None, "b": "a"},
                {"a": "a", "b": None},
            ]
        )

        rows = df.select(F.levenshtein("a", "b").alias("d")).collect()
        assert rows[0]["d"] == 3
        assert rows[1]["d"] == 0
        assert rows[2]["d"] == 1
        assert rows[3]["d"] is None
        assert rows[4]["d"] is None

    def test_soundex_null_and_empty(self, spark):
        df = spark.createDataFrame([{"s": None}, {"s": ""}, {"s": "Robert"}])
        rows = df.select(F.soundex("s").alias("sx")).collect()
        assert rows[0]["sx"] is None
        assert rows[1]["sx"] == ""
        assert rows[2]["sx"] == "R163"

    def test_crc32_known_values_and_null(self, spark):
        df = spark.createDataFrame([{"s": "Hello World"}, {"s": ""}, {"s": None}])
        rows = df.select(F.crc32("s").alias("c")).collect()
        # Known CRC32 for UTF-8 "Hello World"
        assert rows[0]["c"] == 1243066710
        assert rows[1]["c"] == 0
        assert rows[2]["c"] is None

    def test_xxhash64_known_values_and_null(self, spark):
        df = spark.createDataFrame([{"s": "Hello World"}, {"s": None}])
        rows = df.select(F.xxhash64("s").alias("h")).collect()
        assert rows[0]["h"] == 8557436188178888239
        # PySpark returns the seed value (42) for NULL
        assert rows[1]["h"] == 42

    def test_get_json_object_missing_path_and_invalid_json(self, spark):
        df = spark.createDataFrame(
            [
                {"j": '{"a":{"b":[{"c":"X"}]}}'},
                {"j": '{"a":1}'},
                {"j": "not-json"},
                {"j": None},
            ]
        )

        rows = df.select(
            F.get_json_object("j", "$.a").alias("a"),
            F.get_json_object("j", "$.a.b[0].c").alias("nested"),
            F.get_json_object("j", "$.missing").alias("missing"),
        ).collect()

        assert rows[0]["a"] == '{"b":[{"c":"X"}]}'
        assert rows[0]["nested"] == "X"
        assert rows[0]["missing"] is None

        assert rows[1]["a"] == "1"
        assert rows[1]["nested"] is None
        assert rows[1]["missing"] is None

        assert rows[2]["a"] is None
        assert rows[2]["nested"] is None
        assert rows[2]["missing"] is None

        assert rows[3]["a"] is None
        assert rows[3]["nested"] is None
        assert rows[3]["missing"] is None

    def test_json_tuple_missing_fields_and_invalid_json(self, spark):
        df = spark.createDataFrame(
            [
                {"j": '{"name":"Alice","age":25}'},
                {"j": '{"name":"Bob"}'},
                {"j": "not-json"},
                {"j": None},
            ]
        )

        out = df.select(F.json_tuple("j", "name", "age")).collect()
        # json_tuple expands to c0, c1
        assert out[0]["c0"] == "Alice"
        assert out[0]["c1"] == "25"

        assert out[1]["c0"] == "Bob"
        assert out[1]["c1"] is None

        assert out[2]["c0"] is None
        assert out[2]["c1"] is None

        assert out[3]["c0"] is None
        assert out[3]["c1"] is None

    def test_regexp_extract_all_multiple_matches_and_nulls(self, spark):
        df = spark.createDataFrame(
            [
                {"s": "a1 b22 c333"},
                {"s": "no-digits"},
                {"s": None},
            ]
        )

        backend = get_backend_type()
        if backend == BackendType.ROBIN:
            from sparkless.core.exceptions.operation import (
                SparkUnsupportedOperationError,
            )

            with pytest.raises(SparkUnsupportedOperationError):
                df.select(F.regexp_extract_all("s", r"\d+", 0).alias("m")).collect()
            return
        # In PySpark, the regexp argument is treated as a column unless it is a literal.
        # Sparkless expects a Python string. Use backend-appropriate form.
        pattern_digits = F.lit(r"\d+") if backend == BackendType.PYSPARK else r"\d+"
        pattern_group = F.lit(r"(\d+)") if backend == BackendType.PYSPARK else r"(\d+)"

        rows0 = df.select(
            F.regexp_extract_all("s", pattern_digits, 0).alias("m")
        ).collect()
        assert rows0[0]["m"] == ["1", "22", "333"]
        assert rows0[1]["m"] == []
        assert rows0[2]["m"] is None

        rows1 = df.select(
            F.regexp_extract_all("s", pattern_group, 1).alias("g")
        ).collect()
        assert rows1[0]["g"] == ["1", "22", "333"]
        assert rows1[1]["g"] == []
        assert rows1[2]["g"] is None

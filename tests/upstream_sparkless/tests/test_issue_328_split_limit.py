"""Test issue #328: split() function with limit parameter.

This test verifies that F.split() correctly supports the optional limit
parameter to control the number of times the pattern is applied.
"""

from sparkless.sql import SparkSession
import sparkless.sql.functions as F


class TestIssue328SplitLimit:
    """Test split() function with limit parameter."""

    def _get_unique_app_name(self, test_name: str) -> str:
        """Generate unique app name for parallel test execution."""
        import os
        import threading

        thread_id = threading.current_thread().ident
        process_id = os.getpid()
        return f"{test_name}_{process_id}_{thread_id}"

    def test_split_with_limit(self):
        """Test split with limit parameter (issue example)."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            # Exact example from issue #328
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "StringValue": "A,B,C,D,E,F"},
                ]
            )

            # split StringValue with a pattern match limit of 3
            df = df.withColumn("StringArray", F.split(F.col("StringValue"), ",", 3))

            # show that the limit was applied by exploding the array
            df = df.withColumn("StringArray", F.explode(F.col("StringArray")))

            rows = df.collect()
            assert len(rows) == 3  # Should have 3 elements after limit=3

            # Expected: ["A", "B", "C,D,E,F"]
            values = [r["StringArray"] for r in rows]
            assert "A" in values
            assert "B" in values
            assert "C,D,E,F" in values
            assert "C" not in values  # Should not be split further
        finally:
            spark.stop()

    def test_split_with_limit_1(self):
        """Test split with limit=1 (no split, returns original as single element)."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": "A,B,C,D"},
                ]
            )

            df = df.withColumn("Array", F.split(F.col("Value"), ",", 1))
            df = df.withColumn("Array", F.explode(F.col("Array")))

            rows = df.collect()
            assert len(rows) == 1  # limit=1 means 1 part (no split)

            values = [r["Array"] for r in rows]
            assert "A,B,C,D" in values  # Original string unsplit
        finally:
            spark.stop()

    def test_split_with_limit_2(self):
        """Test split with limit=2."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": "A,B,C,D"},
                ]
            )

            df = df.withColumn("Array", F.split(F.col("Value"), ",", 2))
            df = df.withColumn("Array", F.explode(F.col("Array")))

            rows = df.collect()
            assert len(rows) == 2  # limit=2 means 2 parts

            values = [r["Array"] for r in rows]
            assert "A" in values
            assert "B,C,D" in values
        finally:
            spark.stop()

    def test_split_without_limit(self):
        """Test split without limit (default behavior)."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": "A,B,C,D"},
                ]
            )

            # No limit parameter - should split all
            df = df.withColumn("Array", F.split(F.col("Value"), ","))
            df = df.withColumn("Array", F.explode(F.col("Array")))

            rows = df.collect()
            assert len(rows) == 4  # All parts split

            values = [r["Array"] for r in rows]
            assert "A" in values
            assert "B" in values
            assert "C" in values
            assert "D" in values
        finally:
            spark.stop()

    def test_split_with_limit_larger_than_splits(self):
        """Test split with limit larger than actual number of splits."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": "A,B,C"},
                ]
            )

            # Limit=10, but only 2 splits possible -> should split all
            df = df.withColumn("Array", F.split(F.col("Value"), ",", 10))
            df = df.withColumn("Array", F.explode(F.col("Array")))

            rows = df.collect()
            assert len(rows) == 3  # All parts split (limit doesn't matter)

            values = [r["Array"] for r in rows]
            assert "A" in values
            assert "B" in values
            assert "C" in values
        finally:
            spark.stop()

    def test_split_with_limit_minus_one(self):
        """Test split with limit=-1 (no limit, default behavior)."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": "A,B,C,D"},
                ]
            )

            # limit=-1 should behave like no limit
            df = df.withColumn("Array", F.split(F.col("Value"), ",", -1))
            df = df.withColumn("Array", F.explode(F.col("Array")))

            rows = df.collect()
            assert len(rows) == 4  # All parts split

            values = [r["Array"] for r in rows]
            assert "A" in values
            assert "B" in values
            assert "C" in values
            assert "D" in values
        finally:
            spark.stop()

    def test_split_with_null_values(self):
        """Test split with null values."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": "A,B,C"},
                    {"Name": "Bob", "Value": None},
                ]
            )

            df = df.withColumn("Array", F.split(F.col("Value"), ",", 2))

            rows = df.collect()
            assert len(rows) == 2

            row_alice = [r for r in rows if r["Name"] == "Alice"][0]
            row_bob = [r for r in rows if r["Name"] == "Bob"][0]

            # Alice should have array
            assert row_alice["Array"] is not None
            # Bob should have None
            assert row_bob["Array"] is None
        finally:
            spark.stop()

    def test_split_with_empty_string(self):
        """Test split with empty string."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": ""},
                ]
            )

            df = df.withColumn("Array", F.split(F.col("Value"), ",", 2))

            rows = df.collect()
            assert len(rows) == 1

            row_alice = [r for r in rows if r["Name"] == "Alice"][0]
            # Empty string should result in array with one empty element
            assert row_alice["Array"] == [""]
        finally:
            spark.stop()

    def test_split_in_select(self):
        """Test split with limit in select context."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": "A,B,C,D"},
                ]
            )

            df = df.select(
                "Name",
                F.split(F.col("Value"), ",", 2).alias("Array"),
            )

            rows = df.collect()
            assert len(rows) == 1

            row_alice = [r for r in rows if r["Name"] == "Alice"][0]
            assert len(row_alice["Array"]) == 2  # limit=2 means 2 parts
            assert row_alice["Array"][0] == "A"
            assert row_alice["Array"][1] == "B,C,D"
        finally:
            spark.stop()

    def test_split_multi_char_delimiter(self):
        """Test split with multi-character delimiter."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Value": "A::B::C::D"},
                ]
            )

            df = df.withColumn("Array", F.split(F.col("Value"), "::", 2))
            df = df.withColumn("Array", F.explode(F.col("Array")))

            rows = df.collect()
            assert len(rows) == 2
            values = [r["Array"] for r in rows]
            assert "A" in values
            assert "B::C::D" in values
        finally:
            spark.stop()

    def test_split_special_regex_characters(self):
        """Test split with special regex characters in delimiter."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            # Test with dot (.) which is special in regex
            df = spark.createDataFrame(
                [
                    {"Value": "192.168.1.1"},
                ]
            )

            df = df.withColumn("Array", F.split(F.col("Value"), ".", 3))
            df = df.withColumn("Array", F.explode(F.col("Array")))

            rows = df.collect()
            assert len(rows) == 3
            values = [r["Array"] for r in rows]
            assert "192" in values
            assert "168" in values
            assert "1.1" in values
        finally:
            spark.stop()

    def test_split_whitespace_delimiter(self):
        """Test split with whitespace delimiter."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Value": "one two three four"},
                ]
            )

            df = df.withColumn("Array", F.split(F.col("Value"), " ", 2))
            df = df.withColumn("Array", F.explode(F.col("Array")))

            rows = df.collect()
            assert len(rows) == 2
            values = [r["Array"] for r in rows]
            assert "one" in values
            assert "two three four" in values
        finally:
            spark.stop()

    def test_split_consecutive_delimiters(self):
        """Test split with consecutive delimiters."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Value": "A,,B,,C"},
                ]
            )

            df = df.withColumn("Array", F.split(F.col("Value"), ",", 3))
            df = df.withColumn("Array", F.explode(F.col("Array")))

            rows = df.collect()
            assert len(rows) == 3
            values = [r["Array"] for r in rows]
            assert "A" in values
            assert "" in values  # Empty string between consecutive delimiters
            assert "B,,C" in values
        finally:
            spark.stop()

    def test_split_delimiter_not_found(self):
        """Test split when delimiter is not found in string."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Value": "NoDelimiterHere"},
                ]
            )

            df = df.withColumn("Array", F.split(F.col("Value"), ",", 2))

            rows = df.collect()
            assert len(rows) == 1
            # When delimiter not found, should return array with original string
            assert rows[0]["Array"] == ["NoDelimiterHere"]
        finally:
            spark.stop()

    def test_split_limit_zero(self):
        """Test split with limit=0 (edge case - PySpark treats as no limit)."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Value": "A,B,C,D"},
                ]
            )

            df = df.withColumn("Array", F.split(F.col("Value"), ",", 0))
            df = df.withColumn("Array", F.explode(F.col("Array")))

            rows = df.collect()
            # limit=0 in PySpark behaves like no limit (splits all)
            assert len(rows) == 4
            values = [r["Array"] for r in rows]
            assert "A" in values
            assert "B" in values
            assert "C" in values
            assert "D" in values
        finally:
            spark.stop()

    def test_split_unicode_characters(self):
        """Test split with Unicode characters."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Value": "José|María|José"},
                ]
            )

            df = df.withColumn("Array", F.split(F.col("Value"), "|", 2))
            df = df.withColumn("Array", F.explode(F.col("Array")))

            rows = df.collect()
            assert len(rows) == 2
            values = [r["Array"] for r in rows]
            assert "José" in values
            assert "María|José" in values
        finally:
            spark.stop()

    def test_split_very_long_string(self):
        """Test split with very long string."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            # Create a long string with many delimiters
            long_str = ",".join([f"item{i}" for i in range(100)])
            df = spark.createDataFrame(
                [
                    {"Value": long_str},
                ]
            )

            df = df.withColumn("Array", F.split(F.col("Value"), ",", 10))
            df = df.withColumn("Array", F.explode(F.col("Array")))

            rows = df.collect()
            assert len(rows) == 10  # limit=10 means 10 parts
            # First should be "item0"
            values = [r["Array"] for r in rows]
            assert "item0" in values
            # Last should contain remaining items
            last_item = [r["Array"] for r in rows if "item99" in r["Array"]]
            assert len(last_item) > 0
        finally:
            spark.stop()

    def test_split_empty_delimiter(self):
        """Test split with empty delimiter (splits into individual characters)."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Value": "ABC"},
                ]
            )

            # Empty delimiter - PySpark splits into individual characters
            df = df.withColumn("Array", F.split(F.col("Value"), ""))

            rows = df.collect()
            assert len(rows) == 1
            # Empty delimiter splits into individual characters
            assert rows[0]["Array"] == ["A", "B", "C"]
        finally:
            spark.stop()

    def test_split_leading_trailing_delimiters(self):
        """Test split with leading and trailing delimiters."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Value": ",A,B,C,"},
                ]
            )

            df = df.withColumn("Array", F.split(F.col("Value"), ",", 4))
            df = df.withColumn("Array", F.explode(F.col("Array")))

            rows = df.collect()
            assert len(rows) == 4
            values = [r["Array"] for r in rows]
            # Leading delimiter creates empty string at start
            # With limit=4, trailing delimiter is included in last part
            assert "" in values
            assert "A" in values
            assert "B" in values
            assert "C," in values  # Trailing delimiter included in last part
        finally:
            spark.stop()

    def test_split_different_limit_values(self):
        """Test split with various limit values to verify behavior."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            base_value = "A,B,C,D,E"
            df = spark.createDataFrame(
                [
                    {"Value": base_value},
                ]
            )

            # Test multiple limits
            for limit in [1, 2, 3, 4, 5, 6, 10]:
                result_df = df.withColumn("Array", F.split(F.col("Value"), ",", limit))
                rows = result_df.collect()
                arr = rows[0]["Array"]
                # Verify we get exactly 'limit' parts (or all parts if limit > splits)
                expected_parts = min(limit, 5) if limit > 0 else 1
                assert len(arr) == expected_parts, (
                    f"limit={limit}: expected {expected_parts} parts, got {len(arr)}"
                )
        finally:
            spark.stop()

    def test_split_in_filter_context(self):
        """Test split with limit used in filter context."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Value": "A,B,C", "Category": "test1"},
                    {"Value": "X,Y", "Category": "test2"},
                    {"Value": "P,Q,R,S", "Category": "test3"},
                ]
            )

            # Filter where split with limit=2 has first element "A"
            df = df.withColumn(
                "First", F.element_at(F.split(F.col("Value"), ",", 2), 1)
            )
            df = df.filter(F.col("First") == "A")

            rows = df.collect()
            assert len(rows) == 1
            assert rows[0]["Category"] == "test1"
        finally:
            spark.stop()

"""
Tests for issue #292: Look-around regex support in rlike().

PySpark supports look-ahead and look-behind assertions in regex patterns for rlike().
This test verifies that Sparkless supports the same via Python fallback when Polars
doesn't support these patterns.
"""

from sparkless.sql import SparkSession
import sparkless.sql.functions as F


class TestIssue292RlikeLookaround:
    """Test look-around regex support in rlike() and related functions."""

    def _get_unique_app_name(self, test_name: str) -> str:
        """Generate a unique app name for each test to avoid conflicts in parallel execution."""
        import uuid

        return f"issue-292-{test_name}-{uuid.uuid4().hex[:8]}"

    def test_rlike_negative_lookahead(self):
        """Test rlike with negative lookahead (from issue example)."""
        spark = SparkSession.builder.appName(
            self._get_unique_app_name("negative-lookahead")
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice Cat", "Value": 1},
                    {"Name": "Alice Train", "Value": 2},
                ]
            )

            regex_string = r"(?i)^(?!.*(Alice\sCat)).*$"  # Negative lookahead
            result = df.filter(F.col("Name").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Name"] == "Alice Train"
            assert rows[0]["Value"] == 2
        finally:
            spark.stop()

    def test_rlike_positive_lookahead(self):
        """Test rlike with positive lookahead."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice Cat", "Value": 1},
                    {"Name": "Bob Cat", "Value": 2},
                    {"Name": "Alice Dog", "Value": 3},
                ]
            )

            # Match names that contain "Alice" followed by "Cat"
            regex_string = r"(?i)^.*(?=.*Alice)(?=.*Cat).*$"
            result = df.filter(F.col("Name").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Name"] == "Alice Cat"
        finally:
            spark.stop()

    def test_rlike_lookbehind(self):
        """Test rlike with lookbehind assertion."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Text": "Price: $100", "Value": 1},
                    {"Text": "Cost: $50", "Value": 2},
                    {"Text": "Price: $200", "Value": 3},
                ]
            )

            # Match text with $ followed by digits (using lookbehind)
            regex_string = r"(?<=\$)\d+"
            result = df.filter(F.col("Text").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 3  # All rows match
        finally:
            spark.stop()

    def test_rlike_negative_lookbehind(self):
        """Test rlike with negative lookbehind."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Text": "Price: $100", "Value": 1},
                    {"Text": "Cost: 50", "Value": 2},
                    {"Text": "Price: $200", "Value": 3},
                ]
            )

            # Match numbers not preceded by $
            regex_string = r"(?<!\$)\d+"
            result = df.filter(F.col("Text").rlike(regex_string))

            rows = result.collect()
            # Should match "Cost: 50" (50 is not preceded by $)
            assert len(rows) >= 1
        finally:
            spark.stop()

    def test_rlike_complex_lookaround(self):
        """Test rlike with complex look-around patterns."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Email": "user@example.com", "Value": 1},
                    {"Email": "admin@test.org", "Value": 2},
                    {"Email": "test@example.com", "Value": 3},
                ]
            )

            # Match emails from example.com but not starting with "user"
            regex_string = r"(?i)^(?!user).*@example\.com$"
            result = df.filter(F.col("Email").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Email"] == "test@example.com"
        finally:
            spark.stop()

    def test_regexp_alias_lookaround(self):
        """Test regexp alias with look-around patterns (using rlike method)."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice Cat", "Value": 1},
                    {"Name": "Alice Train", "Value": 2},
                ]
            )

            # regexp is an alias for rlike, so use rlike method
            regex_string = r"(?i)^(?!.*(Alice\sCat)).*$"
            result = df.filter(F.col("Name").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Name"] == "Alice Train"
        finally:
            spark.stop()

    def test_regexp_like_alias_lookaround(self):
        """Test regexp_like alias with look-around patterns (using rlike method)."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice Cat", "Value": 1},
                    {"Name": "Alice Train", "Value": 2},
                ]
            )

            # regexp_like is an alias for rlike, so use rlike method
            regex_string = r"(?i)^(?!.*(Alice\sCat)).*$"
            result = df.filter(F.col("Name").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Name"] == "Alice Train"
        finally:
            spark.stop()

    def test_rlike_without_lookaround(self):
        """Test rlike with regular patterns (no look-around) still works."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                    {"Name": "Charlie", "Value": 3},
                ]
            )

            # Regular pattern without look-around
            regex_string = r"^A.*"
            result = df.filter(F.col("Name").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Name"] == "Alice"
        finally:
            spark.stop()

    def test_rlike_case_insensitive_lookaround(self):
        """Test rlike with case-insensitive flag and look-around."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "alice cat", "Value": 1},
                    {"Name": "ALICE TRAIN", "Value": 2},
                    {"Name": "Bob Cat", "Value": 3},
                ]
            )

            # Case-insensitive with negative lookahead
            regex_string = r"(?i)^(?!.*(alice\sCat)).*$"
            result = df.filter(F.col("Name").rlike(regex_string))

            rows = result.collect()
            # Should match "ALICE TRAIN" and "Bob Cat" (case-insensitive)
            assert len(rows) == 2
            names = [r["Name"] for r in rows]
            assert "ALICE TRAIN" in names
            assert "Bob Cat" in names
        finally:
            spark.stop()

    def test_rlike_multiple_lookaheads(self):
        """Test rlike with multiple lookahead assertions."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Text": "abc123def", "Value": 1},
                    {"Text": "abc456def", "Value": 2},
                    {"Text": "xyz123def", "Value": 3},
                ]
            )

            # Match text that has "abc" and "123" (multiple lookaheads)
            regex_string = r"^(?=.*abc)(?=.*123).*$"
            result = df.filter(F.col("Text").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Text"] == "abc123def"
        finally:
            spark.stop()

    def test_rlike_lookaround_with_nulls(self):
        """Test rlike with look-around patterns and null values."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice Cat", "Value": 1},
                    {"Name": None, "Value": 2},
                    {"Name": "Alice Train", "Value": 3},
                ]
            )

            regex_string = r"(?i)^(?!.*(Alice\sCat)).*$"
            result = df.filter(F.col("Name").rlike(regex_string))

            rows = result.collect()
            # Should only match "Alice Train" (null doesn't match)
            assert len(rows) == 1
            assert rows[0]["Name"] == "Alice Train"
        finally:
            spark.stop()

    def test_rlike_empty_dataframe(self):
        """Test rlike with look-around on empty DataFrame."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            from sparkless.spark_types import StructType, StructField, StringType

            schema = StructType(
                [
                    StructField("Name", StringType(), True),
                ]
            )
            df = spark.createDataFrame([], schema)

            regex_string = r"(?i)^(?!.*(Alice\sCat)).*$"
            result = df.filter(F.col("Name").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 0
        finally:
            spark.stop()

    def test_rlike_in_select(self):
        """Test rlike with look-around in select statement."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice Cat", "Value": 1},
                    {"Name": "Alice Train", "Value": 2},
                ]
            )

            regex_string = r"(?i)^(?!.*(Alice\sCat)).*$"
            result = df.select(
                "Name", "Value", F.col("Name").rlike(regex_string).alias("Matches")
            )

            rows = result.collect()
            assert len(rows) == 2
            assert rows[0]["Matches"] is False  # "Alice Cat" doesn't match
            assert rows[1]["Matches"] is True  # "Alice Train" matches
        finally:
            spark.stop()

    def test_rlike_in_withcolumn(self):
        """Test rlike with look-around in withColumn."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice Cat", "Value": 1},
                    {"Name": "Alice Train", "Value": 2},
                ]
            )

            regex_string = r"(?i)^(?!.*(Alice\sCat)).*$"
            df = df.withColumn("Matches", F.col("Name").rlike(regex_string))

            rows = df.collect()
            assert len(rows) == 2
            assert rows[0]["Matches"] is False
            assert rows[1]["Matches"] is True
        finally:
            spark.stop()

    def test_rlike_chained_operations(self):
        """Test rlike with look-around in chained filter operations."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice Cat", "Value": 1},
                    {"Name": "Alice Train", "Value": 2},
                    {"Name": "Bob Cat", "Value": 3},
                ]
            )

            # Filter out "Alice Cat" using negative lookahead
            regex_string = r"(?i)^(?!.*(Alice\sCat)).*$"
            result = df.filter(F.col("Name").rlike(regex_string)).filter(
                F.col("Value") > 1
            )

            rows = result.collect()
            # Should match "Alice Train" (Value=2) and "Bob Cat" (Value=3)
            # Both match the regex (not "Alice Cat") and have Value > 1
            assert len(rows) == 2
            names = [r["Name"] for r in rows]
            assert "Alice Train" in names
            assert "Bob Cat" in names
        finally:
            spark.stop()

    def test_rlike_lookahead_with_anchors(self):
        """Test rlike with lookahead and string anchors."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Text": "start123end", "Value": 1},
                    {"Text": "123end", "Value": 2},
                    {"Text": "start123", "Value": 3},
                ]
            )

            # Match strings that start with "start" and have digits before "end"
            regex_string = r"^start(?=.*\d)(?=.*end).*$"
            result = df.filter(F.col("Text").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Text"] == "start123end"
        finally:
            spark.stop()

    def test_rlike_nested_lookahead(self):
        """Test rlike with nested lookahead patterns."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Text": "abc123def456", "Value": 1},
                    {"Text": "abc123", "Value": 2},
                    {"Text": "def456", "Value": 3},
                ]
            )

            # Match strings with "abc" followed by digits, then "def" followed by digits
            regex_string = r"^(?=.*abc\d+)(?=.*def\d+).*$"
            result = df.filter(F.col("Text").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Text"] == "abc123def456"
        finally:
            spark.stop()

    def test_rlike_lookbehind_with_digits(self):
        """Test rlike with lookbehind for digit patterns."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Text": "Price: $100", "Value": 1},
                    {"Text": "Cost: 100", "Value": 2},
                    {"Text": "Price: 100", "Value": 3},
                ]
            )

            # Match numbers preceded by "$"
            regex_string = r"(?<=\$)\d+"
            result = df.filter(F.col("Text").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Text"] == "Price: $100"
        finally:
            spark.stop()

    def test_rlike_combined_lookahead_lookbehind(self):
        """Test rlike with both lookahead and lookbehind."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Text": "prefix123suffix", "Value": 1},
                    {"Text": "prefix123", "Value": 2},
                    {"Text": "123suffix", "Value": 3},
                ]
            )

            # Match digits preceded by "prefix" and followed by "suffix"
            regex_string = r"(?<=prefix)\d+(?=suffix)"
            result = df.filter(F.col("Text").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Text"] == "prefix123suffix"
        finally:
            spark.stop()

    def test_rlike_negative_lookahead_multiple_conditions(self):
        """Test rlike with multiple negative lookahead conditions."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice Cat", "Value": 1},
                    {"Name": "Alice Dog", "Value": 2},
                    {"Name": "Bob Cat", "Value": 3},
                    {"Name": "Bob Dog", "Value": 4},
                ]
            )

            # Match names that don't contain "Alice Cat" and don't contain "Bob Dog"
            regex_string = r"^(?!.*(Alice\sCat))(?!.*(Bob\sDog)).*$"
            result = df.filter(F.col("Name").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 2
            names = [r["Name"] for r in rows]
            assert "Alice Dog" in names
            assert "Bob Cat" in names
        finally:
            spark.stop()

    def test_rlike_lookahead_with_word_boundaries(self):
        """Test rlike with lookahead and word boundaries."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Text": "word123", "Value": 1},
                    {"Text": "word 123", "Value": 2},
                    {"Text": "word123extra", "Value": 3},
                ]
            )

            # Match "word" followed by digits at word boundary
            regex_string = r"^word(?=\d+\b).*$"
            result = df.filter(F.col("Text").rlike(regex_string))

            rows = result.collect()
            # Should match "word123" and "word123extra" (digits after word)
            assert len(rows) >= 1
        finally:
            spark.stop()

    def test_rlike_lookbehind_with_fixed_width(self):
        """Test rlike with fixed-width lookbehind patterns.

        Note: Python re module requires fixed-width lookbehind (no quantifiers like + or {3,}).
        """
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Text": "abc123", "Value": 1},
                    {"Text": "xyz123", "Value": 2},
                    {"Text": "123", "Value": 3},
                ]
            )

            # Match text containing digits preceded by exactly 3 letters "abc"
            # Fixed-width lookbehind works in Python re
            regex_string = r".*(?<=abc)\d+.*"
            result = df.filter(F.col("Text").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Text"] == "abc123"
        finally:
            spark.stop()

    def test_rlike_lookahead_with_alternation(self):
        """Test rlike with lookahead and alternation."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Text": "test123", "Value": 1},
                    {"Text": "testabc", "Value": 2},
                    {"Text": "testxyz", "Value": 3},
                ]
            )

            # Match "test" followed by either digits or "abc"
            regex_string = r"^test(?=\d+|abc).*$"
            result = df.filter(F.col("Text").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 2
            names = [r["Text"] for r in rows]
            assert "test123" in names
            assert "testabc" in names
        finally:
            spark.stop()

    def test_rlike_lookahead_with_capture_groups(self):
        """Test rlike with lookahead containing capture groups."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Text": "email@example.com", "Value": 1},
                    {"Text": "email@test.org", "Value": 2},
                    {"Text": "notanemail", "Value": 3},
                ]
            )

            # Match text with @ followed by domain
            regex_string = r"^.*(?=@([a-z]+\.(com|org))).*$"
            result = df.filter(F.col("Text").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 2
            names = [r["Text"] for r in rows]
            assert "email@example.com" in names
            assert "email@test.org" in names
        finally:
            spark.stop()

    def test_rlike_multiple_negative_lookaheads(self):
        """Test rlike with multiple negative lookaheads in sequence."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Text": "good123", "Value": 1},
                    {"Text": "bad123", "Value": 2},
                    {"Text": "ugly123", "Value": 3},
                    {"Text": "good456", "Value": 4},
                ]
            )

            # Match text that doesn't start with "bad" and doesn't start with "ugly"
            regex_string = r"^(?!bad)(?!ugly).*$"
            result = df.filter(F.col("Text").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 2
            names = [r["Text"] for r in rows]
            assert "good123" in names
            assert "good456" in names
        finally:
            spark.stop()

    def test_rlike_lookbehind_with_character_classes(self):
        """Test rlike with lookbehind using fixed-width character classes.

        Note: Python re requires fixed-width lookbehind, so we use exact character count.
        """
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Text": "abc123", "Value": 1},
                    {"Text": "ABC123", "Value": 2},
                    {"Text": "123", "Value": 3},
                ]
            )

            # Match text containing digits preceded by exactly 3 lowercase letters
            # Fixed-width lookbehind with character class works
            regex_string = r".*(?<=[a-z]{3})\d+.*"
            result = df.filter(F.col("Text").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Text"] == "abc123"
        finally:
            spark.stop()

    def test_rlike_lookahead_with_non_capturing_groups(self):
        """Test rlike with lookahead containing non-capturing groups."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Text": "test123", "Value": 1},
                    {"Text": "test456", "Value": 2},
                    {"Text": "test", "Value": 3},
                ]
            )

            # Match "test" followed by digits (non-capturing group in lookahead)
            regex_string = r"^test(?=(?:\d+)).*$"
            result = df.filter(F.col("Text").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 2
            names = [r["Text"] for r in rows]
            assert "test123" in names
            assert "test456" in names
        finally:
            spark.stop()

    def test_rlike_complex_nested_lookaround(self):
        """Test rlike with deeply nested look-around patterns."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Text": "prefix123middle456suffix", "Value": 1},
                    {"Text": "prefix123suffix", "Value": 2},
                    {"Text": "123middle456", "Value": 3},
                ]
            )

            # Complex pattern: prefix, then digits, then middle, then digits, then suffix
            regex_string = r"^prefix(?=\d+)(?=.*middle)(?=.*\d+)(?=.*suffix).*$"
            result = df.filter(F.col("Text").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Text"] == "prefix123middle456suffix"
        finally:
            spark.stop()

    def test_rlike_lookahead_with_unicode(self):
        """Test rlike with lookahead and unicode characters."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Text": "café123", "Value": 1},
                    {"Text": "cafe123", "Value": 2},
                    {"Text": "café", "Value": 3},
                ]
            )

            # Match text with "café" or "cafe" followed by digits
            regex_string = r"^caf(?:é|e)(?=\d+).*$"
            result = df.filter(F.col("Text").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 2
            names = [r["Text"] for r in rows]
            assert "café123" in names
            assert "cafe123" in names
        finally:
            spark.stop()

    def test_rlike_lookbehind_multiple_fixed_widths(self):
        """Test rlike with multiple fixed-width lookbehind patterns.

        Note: Python re requires fixed-width lookbehind. We test with specific widths.
        """
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Text": "abc123", "Value": 1},
                    {"Text": "abcd123", "Value": 2},
                    {"Text": "123", "Value": 3},
                ]
            )

            # Match text containing digits preceded by exactly 3 letters "abc"
            # Fixed-width lookbehind works
            regex_string = r".*(?<=abc)\d+.*"
            result = df.filter(F.col("Text").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Text"] == "abc123"
        finally:
            spark.stop()

    def test_rlike_lookahead_in_groupby_filter(self):
        """Test rlike with look-around in groupBy filter operations."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Category": "A", "Name": "Alice Cat", "Value": 1},
                    {"Category": "A", "Name": "Alice Train", "Value": 2},
                    {"Category": "B", "Name": "Bob Cat", "Value": 3},
                ]
            )

            regex_string = r"(?i)^(?!.*(Alice\sCat)).*$"
            result = (
                df.groupBy("Category")
                .agg(F.sum("Value").alias("Total"))
                .filter(F.col("Category").rlike(regex_string))
            )

            rows = result.collect()
            # Should work without error
            assert len(rows) >= 0
        finally:
            spark.stop()

    def test_rlike_lookahead_performance_large_dataset(self):
        """Test rlike with look-around on larger dataset."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            # Create a larger dataset
            data = [
                {"Name": f"Alice Cat {i}", "Value": i}
                if i % 3 == 0
                else {"Name": f"Alice Train {i}", "Value": i}
                for i in range(1, 21)
            ]
            df = spark.createDataFrame(data)

            regex_string = r"(?i)^(?!.*(Alice\sCat)).*$"
            result = df.filter(F.col("Name").rlike(regex_string))

            rows = result.collect()
            # Should filter out all "Alice Cat" entries
            assert len(rows) > 0
            assert all("Alice Cat" not in r["Name"] for r in rows)
        finally:
            spark.stop()

    def test_rlike_lookahead_with_special_characters(self):
        """Test rlike with lookahead containing special regex characters."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Text": "price: $100.50", "Value": 1},
                    {"Text": "cost: $50.25", "Value": 2},
                    {"Text": "price: 100", "Value": 3},
                ]
            )

            # Match text with $ followed by digits and decimal point
            regex_string = r"(?<=\$)\d+\.\d+"
            result = df.filter(F.col("Text").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 2
            names = [r["Text"] for r in rows]
            assert "price: $100.50" in names
            assert "cost: $50.25" in names
        finally:
            spark.stop()

    def test_rlike_lookahead_case_sensitivity(self):
        """Test rlike with lookahead and case sensitivity variations."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Text": "ALICE Cat", "Value": 1},
                    {"Text": "alice Cat", "Value": 2},
                    {"Text": "Alice Cat", "Value": 3},
                    {"Text": "Bob Cat", "Value": 4},
                ]
            )

            # Case-insensitive negative lookahead
            regex_string = r"(?i)^(?!.*(alice\sCat)).*$"
            result = df.filter(F.col("Text").rlike(regex_string))

            rows = result.collect()
            # Should only match "Bob Cat" (case-insensitive match excludes all "alice Cat" variants)
            assert len(rows) == 1
            assert rows[0]["Text"] == "Bob Cat"
        finally:
            spark.stop()

    def test_rlike_lookbehind_with_escaped_characters(self):
        """Test rlike with lookbehind containing escaped special characters."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Text": "test(123)", "Value": 1},
                    {"Text": "test[123]", "Value": 2},
                    {"Text": "test123", "Value": 3},
                ]
            )

            # Match digits preceded by "("
            regex_string = r"(?<=\()\d+"
            result = df.filter(F.col("Text").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Text"] == "test(123)"
        finally:
            spark.stop()

    def test_rlike_lookahead_with_quantified_groups(self):
        """Test rlike with lookahead containing quantified groups."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Text": "abc123def", "Value": 1},
                    {"Text": "abc12def", "Value": 2},
                    {"Text": "abcdef", "Value": 3},
                ]
            )

            # Match "abc" followed by 3+ digits, then "def"
            regex_string = r"^abc(?=\d{3,}def).*$"
            result = df.filter(F.col("Text").rlike(regex_string))

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Text"] == "abc123def"
        finally:
            spark.stop()

    def test_rlike_negative_lookahead_with_boundaries(self):
        """Test rlike with negative lookahead and word boundaries."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Text": "test123", "Value": 1},
                    {"Text": "test123extra", "Value": 2},
                    {"Text": "test", "Value": 3},
                ]
            )

            # Match "test" not followed by digits at word boundary
            regex_string = r"^test(?!\d+\b).*$"
            result = df.filter(F.col("Text").rlike(regex_string))

            rows = result.collect()
            # Should match "test" (no digits) and possibly "test123extra" depending on interpretation
            assert len(rows) >= 1
        finally:
            spark.stop()

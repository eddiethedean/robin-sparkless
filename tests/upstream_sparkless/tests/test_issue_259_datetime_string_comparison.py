"""Tests for issue #259: datetime/date/Timestamp vs string comparisons.

Issue #259 reports that comparing datetime.date, datetime.datetime, or Timestamp
columns against string columns raises a TypeError in Sparkless, while PySpark
supports these cross-type comparisons.
"""

from datetime import date, datetime
from typing import Iterable, List, Tuple

import pytest

from sparkless import SparkSession, functions as F


class TestIssue259DatetimeStringComparison:
    """Regression tests for datetime/date/Timestamp vs string comparisons."""

    def test_date_column_vs_string_column_filter(self) -> None:
        """Exact repro from GitHub issue #259 using date vs string columns."""
        spark = SparkSession("Example")
        try:
            df = spark.createDataFrame(
                [
                    {
                        "Name": "Alice",
                        "date_timestamp": date(2026, 1, 1),
                        "date_string": "2025-06-15",
                    },
                    {
                        "Name": "Bob",
                        "date_timestamp": date(2026, 1, 1),
                        "date_string": "2026-01-30",
                    },
                    {
                        "Name": "Charlie",
                        "date_timestamp": date(2026, 1, 1),
                        "date_string": "2026-01-01",
                    },
                ]
            )

            # Compare date column to string column; should not raise TypeError
            result = df.filter(F.col("date_timestamp") > F.col("date_string")).collect()

            # PySpark expected:
            # +-----+-----------+--------------+
            # | Name|date_string|date_timestamp|
            # +-----+-----------+--------------+
            # |Alice| 2025-06-15|    2026-01-01|
            # +-----+-----------+--------------+
            assert len(result) == 1
            assert result[0]["Name"] == "Alice"
            assert result[0]["date_timestamp"] == date(2026, 1, 1)
            assert result[0]["date_string"] == "2025-06-15"
        finally:
            spark.stop()

    @pytest.mark.parametrize(  # type: ignore[misc,untyped-decorator]
        "op,expected_names",
        [
            (">", {"Alice"}),
            (">=", {"Alice", "Charlie"}),
            ("<", {"Bob"}),
            ("<=", {"Bob", "Charlie"}),
            ("==", {"Charlie"}),
            ("!=", {"Alice", "Bob"}),
        ],
    )
    def test_date_column_vs_string_column_all_operators(
        self, op: str, expected_names: Iterable[str]
    ) -> None:
        """Verify all comparison operators match PySpark semantics for date vs string columns."""
        spark = SparkSession("Example")
        try:
            df = spark.createDataFrame(
                [
                    {
                        "Name": "Alice",
                        "d": date(2026, 1, 1),
                        "d_str": "2025-06-15",
                    },
                    {
                        "Name": "Bob",
                        "d": date(2026, 1, 1),
                        "d_str": "2026-01-30",
                    },
                    {
                        "Name": "Charlie",
                        "d": date(2026, 1, 1),
                        "d_str": "2026-01-01",
                    },
                ]
            )

            # Build comparison; handle each operator explicitly to match PySpark semantics.
            if op == ">":
                filtered = df.filter(F.col("d") > F.col("d_str"))
            elif op == ">=":
                filtered = df.filter(F.col("d") >= F.col("d_str"))
            elif op == "<":
                filtered = df.filter(F.col("d") < F.col("d_str"))
            elif op == "<=":
                filtered = df.filter(F.col("d") <= F.col("d_str"))
            elif op == "==":
                filtered = df.filter(F.col("d") == F.col("d_str"))
            elif op == "!=":
                filtered = df.filter(F.col("d") != F.col("d_str"))
            else:  # pragma: no cover - defensive
                pytest.fail(f"Unsupported operator {op}")

            names = {row["Name"] for row in filtered.collect()}
            assert names == set(expected_names)
        finally:
            spark.stop()

    def test_datetime_column_vs_string_column_filter(self) -> None:
        """Test datetime.datetime column compared to ISO datetime string column."""
        spark = SparkSession("Example")
        try:
            df = spark.createDataFrame(
                [
                    {
                        "Name": "Early",
                        "ts": datetime(2025, 1, 1, 12, 0, 0),
                        "ts_str": "2024-12-31 23:59:59",
                    },
                    {
                        "Name": "Equal",
                        "ts": datetime(2025, 1, 1, 12, 0, 0),
                        "ts_str": "2025-01-01 12:00:00",
                    },
                    {
                        "Name": "Later",
                        "ts": datetime(2025, 1, 2, 0, 0, 0),
                        "ts_str": "2025-01-03 00:00:00",
                    },
                ]
            )

            # Keep rows where ts > ts_str
            result = df.filter(F.col("ts") > F.col("ts_str")).collect()
            names = {row["Name"] for row in result}

            # Only "Early" should satisfy ts > ts_str
            assert names == {"Early"}
        finally:
            spark.stop()

    def test_datetime_column_vs_string_column_all_operators(self) -> None:
        """Verify all comparison operators for datetime vs string columns behave like PySpark."""
        spark = SparkSession("Example")
        try:
            base = datetime(2025, 1, 1, 12, 0, 0)
            df = spark.createDataFrame(
                [
                    {"Name": "Less", "ts": base, "ts_str": "2025-01-02 00:00:00"},
                    {"Name": "Equal", "ts": base, "ts_str": "2025-01-01 12:00:00"},
                    {"Name": "Greater", "ts": base, "ts_str": "2024-12-31 23:59:59"},
                ]
            )

            cases: List[Tuple[str, Iterable[str]]] = [
                (">", {"Greater"}),
                (">=", {"Greater", "Equal"}),
                ("<", {"Less"}),
                ("<=", {"Less", "Equal"}),
                ("==", {"Equal"}),
                ("!=", {"Less", "Greater"}),
            ]

            for op, expected in cases:
                if op == ">":
                    filtered = df.filter(F.col("ts") > F.col("ts_str"))
                elif op == ">=":
                    filtered = df.filter(F.col("ts") >= F.col("ts_str"))
                elif op == "<":
                    filtered = df.filter(F.col("ts") < F.col("ts_str"))
                elif op == "<=":
                    filtered = df.filter(F.col("ts") <= F.col("ts_str"))
                elif op == "==":
                    filtered = df.filter(F.col("ts") == F.col("ts_str"))
                elif op == "!=":
                    filtered = df.filter(F.col("ts") != F.col("ts_str"))
                else:  # pragma: no cover - defensive
                    pytest.fail(f"Unsupported operator {op}")

                names = {row["Name"] for row in filtered.collect()}
                assert names == set(expected), f"Operator {op} mismatch"
        finally:
            spark.stop()

    def test_date_column_vs_string_literal(self) -> None:
        """Test comparisons between date column and string literal thresholds."""
        spark = SparkSession("Example")
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "A", "d": date(2025, 6, 14)},
                    {"Name": "B", "d": date(2025, 6, 15)},
                    {"Name": "C", "d": date(2025, 6, 16)},
                ]
            )

            # Use string literal on the right-hand side
            result_gt = df.filter(F.col("d") > "2025-06-15").collect()
            names_gt = {row["Name"] for row in result_gt}
            assert names_gt == {"C"}

            # Use string literal on the left-hand side
            result_lt = df.filter(F.col("d") < "2025-06-15").collect()
            names_lt = {row["Name"] for row in result_lt}
            assert names_lt == {"A"}
        finally:
            spark.stop()

    def test_string_column_vs_date_literal_all_operators(self) -> None:
        """Ensure string column compared to date literal behaves like PySpark (coercion on string side)."""
        spark = SparkSession("Example")
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "A", "d_str": "2025-06-14"},
                    {"Name": "B", "d_str": "2025-06-15"},
                    {"Name": "C", "d_str": "2025-06-16"},
                ]
            )

            date_lit = date(2025, 6, 15)

            cases: List[Tuple[str, Iterable[str]]] = [
                (">", {"C"}),
                (">=", {"B", "C"}),
                ("<", {"A"}),
                ("<=", {"A", "B"}),
                ("==", {"B"}),
                ("!=", {"A", "C"}),
            ]

            for op, expected in cases:
                if op == ">":
                    filtered = df.filter(F.col("d_str") > date_lit)
                elif op == ">=":
                    filtered = df.filter(F.col("d_str") >= date_lit)
                elif op == "<":
                    filtered = df.filter(F.col("d_str") < date_lit)
                elif op == "<=":
                    filtered = df.filter(F.col("d_str") <= date_lit)
                elif op == "==":
                    filtered = df.filter(F.col("d_str") == date_lit)
                elif op == "!=":
                    filtered = df.filter(F.col("d_str") != date_lit)
                else:  # pragma: no cover - defensive
                    pytest.fail(f"Unsupported operator {op}")

                names = {row["Name"] for row in filtered.collect()}
                assert names == set(expected), f"Operator {op} mismatch"
        finally:
            spark.stop()

    def test_invalid_or_null_date_strings_do_not_raise(self) -> None:
        """Invalid or null date strings should not raise and should behave like null comparisons."""
        spark = SparkSession("Example")
        try:
            df = spark.createDataFrame(
                [
                    {
                        "Name": "Valid",
                        "d": date(2025, 6, 16),
                        "d_str": "2025-06-15",
                    },
                    {
                        "Name": "Invalid",
                        "d": date(2025, 6, 16),
                        "d_str": "not-a-date",
                    },
                    {
                        "Name": "NullString",
                        "d": date(2025, 6, 16),
                        "d_str": None,
                    },
                ]
            )

            # Filter where date > string; invalid/None strings should be treated as null comparisons
            result = df.filter(F.col("d") > F.col("d_str")).collect()
            names = {row["Name"] for row in result}

            # Only the valid parseable date string should participate in comparison
            assert names == {"Valid"}
        finally:
            spark.stop()

    def test_string_values_that_look_like_dates_but_both_sides_string(self) -> None:
        """When both sides are strings, comparisons should remain lexicographic (no coercion)."""
        spark = SparkSession("Example")
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "A", "left": "2024-01-10", "right": "2024-01-2"},
                    {"Name": "B", "left": "2024-01-2", "right": "2024-01-10"},
                ]
            )

            # No date objects involved here, so string comparison semantics apply
            result = df.filter(F.col("left") > F.col("right")).collect()
            names = {row["Name"] for row in result}

            # In lexicographic comparison, "2024-01-2" > "2024-01-10"
            assert names == {"B"}
        finally:
            spark.stop()

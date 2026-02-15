"""
Tests for #354: Column name attribute (PySpark parity).

PySpark's Column has a .name attribute for simple column references (e.g. F.col("x").name → "x").
"""

from __future__ import annotations

import robin_sparkless as rs


def test_column_name_simple_reference() -> None:
    """col("salary").name returns "salary" (PySpark parity)."""
    c = rs.col("salary")
    assert c.name == "salary"


def test_column_name_other_names() -> None:
    """col("x").name and col("other_col").name return the given name."""
    assert rs.col("x").name == "x"
    assert rs.col("other_col").name == "other_col"


def test_column_name_aliased() -> None:
    """Aliased column .name returns the alias."""
    c = rs.col("salary").alias("sal")
    assert c.name == "sal"

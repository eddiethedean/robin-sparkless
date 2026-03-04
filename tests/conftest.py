"""
Shared test helpers for backend detection across test packages.

This mirrors the backend-selection used in tests/python/conftest.py so that
upstream-style helpers like is_pyspark_backend() continue to work.
"""

from __future__ import annotations

import os


def is_pyspark_backend() -> bool:
    """True when tests should run with real PySpark (no sparkless)."""
    backend = (
        os.getenv("MOCK_SPARK_TEST_BACKEND")
        or os.getenv("SPARKLESS_TEST_BACKEND")
        or ""
    )
    return backend.strip().lower() == "pyspark"


"""
Test fixtures and utilities for unified PySpark and mock-spark testing.

This module provides infrastructure for running tests with either PySpark or mock-spark,
enabling direct comparison testing and baseline validation.
"""

from .spark_backend import (
    SparkBackend,
    get_backend_from_env,
    get_backend_from_marker,
    create_spark_session,
)
from .spark_imports import get_spark_imports, SparkImports

__all__ = [
    "SparkBackend",
    "get_backend_from_env",
    "get_backend_from_marker",
    "create_spark_session",
    "get_spark_imports",
    "SparkImports",
]

"""Exception aliases for PySpark compatibility.

These names are imported throughout the codebase and in tests. They are all
aliases for a single base exception type exposed by the native extension
(`sparkless._native.SparklessError`). When the native module is unavailable
(e.g. in some tooling contexts), they fall back to `RuntimeError`.
"""

from typing import Type

try:
    # Native exception provided by the Rust extension.
    from sparkless._native import SparklessError as SparklessError
except ImportError:  # pragma: no cover - exercised implicitly in type-checking tools
    # Fallback used in environments without the native extension.
    SparklessError = RuntimeError  # type: ignore[misc,assignment]


AnalysisException: Type[BaseException] = SparklessError
PySparkValueError: Type[BaseException] = SparklessError
PySparkTypeError: Type[BaseException] = SparklessError
PySparkRuntimeError: Type[BaseException] = SparklessError
IllegalArgumentException: Type[BaseException] = SparklessError

__all__ = [
    "AnalysisException",
    "PySparkValueError",
    "PySparkTypeError",
    "PySparkRuntimeError",
    "IllegalArgumentException",
]

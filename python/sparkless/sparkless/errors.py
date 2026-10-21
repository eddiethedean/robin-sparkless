"""Exception aliases for PySpark compatibility.

These names are imported throughout the codebase and in tests. They expose
native exception classes from `sparkless._native`. When the native module is
unavailable (e.g. in some tooling contexts), they fall back to `RuntimeError`.
"""

# mypy: disable-error-code=no-redef

from typing import Type

try:
    # Native exception provided by the Rust extension.
    from sparkless._native import SparklessError as _SparklessError
    from sparkless._native import SparklessCancelledError as _SparklessCancelledError

    SparklessError = _SparklessError
    SparklessCancelledError = _SparklessCancelledError
    try:
        from sparkless._native import (
            SparklessExternalCatalogError as _SparklessExternalCatalogError,
        )

        SparklessExternalCatalogError = _SparklessExternalCatalogError
    except ImportError:
        # Older native builds do not expose the specialized exception.
        SparklessExternalCatalogError = SparklessError  # type: ignore[misc, assignment]
except ImportError:  # pragma: no cover - exercised implicitly in type-checking tools
    # Fallback used in environments without the native extension.
    SparklessError: Type[BaseException] = RuntimeError
    SparklessCancelledError: Type[BaseException] = SparklessError
    SparklessExternalCatalogError: Type[BaseException] = SparklessError


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
    "SparklessCancelledError",
    "SparklessExternalCatalogError",
]

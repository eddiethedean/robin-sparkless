# mypy: disable-error-code=import-untyped
try:
    from sparkless.errors import (
        AnalysisException,
        IllegalArgumentException,
        PySparkRuntimeError,
        PySparkTypeError,
        PySparkValueError,
    )
except ImportError:
    try:
        from sparkless._native import SparklessError
    except ImportError:
        SparklessError = RuntimeError
    AnalysisException = SparklessError
    IllegalArgumentException = SparklessError
    PySparkRuntimeError = SparklessError
    PySparkTypeError = SparklessError
    PySparkValueError = SparklessError

__all__ = [
    "AnalysisException",
    "IllegalArgumentException",
    "PySparkValueError",
    "PySparkTypeError",
    "PySparkRuntimeError",
]

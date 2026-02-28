# Upstream exception names for test compatibility.
from typing import Type

SparklessError: Type[BaseException]
try:
    from sparkless._native import SparklessError as _SE

    SparklessError = _SE
except ImportError:
    SparklessError = RuntimeError

AnalysisException = SparklessError
PySparkValueError = SparklessError
PySparkTypeError = SparklessError
PySparkRuntimeError = SparklessError
IllegalArgumentException = SparklessError

__all__ = [
    "AnalysisException",
    "PySparkValueError",
    "PySparkTypeError",
    "PySparkRuntimeError",
    "IllegalArgumentException",
]

# Upstream exception names for test compatibility.
try:
    from sparkless._native import SparklessError
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

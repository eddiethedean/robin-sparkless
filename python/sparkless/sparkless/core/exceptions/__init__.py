from typing import Type

AnalysisException: Type[BaseException]
IllegalArgumentException: Type[BaseException]
PySparkRuntimeError: Type[BaseException]
PySparkTypeError: Type[BaseException]
PySparkValueError: Type[BaseException]

try:
    from sparkless.errors import (
        AnalysisException as _AE,
        IllegalArgumentException as _IAE,
        PySparkRuntimeError as _PSRE,
        PySparkTypeError as _PSTE,
        PySparkValueError as _PSVE,
    )
    AnalysisException = _AE
    IllegalArgumentException = _IAE
    PySparkRuntimeError = _PSRE
    PySparkTypeError = _PSTE
    PySparkValueError = _PSVE
except ImportError:
    try:
        from sparkless._native import SparklessError
    except ImportError:
        SparklessError = RuntimeError  # type: ignore[assignment,misc]
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

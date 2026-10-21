AnalysisException: type[BaseException]
IllegalArgumentException: type[BaseException]
PySparkRuntimeError: type[BaseException]
PySparkTypeError: type[BaseException]
PySparkValueError: type[BaseException]

SparklessError: type[BaseException]
SparklessCancelledError: type[BaseException]

try:
    from sparkless.errors import (
        AnalysisException as _AE,
        IllegalArgumentException as _IAE,
        PySparkRuntimeError as _PSRE,
        PySparkTypeError as _PSTE,
        PySparkValueError as _PSVE,
        SparklessCancelledError as _SCE,
    )

    AnalysisException = _AE
    IllegalArgumentException = _IAE
    PySparkRuntimeError = _PSRE
    PySparkTypeError = _PSTE
    PySparkValueError = _PSVE
except ImportError:
    try:
        from sparkless._native import SparklessCancelledError as _SCE
        from sparkless._native import SparklessError as _SparklessError

        SparklessError = _SparklessError
        SparklessCancelledError = _SCE
    except ImportError:
        SparklessError = RuntimeError
        SparklessCancelledError = SparklessError
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
    "SparklessCancelledError",
]

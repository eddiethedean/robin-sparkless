AnalysisException: type[BaseException]
IllegalArgumentException: type[BaseException]
PySparkRuntimeError: type[BaseException]
PySparkTypeError: type[BaseException]
PySparkValueError: type[BaseException]

SparklessError: type[BaseException]
SparklessCancelledError: type[BaseException]
SparklessExternalCatalogError: type[BaseException]

try:
    from sparkless.errors import (
        AnalysisException as _AE,
        IllegalArgumentException as _IAE,
        PySparkRuntimeError as _PSRE,
        PySparkTypeError as _PSTE,
        PySparkValueError as _PSVE,
        SparklessCancelledError as _SCE,
        SparklessExternalCatalogError as _SECE,
    )

    AnalysisException = _AE
    IllegalArgumentException = _IAE
    PySparkRuntimeError = _PSRE
    PySparkTypeError = _PSTE
    PySparkValueError = _PSVE
    SparklessCancelledError = _SCE
    SparklessExternalCatalogError = _SECE
except ImportError:
    try:
        from sparkless._native import SparklessCancelledError as _SCE
        from sparkless._native import SparklessError as _SparklessError
        from sparkless._native import SparklessExternalCatalogError as _SECE

        SparklessError = _SparklessError
        SparklessCancelledError = _SCE
        SparklessExternalCatalogError = _SECE
    except ImportError:
        SparklessError = RuntimeError
        SparklessCancelledError = SparklessError
        SparklessExternalCatalogError = SparklessError
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
    "SparklessExternalCatalogError",
]

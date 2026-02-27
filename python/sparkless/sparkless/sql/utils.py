# PySpark-style: from sparkless.sql.utils import AnalysisException, ParseException, ...
from typing import Type

SparklessError: Type[BaseException]
try:
    from sparkless._native import SparklessError as _SE
    SparklessError = _SE
except ImportError:
    SparklessError = RuntimeError

AnalysisException = SparklessError
ParseException = SparklessError
IllegalArgumentException = SparklessError
QueryExecutionException = SparklessError
SparkUpgradeException = SparklessError
PySparkAttributeError = SparklessError
ConfigurationException = SparklessError
PySparkValueError = SparklessError
PySparkTypeError = SparklessError
PySparkRuntimeError = SparklessError

__all__ = [
    "AnalysisException",
    "ParseException",
    "IllegalArgumentException",
    "QueryExecutionException",
    "SparkUpgradeException",
    "PySparkAttributeError",
    "ConfigurationException",
    "PySparkValueError",
    "PySparkTypeError",
    "PySparkRuntimeError",
]

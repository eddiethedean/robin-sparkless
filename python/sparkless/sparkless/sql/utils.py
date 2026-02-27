# PySpark-style: from sparkless.sql.utils import AnalysisException, ParseException, ...
# mypy: disable-error-code=import-untyped
try:
    from sparkless._native import SparklessError
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

"""Operation-specific exceptions (e.g. column not found) for PySpark parity."""

from sparkless.core.exceptions import AnalysisException

# Alias so tests expecting SparkColumnNotFoundError get a consistent type.
# Engine may raise AnalysisException/SparklessError with "not found" or "cannot resolve".
SparkColumnNotFoundError = AnalysisException

__all__ = ["SparkColumnNotFoundError"]

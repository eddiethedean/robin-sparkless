"""
Unified import abstraction for PySpark and mock-spark.

Provides a single import interface that automatically selects the correct
imports based on backend configuration.
"""

from typing import Any, Optional, Tuple
from .spark_backend import BackendType, get_backend_type


class SparkImports:
    """Container for Spark-related imports based on backend."""

    def __init__(self, backend: Optional[BackendType] = None):
        """Initialize imports based on backend type.

        Args:
            backend: Backend type to use. If None, determined from environment.
        """
        if backend is None:
            backend = get_backend_type()

        self.backend = backend
        self._load_imports()

    def _load_imports(self) -> None:
        """Load imports based on backend type."""
        if self.backend == BackendType.PYSPARK:
            self._load_pyspark_imports()
        else:
            self._load_mock_spark_imports()

    def _load_pyspark_imports(self) -> None:
        """Load PySpark imports."""
        try:
            from pyspark.sql import SparkSession, functions as F
            from pyspark.sql.types import (
                StructType,
                StructField,
                StringType,
                IntegerType,
                LongType,
                DoubleType,
                FloatType,
                BooleanType,
                DateType,
                TimestampType,
                ArrayType,
                MapType,
                DecimalType,
                Row,
            )
            from pyspark.sql import Window
            from pyspark.sql import DataFrameReader

            self.SparkSession = SparkSession
            self.F = F
            self.StructType = StructType
            self.StructField = StructField
            self.StringType = StringType
            self.IntegerType = IntegerType
            self.LongType = LongType
            self.DoubleType = DoubleType
            self.FloatType = FloatType
            self.BooleanType = BooleanType
            self.DateType = DateType
            self.TimestampType = TimestampType
            self.ArrayType = ArrayType
            self.MapType = MapType
            self.DecimalType = DecimalType
            self.Row = Row
            self.Window = Window
            self.DataFrameReader = DataFrameReader

        except ImportError as e:
            raise ImportError(
                "PySpark is not available. Install with: pip install pyspark"
            ) from e

    def _load_mock_spark_imports(self) -> None:
        """Load mock-spark imports."""
        from sparkless.sql import SparkSession, functions as F
        from sparkless.sql.types import (
            StructType,
            StructField,
            StringType,
            IntegerType,
            LongType,
            DoubleType,
            FloatType,
            BooleanType,
            DateType,
            TimestampType,
            ArrayType,
            MapType,
            DecimalType,
            Row,
        )
        from sparkless.window import Window
        from sparkless.dataframe import DataFrameReader

        self.SparkSession = SparkSession
        self.F = F
        self.StructType = StructType
        self.StructField = StructField
        self.StringType = StringType
        self.IntegerType = IntegerType
        self.LongType = LongType
        self.DoubleType = DoubleType
        self.FloatType = FloatType
        self.BooleanType = BooleanType
        self.DateType = DateType
        self.TimestampType = TimestampType
        self.ArrayType = ArrayType
        self.MapType = MapType
        self.DecimalType = DecimalType
        self.Row = Row
        self.Window = Window
        self.DataFrameReader = DataFrameReader


def get_spark_imports(backend: Optional[BackendType] = None) -> SparkImports:
    """Get Spark imports based on backend configuration.

    Args:
        backend: Optional backend type. If None, determined from environment.

    Returns:
        SparkImports instance with appropriate imports.
    """
    return SparkImports(backend=backend)


# Convenience function for direct imports
def get_imports(backend: Optional[BackendType] = None) -> Tuple[Any, Any, Any]:
    """Get SparkSession, F, and StructType as a tuple.

    Args:
        backend: Optional backend type. If None, determined from environment.

    Returns:
        Tuple of (SparkSession, F, StructType).
    """
    imports = get_spark_imports(backend)
    return (imports.SparkSession, imports.F, imports.StructType)

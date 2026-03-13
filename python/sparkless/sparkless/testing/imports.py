"""Unified import abstraction for sparkless and PySpark.

This module provides a SparkImports class that loads the appropriate
Spark-related imports (SparkSession, functions, types) based on the
test mode.
"""

from __future__ import annotations

from typing import Any, Optional, TYPE_CHECKING

from .mode import Mode, get_mode

if TYPE_CHECKING:
    from types import ModuleType


class SparkImports:
    """Container for Spark-related imports based on test mode.

    This class provides a unified interface for accessing SparkSession,
    functions (F), Window, Row, and all data types regardless of whether
    you're using sparkless or PySpark.

    Attributes:
        mode: The Mode this imports container was created for.
        SparkSession: The SparkSession class.
        F: The functions module (also available as `functions`).
        Window: The Window class for window functions.
        Row: The Row class for creating rows.
        StructType, StructField, StringType, IntegerType, LongType,
        DoubleType, FloatType, BooleanType, DateType, TimestampType,
        ArrayType, MapType, DecimalType, BinaryType: Data type classes.

    Example:
        >>> imports = get_imports()
        >>> spark = imports.SparkSession.builder.appName("test").getOrCreate()
        >>> df = spark.createDataFrame([(1, "a")], ["id", "val"])
        >>> df.select(imports.F.upper("val")).show()
    """

    def __init__(self, mode: Optional[Mode] = None):
        """Initialize imports for the given mode.

        Args:
            mode: The test mode. If None, uses get_mode() to determine
                the current mode from the environment.
        """
        if mode is None:
            mode = get_mode()

        self.mode = mode
        self._load_imports()

    def _load_imports(self) -> None:
        """Load imports based on the current mode."""
        if self.mode == Mode.PYSPARK:
            self._load_pyspark_imports()
        else:
            self._load_sparkless_imports()

    def _load_pyspark_imports(self) -> None:
        """Load PySpark imports."""
        try:
            from pyspark.sql import SparkSession
            from pyspark.sql import functions as F
            from pyspark.sql import Window
            from pyspark.sql import DataFrameReader
            from pyspark.sql.types import (
                ArrayType,
                BinaryType,
                BooleanType,
                DateType,
                DecimalType,
                DoubleType,
                FloatType,
                IntegerType,
                LongType,
                MapType,
                Row,
                StringType,
                StructField,
                StructType,
                TimestampType,
            )

            self.SparkSession = SparkSession
            self.F = F
            self.functions = F
            self.Window = Window
            self.DataFrameReader = DataFrameReader
            self.Row = Row

            # Data types
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
            self.BinaryType = BinaryType

            # PySpark has no _native module
            self._native = None

        except ImportError as e:
            raise ImportError(
                "PySpark is not available. Install with: pip install pyspark"
            ) from e

    def _load_sparkless_imports(self) -> None:
        """Load sparkless imports."""
        from sparkless.sql import SparkSession
        from sparkless.sql import functions as F
        from sparkless.sql.window import Window
        from sparkless.dataframe import DataFrameReader
        from sparkless.sql.types import (
            ArrayType,
            BinaryType,
            BooleanType,
            DateType,
            DecimalType,
            DoubleType,
            FloatType,
            IntegerType,
            LongType,
            MapType,
            Row,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        self.SparkSession = SparkSession
        self.F = F
        self.functions = F
        self.Window = Window
        self.DataFrameReader = DataFrameReader
        self.Row = Row

        # Data types
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
        self.BinaryType = BinaryType

        # Sparkless native module
        import sparkless._native as _native
        self._native = _native


def get_imports(mode: Optional[Mode] = None) -> SparkImports:
    """Get Spark imports for the given mode.

    This is the main entry point for getting mode-appropriate imports.

    Args:
        mode: The test mode. If None, uses get_mode() to determine
            the current mode from the environment.

    Returns:
        SparkImports: Container with all Spark-related imports.

    Example:
        >>> imports = get_imports()
        >>> spark = imports.SparkSession.builder.appName("test").getOrCreate()
        >>> F = imports.F
        >>> df = spark.createDataFrame([(1,)], ["id"])
        >>> df.select(F.col("id") + 1).collect()
    """
    return SparkImports(mode=mode)

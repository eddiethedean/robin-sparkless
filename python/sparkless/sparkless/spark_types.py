"""Top-level schema/type aliases mirroring PySpark `pyspark.sql.types`.

This module allows imports such as:

* `from sparkless.spark_types import StructType, StructField`
* `from sparkless import StringType, IntegerType`

All concrete implementations live in `sparkless.sql.types` and are
re-exported here for convenience and compatibility with upstream
Sparkless and PySpark examples.
"""

from sparkless.sql.types import (
    StructType,
    StructField,
    StringType,
    CharType,
    VarcharType,
    IntervalType,
    IntegerType,
    LongType,
    DoubleType,
    FloatType,
    BooleanType,
    BinaryType,
    DateType,
    TimestampType,
    ArrayType,
    MapType,
    DecimalType,
    Row,
    DataType,
)

__all__ = [
    "StructType",
    "StructField",
    "StringType",
    "CharType",
    "VarcharType",
    "IntervalType",
    "IntegerType",
    "LongType",
    "DoubleType",
    "FloatType",
    "BooleanType",
    "BinaryType",
    "DateType",
    "TimestampType",
    "ArrayType",
    "MapType",
    "DecimalType",
    "Row",
    "DataType",
]

# sparkless: PySpark-like DataFrame API in Python, no JVM. Backed by robin-sparkless (Rust/Polars).
__version__ = "4.0.0"

try:
    _mod = __import__("sparkless._native", fromlist=[
        "SparklessError", "PySparkSession", "PySparkSessionBuilder", "PyDataFrame",
        "PyColumn", "PyGroupedData", "PyDataFrameReader", "PyDataFrameWriter",
        "column", "lit_i64", "lit_str", "lit_bool", "lit_f64", "lit_null",
        "upper", "lower", "substring", "trim", "cast", "when",
        "count", "sum", "avg", "min", "max",
    ])
except ImportError:
    _mod = __import__("_native", fromlist=[
        "SparklessError", "PySparkSession", "PySparkSessionBuilder", "PyDataFrame",
        "PyColumn", "PyGroupedData", "PyDataFrameReader", "PyDataFrameWriter",
        "column", "lit_i64", "lit_str", "lit_bool", "lit_f64", "lit_null",
        "upper", "lower", "substring", "trim", "cast", "when",
        "count", "sum", "avg", "min", "max",
    ])

SparklessError = _mod.SparklessError
_SparkSession = _mod.PySparkSession
_SparkSessionBuilder = _mod.PySparkSessionBuilder
_DataFrame = _mod.PyDataFrame
_Column = _mod.PyColumn
_GroupedData = _mod.PyGroupedData
_DataFrameReader = _mod.PyDataFrameReader
_DataFrameWriter = _mod.PyDataFrameWriter
column = _mod.column
col = column  # PySpark alias
lit_i64 = _mod.lit_i64
lit_str = _mod.lit_str
lit_bool = _mod.lit_bool
lit_f64 = _mod.lit_f64
lit_null = _mod.lit_null
upper = _mod.upper
lower = _mod.lower
substring = _mod.substring
trim = _mod.trim
cast = _mod.cast
when = _mod.when
count = _mod.count
sum = _mod.sum
avg = _mod.avg
min = _mod.min
max = _mod.max

# PySpark-style names
SparkSession = _SparkSession
SparkSessionBuilder = _SparkSessionBuilder
DataFrame = _DataFrame
Column = _Column
GroupedData = _GroupedData
DataFrameReader = _DataFrameReader
DataFrameWriter = _DataFrameWriter

# PySpark-style: from sparkless.sql import SparkSession
__all__ = [
    "SparklessError",
    "sql",
    "col",
]


class _SQLModule:
    """Lazy submodule so 'from sparkless.sql import SparkSession' works."""

    @property
    def SparkSession(self):
        return _SparkSession

    @property
    def SparkSessionBuilder(self):
        return _SparkSessionBuilder

    @property
    def DataFrame(self):
        return _DataFrame

    @property
    def Column(self):
        return _Column

    @property
    def GroupedData(self):
        return _GroupedData

    @property
    def DataFrameReader(self):
        return _DataFrameReader

    @property
    def DataFrameWriter(self):
        return _DataFrameWriter

    @property
    def functions(self):
        import sparkless.sql.functions as f
        return f


sql = _SQLModule()

# sparkless: PySpark-like DataFrame API in Python, no JVM. Backed by robin-sparkless (Rust/Polars).
__version__ = "4.0.0"

try:
    _mod = __import__("sparkless._native", fromlist=[
        "SparklessError", "PySparkSession", "PySparkSessionBuilder", "PyDataFrame",
        "PyColumn", "PyGroupedData", "PyDataFrameReader", "PyDataFrameWriter",
        "column", "lit", "lit_i64", "lit_str", "lit_bool", "lit_f64", "lit_null",
        "upper", "lower", "substring", "trim", "cast", "when",
        "count", "sum", "avg", "min", "max",
        "regexp_replace", "regexp_extract", "regexp_extract_all", "regexp_like",
        "split", "coalesce", "format_string", "greatest", "least",
        "array_distinct", "posexplode",
        "to_timestamp", "to_date", "current_date", "datediff", "unix_timestamp", "from_unixtime",
        "year", "month", "dayofmonth", "dayofweek", "date_add", "date_sub", "date_format",
    ])
except ImportError:
    _mod = __import__("_native", fromlist=[
        "SparklessError", "PySparkSession", "PySparkSessionBuilder", "PyDataFrame",
        "PyColumn", "PyGroupedData", "PyDataFrameReader", "PyDataFrameWriter",
        "column", "lit", "lit_i64", "lit_str", "lit_bool", "lit_f64", "lit_null",
        "upper", "lower", "substring", "trim", "cast", "when",
        "count", "sum", "avg", "min", "max",
        "regexp_replace", "regexp_extract", "regexp_extract_all", "regexp_like",
        "split", "coalesce", "format_string", "greatest", "least",
        "array_distinct", "posexplode",
        "to_timestamp", "to_date", "current_date", "datediff", "unix_timestamp", "from_unixtime",
        "year", "month", "dayofmonth", "dayofweek", "date_add", "date_sub", "date_format",
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
lit = _mod.lit
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

regexp_replace = _mod.regexp_replace
regexp_extract = _mod.regexp_extract
regexp_extract_all = _mod.regexp_extract_all
regexp_like = _mod.regexp_like
split = _mod.split
coalesce = _mod.coalesce
format_string = _mod.format_string
printf = format_string  # PySpark alias
greatest = _mod.greatest
least = _mod.least
array_distinct = _mod.array_distinct


def posexplode(col_or_name):
    """Explode array with position. Accepts column name (str) or Column. Returns (pos_col, val_col) or wrapper with .alias(pos_name, val_name) for select."""
    if isinstance(col_or_name, str):
        col_or_name = column(col_or_name)
    pos_col, val_col = _mod.posexplode(col_or_name)

    class _PosexplodeResult:
        def __init__(self, pos, val):
            self._pos = pos
            self._val = val

        def alias(self, pos_name, val_name):
            return (self._pos.alias(pos_name), self._val.alias(val_name))

        def __iter__(self):
            return iter((self._pos, self._val))

    return _PosexplodeResult(pos_col, val_col)


to_timestamp = _mod.to_timestamp
to_date = _mod.to_date
current_date = _mod.current_date
datediff = _mod.datediff
unix_timestamp = _mod.unix_timestamp
from_unixtime = _mod.from_unixtime
year = _mod.year
month = _mod.month
dayofmonth = _mod.dayofmonth
dayofweek = _mod.dayofweek
date_add = _mod.date_add
date_sub = _mod.date_sub
date_format = _mod.date_format

# PySpark-style names
SparkSession = _SparkSession
SparkSessionBuilder = _SparkSessionBuilder
DataFrame = _DataFrame
Column = _Column
GroupedData = _GroupedData
DataFrameReader = _DataFrameReader
DataFrameWriter = _DataFrameWriter

# PySpark-style: from sparkless import F, functions, StringType, ...
def __getattr__(name):
    if name in ("F", "functions"):
        import sparkless.sql.functions as f
        return f
    if name in ("StringType", "StructType", "StructField", "Row", "IntegerType", "LongType", "DoubleType", "FloatType", "BooleanType", "DateType", "TimestampType", "ArrayType", "MapType", "DecimalType", "CharType", "VarcharType", "DataType"):
        from sparkless.sql import types as t
        return getattr(t, name)
    if name == "Window":
        from sparkless.sql.window import Window
        return Window
    if name == "row_number":
        import sparkless.sql.functions as f
        return f.row_number
    if name == "percent_rank":
        import sparkless.sql.functions as f
        return f.percent_rank
    if name == "rank":
        import sparkless.sql.functions as f
        return f.rank
    if name == "dense_rank":
        import sparkless.sql.functions as f
        return f.dense_rank
    if name == "ntile":
        import sparkless.sql.functions as f
        return f.ntile
    if name == "lag":
        import sparkless.sql.functions as f
        return f.lag
    if name == "lead":
        import sparkless.sql.functions as f
        return f.lead
    if name == "first_value":
        import sparkless.sql.functions as f
        return f.first_value
    if name == "last_value":
        import sparkless.sql.functions as f
        return f.last_value
    if name in ("asc", "desc"):
        import sparkless.sql.functions as f
        return getattr(f, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# PySpark-style: from sparkless.sql import SparkSession
__all__ = [
    "SparklessError",
    "sql",
    "col",
    "F",
    "functions",
    "Window",
    "row_number",
    "percent_rank",
    "rank",
    "dense_rank",
    "ntile",
    "lag",
    "lead",
    "first_value",
    "last_value",
]


def create_map(*cols):
    """Top-level create_map for robin_sparkless tests."""
    from sparkless.sql import functions as f

    return f.create_map(*cols)


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

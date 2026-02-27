# sparkless: PySpark-like DataFrame API in Python, no JVM. Backed by robin-sparkless (Rust/Polars).
__version__ = "4.0.0"

try:
    _mod = __import__(
        "sparkless._native",
        fromlist=[
            "SparklessError",
            "PySparkSession",
            "PySparkSessionBuilder",
            "PyDataFrame",
            "PyColumn",
            "PyGroupedData",
            "PyDataFrameReader",
            "PyDataFrameWriter",
            "column",
            "lit",
            "lit_i64",
            "lit_str",
            "lit_bool",
            "lit_f64",
            "lit_null",
            "upper",
            "lower",
            "substring",
            "trim",
            "cast",
            "when",
            "count",
            "sum",
            "avg",
            "min",
            "max",
            "regexp_replace",
            "regexp_extract",
            "regexp_extract_all",
            "regexp_like",
            "split",
            "coalesce",
            "format_string",
            "greatest",
            "least",
            "array_distinct",
            "posexplode",
            "to_timestamp",
            "to_date",
            "current_date",
            "current_timestamp",
            "input_file_name",
            "datediff",
            "unix_timestamp",
            "from_unixtime",
            "year",
            "month",
            "dayofmonth",
            "dayofweek",
            "date_add",
            "date_sub",
            "date_format",
            "length",
            "floor",
            "round",
            "ltrim",
            "rtrim",
            "hour",
            "minute",
            "second",
            "reverse",
            "exp",
            "soundex",
            "repeat",
            "initcap",
            "levenshtein",
            "try_cast",
            "try_add",
            "concat",
            "concat_ws",
            "array",
            "struct_",
            "asinh",
            "atanh",
            "cosh",
            "sinh",
            "last_day",
            "months_between",
            "timestamp_seconds",
            "to_utc_timestamp",
            "approx_count_distinct",
            "date_trunc",
            "first",
            "translate",
            "substring_index",
            "crc32",
            "xxhash64",
            "get_json_object",
            "json_tuple",
            "size",
            "array_contains",
            "explode",
        ],
    )
except ImportError:
    _mod = __import__(
        "_native",
        fromlist=[
            "SparklessError",
            "PySparkSession",
            "PySparkSessionBuilder",
            "PyDataFrame",
            "PyColumn",
            "PyGroupedData",
            "PyDataFrameReader",
            "PyDataFrameWriter",
            "column",
            "lit",
            "lit_i64",
            "lit_str",
            "lit_bool",
            "lit_f64",
            "lit_null",
            "upper",
            "lower",
            "substring",
            "trim",
            "cast",
            "when",
            "count",
            "sum",
            "avg",
            "min",
            "max",
            "regexp_replace",
            "regexp_extract",
            "regexp_extract_all",
            "regexp_like",
            "split",
            "coalesce",
            "format_string",
            "greatest",
            "least",
            "array_distinct",
            "posexplode",
            "to_timestamp",
            "to_date",
            "current_date",
            "current_timestamp",
            "input_file_name",
            "datediff",
            "unix_timestamp",
            "from_unixtime",
            "year",
            "month",
            "dayofmonth",
            "dayofweek",
            "date_add",
            "date_sub",
            "date_format",
            "length",
            "floor",
            "round",
            "ltrim",
            "rtrim",
            "hour",
            "minute",
            "second",
            "reverse",
            "exp",
            "soundex",
            "repeat",
            "initcap",
            "levenshtein",
            "try_cast",
            "try_add",
            "concat",
            "concat_ws",
            "array",
            "struct_",
            "asinh",
            "atanh",
            "cosh",
            "sinh",
            "last_day",
            "months_between",
            "timestamp_seconds",
            "to_utc_timestamp",
            "approx_count_distinct",
            "date_trunc",
            "first",
            "translate",
            "substring_index",
            "crc32",
            "xxhash64",
            "get_json_object",
            "json_tuple",
            "size",
            "array_contains",
            "explode",
        ],
    )

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
_native_when = _mod.when
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
current_timestamp = _mod.current_timestamp
input_file_name = _mod.input_file_name
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
length = _mod.length
floor = _mod.floor
round = _mod.round
ltrim = _mod.ltrim
rtrim = _mod.rtrim
reverse = _mod.reverse
exp = _mod.exp
hour = _mod.hour
minute = _mod.minute
second = _mod.second
soundex = _mod.soundex
repeat = _mod.repeat
initcap = _mod.initcap
levenshtein = _mod.levenshtein
try_cast = _mod.try_cast
try_add = _mod.try_add
concat = _mod.concat
concat_ws = _mod.concat_ws
array = _mod.array
struct = _mod.struct_  # PySpark alias (Rust uses struct_)
asinh = _mod.asinh
atanh = _mod.atanh
cosh = _mod.cosh
sinh = _mod.sinh
last_day = _mod.last_day
months_between = _mod.months_between
timestamp_seconds = _mod.timestamp_seconds
to_utc_timestamp = _mod.to_utc_timestamp
approx_count_distinct = _mod.approx_count_distinct
date_trunc = _mod.date_trunc
first = _mod.first
translate = _mod.translate
substring_index = _mod.substring_index
crc32 = _mod.crc32
xxhash64 = _mod.xxhash64
get_json_object = _mod.get_json_object
json_tuple = _mod.json_tuple
size = _mod.size
array_contains = _mod.array_contains
explode = _mod.explode

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
    if name in (
        "StringType",
        "StructType",
        "StructField",
        "Row",
        "IntegerType",
        "LongType",
        "DoubleType",
        "FloatType",
        "BooleanType",
        "DateType",
        "TimestampType",
        "ArrayType",
        "MapType",
        "DecimalType",
        "CharType",
        "VarcharType",
        "DataType",
    ):
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
    if name == "expr":
        import sparkless.sql.functions as f

        return f.expr
    if name == "lit":
        import sparkless.sql.functions as f

        return f.lit
    if name in ("pow", "power"):
        import sparkless.sql.functions as f

        return getattr(f, name)
    if name == "udf":
        import sparkless.sql.functions as f

        return f.udf
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# PySpark-style: from sparkless.sql import SparkSession
__all__ = [
    "SparklessError",
    "sql",
    "col",
    "lit",
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
    "udf",
]


def when(condition, value=None):
    """PySpark-compatible when(). Accepts Column or str condition, optional value."""
    from sparkless.sql.functions import when as _when

    return _when(condition, value)


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

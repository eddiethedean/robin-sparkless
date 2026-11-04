"""Private strings SQL function implementations."""

from __future__ import annotations

from ._shared import (
    Optional,
    _as_col,
    _col_result,
    _native,
    _native_fn,
    _ni,
    _schema_of_json,
)


def lpad(col_or_name, len, pad):
    return _native.native_lpad(_as_col(col_or_name), len, pad)


def rpad(col_or_name, len, pad):
    return _native.native_rpad(_as_col(col_or_name), len, pad)


def ltrim(col_or_name):
    return _native.native_ltrim(_as_col(col_or_name))


def rtrim(col_or_name):
    return _native.native_rtrim(_as_col(col_or_name))


def initcap(col_or_name):
    return _native.native_initcap(_as_col(col_or_name))


def soundex(col_or_name):
    return _native_fn("soundex")(_as_col(col_or_name))


def length(col_or_name):
    return _native.native_length(_as_col(col_or_name))


def reverse(col_or_name):
    return _native.native_reverse(_as_col(col_or_name))


def translate(col_or_name, matching, replace):
    return _native_fn("translate")(_as_col(col_or_name), matching, replace)


def instr(col_or_name, substr):
    return _native.native_instr(_as_col(col_or_name), substr)


def locate(substr, col_or_name, pos=1):
    return _native.native_locate(substr, _as_col(col_or_name), pos)


def repeat(col_or_name, n):
    return _native_fn("repeat")(_as_col(col_or_name), n)


def regexp_extract(col_or_name, pattern, idx=0):
    return _native_fn("regexp_extract")(_as_col(col_or_name), pattern, idx)


def overlay(col_or_name, replace, pos, length=-1):
    """Replace substring at 1-based position (PySpark overlay)."""
    return _col_result(
        _native.overlay(_as_col(col_or_name), str(replace), int(pos), int(length))
    )


def map_from_arrays(keys, values):
    """Build map from two array columns (PySpark map_from_arrays)."""
    return _col_result(_native.map_from_arrays(_as_col(keys), _as_col(values)))


def from_json(col_or_name, schema=None, options=None):
    """Parse JSON string column into struct (PySpark from_json). schema: DDL string or StructType."""
    _ = options  # accepted for API parity
    schema_str: Optional[str] = None
    if schema is not None:
        if isinstance(schema, str):
            schema_str = schema
        elif hasattr(schema, "json"):
            schema_str = str(schema.json())
        elif hasattr(schema, "simpleString"):
            schema_str = str(schema.simpleString())
        else:
            raise TypeError("from_json schema must be str or StructType-like")
    return _col_result(_native.from_json(_as_col(col_or_name), schema_str))


def parse_json(col_or_name):
    """Parse JSON string column into VARIANT (PySpark 4 parse_json)."""
    return _col_result(_native.parse_json(_as_col(col_or_name)))


def to_json(col_or_name):
    """Serialize struct column to JSON string (PySpark to_json)."""
    return _col_result(_native.to_json(_as_col(col_or_name)))


schema_of_json = _schema_of_json


def decode(col_or_name, charset="UTF-8"):
    """Decode binary hex string to text (PySpark decode)."""
    return _col_result(_native.decode(_as_col(col_or_name), charset))


def encode(col_or_name, charset="UTF-8"):
    """Encode string to hex binary (PySpark encode)."""
    return _col_result(_native.encode(_as_col(col_or_name), charset))


def char(col_or_name):
    """Int code point to single-character string (PySpark char)."""
    return _col_result(_native.char(_as_col(col_or_name)))


def factorial(col_or_name):
    return _col_result(_native.factorial(_as_col(col_or_name)))


def cbrt(col_or_name):
    return _col_result(_native.cbrt(_as_col(col_or_name)))


def hypot(x, y):
    return _col_result(_native.hypot(_as_col(x), _as_col(y)))


window = _ni("window")


__all__ = [
    "cbrt",
    "char",
    "decode",
    "encode",
    "factorial",
    "from_json",
    "hypot",
    "initcap",
    "instr",
    "length",
    "locate",
    "lpad",
    "ltrim",
    "map_from_arrays",
    "overlay",
    "parse_json",
    "regexp_extract",
    "repeat",
    "reverse",
    "rpad",
    "rtrim",
    "schema_of_json",
    "soundex",
    "to_json",
    "translate",
    "window",
]

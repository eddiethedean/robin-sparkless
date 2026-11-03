"""Private numeric SQL function implementations."""

from __future__ import annotations

from ._shared import (
    ColumnOrName,
    Optional,
    Union,
    _ColumnType,
    _as_col,
    _coalesce,
    _col_result,
    _native,
    _native_fn,
    lit_bool,
)


def floor(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("floor")(_as_col(c)))


def ceil(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("ceil")(_as_col(c)))


ceiling = ceil


def abs(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("abs")(_as_col(c)))


def sqrt(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("sqrt")(_as_col(c)))


def log(
    col_or_base: ColumnOrName,
    base_or_col: Optional[Union[ColumnOrName, int, float]] = None,
) -> _ColumnType:
    """Natural log, or log with base. PySpark: log(column) or log(base, column)."""
    if base_or_col is None:
        return _col_result(_native_fn("log")(_as_col(col_or_base)))
    # Two args: PySpark uses log(base, column); accept (base, column) or (column, base)
    if isinstance(base_or_col, (int, float)):
        return _col_result(
            _native_fn("log_with_base")(_as_col(col_or_base), float(base_or_col))
        )
    if isinstance(col_or_base, (int, float)):
        return _col_result(
            _native_fn("log_with_base")(_as_col(base_or_col), float(col_or_base))
        )
    raise TypeError(
        "log(base, column) or log(column, base): one argument must be a numeric base (int/float), the other a Column"
    )


def exp(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("exp")(_as_col(c)))


def pow(col1: ColumnOrName, col2: Union[int, float, _ColumnType]) -> _ColumnType:
    """Power: col1 ** col2. col2 can be int, float, or Column (e.g. F.lit(2))."""
    return _col_result(_native_fn("pow")(_as_col(col1), col2))


power = pow


def round(c: ColumnOrName, scale: int = 0) -> _ColumnType:
    return _col_result(_native_fn("round")(_as_col(c), scale))


def signum(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("signum")(_as_col(c)))


def sin(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("sin")(_as_col(c)))


def cos(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("cos")(_as_col(c)))


def tan(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("tan")(_as_col(c)))


def asin(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("asin")(_as_col(c)))


def acos(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("acos")(_as_col(c)))


def atan(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("atan")(_as_col(c)))


def pmod(dividend: ColumnOrName, divisor: ColumnOrName) -> _ColumnType:
    """Positive modulus (PySpark pmod)."""
    return _col_result(_native_fn("pmod")(_as_col(dividend), _as_col(divisor)))


def atan2(y: ColumnOrName, x: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("atan2")(_as_col(y), _as_col(x)))


def degrees(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("degrees")(_as_col(c)))


def radians(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("radians")(_as_col(c)))


def log2(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("log2")(_as_col(c)))


def log10(c: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("log10")(_as_col(c)))


def asc_nulls_last(c: ColumnOrName) -> _ColumnType:
    """Ascending sort order with nulls last (PySpark functions.asc_nulls_last)."""
    return _as_col(c).asc_nulls_last()


def desc_nulls_first(c: ColumnOrName) -> _ColumnType:
    """Descending sort order with nulls first (PySpark functions.desc_nulls_first)."""
    return _as_col(c).desc_nulls_first()


def desc_nulls_last(c: ColumnOrName) -> _ColumnType:
    """Descending sort order with nulls last (PySpark functions.desc_nulls_last)."""
    return _as_col(c).desc_nulls_last()


def greatest(*cols: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("greatest")(tuple([_as_col(c) for c in cols])))


def least(*cols: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("least")(tuple([_as_col(c) for c in cols])))


def coalesce(*cols: ColumnOrName) -> _ColumnType:
    return _col_result(_native_fn("coalesce")(tuple([_as_col(c) for c in cols])))


def nanvl(col1: ColumnOrName, col2: ColumnOrName) -> _ColumnType:
    """Replace NaN with value. PySpark: F.nanvl(col1, col2)."""
    return _col_result(_native_fn("nanvl")(_as_col(col1), _as_col(col2)))


def isnan(c: ColumnOrName) -> _ColumnType:
    """True where the float value is NaN. PySpark: F.isnan(col).

    PySpark semantics:
    - Numeric columns: only real NaN => True; None/null => False.
    - String columns: \"NaN\" (any case/whitespace) => True; other strings/None => False.
    - Other types: always False.

    The native implementation follows these rules but can still yield nulls in some edge
    paths; coalesce with False to ensure the result is always non-null (matches PySpark).
    """
    base = _native_fn("isnan")(_as_col(c))
    # _coalesce is sparkless.coalesce imported above; lit_bool(False) is a native bool literal.
    return _col_result(_coalesce(base, lit_bool(False)))


def hash(*cols: ColumnOrName) -> _ColumnType:
    """Hash expression (PySpark hash). Murmur3 32-bit over one or more columns."""
    if not cols:
        raise ValueError("hash requires at least one column")
    py_cols = [_as_col(c) for c in cols]
    return _col_result(_native.native_hash(*py_cols))


def xxhash64(c: ColumnOrName) -> _ColumnType:
    return _native.native_xxhash64(_as_col(c))


def md5(c: ColumnOrName) -> _ColumnType:
    return _native_fn("md5")(_as_col(c))


def sha1(c: ColumnOrName) -> _ColumnType:
    return _native_fn("sha1")(_as_col(c))


def sha2(c: ColumnOrName, numBits: int) -> _ColumnType:
    return _native_fn("sha2")(_as_col(c), numBits)


def crc32(c: ColumnOrName) -> _ColumnType:
    return _native.native_crc32(_as_col(c))


def base64(c: ColumnOrName) -> _ColumnType:
    return _native.native_base64(_as_col(c))


def unbase64(c: ColumnOrName) -> _ColumnType:
    return _native.native_unbase64(_as_col(c))


def ascii(c: ColumnOrName) -> _ColumnType:
    return _native.native_ascii(_as_col(c))


def hex(c: ColumnOrName) -> _ColumnType:
    return _native.native_hex(_as_col(c))


def unhex(c: ColumnOrName) -> _ColumnType:
    return _native_fn("unhex")(_as_col(c))


def bin(c: ColumnOrName) -> _ColumnType:
    return _native_fn("bin")(_as_col(c))


def conv(c: ColumnOrName, fromBase: int, toBase: int) -> _ColumnType:
    return _native_fn("conv")(_as_col(c), fromBase, toBase)


def format_number(c: ColumnOrName, d: int) -> _ColumnType:
    return _native_fn("format_number")(_as_col(c), d)


__all__ = [
    "abs",
    "acos",
    "asc_nulls_last",
    "ascii",
    "asin",
    "atan",
    "atan2",
    "base64",
    "bin",
    "ceil",
    "ceiling",
    "coalesce",
    "conv",
    "cos",
    "crc32",
    "degrees",
    "desc_nulls_first",
    "desc_nulls_last",
    "exp",
    "floor",
    "format_number",
    "greatest",
    "hash",
    "hex",
    "isnan",
    "least",
    "log",
    "log10",
    "log2",
    "md5",
    "nanvl",
    "pmod",
    "pow",
    "power",
    "radians",
    "round",
    "sha1",
    "sha2",
    "signum",
    "sin",
    "sqrt",
    "tan",
    "unbase64",
    "unhex",
    "xxhash64",
]

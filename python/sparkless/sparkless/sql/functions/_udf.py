"""Private udf SQL function implementations."""

from __future__ import annotations

from ._shared import (
    _DEFAULT_UDF_RETURN_TYPE,
    PySparkValueError,
    _PYTHON_UDF_REGISTRY,
    _ensure_udf_executor_registered,
    tcast,
)

from ._session import _active_session


def _python_udf_executor(df, column_name, udf_name, arg_names, arg_literal_jsons=None):
    """Run a registered Python UDF over the DataFrame: collect, apply per row, create new DataFrame. Called from Rust with_column.
    arg_literal_jsons: optional list of Optional[str]; if arg_literal_jsons[i] is not None, use json.loads(...) as the arg value (literal column)."""
    import json as _json

    import sparkless.sql.types as T

    if udf_name not in _PYTHON_UDF_REGISTRY:
        raise PySparkValueError(f"Unknown UDF name: {udf_name}")
    func, return_type = _PYTHON_UDF_REGISTRY[udf_name]
    rt = tcast("T.DataType", return_type)
    session = _active_session()
    rows = df.collect()
    if not rows:
        # Empty DataFrame: replace or append column (same as non-empty path)
        existing_names = [f.name for f in df.schema.fields]
        if column_name in existing_names:
            extended_fields = [
                T.StructField(
                    f.name,
                    rt if f.name == column_name else f.dataType,
                    f.nullable if f.name != column_name else True,
                )
                for f in df.schema.fields
            ]
        else:
            extended_fields = list(df.schema.fields) + [
                T.StructField(column_name, rt, True)
            ]
        extended_schema = T.StructType(extended_fields)
        return session.createDataFrame([], schema=extended_schema)
    field_names = [f.name for f in df.schema.fields]
    # Build dicts from schema + row values. Row.__iter__ yields keys (_fields), so use positional indexing.
    row_dicts = [
        {field_names[i]: row[i] for i in range(len(field_names))} for row in rows
    ]
    arg_names_list = list(arg_names)
    literals = (
        arg_literal_jsons
        if arg_literal_jsons is not None
        else [None] * len(arg_names_list)
    )
    if len(literals) < len(arg_names_list):
        literals = list(literals) + [None] * (len(arg_names_list) - len(literals))

    def arg_value(d, i):
        if literals[i] is not None:
            return _json.loads(literals[i])
        return d[arg_names_list[i]]

    new_vals = [
        func(*[arg_value(d, i) for i in range(len(arg_names_list))]) for d in row_dicts
    ]
    for i, d in enumerate(row_dicts):
        d[column_name] = new_vals[i]
    # Replace existing column if column_name already in schema, else append (PySpark withColumn semantics).
    existing_names = [f.name for f in df.schema.fields]
    if column_name in existing_names:
        extended_fields = [
            T.StructField(
                f.name,
                rt if f.name == column_name else f.dataType,
                f.nullable if f.name != column_name else True,
            )
            for f in df.schema.fields
        ]
    else:
        extended_fields = list(df.schema.fields) + [
            T.StructField(column_name, rt, True)
        ]
    extended_schema = T.StructType(extended_fields)
    # Pass rows as list-of-lists in schema order so createDataFrame does not reorder by sorted dict keys.
    row_lists = [[d[f.name] for f in extended_schema.fields] for d in row_dicts]
    return session.createDataFrame(row_lists, schema=extended_schema)


class _SparklessUdfColumn:
    """Wraps a native UDF Column so comparisons build PyUdfFilterColumn (issue #1664)."""

    __slots__ = ("_sparkless_udf_inner",)

    def __init__(self, inner):
        self._sparkless_udf_inner = inner

    def _udf_filter(self, other, op: str):
        import sparkless_native._native as _native

        return _native.make_udf_filter_column(self._sparkless_udf_inner, op, other)

    def __gt__(self, other):
        return self._udf_filter(other, "gt")

    def __ge__(self, other):
        return self._udf_filter(other, "ge")

    def __lt__(self, other):
        return self._udf_filter(other, "lt")

    def __le__(self, other):
        return self._udf_filter(other, "le")

    def __eq__(self, other):
        return self._udf_filter(other, "eq")

    def __ne__(self, other):
        return self._udf_filter(other, "ne")

    def __getattr__(self, name):
        return getattr(self._sparkless_udf_inner, name)


def udf(f=None, returnType=None):
    """Create a Python UDF. Use as F.udf(func, returnType) or @udf(returnType) def func ...

    When the returned callable is used in withColumn with column(s), the UDF runs row-by-row
    (e.g. df.withColumn("y", my_udf(F.col("x")))).
    """
    import uuid

    import sparkless_native._native as _native

    # Lazy default: avoid circular import
    global _DEFAULT_UDF_RETURN_TYPE
    if _DEFAULT_UDF_RETURN_TYPE is None:
        import sparkless.sql.types as T

        _DEFAULT_UDF_RETURN_TYPE = T.StringType()

    def register_and_wrap(func, return_type):
        from sparkless.sql.functions import col as _col

        _ensure_udf_executor_registered()
        udf_name = "udf_" + uuid.uuid4().hex
        _PYTHON_UDF_REGISTRY[udf_name] = (func, return_type)

        def wrapper(*cols):
            # When used as instance method (e.g. self.add(F.col('a'), F.col('b'))), first arg is self; drop it.
            # When descriptor is accessed on the class (e.g. C.add during class build), first arg can be the class; return wrapper so the attribute stays callable.
            if (
                cols
                and not isinstance(cols[0], str)
                and not getattr(cols[0], "get_udf_call_info", None)
            ):
                first = cols[0]
                if isinstance(first, type):
                    return wrapper
                cols = cols[1:]
            # If we were called with no column args (e.g. wrapper() or wrapper(owner) after strip), do not return a UDF column; return wrapper so decorator leaves the name as the callable.
            if not cols:
                return wrapper
            if (
                len(cols) == 1
                and callable(cols[0])
                and not getattr(cols[0], "get_udf_call_info", None)
            ):
                raise TypeError(
                    "UDF must be called with Column(s), e.g. my_udf(F.col('a'), F.col('b')). "
                    "Got a single callable - ensure you call the UDF with column arguments."
                )
            resolved = [_col(c) if isinstance(c, str) else c for c in cols]
            return _SparklessUdfColumn(_native.create_udf_column(udf_name, resolved))

        return wrapper

    # @udf(returnType) or @udf() — decorator with optional return type
    if f is None:
        return_type = returnType if returnType is not None else _DEFAULT_UDF_RETURN_TYPE

        def decorator(func):
            return register_and_wrap(func, return_type)

        return decorator
    # @udf(returnType) with single arg: that arg is the return type (decorator). Distinguish from F.udf(func).
    if returnType is None and f is not None:
        # Type-like: a class or an instance of a type (e.g. IntegerType(), StringType())
        is_return_type = isinstance(f, type) or getattr(
            type(f), "__name__", ""
        ).endswith("Type")
        if is_return_type:
            return_type = f

            def decorator(func):
                return register_and_wrap(func, return_type)

            return decorator
    # F.udf(f, returnType)
    return_type = returnType if returnType is not None else _DEFAULT_UDF_RETURN_TYPE
    return register_and_wrap(f, return_type)


__all__ = ["_SparklessUdfColumn", "_python_udf_executor", "udf"]

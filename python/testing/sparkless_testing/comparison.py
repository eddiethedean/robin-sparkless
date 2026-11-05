"""DataFrame comparison utilities for testing.

This module provides functions to compare DataFrames and rows between
sparkless and PySpark, with appropriate tolerance and normalization
for floating point values, dates, and other types.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Mapping, Optional, Tuple


@dataclass
class ComparisonResult:
    """Result of a DataFrame comparison.

    Attributes:
        equivalent: True if the DataFrames are equivalent.
        errors: List of error messages describing differences.
        row_count_match: True if row counts match.
        schema_match: True if schemas match.
        column_match: True if column names match.
    """

    equivalent: bool
    errors: List[str] = field(default_factory=list)
    row_count_match: bool = True
    schema_match: bool = True
    column_match: bool = True


def compare_dataframes(
    actual: Any,
    expected: Any,
    *,
    tolerance: float = 1e-6,
    check_schema: bool = True,
    check_order: bool = False,
) -> ComparisonResult:
    """Compare two DataFrames and return a detailed comparison result.

    This function handles both DataFrame objects and expected output
    dictionaries (with 'data' and optionally 'schema' keys).

    Args:
        actual: The actual DataFrame to compare.
        expected: The expected DataFrame or expected output dict.
        tolerance: Tolerance for floating point comparisons.
        check_schema: Whether to compare schemas.
        check_order: Whether row order must match.

    Returns:
        ComparisonResult: Detailed comparison result.

    Example:
        >>> result = compare_dataframes(df1, df2, tolerance=1e-5)
        >>> if not result.equivalent:
        ...     print(result.errors)
    """
    errors: List[str] = []

    # Handle expected as dict (from expected output files)
    if isinstance(expected, dict):
        expected_data = expected.get("data", [])
        expected_schema = expected.get("schema")
    else:
        # Expected is a DataFrame
        try:
            expected_data = expected.collect()
            expected_schema = expected.schema if check_schema else None
        except Exception as e:
            return ComparisonResult(
                equivalent=False,
                errors=[f"Failed to collect expected DataFrame: {e}"],
            )

    # Collect actual data
    try:
        actual_data = actual.collect()
        actual_schema = actual.schema if check_schema else None
    except Exception as e:
        return ComparisonResult(
            equivalent=False,
            errors=[f"Failed to collect actual DataFrame: {e}"],
        )

    # Compare row counts
    row_count_match = len(actual_data) == len(expected_data)
    if not row_count_match:
        errors.append(
            f"Row count mismatch: got {len(actual_data)}, expected {len(expected_data)}"
        )
        return ComparisonResult(
            equivalent=False,
            errors=errors,
            row_count_match=False,
        )

    # Compare schemas if requested
    schema_match = True
    if check_schema and actual_schema is not None and expected_schema is not None:
        schema_match = _compare_schemas(actual_schema, expected_schema)
        if not schema_match:
            errors.append("Schema mismatch")

    # Compare columns
    try:
        actual_columns = actual.columns
        if isinstance(expected, dict):
            # The schema remains useful for empty expected results.
            expected_columns = _schema_column_names(expected_schema)
            if expected_columns is None:
                first_dict = next(
                    (r for r in expected_data if isinstance(r, dict)),
                    None,
                )
                expected_columns = list(first_dict.keys()) if first_dict else []
        else:
            expected_columns = expected.columns

        column_match = set(actual_columns) == set(expected_columns)
        if not column_match:
            errors.append(
                f"Column mismatch: got {set(actual_columns)}, expected {set(expected_columns)}"
            )
    except Exception:
        column_match = True  # Skip column check if it fails

    # Convert to dicts for comparison
    actual_dicts = [_row_to_dict(r) for r in actual_data]
    if isinstance(expected, dict):
        expected_dicts = expected_data
    else:
        expected_dicts = [_row_to_dict(r) for r in expected_data]

    # Sort if order doesn't matter
    if not check_order:
        actual_dicts = _sort_rows_by_key(actual_dicts)
        expected_dicts = _sort_rows_by_key(expected_dicts)

    # Compare rows
    for i, (actual_row, expected_row) in enumerate(zip(actual_dicts, expected_dicts)):
        row_errors = _compare_row_dicts(actual_row, expected_row, tolerance, i)
        errors.extend(row_errors)

    return ComparisonResult(
        equivalent=len(errors) == 0,
        errors=errors,
        row_count_match=row_count_match,
        schema_match=schema_match,
        column_match=column_match,
    )


def assert_dataframes_equal(
    actual: Any,
    expected: Any,
    *,
    tolerance: float = 1e-6,
    check_schema: bool = True,
    check_order: bool = False,
    msg: Optional[str] = None,
) -> None:
    """Assert that two DataFrames are equal.

    Args:
        actual: The actual DataFrame to compare.
        expected: The expected DataFrame or expected output dict.
        tolerance: Tolerance for floating point comparisons.
        check_schema: Whether to compare schemas.
        check_order: Whether row order must match.
        msg: Optional custom error message prefix.

    Raises:
        AssertionError: If DataFrames are not equal.

    Example:
        >>> assert_dataframes_equal(df1, df2, tolerance=1e-5)
        >>> assert_dataframes_equal(df1, df2, check_order=False)
    """
    result = compare_dataframes(
        actual,
        expected,
        tolerance=tolerance,
        check_schema=check_schema,
        check_order=check_order,
    )

    if not result.equivalent:
        error_msg = "\n".join(result.errors)
        if msg:
            error_msg = f"{msg}: {error_msg}"
        raise AssertionError(error_msg)


def assert_rows_equal(
    actual: List[Any],
    expected: List[Any],
    *,
    order_matters: bool = True,
    tolerance: float = 1e-6,
) -> None:
    """Compare two lists of rows (dicts or Row objects).

    Args:
        actual: Result from DataFrame.collect() (any backend).
        expected: Expected rows (e.g. from PySpark or precomputed).
        order_matters: If False, sort both before comparing.
        tolerance: Tolerance for floating point comparisons.

    Raises:
        AssertionError: If row counts differ or any row differs.

    Example:
        >>> actual = df.collect()
        >>> expected = [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
        >>> assert_rows_equal(actual, expected)
    """
    actual_dicts = [_row_to_dict(r) if not isinstance(r, dict) else r for r in actual]
    expected_dicts = [
        _row_to_dict(r) if not isinstance(r, dict) else r for r in expected
    ]

    if len(actual_dicts) != len(expected_dicts):
        raise AssertionError(
            f"Row count mismatch: got {len(actual_dicts)}, expected {len(expected_dicts)}"
        )

    if not order_matters:
        actual_dicts = _sort_rows_by_key(actual_dicts)
        expected_dicts = _sort_rows_by_key(expected_dicts)

    for i, (actual_row, expected_row) in enumerate(zip(actual_dicts, expected_dicts)):
        errors = _compare_row_dicts(actual_row, expected_row, tolerance, i)
        if errors:
            raise AssertionError(errors[0])


def _row_to_dict(row: Any) -> Dict[str, Any]:
    """Convert a Row object to a plain Python dict.

    Handles PySpark Row.asDict(), sparkless rows, and dict-like objects.
    """
    if isinstance(row, dict):
        d = row
    elif hasattr(row, "asDict"):
        d = row.asDict()
    else:
        d = dict(row)

    out: Dict[str, Any] = {}
    for k, v in d.items():
        if (
            v is not None
            and hasattr(v, "__iter__")
            and not isinstance(v, (str, bytes, dict))
        ):
            try:
                out[k] = list(v)
            except (TypeError, ValueError):
                out[k] = v
        else:
            out[k] = v
    return out


def _compare_row_dicts(
    actual: Dict[str, Any],
    expected: Dict[str, Any],
    tolerance: float,
    index: int,
) -> List[str]:
    """Compare two row dicts and return list of error messages."""
    errors: List[str] = []
    keys = set(actual.keys()) | set(expected.keys())

    for key in sorted(keys):
        if key not in actual:
            errors.append(f"Row {index}: missing key '{key}' in actual")
            continue
        if key not in expected:
            errors.append(f"Row {index}: extra key '{key}' in actual")
            continue

        a, e = actual[key], expected[key]
        if not _values_equal(a, e, tolerance):
            errors.append(f"Row {index} key '{key}': {a!r} != {e!r}")

    return errors


def _values_equal(actual: Any, expected: Any, tolerance: float = 1e-6) -> bool:
    """Compare two values with appropriate handling for different types."""
    # Handle None
    if actual is None and expected is None:
        return True
    if actual is None or expected is None:
        return False

    # Handle NaN
    if isinstance(actual, float) and isinstance(expected, float):
        if math.isnan(actual) and math.isnan(expected):
            return True
        return math.isclose(actual, expected, abs_tol=tolerance)

    # Handle dates and datetimes
    if _is_date_like(actual) or _is_date_like(expected):
        actual_normalized = _normalize_date_like(actual)
        expected_normalized = _normalize_date_like(expected)
        return actual_normalized == expected_normalized

    # Handle lists
    if isinstance(actual, list) and isinstance(expected, list):
        if len(actual) != len(expected):
            return False
        return all(_values_equal(a, e, tolerance) for a, e in zip(actual, expected))

    # Handle dicts
    if isinstance(actual, dict) and isinstance(expected, dict):
        if set(actual.keys()) != set(expected.keys()):
            return False
        return all(
            _values_equal(actual[k], expected[k], tolerance) for k in actual.keys()
        )

    # Default comparison
    return bool(actual == expected)


def _is_date_like(value: Any) -> bool:
    """Check if a value is date-like."""
    if isinstance(value, (date, datetime)):
        return True
    if isinstance(value, str) and len(value) >= 10:
        if value[4:5] == "-" and value[7:8] == "-":
            return (
                value[:4].isdigit() and value[5:7].isdigit() and value[8:10].isdigit()
            )
    return False


def _normalize_date_like(value: Any) -> Tuple[Optional[str], Optional[str]]:
    """Normalize a date-like value to (date_part, time_part) for comparison."""
    if isinstance(value, datetime):
        return (value.date().isoformat(), value.time().isoformat())
    if isinstance(value, date):
        return (value.isoformat(), None)
    if isinstance(value, str) and _is_date_like(value):
        if "T" in value or " " in value:
            parts = value.replace("T", " ").split(" ", 1)
            return (parts[0], parts[1] if len(parts) > 1 else None)
        return (value[:10], None)
    return (None, None)


def _sort_rows_by_key(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Sort rows by a canonical key for order-independent comparison."""

    def key_fn(row: Dict[str, Any]) -> str:
        return str(sorted((k, _sort_key_value(v)) for k, v in row.items()))

    return sorted(rows, key=key_fn)


def _sort_key_value(value: Any) -> Tuple[int, str]:
    """Convert value to a sortable tuple (avoids str vs int TypeError)."""
    if value is None:
        return (0, "")
    return (1, f"{type(value).__name__}:{repr(value)}")


def _compare_schemas(actual_schema: Any, expected_schema: Any) -> bool:
    """Compare schema fields, nullability, metadata, and nested data types."""
    actual_fields = _schema_fields(actual_schema)
    expected_fields = _schema_fields(expected_schema)
    if actual_fields is None or expected_fields is None:
        return False
    if len(actual_fields) != len(expected_fields):
        return False

    for actual_field, expected_field in zip(actual_fields, expected_fields):
        actual_name = _field_value(actual_field, "name")
        expected_name = _field_value(expected_field, "name")
        if actual_name != expected_name:
            return False

        actual_type = _field_value(actual_field, "dataType", "data_type", "type")
        expected_type = _field_value(expected_field, "dataType", "data_type", "type")
        if actual_type is None or expected_type is None:
            return False
        if not _compare_types(actual_type, expected_type):
            return False

        actual_nullable = _field_value(actual_field, "nullable")
        expected_nullable = _field_value(expected_field, "nullable")
        if (
            expected_nullable is not None
            and actual_nullable is not None
            and actual_nullable != expected_nullable
        ):
            return False

        actual_metadata = _field_value(actual_field, "metadata")
        expected_metadata = _field_value(expected_field, "metadata")
        if (
            expected_metadata is not None
            and actual_metadata is not None
            and actual_metadata != expected_metadata
        ):
            return False

    return True


def _compare_types(actual_type: Any, expected_type: Any) -> bool:
    """Compare primitive and nested Spark-compatible data types."""
    if actual_type is None or expected_type is None:
        return False

    actual_kind = _type_kind(actual_type)
    expected_kind = _type_kind(expected_type)
    if actual_kind != expected_kind:
        return False

    if actual_kind == "decimal":
        actual_precision, actual_scale = _decimal_parameters(actual_type)
        expected_precision, expected_scale = _decimal_parameters(expected_type)
        return actual_precision == expected_precision and actual_scale == expected_scale

    if actual_kind == "struct":
        return _compare_schemas(actual_type, expected_type)

    if actual_kind == "array":
        actual_element = _type_property(actual_type, "elementType", "element_type")
        expected_element = _type_property(expected_type, "elementType", "element_type")
        actual_nullable = _type_property(actual_type, "containsNull", "contains_null")
        expected_nullable = _type_property(expected_type, "containsNull", "contains_null")
        return _compare_types(actual_element, expected_element) and (
            expected_nullable is None
            or actual_nullable is None
            or actual_nullable == expected_nullable
        )

    if actual_kind == "map":
        actual_key = _type_property(actual_type, "keyType", "key_type")
        expected_key = _type_property(expected_type, "keyType", "key_type")
        actual_value = _type_property(actual_type, "valueType", "value_type")
        expected_value = _type_property(expected_type, "valueType", "value_type")
        actual_nullable = _type_property(
            actual_type, "valueContainsNull", "value_contains_null"
        )
        expected_nullable = _type_property(
            expected_type, "valueContainsNull", "value_contains_null"
        )
        return (
            _compare_types(actual_key, expected_key)
            and _compare_types(actual_value, expected_value)
            and (
                expected_nullable is None
                or actual_nullable is None
                or actual_nullable == expected_nullable
            )
        )

    return _normalize_type_name(_type_name(actual_type)) == _normalize_type_name(
        _type_name(expected_type)
    )


def _schema_fields(schema: Any) -> Optional[List[Any]]:
    if isinstance(schema, Mapping):
        fields = schema.get("fields")
    elif isinstance(schema, (list, tuple)):
        fields = schema
    else:
        fields = getattr(schema, "fields", None)
    return list(fields) if isinstance(fields, (list, tuple)) else None


def _schema_column_names(schema: Any) -> Optional[List[str]]:
    fields = _schema_fields(schema)
    if fields is None:
        return None
    names = [_field_value(field, "name") for field in fields]
    if any(name is None for name in names):
        return None
    return names


def _field_value(field: Any, *names: str) -> Any:
    for name in names:
        if isinstance(field, Mapping) and name in field:
            return field[name]
        if hasattr(field, name):
            return getattr(field, name)
    return None


def _normalize_type_name(type_name: Any) -> Any:
    if not isinstance(type_name, str):
        return type_name
    return (
        re.sub(r"[\s_-]+", "", type_name.lower())
        .replace("integer", "int")
        .replace("bigint", "long")
    )


def _type_kind(data_type: Any) -> Any:
    if isinstance(data_type, Mapping):
        return _normalize_type_name(data_type.get("type"))
    if isinstance(data_type, str):
        return _normalize_type_name(data_type.split("(", 1)[0].split("<", 1)[0])
    class_name = data_type.__class__.__name__
    if class_name.endswith("Type"):
        return _normalize_type_name(class_name[:-4])
    return _normalize_type_name(_type_name(data_type))


def _type_property(data_type: Any, *names: str) -> Any:
    return _field_value(data_type, *names)


def _decimal_parameters(data_type: Any) -> Tuple[Any, Any]:
    precision = _type_property(data_type, "precision")
    scale = _type_property(data_type, "scale")
    if precision is None or scale is None:
        type_name = re.sub(r"\s+", "", str(_type_name(data_type)))
        match = re.fullmatch(r"decimal\((\d+),(\d+)\)", type_name)
        if match:
            return int(match.group(1)), int(match.group(2))
    return precision, scale


def _type_name(data_type: Any) -> Any:
    if isinstance(data_type, str):
        return data_type
    if data_type is None:
        return None
    simple_string = getattr(data_type, "simpleString", None)
    if callable(simple_string):
        return simple_string()
    return data_type.__class__.__name__

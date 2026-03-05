"""
Comparison utilities for comparing results between PySpark and mock-spark.

Provides functions to compare DataFrames, schemas, and other Spark objects
with appropriate tolerance and normalization.
"""

from typing import Any, List, Optional, Tuple
import math


def compare_schemas(
    mock_schema: Any, pyspark_schema: Any, strict: bool = False
) -> bool:
    """Compare schemas between mock-spark and PySpark.

    Args:
        mock_schema: mock-spark schema (StructType).
        pyspark_schema: PySpark schema (StructType).
        strict: If True, require exact match including nullable flags.

    Returns:
        True if schemas match, False otherwise.
    """
    # Get field lists
    mock_fields = mock_schema.fields if hasattr(mock_schema, "fields") else []
    pyspark_fields = pyspark_schema.fields if hasattr(pyspark_schema, "fields") else []

    if len(mock_fields) != len(pyspark_fields):
        return False

    for mock_field, pyspark_field in zip(mock_fields, pyspark_fields):
        # Compare field names
        mock_name = (
            mock_field.name
            if hasattr(mock_field, "name")
            else getattr(mock_field, "name", None)
        )
        pyspark_name = (
            pyspark_field.name
            if hasattr(pyspark_field, "name")
            else getattr(pyspark_field, "name", None)
        )

        if mock_name != pyspark_name:
            return False

        # Compare data types
        mock_type = (
            mock_field.dataType
            if hasattr(mock_field, "dataType")
            else getattr(mock_field, "dataType", None)
        )
        pyspark_type = (
            pyspark_field.dataType
            if hasattr(pyspark_field, "dataType")
            else getattr(pyspark_field, "dataType", None)
        )

        if not _compare_types(mock_type, pyspark_type):
            return False

        # Compare nullable if strict
        if strict:
            mock_nullable = (
                mock_field.nullable
                if hasattr(mock_field, "nullable")
                else getattr(mock_field, "nullable", True)
            )
            pyspark_nullable = (
                pyspark_field.nullable
                if hasattr(pyspark_field, "nullable")
                else getattr(pyspark_field, "nullable", True)
            )
            if mock_nullable != pyspark_nullable:
                return False

    return True


def _compare_types(mock_type: Any, pyspark_type: Any) -> bool:
    """Compare data types between mock-spark and PySpark.

    Args:
        mock_type: mock-spark data type.
        pyspark_type: PySpark data type.

    Returns:
        True if types match, False otherwise.
    """
    # Get type names
    mock_type_name = mock_type.__class__.__name__ if mock_type is not None else None
    pyspark_type_name = (
        pyspark_type.__class__.__name__ if pyspark_type is not None else None
    )

    # Normalize type names (handle differences in naming)
    type_mapping = {
        "StringType": "StringType",
        "IntegerType": "IntegerType",
        "LongType": "LongType",
        "DoubleType": "DoubleType",
        "FloatType": "FloatType",
        "BooleanType": "BooleanType",
        "DateType": "DateType",
        "TimestampType": "TimestampType",
    }

    # Type narrowing: ensure type names are strings
    mock_type_str = str(mock_type_name) if mock_type_name is not None else ""
    pyspark_type_str = str(pyspark_type_name) if pyspark_type_name is not None else ""
    mock_normalized = type_mapping.get(mock_type_str, mock_type_str)
    pyspark_normalized = type_mapping.get(pyspark_type_str, pyspark_type_str)

    return mock_normalized == pyspark_normalized


def compare_dataframes(
    mock_df: Any,
    pyspark_df: Any,
    tolerance: float = 1e-6,
    check_schema: bool = True,
    check_order: bool = False,
) -> Tuple[bool, Optional[str]]:
    """Compare DataFrames between mock-spark and PySpark.

    Args:
        mock_df: mock-spark DataFrame.
        pyspark_df: PySpark DataFrame.
        tolerance: Tolerance for floating point comparisons.
        check_schema: Whether to compare schemas.
        check_order: Whether row order must match.

    Returns:
        Tuple of (is_equal, error_message).
    """
    # Compare row counts
    try:
        mock_count = mock_df.count()
        pyspark_count = pyspark_df.count()
    except Exception as e:
        return False, f"Failed to count rows: {e}"

    # Guard: count() must return a number (e.g. int); some backends may return Column-like by mistake
    if not isinstance(mock_count, (int, float)):
        return (
            False,
            f"Failed to count rows: mock count returned {type(mock_count).__name__}, expected int",
        )
    if not isinstance(pyspark_count, (int, float)):
        return (
            False,
            f"Failed to count rows: pyspark count returned {type(pyspark_count).__name__}, expected int",
        )

    if mock_count != pyspark_count:
        return False, f"Row count mismatch: {mock_count} vs {pyspark_count}"

    # Compare schemas
    if check_schema:
        try:
            mock_schema = mock_df.schema
            pyspark_schema = pyspark_df.schema
            if not compare_schemas(mock_schema, pyspark_schema):
                return False, "Schema mismatch"
        except Exception as e:
            return False, f"Failed to compare schemas: {e}"

    # Compare columns
    try:
        mock_columns = mock_df.columns
        pyspark_columns = pyspark_df.columns
        if set(mock_columns) != set(pyspark_columns):
            return (
                False,
                f"Column mismatch: {set(mock_columns)} vs {set(pyspark_columns)}",
            )
    except Exception as e:
        return False, f"Failed to get columns: {e}"

    # Compare data
    try:
        mock_data = mock_df.collect()
        pyspark_data = pyspark_df.collect()
    except Exception as e:
        return False, f"Failed to collect data: {e}"

    # Sort if order doesn't matter
    if not check_order:
        mock_data = _sort_rows(mock_data, mock_columns)
        pyspark_data = _sort_rows(pyspark_data, pyspark_columns)

    # Compare rows
    for i, (mock_row, pyspark_row) in enumerate(zip(mock_data, pyspark_data)):
        for col in mock_columns:
            mock_val = _get_row_value(mock_row, col)
            pyspark_val = _get_row_value(pyspark_row, col)

            if not _compare_values(mock_val, pyspark_val, tolerance):
                return (
                    False,
                    f"Row {i}, column {col}: {mock_val} vs {pyspark_val}",
                )

    return True, None


def _get_row_value(row: Any, column: str) -> Any:
    """Get value from a row by column name.

    Args:
        row: Row object (dict, Row, or object with attributes).
        column: Column name.

    Returns:
        Row value.
    """
    if isinstance(row, dict):
        return row.get(column)
    elif hasattr(row, column):
        return getattr(row, column)
    elif hasattr(row, "__getitem__"):
        try:
            return row[column]
        except (KeyError, IndexError):
            return None
    else:
        return None


def _is_column_like(val: Any) -> bool:
    """True if value looks like an unresolved Column (should not be compared with > or ==)."""
    if val is None:
        return False
    name = getattr(type(val), "__name__", "")
    return name == "Column" or (hasattr(val, "name") and "column" in name.lower())


def _compare_values(mock_val: Any, pyspark_val: Any, tolerance: float = 1e-6) -> bool:
    """Compare two values with appropriate handling for different types.

    Args:
        mock_val: mock-spark value.
        pyspark_val: PySpark value.
        tolerance: Tolerance for floating point comparisons.

    Returns:
        True if values match, False otherwise.
    """
    # Handle None
    if mock_val is None and pyspark_val is None:
        return True
    if mock_val is None or pyspark_val is None:
        return False

    # Avoid comparing Column-like objects (e.g. unresolved expressions)
    if _is_column_like(mock_val) or _is_column_like(pyspark_val):
        return False

    # Handle floating point
    if isinstance(mock_val, float) and isinstance(pyspark_val, float):
        return math.isclose(mock_val, pyspark_val, abs_tol=tolerance)

    # Handle lists
    if isinstance(mock_val, list) and isinstance(pyspark_val, list):
        if len(mock_val) != len(pyspark_val):
            return False
        return all(
            _compare_values(m, p, tolerance) for m, p in zip(mock_val, pyspark_val)
        )

    # Handle dicts
    if isinstance(mock_val, dict) and isinstance(pyspark_val, dict):
        if set(mock_val.keys()) != set(pyspark_val.keys()):
            return False
        return all(
            _compare_values(mock_val[k], pyspark_val[k], tolerance) for k in mock_val
        )

    # Default comparison
    return bool(mock_val == pyspark_val)


def _sort_rows(rows: List[Any], columns: List[str]) -> List[Any]:
    """Sort rows by all columns for consistent ordering.

    Args:
        rows: List of row objects.
        columns: Column names to sort by.

    Returns:
        Sorted list of rows.
    """

    def sort_key(row: Any) -> tuple:
        """Generate sort key from row."""
        values = []
        for col in sorted(columns):
            val = _get_row_value(row, col)
            # Convert None to sortable value
            if val is None:
                values.append("")
            elif _is_column_like(val):
                values.append("")  # Column-like: avoid comparison in sorted()
            elif isinstance(val, (int, float)):
                values.append(
                    str(val)
                )  # Convert numbers to strings for consistent typing
            else:
                values.append(str(val))
        return tuple(values)

    return sorted(rows, key=sort_key)


def assert_dataframes_equal(
    mock_df: Any,
    pyspark_df: Any,
    tolerance: float = 1e-6,
    check_schema: bool = True,
    check_order: bool = False,
    msg: Optional[str] = None,
) -> None:
    """Assert that two DataFrames are equal.

    Args:
        mock_df: mock-spark DataFrame.
        pyspark_df: PySpark DataFrame.
        tolerance: Tolerance for floating point comparisons.
        check_schema: Whether to compare schemas.
        check_order: Whether row order must match.
        msg: Optional custom error message.

    Raises:
        AssertionError: If DataFrames are not equal.
    """
    is_equal, error_msg = compare_dataframes(
        mock_df, pyspark_df, tolerance, check_schema, check_order
    )

    if not is_equal:
        error = error_msg or "DataFrames are not equal"
        if msg:
            error = f"{msg}: {error}"
        raise AssertionError(error)

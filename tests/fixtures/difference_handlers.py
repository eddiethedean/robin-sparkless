"""
Handlers for known differences between PySpark and mock-spark.

Provides utilities to normalize results and handle backend-specific
differences when comparing results.
"""

from typing import Any, Callable, Dict, List
import re


def normalize_row_ordering(rows: List[Any], columns: List[str]) -> List[Any]:
    """Normalize row ordering for comparison.

    PySpark doesn't guarantee row order, so we sort rows before comparison.

    Args:
        rows: List of row objects.
        columns: Column names to sort by.

    Returns:
        Sorted list of rows.
    """
    from .comparison import _sort_rows

    return _sort_rows(rows, columns)


def normalize_type_representation(value: Any) -> Any:
    """Normalize type representation for comparison.

    Handles differences in how types are represented between backends.

    Args:
        value: Value to normalize.

    Returns:
        Normalized value.
    """
    # Handle Decimal types
    if hasattr(value, "__class__") and "Decimal" in value.__class__.__name__:
        return float(value)

    # Handle datetime types
    if hasattr(value, "isoformat"):
        return value.isoformat()

    # Handle Row objects
    if hasattr(value, "asDict"):
        return value.asDict()
    elif hasattr(value, "__dict__"):
        return value.__dict__

    return value


def normalize_error_message(message: str) -> str:
    """Normalize error messages for comparison.

    Error messages may differ slightly between PySpark and mock-spark.
    This function normalizes them to focus on the error type rather than
    exact message text.

    Args:
        message: Error message to normalize.

    Returns:
        Normalized error message.
    """
    # Remove file paths and line numbers
    message = re.sub(r'File "[^"]+", line \d+', "", message)

    # Normalize common patterns
    message = re.sub(r"\s+", " ", message)  # Normalize whitespace
    message = message.strip()

    return message


def handle_floating_point_precision(value: float, precision: int = 6) -> float:
    """Round floating point values to handle precision differences.

    Args:
        value: Floating point value.
        precision: Number of decimal places.

    Returns:
        Rounded value.
    """
    return round(value, precision)


def normalize_dataframe_for_comparison(df: Any) -> Any:
    """Normalize DataFrame for comparison.

    Applies various normalizations to make DataFrames comparable.

    Args:
        df: DataFrame to normalize.

    Returns:
        Normalized DataFrame (or same DataFrame if no normalization needed).
    """
    # For now, return as-is
    # Could add sorting, type normalization, etc.
    return df


class DifferenceHandler:
    """Handler for known differences between backends."""

    def __init__(self) -> None:
        """Initialize difference handler."""
        self.known_differences: Dict[str, Callable[..., Any]] = {}

    def register_handler(
        self, difference_type: str, handler: Callable[..., Any]
    ) -> None:
        """Register a handler for a known difference type.

        Args:
            difference_type: Type of difference (e.g., 'row_ordering', 'precision').
            handler: Function to handle the difference.
        """
        self.known_differences[difference_type] = handler

    def handle_difference(
        self, difference_type: str, value: Any, *args: Any, **kwargs: Any
    ) -> Any:
        """Handle a known difference.

        Args:
            difference_type: Type of difference.
            value: Value to handle.
            *args: Additional arguments for handler.
            **kwargs: Additional keyword arguments for handler.

        Returns:
            Handled value.
        """
        handler = self.known_differences.get(difference_type)
        if handler:
            return handler(value, *args, **kwargs)
        return value


# Global difference handler instance
_default_handler = DifferenceHandler()
_default_handler.register_handler("row_ordering", normalize_row_ordering)
_default_handler.register_handler("type_representation", normalize_type_representation)
_default_handler.register_handler("error_message", normalize_error_message)
_default_handler.register_handler("floating_point", handle_floating_point_precision)


def get_difference_handler() -> DifferenceHandler:
    """Get the default difference handler.

    Returns:
        Default DifferenceHandler instance.
    """
    return _default_handler

"""Test mode detection for dual sparkless/pyspark testing.

This module provides the Mode enum and functions for detecting the current
test mode from the SPARKLESS_TEST_MODE environment variable.
"""

from __future__ import annotations

import os
from enum import Enum
from typing import Optional


class Mode(Enum):
    """Test backend mode for dual sparkless/pyspark testing.

    Use SPARKLESS for testing with the sparkless (Rust/Polars) backend.
    Use PYSPARK for testing with real PySpark (requires Java).
    """

    SPARKLESS = "sparkless"
    PYSPARK = "pyspark"


# Environment variable name for test mode selection
ENV_VAR_NAME = "SPARKLESS_TEST_MODE"


def get_mode() -> Mode:
    """Get the current test mode from environment.

    Reads the SPARKLESS_TEST_MODE environment variable. If not set or invalid,
    defaults to Mode.SPARKLESS.

    Returns:
        Mode: The current test mode (SPARKLESS or PYSPARK).

    Example:
        >>> import os
        >>> os.environ["SPARKLESS_TEST_MODE"] = "pyspark"
        >>> get_mode()
        <Mode.PYSPARK: 'pyspark'>
    """
    mode_str = os.environ.get(ENV_VAR_NAME, "").strip().lower()

    if mode_str == "pyspark":
        return Mode.PYSPARK

    # Default to sparkless for empty, "sparkless", or any other value
    return Mode.SPARKLESS


def is_pyspark_mode() -> bool:
    """Check if currently running in PySpark mode.

    Convenience function equivalent to get_mode() == Mode.PYSPARK.

    Returns:
        bool: True if SPARKLESS_TEST_MODE is set to "pyspark".

    Example:
        >>> import os
        >>> os.environ["SPARKLESS_TEST_MODE"] = "pyspark"
        >>> is_pyspark_mode()
        True
    """
    return get_mode() == Mode.PYSPARK


def is_sparkless_mode() -> bool:
    """Check if currently running in sparkless mode.

    Convenience function equivalent to get_mode() == Mode.SPARKLESS.

    Returns:
        bool: True if running in sparkless mode (default).

    Example:
        >>> import os
        >>> os.environ.pop("SPARKLESS_TEST_MODE", None)
        >>> is_sparkless_mode()
        True
    """
    return get_mode() == Mode.SPARKLESS


def set_mode(mode: Mode) -> None:
    """Set the test mode via environment variable.

    This is primarily useful for programmatic mode switching in tests
    or test setup scripts.

    Args:
        mode: The Mode to set.

    Example:
        >>> set_mode(Mode.PYSPARK)
        >>> get_mode()
        <Mode.PYSPARK: 'pyspark'>
    """
    os.environ[ENV_VAR_NAME] = mode.value


def _parse_mode(mode_str: Optional[str]) -> Mode:
    """Parse a mode string to Mode enum.

    Args:
        mode_str: String like "sparkless" or "pyspark", or None.

    Returns:
        Mode: Parsed mode, defaults to SPARKLESS for None or invalid values.
    """
    if mode_str is None:
        return Mode.SPARKLESS

    mode_lower = mode_str.strip().lower()
    if mode_lower == "pyspark":
        return Mode.PYSPARK

    return Mode.SPARKLESS

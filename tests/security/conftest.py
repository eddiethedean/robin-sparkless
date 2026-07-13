"""Shared helpers for security regression tests."""

from __future__ import annotations

import pytest

try:
    from sparkless.errors import SparklessError
except ImportError:  # pragma: no cover - tooling without native ext
    SparklessError = RuntimeError  # type: ignore[misc,assignment]

# Security paths raise SparklessError (native) with user-facing messages.
SECURITY_ERROR = (SparklessError, RuntimeError, ValueError, OSError)


def assert_security_error(exc: BaseException, *fragments: str) -> None:
    """Assert exception message contains all expected substrings."""
    msg = str(exc).lower()
    for fragment in fragments:
        assert fragment.lower() in msg, (
            f"expected {fragment!r} in error message, got: {exc!r}"
        )


@pytest.fixture
def security_error_types():
    """Exception types raised by path/JDBC security guards."""
    return SECURITY_ERROR

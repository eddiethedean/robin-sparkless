"""
Shared test helpers for backend selection across test packages.

Backend is selected via MOCK_SPARK_TEST_BACKEND or SPARKLESS_TEST_BACKEND.
Test logic is the same for both backends; no backend checks.
"""

from __future__ import annotations


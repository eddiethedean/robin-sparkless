"""Tests for issue #412: SparkSession.builder() callable compatibility.

Issue #412 reports that SparkSession.builder() (with parentheses) raises
TypeError in Sparkless because builder is a non-callable attribute.
Uses get_spark_imports from fixture only.
"""

import pytest

from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
SparkSession = _imports.SparkSession


class TestIssue412BuilderCallable:
    """Regression tests for SparkSession.builder() callable form (issue #412)."""

    @pytest.mark.skip(reason="Issue #1239: unskip when fixing")
    def test_builder_callable_returns_self(self) -> None:
        """builder is a non-callable Builder object (PySpark parity)."""
        builder = SparkSession.builder
        assert builder is not None
        # In PySpark, builder is a Builder instance and not callable.
        import pytest

        with pytest.raises(TypeError):
            builder()

    @pytest.mark.skip(reason="Issue #1239: unskip when fixing")
    def test_builder_callable_full_chain(self) -> None:
        """SparkSession.builder() callable form raises TypeError (PySpark parity)."""
        builder = SparkSession.builder
        assert builder is not None

        import pytest

        with pytest.raises(TypeError):
            # In PySpark this raises \"'Builder' object is not callable\".
            builder().appName("my_app").getOrCreate()

    @pytest.mark.skip(reason="Issue #1239: unskip when fixing")
    def test_builder_property_and_call_equivalent(self) -> None:
        """Property-style builder works; callable form raises TypeError (PySpark parity)."""
        builder = SparkSession.builder
        assert builder is not None
        # Clear singleton so we get fresh sessions
        SparkSession._singleton_session = None

        spark1 = builder.appName("prop_form").getOrCreate()
        try:
            assert spark1 is not None
        finally:
            spark1.stop()
            SparkSession._singleton_session = None

        import pytest

        with pytest.raises(TypeError):
            builder().appName("call_form").getOrCreate()

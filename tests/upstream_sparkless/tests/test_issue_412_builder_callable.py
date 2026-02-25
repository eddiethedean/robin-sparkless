"""Tests for issue #412: SparkSession.builder() callable compatibility.

Issue #412 reports that SparkSession.builder() (with parentheses) raises
TypeError in Sparkless because builder is a non-callable attribute. Some
PySpark users or code generators use builder() as a factory method.

These tests verify that:
- SparkSession.builder() returns the same builder instance as SparkSession.builder
- SparkSession.builder().appName(...).getOrCreate() works identically to the
  property form
"""

from sparkless.sql import SparkSession


class TestIssue412BuilderCallable:
    """Regression tests for SparkSession.builder() callable form (issue #412)."""

    def test_builder_callable_returns_self(self) -> None:
        """builder() should return the same builder instance (for method chaining)."""
        builder = SparkSession.builder
        assert builder is not None
        builder_from_property = builder
        builder_from_call = builder()
        assert builder_from_call is builder_from_property

    def test_builder_callable_full_chain(self) -> None:
        """Exact reproduction: SparkSession.builder().appName(...).getOrCreate() should work."""
        builder = SparkSession.builder
        assert builder is not None
        spark = builder().appName("my_app").getOrCreate()
        try:
            assert spark.app_name == "my_app"
            df = spark.createDataFrame([{"id": 1, "name": "test"}])
            assert df.count() == 1
        finally:
            spark.stop()

    def test_builder_property_and_call_equivalent(self) -> None:
        """Both builder and builder() should produce equivalent sessions."""
        builder = SparkSession.builder
        assert builder is not None
        # Clear singleton so we get fresh sessions
        SparkSession._singleton_session = None

        spark1 = builder.appName("prop_form").getOrCreate()
        spark1.stop()
        SparkSession._singleton_session = None

        spark2 = builder().appName("call_form").getOrCreate()
        try:
            assert spark2.app_name == "call_form"
        finally:
            spark2.stop()

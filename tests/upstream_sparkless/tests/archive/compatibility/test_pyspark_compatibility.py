"""
Tests for PySpark compatibility - ensuring mock-spark can be used as a drop-in replacement.

This test suite verifies that mock-spark matches PySpark's module structure and behavior,
enabling tests to switch between engines using only import path changes.
"""

import warnings
from types import ModuleType

import pytest

from sparkless.sql import functions
from sparkless.sql.functions import col, upper, lit
from sparkless.sql.utils import (
    AnalysisException,
    ParseException,
    IllegalArgumentException,
    PySparkValueError,
    PySparkTypeError,
    PySparkRuntimeError,
)
from sparkless.sql.types import StructType, StructField, StringType, IntegerType


class TestFunctionsModuleCompatibility:
    """Test that functions module behaves like PySpark's functions module."""

    def test_functions_is_module(self):
        """Test that functions is a module, not a class."""
        assert isinstance(functions, ModuleType), (
            "functions should be a module (like PySpark), not a class"
        )

    def test_functions_type_check(self):
        """Test that type(functions) returns ModuleType."""
        assert isinstance(functions, ModuleType), (
            "type(functions) should be ModuleType, matching PySpark behavior"
        )

    def test_functions_not_callable(self):
        """Test that functions module is not callable (like PySpark)."""
        with pytest.raises(TypeError, match="'module' object is not callable"):
            functions()  # Should raise TypeError, not work

    def test_functions_import_works(self):
        """Test that functions can be imported from sql.functions."""

        assert callable(col)
        assert callable(upper)
        assert callable(lit)

    def test_functions_module_level_access(self):
        """Test that functions are accessible at module level."""
        # These should work like PySpark
        assert hasattr(functions, "col")
        assert hasattr(functions, "upper")
        assert hasattr(functions, "lit")
        assert callable(functions.col)
        assert callable(functions.upper)
        assert callable(functions.lit)

    def test_functions_f_namespace_still_works(self):
        """Test that F namespace still works for backward compatibility."""
        from sparkless.sql import functions as F

        assert hasattr(F, "col")
        assert hasattr(F, "upper")
        assert callable(F.col)
        assert callable(F.upper)

    def test_functions_direct_import(self):
        """Test direct import of functions from sql.functions."""
        # This should work like PySpark
        from sparkless.sql import SparkSession

        spark = SparkSession("test")
        try:
            # Test that they work (requires active session)
            result = col("test")
            assert result is not None
        finally:
            spark.stop()


class TestFunctionsClassDeprecation:
    """Test that Functions() instantiation shows deprecation warning."""

    def test_functions_class_instantiation_warning(self):
        """Test that instantiating Functions() raises deprecation warning."""
        from sparkless.functions import Functions

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            Functions()  # Should work but warn

            # Check that deprecation warning was raised
            assert len(w) > 0, (
                "Expected deprecation warning for Functions() instantiation"
            )
            assert any("deprecated" in str(warning.message).lower() for warning in w), (
                "Expected deprecation warning message"
            )
            assert issubclass(w[0].category, DeprecationWarning)


class TestSQLUtilsCompatibility:
    """Test that sql.utils exports exceptions matching PySpark structure."""

    def test_analysis_exception_import(self):
        """Test that AnalysisException can be imported from sql.utils."""

        assert AnalysisException is not None
        # Test that it can be raised
        with pytest.raises(AnalysisException, match="test error"):
            raise AnalysisException("test error")

    def test_parse_exception_import(self):
        """Test that ParseException can be imported from sql.utils."""

        assert ParseException is not None
        with pytest.raises(ParseException):
            raise ParseException("parse error")

    def test_illegal_argument_exception_import(self):
        """Test that IllegalArgumentException can be imported from sql.utils."""

        assert IllegalArgumentException is not None
        with pytest.raises(IllegalArgumentException):
            raise IllegalArgumentException("invalid argument")

    def test_pyspark_value_error_import(self):
        """Test that PySparkValueError can be imported from sql.utils."""

        assert PySparkValueError is not None
        with pytest.raises(PySparkValueError):
            raise PySparkValueError("value error")

    def test_pyspark_type_error_import(self):
        """Test that PySparkTypeError can be imported from sql.utils."""

        assert PySparkTypeError is not None
        with pytest.raises(PySparkTypeError):
            raise PySparkTypeError("type error")

    def test_pyspark_runtime_error_import(self):
        """Test that PySparkRuntimeError can be imported from sql.utils."""

        assert PySparkRuntimeError is not None
        with pytest.raises(PySparkRuntimeError):
            raise PySparkRuntimeError("runtime error")

    def test_all_exceptions_available(self):
        """Test that all common exceptions are available."""
        from sparkless.sql.utils import (
            QueryExecutionException,
            SparkUpgradeException,
            PySparkAttributeError,
            ConfigurationException,
        )

        # Just verify they're all importable
        assert all(
            exception is not None
            for exception in [
                AnalysisException,
                ParseException,
                QueryExecutionException,
                SparkUpgradeException,
                IllegalArgumentException,
                PySparkValueError,
                PySparkTypeError,
                PySparkRuntimeError,
                PySparkAttributeError,
                ConfigurationException,
            ]
        )


class TestSQLTypesCompatibility:
    """Test that sql.types exports match PySpark structure."""

    def test_types_import_works(self):
        """Test that types can be imported from sql.types."""

        assert StructType is not None
        assert StructField is not None
        assert StringType is not None
        assert IntegerType is not None

    def test_types_usage(self):
        """Test that types work correctly."""

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )

        assert schema is not None
        assert len(schema.fields) == 2


class TestImportPathCompatibility:
    """Test that import paths match PySpark structure."""

    def test_functions_import_path(self):
        """Test that functions import path matches PySpark."""
        # PySpark: from pyspark.sql import functions as F
        # Sparkless: from sparkless.sql import functions as F
        from sparkless.sql import functions as F

        assert isinstance(F, ModuleType)
        assert hasattr(F, "col")

    def test_utils_import_path(self):
        """Test that utils import path matches PySpark."""
        # PySpark: from pyspark.sql.utils import AnalysisException
        # Sparkless: from sparkless.sql.utils import AnalysisException

        assert AnalysisException is not None

    def test_types_import_path(self):
        """Test that types import path matches PySpark."""
        # PySpark: from pyspark.sql.types import StructType
        # Sparkless: from sparkless.sql.types import StructType

        assert StructType is not None

    def test_drop_in_replacement_pattern(self):
        """Test the drop-in replacement pattern from recommendations."""
        import os

        # Simulate switching between engines
        SPARK_MODE = os.environ.get("SPARK_MODE", "mock")

        if SPARK_MODE == "real":
            # Would use: from pyspark.sql.types import StructType
            # Would use: from pyspark.sql import functions as F
            # Would use: from pyspark.sql.utils import AnalysisException
            pass
        else:
            # Mock-spark with same paths
            from sparkless.sql.types import StructType
            from sparkless.sql import functions as F
            from sparkless.sql.utils import AnalysisException

            # All should work
            assert StructType is not None
            assert isinstance(F, ModuleType)
            assert AnalysisException is not None


class TestBackwardCompatibility:
    """Test that old import paths still work."""

    def test_old_exception_import_still_works(self):
        """Test that old exception import path still works."""
        # Old path: from sparkless import AnalysisException
        from sparkless import AnalysisException

        assert AnalysisException is not None
        with pytest.raises(AnalysisException):
            raise AnalysisException("test")

    def test_old_functions_import_still_works(self):
        """Test that old functions import path still works."""
        # Old path: from sparkless import Functions, F
        from sparkless import Functions, F

        assert Functions is not None
        assert F is not None
        assert hasattr(F, "col")

    def test_old_types_import_still_works(self):
        """Test that old types import path still works."""
        # Old path: from sparkless import StructType
        from sparkless import StructType

        assert StructType is not None

"""
Global pytest configuration for mock-spark tests.

This configuration ensures proper resource cleanup to prevent test leaks.
Supports both mock-spark and PySpark backends for unified testing.
"""

import contextlib
import gc
import os
import pytest

# Prevent numpy crashes on macOS ARM chips with Python 3.9
os.environ["VECLIB_MAXIMUM_THREADS"] = "1"

# Set JAVA_HOME for PySpark if not already set - must be done before any PySpark imports
# Resolve to actual Java installation path (not symlink) for better compatibility
if "JAVA_HOME" not in os.environ:
    # Try common Java installation paths (macOS Homebrew)
    java_home_candidates = [
        "/opt/homebrew/opt/openjdk@11",
        "/opt/homebrew/opt/openjdk@17",
        "/opt/homebrew/opt/openjdk",
    ]
    for candidate in java_home_candidates:
        java_bin_path = os.path.join(candidate, "bin", "java")
        if os.path.exists(java_bin_path):
            # Resolve symlink to actual Java installation
            try:
                actual_java_path = os.path.realpath(java_bin_path)
                # Go up from bin/java to find actual JAVA_HOME
                actual_java_bin = os.path.dirname(actual_java_path)
                actual_java_home = os.path.dirname(actual_java_bin)
                # Verify this is a valid Java home
                if os.path.exists(actual_java_home) and os.path.exists(
                    os.path.join(actual_java_home, "bin", "java")
                ):
                    os.environ["JAVA_HOME"] = actual_java_home
                    # Also add to PATH to ensure java command is found
                    java_bin = os.path.join(actual_java_home, "bin")
                    if java_bin not in os.environ.get("PATH", ""):
                        os.environ["PATH"] = f"{java_bin}:{os.environ.get('PATH', '')}"
                    break
            except Exception:
                # Fallback to candidate if resolution fails
                os.environ["JAVA_HOME"] = candidate
                java_bin = os.path.join(candidate, "bin")
                if java_bin not in os.environ.get("PATH", ""):
                    os.environ["PATH"] = f"{java_bin}:{os.environ.get('PATH', '')}"
                break


@pytest.fixture(scope="function", autouse=True)
def cleanup_after_each_test():
    """Automatically clean up resources after each test.

    This fixture runs after every test to ensure backend connections
    and other resources are properly cleaned up, preventing test leaks.
    """
    yield
    # Force garbage collection to trigger __del__ methods
    gc.collect()


def _ensure_robin_backend_type(session):
    """Set backend_type='robin' on session when SPARKLESS_TEST_BACKEND=robin (for pytest-xdist)."""
    if (os.environ.get("SPARKLESS_TEST_BACKEND") or "").strip().lower() == "robin":
        try:
            setattr(session, "backend_type", "robin")
        except AttributeError:
            # Native PySparkSession exposes backend_type read-only; value is already 'robin' at construction.
            pass


@pytest.fixture
def mock_spark_session():
    """Create a SparkSession with automatic cleanup."""
    from sparkless import SparkSession

    session = SparkSession("test_app")
    _ensure_robin_backend_type(session)
    # When robin mode is requested, ensure we did not silently get polars
    if (
        os.environ.get("SPARKLESS_TEST_BACKEND") or ""
    ).strip().lower() == "robin" and getattr(session, "backend_type", None) != "robin":
        raise RuntimeError(
            f"Robin mode was requested but mock_spark_session has backend_type={getattr(session, 'backend_type', None)!r}. "
            "SPARKLESS_BACKEND should be set by conftest."
        )
    yield session
    # Explicitly clean up
    with contextlib.suppress(BaseException):
        session.stop()
    gc.collect()


@pytest.fixture
def isolated_session():
    """Create an isolated SparkSession for tests requiring isolation."""
    from sparkless import SparkSession
    import uuid

    # Use unique name to ensure isolation
    session_name = f"test_isolated_{uuid.uuid4().hex[:8]}"
    session = SparkSession(session_name)
    _ensure_robin_backend_type(session)
    if (
        os.environ.get("SPARKLESS_TEST_BACKEND") or ""
    ).strip().lower() == "robin" and getattr(session, "backend_type", None) != "robin":
        raise RuntimeError(
            f"Robin mode was requested but isolated_session has backend_type={getattr(session, 'backend_type', None)!r}."
        )
    yield session
    with contextlib.suppress(BaseException):
        session.stop()
    gc.collect()


@pytest.fixture
def spark(request):
    """Unified SparkSession fixture that works with Robin (default), mock-spark, and PySpark.

    Backend selection priority:
    1. pytest marker: @pytest.mark.backend('mock'|'pyspark'|'both'|'robin')
    2. Environment variable: MOCK_SPARK_TEST_BACKEND or SPARKLESS_TEST_BACKEND
    3. Default: Robin (mock mode — native robin-sparkless backend)

    Examples:
        # Use Robin (default)
        def test_something(spark):
            df = spark.createDataFrame([{"id": 1}])

        # Force PySpark
        @pytest.mark.backend('pyspark')
        def test_with_pyspark(spark):
            df = spark.createDataFrame([{"id": 1}])

        # Compare both
        @pytest.mark.backend('both')
        def test_comparison(mock_spark_session, pyspark_session):
            # Both sessions available
    """
    from tests.fixtures.spark_backend import (
        SparkBackend,
        BackendType,
        get_backend_type,
    )

    # Handle backward compatibility - request may not be available in all contexts
    try:
        backend = get_backend_type(request)
    except (AttributeError, TypeError):
        # Fallback: use Robin (mock mode)
        backend = BackendType.ROBIN

    if backend == BackendType.BOTH:
        # For comparison mode, return mock-spark by default
        # Tests should use mock_spark_session and pyspark_session fixtures
        backend = BackendType.MOCK

    # Use test name in app name for better isolation in parallel tests
    test_name = "test_app"
    if hasattr(request, "node") and hasattr(request.node, "name"):
        # Include test name for better isolation
        test_name = f"test_{request.node.name[:50]}"  # Limit length

    try:
        # Only pass enable_delta for PySpark sessions
        kwargs = {}
        if backend == BackendType.PYSPARK:
            kwargs["enable_delta"] = False  # Disable Delta by default for tests
        session = SparkBackend.create_session(
            app_name=test_name,
            backend=backend,
            request=request if hasattr(request, "node") else None,
            **kwargs,
        )
        # Set backend_type on session so validation passes (robin-sparkless sets it at construction; setattr may be read-only)
        if backend == BackendType.ROBIN:
            try:
                setattr(session, "backend_type", "robin")
            except AttributeError:
                pass
        # Ensure we never silently run in wrong backend when robin was requested
        if backend == BackendType.ROBIN:
            actual = getattr(session, "backend_type", None)
            if actual != "robin":
                raise RuntimeError(
                    f"Robin mode was requested but session has backend_type={actual!r}. "
                    "Tests must not silently run in polars/mock when SPARKLESS_TEST_BACKEND=robin."
                )
    except ValueError:
        # When Robin is requested but not available, do not skip: let the test fail so we never
        # silently run in the wrong backend.
        raise
    except (ImportError, RuntimeError) as e:
        # Skip test if PySpark session creation fails
        # This handles known Python 3.11/PySpark 3.2.4 compatibility issues
        # and Java gateway errors
        error_msg = str(e)
        if (
            "Could not serialize" in error_msg
            or "pickle" in error_msg.lower()
            or "Java gateway" in error_msg
            or "Failed to create PySpark session" in error_msg
        ):
            pytest.skip(f"PySpark session creation failed: {e}")
        raise

    yield session

    # Cleanup
    with contextlib.suppress(BaseException):
        session.stop()
    gc.collect()


@pytest.fixture
def spark_backend(request):
    """Get the current backend type being used.

    Returns:
        BackendType enum value.
    """
    from tests.fixtures.spark_backend import get_backend_type

    try:
        return get_backend_type(request)
    except (AttributeError, TypeError):
        # Fallback for backward compatibility
        from tests.fixtures.spark_backend import BackendType

        return BackendType.MOCK


@pytest.fixture
def pyspark_session(request):
    """Create a PySpark SparkSession for comparison testing.

    Skips test if PySpark is not available.
    """
    from tests.fixtures.spark_backend import SparkBackend

    try:
        session = SparkBackend.create_pyspark_session("test_app", enable_delta=False)
        yield session
        with contextlib.suppress(BaseException):
            session.stop()
        gc.collect()
    except (ImportError, RuntimeError) as e:
        pytest.skip(f"PySpark not available: {e}")


@pytest.fixture
def mock_spark():
    """Provide mock spark session for compatibility tests."""
    from sparkless import SparkSession

    session = SparkSession("test_app")
    _ensure_robin_backend_type(session)
    if (
        os.environ.get("SPARKLESS_TEST_BACKEND") or ""
    ).strip().lower() == "robin" and getattr(session, "backend_type", None) != "robin":
        raise RuntimeError(
            f"Robin mode was requested but mock_spark has backend_type={getattr(session, 'backend_type', None)!r}."
        )
    yield session
    with contextlib.suppress(BaseException):
        session.stop()
    gc.collect()


@pytest.fixture
def temp_file_storage_path():
    """Provide a temporary directory for file storage backend tests.

    This fixture ensures that file storage backends created in tests
    use temporary directories that are automatically cleaned up,
    preventing test artifacts from being created in the repository root.
    """
    import tempfile
    import os

    with tempfile.TemporaryDirectory() as tmp_dir:
        # Create a subdirectory for the storage path
        storage_path = os.path.join(tmp_dir, "test_storage")
        yield storage_path
        # Cleanup is handled by TemporaryDirectory context manager


def pytest_configure(config):
    """Configure pytest with custom markers. Default backend is Robin (mock mode)."""
    # Use Robin by default; sync SPARKLESS_BACKEND so session creation uses robin-sparkless.
    # If SPARKLESS_TEST_BACKEND is set to mock/pyspark, leave SPARKLESS_BACKEND unset or as-is.
    _test_backend = (os.environ.get("SPARKLESS_TEST_BACKEND") or "").strip().lower()
    if _test_backend in ("", "robin"):
        os.environ["SPARKLESS_BACKEND"] = "robin"

    config.addinivalue_line(
        "markers", "delta: mark test as requiring Delta Lake (may be skipped)"
    )
    config.addinivalue_line(
        "markers", "performance: mark test as a performance benchmark"
    )
    config.addinivalue_line(
        "markers",
        "compatibility: mark test as compatibility test using expected outputs",
    )
    config.addinivalue_line(
        "markers", "unit: mark test as unit test (no external dependencies)"
    )
    config.addinivalue_line(
        "markers", "timeout: mark tests that rely on pytest-timeout"
    )
    config.addinivalue_line(
        "markers",
        "backend(mock|pyspark|both|robin): mark test to run with specific backend(s)",
    )

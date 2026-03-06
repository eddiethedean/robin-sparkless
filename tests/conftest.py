"""
Global pytest configuration for unified Python tests.

Merge of tests/python and tests/upstream_sparkless conftests.
Backend selection: pytest marker, then MOCK_SPARK_TEST_BACKEND / SPARKLESS_TEST_BACKEND.
"""

from __future__ import annotations

import contextlib
import gc
import os
import re
import sys
import uuid

import pytest

# Prevent numpy crashes on macOS ARM chips with Python 3.9
os.environ["VECLIB_MAXIMUM_THREADS"] = "1"

# Ensure PySpark workers use the same Python as the driver (avoids PYTHON_VERSION_MISMATCH)
if "PYSPARK_PYTHON" not in os.environ:
    os.environ["PYSPARK_PYTHON"] = sys.executable
if "PYSPARK_DRIVER_PYTHON" not in os.environ:
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def _is_pyspark_mode() -> bool:
    """True when tests should run with real PySpark (no sparkless)."""
    return (
        os.getenv("MOCK_SPARK_TEST_BACKEND")
        or os.getenv("SPARKLESS_TEST_BACKEND")
        or ""
    ).strip().lower() == "pyspark"


# In non-PySpark mode, configure sparkless for multiprocessing (pytest-xdist).
if not _is_pyspark_mode():
    try:
        import sparkless as _rs  # type: ignore[import-not-found]
    except ImportError:
        _rs = None  # type: ignore[assignment]
    if (
        _rs is not None
        and getattr(_rs, "_configure_for_multiprocessing", None) is not None
    ):
        _rs._configure_for_multiprocessing()

# Set JAVA_HOME for PySpark if not already set - must be done before any PySpark imports
if "JAVA_HOME" not in os.environ:
    java_home_candidates = [
        "/opt/homebrew/opt/openjdk@11",
        "/opt/homebrew/opt/openjdk@17",
        "/opt/homebrew/opt/openjdk",
    ]
    for candidate in java_home_candidates:
        java_bin_path = os.path.join(candidate, "bin", "java")
        if os.path.exists(java_bin_path):
            try:
                actual_java_path = os.path.realpath(java_bin_path)
                actual_java_bin = os.path.dirname(actual_java_path)
                actual_java_home = os.path.dirname(actual_java_bin)
                if os.path.exists(actual_java_home) and os.path.exists(
                    os.path.join(actual_java_home, "bin", "java")
                ):
                    os.environ["JAVA_HOME"] = actual_java_home
                    java_bin = os.path.join(actual_java_home, "bin")
                    if java_bin not in os.environ.get("PATH", ""):
                        os.environ["PATH"] = f"{java_bin}:{os.environ.get('PATH', '')}"
                    break
            except Exception:
                os.environ["JAVA_HOME"] = candidate
                java_bin = os.path.join(candidate, "bin")
                if java_bin not in os.environ.get("PATH", ""):
                    os.environ["PATH"] = f"{java_bin}:{os.environ.get('PATH', '')}"
                break


@pytest.fixture(scope="function", autouse=True)
def cleanup_after_each_test():
    """Automatically clean up resources after each test."""
    yield
    gc.collect()


def _ensure_robin_backend_type(session):
    """Set backend_type='robin' on session when SPARKLESS_TEST_BACKEND=robin."""
    if (os.environ.get("SPARKLESS_TEST_BACKEND") or "").strip().lower() == "robin":
        try:
            setattr(session, "backend_type", "robin")
        except AttributeError:
            pass


@pytest.fixture
def mock_spark_session():
    """Create a SparkSession with automatic cleanup."""
    from tests.fixtures.spark_backend import BackendType
    from tests.fixtures.spark_imports import get_spark_imports

    SparkSession = get_spark_imports(BackendType.MOCK).SparkSession
    session = SparkSession("test_app")
    _ensure_robin_backend_type(session)
    if (
        os.environ.get("SPARKLESS_TEST_BACKEND") or ""
    ).strip().lower() == "robin" and getattr(session, "backend_type", None) != "robin":
        raise RuntimeError(
            f"Robin mode was requested but mock_spark_session has backend_type={getattr(session, 'backend_type', None)!r}. "
            "SPARKLESS_BACKEND should be set by conftest."
        )
    yield session
    with contextlib.suppress(BaseException):
        session.stop()
    gc.collect()


class _SharedSessionWrapper:
    """Wraps a shared SparkSession so stop() is a no-op (prevents one test from killing the session for others)."""

    __slots__ = ("_session",)

    def __init__(self, session):
        self._session = session

    def stop(self):
        pass  # no-op so shared session is not stopped by individual tests

    def __getattr__(self, name):
        return getattr(self._session, name)


def _use_shared_session() -> bool:
    """Use a single session per worker/run for Robin backend. PySpark uses per-test sessions so each test gets a valid session (avoids None/sc issues with xdist)."""
    if os.environ.get("SPARKLESS_SHARED_SESSION", "1").strip().lower() in (
        "0",
        "false",
        "no",
    ):
        return False
    # Only use shared session for Robin; PySpark per-test sessions are more reliable with xdist
    return not _is_pyspark_mode()


@pytest.fixture(scope="session")
def _shared_robin_session():
    """One SparkSession for the whole test run (Robin backend). Speeds tests; table names must be unique per test via table_prefix."""
    from tests.fixtures.spark_backend import (
        SparkBackend,
        BackendType,
        get_backend_from_env,
    )

    if not _use_shared_session():
        pytest.skip("shared session disabled")
    env_backend = get_backend_from_env()
    if env_backend not in (None, BackendType.ROBIN):
        pytest.skip("shared session only for Robin backend")
    session = SparkBackend.create_mock_spark_session(
        "shared_robin_test", backend_type="robin"
    )
    _ensure_robin_backend_type(session)
    yield session
    with contextlib.suppress(BaseException):
        session.stop()
    gc.collect()


@pytest.fixture(scope="session")
def _shared_pyspark_session():
    """One PySpark SparkSession per worker (session scope). Speeds PySpark tests; use table_prefix for any tables/views."""
    from tests.fixtures.spark_backend import (
        SparkBackend,
        BackendType,
        get_backend_from_env,
    )

    if not _use_shared_session():
        pytest.skip("shared session disabled")
    env_backend = get_backend_from_env()
    if env_backend != BackendType.PYSPARK:
        pytest.skip("shared PySpark session only for PySpark backend")
    session = None
    try:
        session = SparkBackend.create_pyspark_session(
            "shared_pyspark_worker",
            enable_delta=False,
        )
        yield session
    finally:
        if session is not None:
            with contextlib.suppress(BaseException):
                session.stop()
        gc.collect()


@pytest.fixture
def table_prefix(request: pytest.FixtureRequest) -> str:
    """Unique prefix for table/view names when using the shared session (Robin or PySpark). Use e.g. saveAsTable(f'{table_prefix}_mytable')."""
    name = getattr(request.node, "name", "test")[:40]
    safe = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    return f"t_{safe}_{uuid.uuid4().hex[:6]}"


@pytest.fixture
def isolated_session():
    """Create an isolated SparkSession for tests requiring isolation."""
    import uuid

    from tests.fixtures.spark_backend import BackendType
    from tests.fixtures.spark_imports import get_spark_imports

    SparkSession = get_spark_imports(BackendType.MOCK).SparkSession
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
    """Unified SparkSession fixture. Uses tests.utils.get_spark when only env-based backend is needed."""
    # When running from tests/python-style (no fixtures.spark_backend), use tests.utils.get_spark
    try:
        from tests.fixtures.spark_backend import (
            SparkBackend,
            BackendType,
            get_backend_type,
        )
    except ImportError:
        # Fallback: no fixtures, create session from env (pyspark vs sparkless)
        _backend = (
            (
                os.getenv("MOCK_SPARK_TEST_BACKEND")
                or os.getenv("SPARKLESS_TEST_BACKEND")
                or ""
            )
            .strip()
            .lower()
        )
        if _backend == "pyspark":
            from pyspark.sql import SparkSession as _PySparkSession  # type: ignore[import-not-found]

            session = (
                _PySparkSession.builder.appName("test")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate()
            )
        else:
            from sparkless.sql import SparkSession as _SparklessSession  # type: ignore[import-not-found]

            _builder = getattr(
                _SparklessSession.builder,
                "__call__",
                lambda: _SparklessSession.builder,
            )()
            session = _builder.app_name("test").get_or_create()
        yield session
        with contextlib.suppress(BaseException):
            session.stop()
        gc.collect()
        return

    try:
        backend = get_backend_type(request)
    except (AttributeError, TypeError):
        from tests.fixtures.spark_backend import BackendType

        backend = BackendType.ROBIN

    if backend == BackendType.BOTH:
        backend = BackendType.MOCK

    # Use one shared session per worker/run to speed test runs (table names must be unique via table_prefix)
    if backend == BackendType.ROBIN and _use_shared_session():
        session = request.getfixturevalue("_shared_robin_session")
        if getattr(session, "backend_type", None) != "robin":
            raise RuntimeError(
                "Robin mode was requested but shared session has wrong backend_type."
            )
        yield _SharedSessionWrapper(session)
        return
    if backend == BackendType.PYSPARK and _use_shared_session():
        session = request.getfixturevalue("_shared_pyspark_session")
        yield _SharedSessionWrapper(session)
        return

    test_name = "test_app"
    if hasattr(request, "node") and hasattr(request.node, "name"):
        test_name = f"test_{request.node.name[:50]}"

    try:
        kwargs = {}
        if backend == BackendType.PYSPARK:
            kwargs["enable_delta"] = False
        session = SparkBackend.create_session(
            app_name=test_name,
            backend=backend,
            request=request if hasattr(request, "node") else None,
            **kwargs,
        )
        if backend == BackendType.ROBIN:
            try:
                setattr(session, "backend_type", "robin")
            except AttributeError:
                pass
        if backend == BackendType.ROBIN:
            actual = getattr(session, "backend_type", None)
            if actual != "robin":
                raise RuntimeError(
                    f"Robin mode was requested but session has backend_type={actual!r}. "
                    "Tests must not silently run in polars/mock when SPARKLESS_TEST_BACKEND=robin."
                )
    except ValueError:
        raise
    except (ImportError, RuntimeError) as e:
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

    with contextlib.suppress(BaseException):
        session.stop()
    gc.collect()


@pytest.fixture
def spark_backend(request):
    """Get the current backend type being used."""
    from tests.fixtures.spark_backend import get_backend_type

    try:
        return get_backend_type(request)
    except (AttributeError, TypeError):
        from tests.fixtures.spark_backend import BackendType

        return BackendType.MOCK


@pytest.fixture
def pyspark_session(request):
    """Create a PySpark SparkSession for comparison testing."""
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
    from tests.fixtures.spark_backend import BackendType
    from tests.fixtures.spark_imports import get_spark_imports

    SparkSession = get_spark_imports(BackendType.MOCK).SparkSession
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
    """Provide a temporary directory for file storage backend tests."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp_dir:
        storage_path = os.path.join(tmp_dir, "test_storage")
        yield storage_path


def pytest_configure(config):
    """Configure pytest with custom markers."""
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
    config.addinivalue_line(
        "markers",
        "xdist_group(name): pytest-xdist: run tests with the same group name on one worker (use --dist loadgroup)",
    )


def pytest_collection_modifyitems(config, items):
    """When running with pytest-xdist (-n N), keep SQL/temp-view tests on one worker (fixes #1144).
    Use: pytest tests/parity/sql tests/unit/session -n N --dist loadgroup
    so the sql_views group runs on a single worker and the session catalog is shared."""
    numprocesses = getattr(config.option, "numprocesses", None)
    if not numprocesses or numprocesses <= 1:
        return
    sql_group = pytest.mark.xdist_group(name="sql_views")
    for item in items:
        path_str = os.fspath(getattr(item, "fspath", ""))
        if "parity/sql" in path_str or "unit/session" in path_str:
            item.add_marker(sql_group)

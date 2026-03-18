"""
Global pytest configuration using sparkless.testing.

Uses SPARKLESS_TEST_MODE environment variable to select the test backend:
- SPARKLESS_TEST_MODE=sparkless (default): Run with sparkless backend
- SPARKLESS_TEST_MODE=pyspark: Run with PySpark backend
"""

from __future__ import annotations

import contextlib
import gc
import os
import re
import sys
import tempfile
import uuid
from pathlib import Path
import shutil

import pytest

# Prevent numpy crashes on macOS ARM chips with Python 3.9
os.environ.setdefault("VECLIB_MAXIMUM_THREADS", "1")

# Ensure PySpark workers use the same Python as the driver
if "PYSPARK_PYTHON" not in os.environ:
    os.environ["PYSPARK_PYTHON"] = sys.executable
if "PYSPARK_DRIVER_PYTHON" not in os.environ:
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Import from sparkless.testing
from sparkless.testing import (
    Mode,
    get_mode,
    is_pyspark_mode,
    create_session,
    get_imports,
    SparkImports,
)
from sparkless.testing.fixtures import _SharedSessionWrapper

# Register sparkless.testing as the pytest plugin
pytest_plugins = ["sparkless.testing"]

# Configure sparkless for multiprocessing (pytest-xdist) in sparkless mode
if not is_pyspark_mode():
    try:
        import sparkless as _rs
    except ImportError:
        _rs = None  # type: ignore[assignment]
    if (
        _rs is not None
        and getattr(_rs, "_configure_for_multiprocessing", None) is not None
    ):
        _rs._configure_for_multiprocessing()  # type: ignore[operator]


# Remove SPARK_HOME to use pyspark's bundled jars instead of local install
# This ensures consistent behavior and proper Delta Lake jar loading
if "SPARK_HOME" in os.environ:
    del os.environ["SPARK_HOME"]

# Set JAVA_HOME for PySpark if not already set
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


def _use_shared_session() -> bool:
    """Check if shared session mode is enabled.

    Default is per-test sessions. Set SPARKLESS_SHARED_SESSION=1 to use
    one session per run (faster but requires unique table names via table_prefix).
    """
    if os.environ.get("SPARKLESS_SHARED_SESSION", "0").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        if os.environ.get("PYTEST_XDIST_WORKER"):
            return False
        return not is_pyspark_mode()
    return False


@pytest.fixture(scope="session")
def _shared_sparkless_session():
    """One SparkSession for the whole test run (sparkless backend)."""
    if not _use_shared_session():
        pytest.skip("shared session disabled")
    if is_pyspark_mode():
        pytest.skip("shared session only for sparkless backend")

    session = create_session(app_name="shared_sparkless_test", mode=Mode.SPARKLESS)
    yield session
    with contextlib.suppress(BaseException):
        session.stop()
    gc.collect()


@pytest.fixture(scope="session")
def _shared_pyspark_session():
    """One PySpark SparkSession per worker (session scope)."""
    if not _use_shared_session():
        pytest.skip("shared session disabled")
    if not is_pyspark_mode():
        pytest.skip("shared PySpark session only for PySpark backend")

    session = None
    try:
        session = create_session(app_name="shared_pyspark_worker", mode=Mode.PYSPARK)
        yield session
    except ImportError as e:
        if "pyspark" in str(e).lower() or "PySpark is not available" in str(e):
            pytest.skip(f"PySpark not installed: {e}")
        raise
    finally:
        if session is not None:
            with contextlib.suppress(BaseException):
                session.stop()
        gc.collect()


@pytest.fixture
def table_prefix(request: pytest.FixtureRequest) -> str:
    """Unique prefix for table/view names when using the shared session."""
    name = getattr(request.node, "name", "test")[:40]
    safe = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    return f"t_{safe}_{uuid.uuid4().hex[:6]}"


@pytest.fixture
def spark_mode() -> Mode:
    """Get the current test mode (SPARKLESS or PYSPARK)."""
    return get_mode()


@pytest.fixture
def spark_imports() -> SparkImports:
    """Get mode-appropriate Spark imports."""
    return get_imports()


@pytest.fixture
def isolated_session(request: pytest.FixtureRequest):
    """Create an isolated SparkSession for tests requiring isolation."""
    mode = get_mode()
    session_name = f"test_isolated_{uuid.uuid4().hex[:8]}"

    enable_delta = False
    if request.node.get_closest_marker("delta"):
        enable_delta = True
    elif os.environ.get("SPARKLESS_ENABLE_DELTA", "0").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        enable_delta = True

    try:
        session = create_session(
            app_name=session_name, mode=mode, enable_delta=enable_delta
        )
    except (ImportError, RuntimeError) as e:
        error_msg = str(e)
        if "pyspark" in error_msg.lower() or "Java" in error_msg:
            pytest.skip(f"Session creation failed: {e}")
        raise

    yield session
    with contextlib.suppress(BaseException):
        session.stop()
    gc.collect()


@pytest.fixture
def spark(request: pytest.FixtureRequest):
    """Unified SparkSession fixture. Uses SPARKLESS_TEST_MODE to select backend."""
    mode = get_mode()

    # Check for @pytest.mark.backend marker
    marker = request.node.get_closest_marker("backend")
    if marker and marker.args:
        marker_backend = marker.args[0].lower()
        if marker_backend == "pyspark":
            mode = Mode.PYSPARK
        elif marker_backend == "sparkless":
            mode = Mode.SPARKLESS

    # Check if Delta Lake should be enabled (for PySpark mode)
    enable_delta = False
    if request.node.get_closest_marker("delta"):
        enable_delta = True
    elif os.environ.get("SPARKLESS_ENABLE_DELTA", "0").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        enable_delta = True

    # Check if Hive support should be enabled (PySpark only; for CTAS etc.)
    enable_hive = bool(request.node.get_closest_marker("hive"))

    # Use shared session if enabled (but not for delta or hive tests)
    if mode == Mode.SPARKLESS and _use_shared_session() and not enable_delta:
        session = request.getfixturevalue("_shared_sparkless_session")
        yield _SharedSessionWrapper(session)
        return
    if (
        mode == Mode.PYSPARK
        and _use_shared_session()
        and not enable_delta
        and not enable_hive
    ):
        session = request.getfixturevalue("_shared_pyspark_session")
        yield _SharedSessionWrapper(session)
        return

    # Create a new session
    test_name = "test_app"
    if hasattr(request, "node") and hasattr(request.node, "name"):
        test_name = f"test_{request.node.name[:50]}"

    # For Delta-marked tests, provide an explicit warehouse directory so sparkless
    # has a stable disk location for table-backed Delta operations like saveAsTable.
    warehouse_dir = None
    extra_config = {}
    if enable_delta and mode == Mode.SPARKLESS:
        warehouse_dir = tempfile.mkdtemp(prefix="sparkless_warehouse_")
        extra_config["spark.sql.warehouse.dir"] = warehouse_dir

    try:
        session = create_session(
            app_name=test_name,
            mode=mode,
            enable_delta=enable_delta,
            enable_hive=enable_hive if mode == Mode.PYSPARK else False,
            **extra_config,
        )
    except (ImportError, RuntimeError) as e:
        error_msg = str(e)
        if (
            "Could not serialize" in error_msg
            or "pickle" in error_msg.lower()
            or "Java gateway" in error_msg
            or "Failed to create PySpark session" in error_msg
            or "PySpark is not available" in error_msg
            or "No module named 'pyspark'" in error_msg
        ):
            pytest.skip(f"PySpark session creation failed: {e}")
        raise

    yield session

    with contextlib.suppress(BaseException):
        session.stop()
    gc.collect()
    if warehouse_dir is not None:
        shutil.rmtree(warehouse_dir, ignore_errors=True)


@pytest.fixture
def temp_file_storage_path():
    """Provide a temporary directory for file storage backend tests."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        storage_path = os.path.join(tmp_dir, "test_storage")
        yield storage_path


def pytest_configure(config: pytest.Config) -> None:
    """Configure pytest with custom markers."""
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
        "backend(sparkless|pyspark): mark test to run with specific backend",
    )
    config.addinivalue_line(
        "markers",
        "sparkless_only: mark test to run only in sparkless mode",
    )
    config.addinivalue_line(
        "markers",
        "pyspark_only: mark test to run only in PySpark mode",
    )
    config.addinivalue_line(
        "markers",
        "hive: mark test as requiring Hive support (PySpark: enableHiveSupport)",
    )
    config.addinivalue_line(
        "markers",
        "integration: mark test as integration test (may require external setup)",
    )


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    """Apply backend marker so tests run with requested backend; no skip by mode.

    All tests run in both sparkless and pyspark mode by default. Tests must not
    branch on backend; use the same scenario and logic for each (spark + spark_imports).
    """
    # Enforce: no backend-conditional logic or backend-only imports in test modules.
    # If a behavior differs, we want the test to fail in sparkless mode (parity signal),
    # not to branch and hide the discrepancy.
    allowlist = {
        Path(__file__).resolve(),
        (Path(__file__).resolve().parent / "sql" / "conftest.py").resolve(),
        (
            Path(__file__).resolve().parent / "unit" / "test_sparkless_testing.py"
        ).resolve(),
    }

    # Collect unique test file paths from collected items.
    test_files: set[Path] = set()
    for item in items:
        p = Path(str(getattr(item, "fspath", ""))).resolve()
        if p.name.startswith("test_") and p.suffix == ".py":
            test_files.add(p)

    # Patterns we explicitly forbid in tests (not in harness/utility modules).
    forbidden_exprs = [
        # Backend branching on spark_mode / Mode
        re.compile(r"\bif\s+spark_mode\b"),
        re.compile(r"\bspark_mode\s*=="),
        re.compile(r"\bspark_mode\s*!="),
        re.compile(r"\bMode\.(PYSPARK|SPARKLESS)\b"),
        # Backend-only markers
        re.compile(r"@pytest\.mark\.(pyspark_only|sparkless_only)\b"),
    ]
    forbidden_import_pyspark = re.compile(r"^\s*(from|import)\s+pyspark\b")
    forbidden_import_sparkless = re.compile(r"^\s*(from|import)\s+sparkless\b")

    violations: list[str] = []
    for path in sorted(test_files):
        if path in allowlist:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except Exception:
            # If we can't read a test file, don't block collection; let pytest report it.
            continue

        # Check forbidden expressions anywhere in file.
        for rx in forbidden_exprs:
            if rx.search(text):
                violations.append(f"{path}: forbidden pattern `{rx.pattern}`")

        # Check imports line-by-line for better precision/allowlisting.
        for lineno, line in enumerate(text.splitlines(), start=1):
            if forbidden_import_pyspark.search(line):
                violations.append(f"{path}:{lineno}: forbidden import `{line.strip()}`")

            # Disallow importing sparkless directly in tests. Allow sparkless.testing (harness).
            if (
                forbidden_import_sparkless.search(line)
                and "sparkless.testing" not in line
            ):
                violations.append(f"{path}:{lineno}: forbidden import `{line.strip()}`")

    if violations:
        msg = (
            "Backend-branching/import violations found in tests. "
            "Tests must be backend-agnostic and assert PySpark behavior with identical logic.\n\n"
            + "\n".join(sorted(set(violations)))
        )
        raise pytest.UsageError(msg)


def pytest_report_header(config: pytest.Config) -> list[str]:
    """Add sparkless testing info to pytest header."""
    mode = get_mode()
    return [f"sparkless.testing mode: {mode.value}"]

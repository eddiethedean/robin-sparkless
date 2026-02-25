"""
Backend abstraction layer for unified PySpark and mock-spark testing.

Provides factory functions and utilities to create SparkSession instances
from either PySpark or mock-spark based on configuration.
"""

import os
import pytest
from typing import Any, Optional, Tuple
from enum import Enum

# Set JAVA_HOME at module level if not already set - PySpark needs this before import
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


class BackendType(Enum):
    """Backend type enumeration."""

    MOCK = "mock"
    PYSPARK = "pyspark"
    BOTH = "both"
    ROBIN = "robin"


def get_backend_from_env() -> Optional[BackendType]:
    """Get backend type from environment variable.

    Reads MOCK_SPARK_TEST_BACKEND or SPARKLESS_TEST_BACKEND (e.g. mock, pyspark, robin).

    Returns:
        BackendType if set, None otherwise.
    """
    backend_str = (
        (
            os.getenv("MOCK_SPARK_TEST_BACKEND")
            or os.getenv("SPARKLESS_TEST_BACKEND")
            or ""
        )
        .strip()
        .lower()
    )
    if not backend_str:
        return None

    try:
        return BackendType(backend_str)
    except ValueError:
        return None


def get_backend_from_marker(request: pytest.FixtureRequest) -> Optional[BackendType]:
    """Get backend type from pytest marker.

    Args:
        request: Pytest fixture request object.

    Returns:
        BackendType if marker present, None otherwise.
    """
    marker = request.node.get_closest_marker("backend")
    if marker is None:
        return None

    backend_str = marker.args[0] if marker.args else None
    if backend_str is None:
        return None

    try:
        return BackendType(backend_str.lower())
    except ValueError:
        return None


def get_backend_type(request: Optional[pytest.FixtureRequest] = None) -> BackendType:
    """Get backend type from marker, environment, or default to mock.

    Args:
        request: Optional pytest fixture request for marker checking.

    Returns:
        BackendType to use for test execution.
    """
    # Check marker first (highest priority)
    if request is not None:
        try:
            marker_backend = get_backend_from_marker(request)
            if marker_backend is not None:
                return marker_backend
        except (AttributeError, TypeError):
            # Request might not have node attribute in some contexts
            pass

    # Check environment variable
    env_backend = get_backend_from_env()
    if env_backend is not None:
        return env_backend

    # Default to mock-spark
    return BackendType.MOCK


class SparkBackend:
    """Backend abstraction for creating SparkSession instances."""

    @staticmethod
    def create_mock_spark_session(app_name: str = "test_app", **kwargs: Any) -> Any:
        """Create a mock-spark SparkSession.

        Args:
            app_name: Application name for the session.
            **kwargs: Additional arguments for SparkSession creation.

        Returns:
            mock-spark SparkSession instance.
        """
        from sparkless import SparkSession

        return SparkSession(app_name, **kwargs)

    @staticmethod
    def create_pyspark_session(
        app_name: str = "test_app", enable_delta: bool = True, **kwargs: Any
    ) -> Any:
        """Create a PySpark SparkSession.

        Args:
            app_name: Application name for the session.
            **kwargs: Additional arguments for SparkSession creation.

        Returns:
            PySpark SparkSession instance.

        Raises:
            ImportError: If PySpark is not available.
            RuntimeError: If PySpark session creation fails.
        """
        # Set environment variables for PySpark BEFORE importing PySpark
        # PySpark reads JAVA_HOME at import time, so it must be set first
        import sys

        python_executable = sys.executable
        # Explicitly set both to ensure driver and worker use the same Python version
        # This prevents Python version mismatch errors
        os.environ["PYSPARK_PYTHON"] = python_executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

        # Set JAVA_HOME if not already set - critical for PySpark JVM startup
        # Must be set BEFORE importing PySpark
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
                            break
                    except Exception:
                        # Fallback to candidate if resolution fails
                        os.environ["JAVA_HOME"] = candidate
                        break

            # If still not set, try to find Java via 'java' command
            if "JAVA_HOME" not in os.environ:
                try:
                    import subprocess

                    result = subprocess.run(
                        ["which", "java"],
                        capture_output=True,
                        text=True,
                        timeout=5,
                    )
                    if result.returncode == 0:
                        java_path = result.stdout.strip()
                        # Resolve symlink to actual path
                        java_path = os.path.realpath(java_path)
                        # Go up from bin/java to find JAVA_HOME
                        java_bin = os.path.dirname(java_path)
                        java_home = os.path.dirname(java_bin)
                        if os.path.exists(java_home) and os.path.exists(
                            os.path.join(java_home, "bin", "java")
                        ):
                            os.environ["JAVA_HOME"] = java_home
                except Exception:
                    pass

        # Ensure JAVA_HOME is set and resolve it to actual path
        if "JAVA_HOME" in os.environ:
            # Verify and resolve JAVA_HOME to actual path
            java_home = os.environ["JAVA_HOME"]
            java_bin_path = os.path.join(java_home, "bin", "java")
            if os.path.exists(java_bin_path):
                try:
                    actual_java_path = os.path.realpath(java_bin_path)
                    actual_java_bin = os.path.dirname(actual_java_path)
                    actual_java_home = os.path.dirname(actual_java_bin)
                    if os.path.exists(actual_java_home):
                        os.environ["JAVA_HOME"] = actual_java_home
                except Exception:
                    pass  # Keep original if resolution fails

        # Ensure JAVA_HOME is set - PySpark requires this
        if "JAVA_HOME" not in os.environ:
            raise RuntimeError(
                "JAVA_HOME is not set and could not be automatically detected. "
                "Please set JAVA_HOME environment variable to your Java installation path."
            )

        # Also ensure Java bin is in PATH for PySpark subprocess
        java_bin = os.path.join(os.environ["JAVA_HOME"], "bin")
        if java_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = f"{java_bin}:{os.environ.get('PATH', '')}"

        try:
            from pyspark.sql import SparkSession as PySparkSession
        except ImportError:
            raise ImportError(
                "PySpark is not available. Install with: pip install pyspark"
            )

        try:
            import uuid

            # Get worker ID from pytest-xdist for better isolation in parallel tests
            worker_id = os.environ.get("PYTEST_XDIST_WORKER", "gw0")
            # Use unique app name, warehouse dir, and master URL for better isolation
            # PySpark matches sessions based on master URL, so we need unique master URLs
            # CRITICAL: Include process ID to ensure isolation across pytest-xdist workers
            import os as os_module

            process_id = os_module.getpid()
            unique_id = f"{worker_id}_{process_id}_{uuid.uuid4().hex[:8]}"
            unique_app_name = f"{app_name}_{unique_id}"
            unique_warehouse = f"/tmp/spark-warehouse-{unique_id}"
            # Use a unique master URL to force a new SparkContext
            # This ensures PySpark doesn't reuse sessions across tests
            unique_master = "local[1]"

            # Stop any existing SparkSession to ensure clean configuration
            # This is critical for parallel test execution
            # PySpark's getOrCreate() reuses sessions even with different warehouse dirs,
            # so we must stop and clear the singleton before creating a new session
            try:
                # Try to get and stop the active session
                active_session = PySparkSession.getActiveSession()
                if active_session is not None:
                    active_session.stop()
                    # Wait a bit for the session to fully stop
                    import time

                    time.sleep(0.1)
            except (AttributeError, Exception):
                pass

            try:
                existing_session = getattr(PySparkSession, "_instantiatedSession", None)
                if existing_session is not None:
                    existing_session.stop()
                    setattr(PySparkSession, "_instantiatedSession", None)
                    # Wait a bit for the session to fully stop
                    import time

                    time.sleep(0.1)
            except (AttributeError, Exception):
                pass

            # Clear any cached SparkContext references
            # This helps ensure a fresh session is created
            # PySpark maintains a global context registry that can cause session reuse
            try:
                from pyspark import SparkContext

                # Stop all active contexts to force new ones
                if hasattr(SparkContext, "_active_spark_context"):
                    active_ctx = SparkContext._active_spark_context
                    if active_ctx is not None:
                        try:
                            active_ctx.stop()
                            import time

                            time.sleep(0.1)
                        except Exception:
                            pass
                    SparkContext._active_spark_context = None
            except (AttributeError, Exception):
                pass

            # Build SparkSession with Delta Lake if enabled
            if enable_delta:
                try:
                    import importlib_metadata

                    # Get Delta version for JAR specification
                    try:
                        delta_version = importlib_metadata.version("delta_spark")
                    except Exception:
                        delta_version = "3.0.0"  # Fallback version

                    # CRITICAL: Stop ALL existing SparkContexts BEFORE creating Delta session
                    # We must ensure no context exists so Delta JARs are loaded into a fresh context
                    try:
                        from pyspark import SparkContext

                        # Stop all active contexts FIRST
                        if hasattr(SparkContext, "_active_spark_context"):
                            active_ctx = SparkContext._active_spark_context
                            if active_ctx is not None:
                                active_ctx.stop()
                                import time

                                time.sleep(0.3)  # Wait longer for context to fully stop
                            SparkContext._active_spark_context = None
                        # Also stop via SparkSession
                        try:
                            active_session = PySparkSession.getActiveSession()
                            if active_session is not None:
                                active_session.stop()
                                import time

                                time.sleep(0.3)
                        except Exception:
                            pass
                        try:
                            if hasattr(PySparkSession, "_instantiatedSession"):
                                inst_session = PySparkSession._instantiatedSession
                                if inst_session is not None:
                                    inst_session.stop()
                                    setattr(
                                        PySparkSession, "_instantiatedSession", None
                                    )
                                    import time

                                    time.sleep(0.3)
                        except Exception:
                            pass
                    except Exception:
                        pass

                    # Manually specify Delta JARs using spark.jars.packages
                    # This is more reliable than configure_spark_with_delta_pip when contexts are reused
                    # Format: io.delta:delta-spark_2.12:VERSION
                    delta_package = f"io.delta:delta-spark_2.12:{delta_version}"
                    builder = (
                        PySparkSession.builder.master(unique_master)
                        .appName(unique_app_name)
                        .config("spark.driver.bindAddress", "127.0.0.1")
                        .config("spark.driver.host", "127.0.0.1")
                        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
                        .config("spark.ui.enabled", "false")
                        .config("spark.sql.adaptive.enabled", "false")
                        .config(
                            "spark.sql.adaptive.coalescePartitions.enabled", "false"
                        )
                        .config("spark.sql.warehouse.dir", unique_warehouse)
                        .config("spark.jars.packages", delta_package)
                        .config("spark.driver.memory", "1g")
                        .config("spark.executor.memory", "1g")
                        .config(
                            "spark.sql.extensions",
                            "io.delta.sql.DeltaSparkSessionExtension",
                        )
                        .config(
                            "spark.sql.catalog.spark_catalog",
                            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                        )
                    )
                    # Explicitly set Java home if available - PySpark needs this for JVM startup
                    if "JAVA_HOME" in os.environ:
                        java_home = os.environ["JAVA_HOME"]
                        # Set both as environment variable and in Spark config
                        builder = builder.config(
                            "spark.executorEnv.JAVA_HOME", java_home
                        )
                        builder = builder.config(
                            "spark.driver.extraJavaOptions", f"-Djava.home={java_home}"
                        )
                    # Explicitly set Python executable for workers to prevent version mismatch
                    # PySpark requires both environment variables and Spark config properties
                    builder = builder.config(
                        "spark.executorEnv.PYSPARK_PYTHON", python_executable
                    )
                    builder = builder.config(
                        "spark.executorEnv.PYSPARK_DRIVER_PYTHON", python_executable
                    )
                    # Also set as Spark config properties (used by Spark to launch Python)
                    builder = builder.config("spark.pyspark.python", python_executable)
                    builder = builder.config(
                        "spark.pyspark.driver.python", python_executable
                    )
                    # Apply any additional config from kwargs
                    for key, value in kwargs.items():
                        if key.startswith("spark."):
                            builder = builder.config(key, str(value))
                    # Create session - this should create a NEW context with Delta JARs loaded
                    session = builder.getOrCreate()
                    # CRITICAL FIX: Verify warehouse directory is correct
                    # PySpark's getOrCreate() may reuse a session with wrong warehouse dir
                    actual_warehouse = session.conf.get("spark.sql.warehouse.dir")
                    expected_warehouse = unique_warehouse
                    # PySpark may add "file:" prefix
                    if not (
                        actual_warehouse == expected_warehouse
                        or actual_warehouse == f"file:{expected_warehouse}"
                    ):
                        # Session was reused with wrong warehouse - force stop and recreate
                        session.stop()
                        setattr(PySparkSession, "_instantiatedSession", None)
                        # Stop the SparkContext to force a new one
                        try:
                            from pyspark import SparkContext

                            if hasattr(SparkContext, "_active_spark_context"):
                                ctx = SparkContext._active_spark_context
                                if ctx is not None:
                                    ctx.stop()
                                SparkContext._active_spark_context = None
                        except Exception:
                            pass
                        import time

                        time.sleep(0.2)
                        # Recreate the session - this time it should use the correct warehouse
                        session = builder.getOrCreate()
                except ImportError:
                    # Delta Lake not available, continue without it
                    # unique_id already defined above
                    builder = PySparkSession.builder.master(unique_master).appName(
                        unique_app_name
                    )
                    builder = builder.config("spark.driver.bindAddress", "127.0.0.1")
                    builder = builder.config("spark.driver.host", "127.0.0.1")
                    builder = builder.config(
                        "spark.sql.execution.arrow.pyspark.enabled", "false"
                    )
                    builder = builder.config("spark.ui.enabled", "false")
                    builder = builder.config("spark.sql.adaptive.enabled", "false")
                    builder = builder.config(
                        "spark.sql.adaptive.coalescePartitions.enabled", "false"
                    )
                    builder = builder.config(
                        "spark.sql.warehouse.dir", unique_warehouse
                    )
                    # Explicitly set Python executable for workers to prevent version mismatch
                    # Set both as environment variables and Spark config properties
                    builder = builder.config(
                        "spark.executorEnv.PYSPARK_PYTHON", python_executable
                    )
                    builder = builder.config(
                        "spark.executorEnv.PYSPARK_DRIVER_PYTHON", python_executable
                    )
                    # Also set as Spark config properties (alternative method)
                    builder = builder.config("spark.pyspark.python", python_executable)
                    builder = builder.config(
                        "spark.pyspark.driver.python", python_executable
                    )
                    for key, value in kwargs.items():
                        if key.startswith("spark."):
                            builder = builder.config(key, str(value))
                    session = builder.getOrCreate()
                    # Verify warehouse directory
                    actual_warehouse = session.conf.get("spark.sql.warehouse.dir")
                    expected_warehouse = unique_warehouse
                    if not (
                        actual_warehouse == expected_warehouse
                        or actual_warehouse == f"file:{expected_warehouse}"
                    ):
                        session.stop()
                        setattr(PySparkSession, "_instantiatedSession", None)
                        try:
                            from pyspark import SparkContext

                            if hasattr(SparkContext, "_active_spark_context"):
                                ctx = SparkContext._active_spark_context
                                if ctx is not None:
                                    ctx.stop()
                                SparkContext._active_spark_context = None
                        except Exception:
                            pass
                        import time

                        time.sleep(0.2)
                        session = builder.getOrCreate()
            else:
                # unique_id already defined above
                builder = PySparkSession.builder.master(unique_master).appName(
                    unique_app_name
                )
                builder = builder.config("spark.driver.bindAddress", "127.0.0.1")
                builder = builder.config("spark.driver.host", "127.0.0.1")
                builder = builder.config(
                    "spark.sql.execution.arrow.pyspark.enabled", "false"
                )
                builder = builder.config("spark.ui.enabled", "false")
                builder = builder.config("spark.sql.adaptive.enabled", "false")
                builder = builder.config(
                    "spark.sql.adaptive.coalescePartitions.enabled", "false"
                )
                builder = builder.config("spark.sql.warehouse.dir", unique_warehouse)
                # Set Java memory limits to prevent OOM errors
                builder = builder.config("spark.driver.memory", "1g")
                builder = builder.config("spark.executor.memory", "1g")
                # Explicitly set Java home if available
                if "JAVA_HOME" in os.environ:
                    builder = builder.config(
                        "spark.driver.extraJavaOptions",
                        f"-Djava.home={os.environ['JAVA_HOME']}",
                    )
                    # Set Java memory limits to prevent OOM errors
                    builder = builder.config("spark.driver.memory", "1g")
                    builder = builder.config("spark.executor.memory", "1g")
                # Explicitly set Java home if available - PySpark needs this for JVM startup
                if "JAVA_HOME" in os.environ:
                    java_home = os.environ["JAVA_HOME"]
                    # Set both as environment variable and in Spark config
                    builder = builder.config("spark.executorEnv.JAVA_HOME", java_home)
                    builder = builder.config(
                        "spark.driver.extraJavaOptions", f"-Djava.home={java_home}"
                    )
                # Explicitly set Python executable for workers to prevent version mismatch
                # Set both as environment variables and Spark config properties
                builder = builder.config(
                    "spark.executorEnv.PYSPARK_PYTHON", python_executable
                )
                builder = builder.config(
                    "spark.executorEnv.PYSPARK_DRIVER_PYTHON", python_executable
                )
                # Also set as Spark config properties (alternative method)
                builder = builder.config("spark.pyspark.python", python_executable)
                builder = builder.config(
                    "spark.pyspark.driver.python", python_executable
                )
                for key, value in kwargs.items():
                    if key.startswith("spark."):
                        builder = builder.config(key, str(value))
                session = builder.getOrCreate()
                # Verify warehouse directory
                actual_warehouse = session.conf.get("spark.sql.warehouse.dir")
                expected_warehouse = unique_warehouse
                if not (
                    actual_warehouse == expected_warehouse
                    or actual_warehouse == f"file:{expected_warehouse}"
                ):
                    session.stop()
                    setattr(PySparkSession, "_instantiatedSession", None)
                    try:
                        from pyspark import SparkContext

                        if hasattr(SparkContext, "_active_spark_context"):
                            ctx = SparkContext._active_spark_context
                            if ctx is not None:
                                ctx.stop()
                            SparkContext._active_spark_context = None
                    except Exception:
                        pass
                    import time

                    time.sleep(0.2)
                    session = builder.getOrCreate()

            # Test that session works
            session.createDataFrame([{"test": 1}]).collect()
            return session
        except Exception as e:
            raise RuntimeError(f"Failed to create PySpark session: {e}") from e

    @staticmethod
    def create_session(
        app_name: str = "test_app",
        backend: Optional[BackendType] = None,
        request: Optional[pytest.FixtureRequest] = None,
        **kwargs: Any,
    ) -> Any:
        """Create a SparkSession based on backend configuration.

        Args:
            app_name: Application name for the session.
            backend: Explicit backend type to use.
            request: Optional pytest fixture request for marker checking.
            **kwargs: Additional arguments for SparkSession creation.

        Returns:
            SparkSession instance (mock-spark or PySpark).

        Raises:
            ValueError: If backend is BOTH (use create_sessions_for_comparison instead).
        """
        if backend is None:
            backend = get_backend_type(request)

        if backend == BackendType.BOTH:
            raise ValueError(
                "BackendType.BOTH requires create_sessions_for_comparison()"
            )

        if backend == BackendType.PYSPARK:
            # Explicitly enable Delta Lake for PySpark sessions
            enable_delta = kwargs.pop("enable_delta", True)
            return SparkBackend.create_pyspark_session(
                app_name, enable_delta=enable_delta, **kwargs
            )
        elif backend == BackendType.ROBIN:
            return SparkBackend.create_mock_spark_session(
                app_name, backend_type="robin", **kwargs
            )
        else:
            return SparkBackend.create_mock_spark_session(app_name, **kwargs)

    @staticmethod
    def create_sessions_for_comparison(
        app_name: str = "test_app", **kwargs: Any
    ) -> Tuple[Any, Any]:
        """Create both mock-spark and PySpark sessions for comparison.

        Args:
            app_name: Application name for the sessions.
            **kwargs: Additional arguments for SparkSession creation.

        Returns:
            Tuple of (mock_spark_session, pyspark_session).

        Raises:
            ImportError: If PySpark is not available.
        """
        mock_session = SparkBackend.create_mock_spark_session(app_name, **kwargs)
        try:
            pyspark_session = SparkBackend.create_pyspark_session(app_name, **kwargs)
        except ImportError:
            pyspark_session = None

        return mock_session, pyspark_session


def create_spark_session(
    app_name: str = "test_app",
    backend: Optional[BackendType] = None,
    request: Optional[pytest.FixtureRequest] = None,
    **kwargs: Any,
) -> Any:
    """Convenience function to create a SparkSession.

    Args:
        app_name: Application name for the session.
        backend: Explicit backend type to use.
        request: Optional pytest fixture request for marker checking.
        **kwargs: Additional arguments for SparkSession creation.

    Returns:
        SparkSession instance.
    """
    return SparkBackend.create_session(
        app_name=app_name, backend=backend, request=request, **kwargs
    )

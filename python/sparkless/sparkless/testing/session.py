"""Session creation for dual sparkless/pyspark testing.

This module provides factory functions for creating SparkSession instances
appropriate for the current test mode.
"""

from __future__ import annotations

import os
import sys
import uuid
from typing import Any, Optional

from .mode import Mode, get_mode


def create_session(
    app_name: str = "test",
    mode: Optional[Mode] = None,
    enable_delta: bool = False,
    **config: Any,
) -> Any:
    """Create a SparkSession for the given mode.

    This is the main entry point for creating test sessions. It handles
    all the configuration needed for both sparkless and PySpark backends.

    Args:
        app_name: Application name for the session.
        mode: The test mode. If None, uses get_mode() to determine
            the current mode from the environment.
        enable_delta: Whether to enable Delta Lake support (PySpark only).
        **config: Additional Spark configuration key-value pairs.

    Returns:
        SparkSession: A SparkSession instance for the requested mode.

    Raises:
        ImportError: If PySpark is requested but not available.
        RuntimeError: If session creation fails.

    Example:
        >>> spark = create_session(app_name="my_test")
        >>> df = spark.createDataFrame([(1, "a")], ["id", "val"])
        >>> df.count()
        1
    """
    if mode is None:
        mode = get_mode()

    if mode == Mode.PYSPARK:
        return _create_pyspark_session(app_name, enable_delta=enable_delta, **config)
    else:
        return _create_sparkless_session(app_name, **config)


def _create_sparkless_session(app_name: str = "test", **config: Any) -> Any:
    """Create a sparkless SparkSession.

    Args:
        app_name: Application name for the session.
        **config: Additional Spark configuration key-value pairs.

    Returns:
        SparkSession: A sparkless SparkSession instance.
    """
    from sparkless.sql import SparkSession

    builder = SparkSession.builder.app_name(app_name)

    # Apply additional config
    for key, value in config.items():
        builder = builder.config(key, str(value))

    return builder.get_or_create()


def _create_pyspark_session(
    app_name: str = "test",
    enable_delta: bool = False,
    **config: Any,
) -> Any:
    """Create a PySpark SparkSession.

    Handles all the configuration needed for PySpark including:
    - JAVA_HOME detection and configuration
    - Python executable configuration for workers
    - Unique warehouse directories for test isolation
    - Delta Lake support (optional)

    Args:
        app_name: Application name for the session.
        enable_delta: Whether to enable Delta Lake support.
        **config: Additional Spark configuration key-value pairs.

    Returns:
        SparkSession: A PySpark SparkSession instance.

    Raises:
        ImportError: If PySpark is not available.
        RuntimeError: If session creation fails.
    """
    # Remove SPARK_HOME to use pyspark's bundled jars instead of local install
    # This ensures consistent behavior and proper Delta Lake jar loading
    if "SPARK_HOME" in os.environ:
        del os.environ["SPARK_HOME"]

    # Set Python executable for workers
    python_executable = sys.executable
    os.environ.setdefault("PYSPARK_PYTHON", python_executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python_executable)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    # Try to set JAVA_HOME if not already set
    _ensure_java_home()

    try:
        from pyspark.sql import SparkSession as PySparkSession
    except ImportError as e:
        raise ImportError(
            "PySpark is not available. Install with: pip install pyspark"
        ) from e

    # Generate unique identifiers for test isolation
    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "gw0")
    process_id = os.getpid()
    unique_id = f"{worker_id}_{process_id}_{uuid.uuid4().hex[:8]}"
    unique_app_name = f"{app_name}_{unique_id}"
    unique_warehouse = f"/tmp/spark-warehouse-{unique_id}"

    # Stop any existing session for clean configuration
    _stop_existing_pyspark_session(PySparkSession)

    # Build session
    builder = (
        PySparkSession.builder.master("local[1]")
        .appName(unique_app_name)
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .config("spark.sql.warehouse.dir", unique_warehouse)
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
    )

    # Set Python executable in Spark config
    builder = builder.config("spark.executorEnv.PYSPARK_PYTHON", python_executable)
    builder = builder.config(
        "spark.executorEnv.PYSPARK_DRIVER_PYTHON", python_executable
    )
    builder = builder.config("spark.pyspark.python", python_executable)
    builder = builder.config("spark.pyspark.driver.python", python_executable)

    # Set JAVA_HOME in Spark config if available
    if "JAVA_HOME" in os.environ:
        java_home = os.environ["JAVA_HOME"]
        builder = builder.config("spark.executorEnv.JAVA_HOME", java_home)
        builder = builder.config(
            "spark.driver.extraJavaOptions", f"-Djava.home={java_home}"
        )

    # Enable Delta Lake if requested
    if enable_delta:
        builder = _configure_delta(builder)

    # Apply additional config
    for key, value in config.items():
        if key.startswith("spark."):
            builder = builder.config(key, str(value))

    try:
        session = builder.getOrCreate()
        # Verify session works
        session.createDataFrame([{"test": 1}]).collect()
        return session
    except Exception as e:
        raise RuntimeError(f"Failed to create PySpark session: {e}") from e


def _ensure_java_home() -> None:
    """Ensure JAVA_HOME is set, attempting auto-detection if needed."""
    if "JAVA_HOME" in os.environ:
        return

    # Try common Java installation paths (macOS Homebrew)
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
                    return
            except Exception:
                os.environ["JAVA_HOME"] = candidate
                java_bin = os.path.join(candidate, "bin")
                if java_bin not in os.environ.get("PATH", ""):
                    os.environ["PATH"] = f"{java_bin}:{os.environ.get('PATH', '')}"
                return

    # Try to find Java via 'which java'
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
            java_path = os.path.realpath(java_path)
            java_bin = os.path.dirname(java_path)
            java_home = os.path.dirname(java_bin)
            if os.path.exists(java_home) and os.path.exists(
                os.path.join(java_home, "bin", "java")
            ):
                os.environ["JAVA_HOME"] = java_home
    except Exception:
        pass


def _stop_existing_pyspark_session(PySparkSession: Any) -> None:
    """Stop any existing PySpark session for clean configuration."""
    import time

    try:
        active_session = PySparkSession.getActiveSession()
        if active_session is not None:
            active_session.stop()
            time.sleep(0.1)
    except (AttributeError, Exception):
        pass

    try:
        existing_session = getattr(PySparkSession, "_instantiatedSession", None)
        if existing_session is not None:
            existing_session.stop()
            setattr(PySparkSession, "_instantiatedSession", None)
            time.sleep(0.1)
    except (AttributeError, Exception):
        pass


def _configure_delta(builder: Any) -> Any:
    """Configure Delta Lake for a PySpark session builder.

    Uses delta-spark's configure_spark_with_delta_pip() function for jar
    downloads, plus the required session extensions and catalog config.

    Args:
        builder: PySpark SparkSession.Builder instance.

    Returns:
        The builder with Delta Lake configuration applied.
    """
    try:
        from delta import configure_spark_with_delta_pip

        # configure_spark_with_delta_pip handles spark.jars.packages
        builder = configure_spark_with_delta_pip(builder)
    except ImportError:
        # Fallback to manual jar configuration if delta package not available
        try:
            import importlib.metadata

            try:
                delta_version = importlib.metadata.version("delta_spark")
            except Exception:
                delta_version = "3.0.0"

            delta_package = f"io.delta:delta-spark_2.12:{delta_version}"
            builder = builder.config("spark.jars.packages", delta_package)
        except ImportError:
            pass

    # Always add the required extensions and catalog (needed for Delta operations)
    builder = builder.config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension",
    )
    builder = builder.config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )

    return builder

"""
Conftest for vendored upstream sparkless tests.

- Inserts this directory into sys.path so "import tests" resolves to
  tests/upstream_sparkless/tests/ (fixtures, unit, parity, etc.).
- Patches sparkless so SparkSession(app_name) and SparkSession(app_name, **kwargs)
  work against this repo's PyO3 package (SparkSession.builder.app_name(...).get_or_create()).
"""

import os
import sys

# Ensure vendored "tests" package is found (tests/upstream_sparkless/tests/)
_UPSTREAM_ROOT = os.path.abspath(os.path.dirname(__file__))
if _UPSTREAM_ROOT not in sys.path:
    sys.path.insert(0, _UPSTREAM_ROOT)


# Shim: upstream uses SparkSession(app_name) or SparkSession(app_name, backend_type="robin").
# Our package exposes SparkSession.builder.app_name(name).get_or_create().
def _install_sparkless_shim():
    import sparkless

    # If our SparkSession is already a proper class with a compatible constructor,
    # do not override it. (Older shims replaced it with a factory function.)
    if isinstance(getattr(sparkless, "SparkSession", None), type):
        return

    _RealSession = sparkless.SparkSession

    def _session_factory(app_name, **kwargs):
        backend_type = kwargs.pop("backend_type", None)
        builder = _RealSession.builder
        # Support both our package (app_name + appName) and upstream (appName only)
        set_app = getattr(builder, "app_name", None) or getattr(builder, "appName")
        if backend_type:
            old = os.environ.get("SPARKLESS_BACKEND")
            os.environ["SPARKLESS_BACKEND"] = backend_type
            try:
                b = set_app(app_name)
                return (
                    getattr(b, "get_or_create", None) or getattr(b, "getOrCreate")
                )()
            finally:
                if old is None:
                    os.environ.pop("SPARKLESS_BACKEND", None)
                else:
                    os.environ["SPARKLESS_BACKEND"] = old
        b = set_app(app_name)
        return (getattr(b, "get_or_create", None) or getattr(b, "getOrCreate"))()

    _session_factory.builder = _RealSession.builder
    sparkless.SparkSession = _session_factory


_install_sparkless_shim()


def _install_backend_shim():
    """Stub sparkless.backend.factory so tests that import it at module level still collect."""
    import types

    factory = types.ModuleType("sparkless.backend.factory")

    # Minimal materializer stub: when tests/materialization code calls create_materializer,
    # return an object that won't crash. Robin-sparkless DataFrames execute directly;
    # this stub exists only so upstream code paths that reference BackendFactory don't raise.
    class _StubMaterializer:
        def materialize(self, *args, **kwargs):
            raise NotImplementedError(
                "BackendFactory.create_materializer stub: robin-sparkless DataFrames "
                "execute directly; this path should not be reached."
            )

        def can_handle_operations(self, ops):
            return True, []

    class _StubStorageBackend:
        pass

    class BackendFactory:
        _robin_available = staticmethod(lambda: True)
        validate_backend_type = staticmethod(lambda _: None)
        get_backend_type = staticmethod(
            lambda _: "polars"
        )  # stub for upstream code paths
        create_materializer = staticmethod(lambda _: _StubMaterializer())
        create_storage_backend = staticmethod(lambda _: _StubStorageBackend())

    factory.BackendFactory = BackendFactory
    sys.modules["sparkless.backend"] = types.ModuleType("sparkless.backend")
    sys.modules["sparkless.backend"].factory = factory
    sys.modules["sparkless.backend.factory"] = factory


_install_backend_shim()


def _install_core_exceptions_shim():
    """Provide SparkColumnNotFoundError, AnalysisException for tests that expect upstream exception types."""
    import types

    try:
        from sparkless._native import SparklessError
    except ImportError:
        SparklessError = RuntimeError  # fallback if not installed
    operation = types.ModuleType("sparkless.core.exceptions.operation")
    operation.SparkColumnNotFoundError = SparklessError
    operation.SparkUnsupportedOperationError = SparklessError
    analysis = types.ModuleType("sparkless.core.exceptions.analysis")
    analysis.AnalysisException = SparklessError
    analysis.ColumnNotFoundException = SparklessError
    validation = types.ModuleType("sparkless.core.exceptions.validation")
    validation.IllegalArgumentException = SparklessError
    exceptions = types.ModuleType("sparkless.core.exceptions")
    exceptions.PySparkValueError = SparklessError
    exceptions.AnalysisException = SparklessError
    exceptions.PySparkRuntimeError = SparklessError
    exceptions.PySparkTypeError = SparklessError
    exceptions.IllegalArgumentException = SparklessError
    exceptions.operation = operation
    exceptions.analysis = analysis
    exceptions.validation = validation
    core = types.ModuleType("sparkless.core")
    core.exceptions = exceptions
    if "sparkless.core" not in sys.modules:
        sys.modules["sparkless.core"] = core
    if "sparkless.core.exceptions" not in sys.modules:
        sys.modules["sparkless.core.exceptions"] = exceptions
    if "sparkless.core.exceptions.operation" not in sys.modules:
        sys.modules["sparkless.core.exceptions.operation"] = operation
    if "sparkless.core.exceptions.analysis" not in sys.modules:
        sys.modules["sparkless.core.exceptions.analysis"] = analysis
    if "sparkless.core.exceptions.validation" not in sys.modules:
        sys.modules["sparkless.core.exceptions.validation"] = validation


_install_core_exceptions_shim()


def pytest_collection_modifyitems(config, items):
    """Apply xfail to tests listed in xfail_list.txt (known unsupported APIs / parity gaps)."""
    import pytest

    xfail_list_path = os.path.join(_UPSTREAM_ROOT, "xfail_list.txt")
    if not os.path.isfile(xfail_list_path):
        return
    patterns = []
    with open(xfail_list_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                patterns.append(line)
    if not patterns:
        return
    for item in items:
        nodeid = item.nodeid
        for pat in patterns:
            if pat in nodeid:
                item.add_marker(
                    pytest.mark.xfail(
                        strict=False, reason="known unsupported API / parity gap"
                    )
                )
                break

"""
Compatibility shim for ``tests.fixtures`` imports when running pytest from the
repository root.

The real fixture implementations live under
``tests/upstream_sparkless/tests/fixtures``; this package simply re-exports
them so imports like ``tests.fixtures.spark_imports`` continue to work.
"""


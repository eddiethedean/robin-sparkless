#!/usr/bin/env python3
"""Verify PySpark 4 + Delta + sparkless are ready for SPARKLESS_TEST_MODE=pyspark runs."""

from __future__ import annotations

import importlib.metadata
import os
import sys


def main() -> int:
    errors: list[str] = []

    # Env hygiene (matches tests/conftest.py + session.py)
    if "SPARK_HOME" in os.environ:
        errors.append(
            f"SPARK_HOME is set ({os.environ['SPARK_HOME']}); unset for bundled PySpark jars"
        )
    java_home = os.environ.get("JAVA_HOME", "")
    if not java_home:
        errors.append("JAVA_HOME is not set (need Java 17 for PySpark 4)")
    elif not os.path.isdir(java_home):
        errors.append(f"JAVA_HOME does not exist: {java_home}")

    for pkg in (
        "pyspark",
        "delta-spark",
        "pytest",
        "pytest_xdist",
        "pandas",
        "pyarrow",
    ):
        try:
            name = pkg.replace("_", "-")
            ver = importlib.metadata.version(name)
            print(f"OK  {name} {ver}")
        except importlib.metadata.PackageNotFoundError:
            errors.append(f"Missing package: {pkg.replace('_', '-')}")

    try:
        import sparkless  # noqa: F401

        print("OK  sparkless import")
    except Exception as e:
        errors.append(f"sparkless import failed: {e}")
        _report(errors)
        return 1

    # Session module sets PYSPARK_SUBMIT_ARGS with delta-spark_2.13 before pyspark import
    from sparkless.testing.session import _delta_maven_package

    delta_pkg = _delta_maven_package()
    submit = os.environ.get("PYSPARK_SUBMIT_ARGS", "")
    print(f"INFO  PYSPARK_SUBMIT_ARGS={submit!r}")
    if delta_pkg and delta_pkg not in submit:
        errors.append(f"Expected {delta_pkg!r} in PYSPARK_SUBMIT_ARGS")

    os.environ["SPARKLESS_TEST_MODE"] = "pyspark"
    from sparkless.testing import create_session

    try:
        spark = create_session("verify_basic", mode=None)
        v = spark.version
        spark.stop()
        print(f"OK  PySpark session version={v}")
    except Exception as e:
        errors.append(f"Basic PySpark session failed: {e}")

    try:
        spark = create_session("verify_delta", mode=None, enable_delta=True)
        spark.sql("SELECT 1 AS x").collect()
        path = "/tmp/sparkless-delta-verify"
        df = spark.createDataFrame([(1, "a")], ["id", "name"])
        df.write.format("delta").mode("overwrite").save(path)
        n = spark.read.format("delta").load(path).count()
        spark.stop()
        print(f"OK  Delta write/read count={n}")
    except Exception as e:
        errors.append(f"Delta session failed: {e}")

    return _report(errors)


def _report(errors: list[str]) -> int:
    if errors:
        print("\nFAILED:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 1
    print("\nAll checks passed — ready for pytest with SPARKLESS_TEST_MODE=pyspark")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

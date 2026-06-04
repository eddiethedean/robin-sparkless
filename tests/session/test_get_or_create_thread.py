"""Regression: getOrCreate singleton reuse registers session on the current thread."""

from __future__ import annotations

import threading

from sparkless.testing import get_imports

imports = get_imports()
F = imports.F
SparkSession = imports.SparkSession


def test_get_or_create_expr_filter_on_other_thread(spark):
    """Worker thread can use F.expr in filter after getOrCreate reuses the singleton."""
    # Main thread holds the active session via the spark fixture.
    spark.createDataFrame([(1,)], ["id"]).count()

    errors: list[str] = []
    done = threading.Event()

    def worker():
        try:
            s = SparkSession.builder.getOrCreate()
            df = s.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
            out = df.filter(F.expr("id = 1")).collect()
            assert len(out) == 1
            assert out[0]["id"] == 1
        except Exception as e:
            errors.append(str(e))
        finally:
            done.set()

    t = threading.Thread(target=worker)
    t.start()
    done.wait(timeout=30)
    t.join(timeout=5)
    assert not errors, errors

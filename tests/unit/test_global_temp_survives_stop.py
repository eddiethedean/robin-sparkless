"""H-6: global_temp views survive stop() on another session."""

from __future__ import annotations

from sparkless.testing import get_imports

_imports = get_imports()
SparkSession = _imports.SparkSession


def test_global_temp_persists_after_other_session_stops() -> None:
    spark_a = SparkSession.builder.appName("a").getOrCreate()
    spark_b = SparkSession.builder.appName("b").getOrCreate()
    try:
        df = spark_a.createDataFrame([{"x": 1}])
        df.createOrReplaceGlobalTempView("persist_view")
        spark_a.stop()
        out = spark_b.table("global_temp.persist_view").collect()
        assert len(out) == 1
        assert out[0]["x"] == 1
    finally:
        spark_b.stop()

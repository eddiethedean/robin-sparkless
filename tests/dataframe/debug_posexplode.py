"""Debug script: run with python tests/debug_posexplode.py. Uses get_imports from fixture only."""

from sparkless.testing import get_imports

_imports = get_imports()
SparkSession = _imports.SparkSession
F = _imports.F

spark = SparkSession.builder.appName("test").getOrCreate()
df = spark.createDataFrame([{"x": [1, 2], "y": "ok"}])
col = F.posexplode("x").alias("pos", "val")  # type: ignore[operator]
print("Column operation:", getattr(col, "operation", None))
print("Column _alias_names:", getattr(col, "_alias_names", None))
result = df.select("y", col)
rows = result.collect()
print("Columns:", list(rows[0].asDict().keys()) if rows else [])
print("Row count:", len(rows))

import json
import time
import statistics
import cProfile
import pstats
from pathlib import Path
from sparkless import SparklessServer
from pyspark.sql import SparkSession
from pyspark.sql.conversion import ArrowTableToRowsConversion

out = {}
with SparklessServer() as server:
    s = SparkSession.builder.remote(server.spark_connect_url).create()
    try:
        for n in [1000, 10000, 100000]:
            times = []
            converts = []
            for _ in range(4):
                t = time.perf_counter()
                table, schema = s.range(n).select("id")._to_table()
                times.append(time.perf_counter() - t)
                t = time.perf_counter()
                rows = ArrowTableToRowsConversion.convert(table, schema)
                converts.append(time.perf_counter() - t)
                assert len(rows) == n and rows[-1].id == n - 1
            out[f"collect_{n}"] = {
                "arrow_ms": statistics.median(times[1:]) * 1000,
                "python_rows_ms": statistics.median(converts[1:]) * 1000,
            }
        for name, make, expected in [
            ("empty_1000", lambda: s.range(1000).filter("id > 1000"), 0),
            ("empty_10000", lambda: s.range(10000).filter("id > 10000"), 0),
            ("count_1m", lambda: s.range(1000000).filter("id > 5"), 999994),
            ("count_10m", lambda: s.range(10000000).filter("id > 5"), 9999994),
        ]:
            vals = []
            for _ in range(4):
                t = time.perf_counter()
                df = make()
                value = len(df.collect()) if name.startswith("empty") else df.count()
                vals.append(time.perf_counter() - t)
                assert value == expected
            out[name] = {"median_ms": statistics.median(vals[1:]) * 1000}
        profile = cProfile.Profile()
        profile.enable()
        s.range(100000).select("id").collect()
        profile.disable()
        with open("output/benchmarks/connect-collect-profile.txt", "w") as f:
            pstats.Stats(profile, stream=f).sort_stats("cumtime").print_stats(25)
    finally:
        s.stop()
Path("output/benchmarks/slowdown-diagnostics.json").write_text(
    json.dumps(out, indent=2) + "\n"
)
print(json.dumps(out, indent=2))

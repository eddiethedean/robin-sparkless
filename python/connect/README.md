# sparkless-connect

Shared Python launchers for Sparkless server products. Install the `client`
extra to get the pinned `pyspark-client==4.0.5` for an existing Spark Connect
endpoint.

```bash
python -m pip install "sparkless-connect[client]==4.20.0"
```

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://127.0.0.1:15002").create()
try:
    spark.range(5).show()
finally:
    spark.stop()
```

To launch a packaged local server, install
[`sparkless-server[connect]`](https://pypi.org/project/sparkless-server/) or
[`sparkless-server-duckdb[connect]`](https://pypi.org/project/sparkless-server-duckdb/)
and import `SparklessConnect` from that product. Each server package supplies
its own executable and launcher. See the
[Spark Connect guide](https://robin-sparkless.readthedocs.io/en/latest/SPARK_CONNECT/)
for shared servers, explicit executable paths, and session lifecycle.

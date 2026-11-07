# sparkless-server

Run the Sparkless HTTP and Spark Connect services with the Polars engine.
This package includes the base server executable and a Python launcher.
Use the `connect` extra to install the pinned `pyspark-client==4.0.5`.

```bash
python -m pip install "sparkless-server[connect]==4.20.0"
```

```python
from sparkless_server import SparklessConnect

with SparklessConnect() as spark:
    assert spark.range(10).filter("id > 5").count() == 4
```

The context manager starts a loopback server and closes the session and server
on exit. For HTTP, install `sparkless-server` and use `SparklessServer` to get
the running process's `http_url`. Packaged executables target Linux x86_64
(glibc 2.28+) and Apple Silicon (macOS 14+).

[Spark Connect guide](https://robin-sparkless.readthedocs.io/en/latest/SPARK_CONNECT/) ·
[HTTP guide](https://robin-sparkless.readthedocs.io/en/latest/HTTP_API/) ·
[DuckDB server package](https://pypi.org/project/sparkless-server-duckdb/)

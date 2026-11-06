# sparkless-server-duckdb

The platform-specific Sparkless server with read-only DuckDB sources and an
optional DuckDB Connect execution backend. Polars remains the default. This
distribution carries the matching Quack extension and license notice.

Set `SPARKLESS_EXECUTION_BACKEND=duckdb` and configure
`SPARKLESS_DUCKDB_EXECUTION_OWNER` before startup to enable DuckDB execution.
That mode supports the documented Spark Connect subset, typed table creation,
and literal inserts. HTTP SQL is disabled in this mode. Install
`sparkless-server-duckdb[connect]` for the pinned public Connect client.

```python
from sparkless_server_duckdb import SparklessServer

with SparklessServer() as server:
    print(server.http_url)
```

See the [DuckDB execution guide](https://robin-sparkless.readthedocs.io/en/latest/DUCKDB_EXECUTION/)
for owner setup, supported operations, persistence, and resource limits.

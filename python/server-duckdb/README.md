# sparkless-server-duckdb

The Polars HTTP and Spark Connect server with the existing read-only DuckDB source support.
This platform-specific distribution carries the matching Quack extension and license notice.
Its transformations execute in Polars; DuckDB execution and writes are a later capability.
Install `sparkless-server-duckdb[connect]` for the pinned public Connect client. This
distribution owns the matching Quack extension and license notice.

```python
from sparkless_server_duckdb import SparklessServer

with SparklessServer() as server:
    print(server.http_url)
```

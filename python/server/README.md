# sparkless-server

The base Polars HTTP and Spark Connect server. This platform-specific distribution contains
the `sparkless-server` executable and a product-bound launcher facade. It has no DuckDB or
Quack dependency. Install `sparkless-server[connect]` to include the pinned public Spark
Connect client, or `sparkless-connect[client]` to connect to a separately managed endpoint.

```python
from sparkless_server import SparklessServer

with SparklessServer() as server:
    print(server.http_url)
```

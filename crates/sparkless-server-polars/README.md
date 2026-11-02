# sparkless-server-polars

Shared HTTP and Spark Connect service implementation composed with the existing
Polars engine. Base server consumers use `sparkless-server`; the DuckDB product
uses `sparkless-server-duckdb`, which enables this adapter's optional source
feature. The base product does not expose a feature that changes its identity.

Spark Connect protocol definitions are vendored under `proto/spark/connect` from
Apache Spark tag `v4.0.5` and generated for Connect builds. The public listener,
session, operation, readiness, and shutdown contracts are shared by both
products. Execution still uses Polars in 4.19.

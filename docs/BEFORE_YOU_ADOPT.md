# Before You Adopt Sparkless

Sparkless is a strong fit for **local PySpark-style development and testing** without a JVM. Read this page before treating it as a drop-in replacement for a production Spark cluster.

## What Sparkless is for

- Unit tests and CI that exercise PySpark-style DataFrame code quickly
- Local development with CSV, Parquet, JSON, Delta, SQL, and temp views
- Dual-mode testing: same suite against Sparkless (fast) or real PySpark (parity check)

## What Sparkless is not

- A full **Apache Spark cluster** replacement (no distributed scheduler, no Spark UI, no executors)
- Guaranteed **100% PySpark parity** — see [PySpark differences](PYSPARK_DIFFERENCES.md) and [Deferred scope](DEFERRED_SCOPE.md)
- A path to run arbitrary **Python UDFs** — Python `@udf` / pandas UDFs are not supported; use built-in functions or Rust UDFs in the engine ([UDF guide](UDF_GUIDE.md))

## Key limitations

| Topic | Sparkless behavior |
|-------|-------------------|
| **Python UDFs** | Not supported at the Python layer. Prefer built-in `functions` or engine Rust UDFs. |
| **Parity gaps** | 200+ fixtures validated; some APIs differ or are stubs. See [Parity status](PARITY_STATUS.md). |
| **PySpark version** | Targets PySpark **3.2–3.5** semantics by default; PySpark 4 profile is opt-in ([PySpark compat profiles](PYSPARK_COMPAT_PROFILES.md)). |
| **Cluster features** | Streaming, MLlib, RDD cluster operations, and many Spark SQL catalog/ACL features are limited or stubbed. |

## Production and security

For hardened deployments (optional):

- `SPARKLESS_HARDENED=1` — enable stricter defaults
- `SPARKLESS_JDBC_ALLOW_ARBITRARY_SQL=false` — disallow arbitrary JDBC SQL; use `dbtable` only
- `SPARKLESS_FILES_BASE=/path/to/sandbox` — confine file read/write paths

See [Production deployment](PRODUCTION.md) and [PySpark differences — Security hardening](PYSPARK_DIFFERENCES.md#security-hardening-optional).

## Decision guide

| If you need… | Consider… |
|--------------|-----------|
| Fast PySpark tests in CI | **Sparkless** |
| Exact production Spark behavior | **Real PySpark** (use `sparkless.testing` dual-mode) |
| Maximum single-node performance, different API | **Polars Python** directly |
| Embed DataFrames in Rust | **`robin-sparkless`** crate ([Quickstart](QUICKSTART.md)) |

## Next steps

- [Python getting started](python_getting_started.md) — install and first session
- [FAQ](FAQ.md) — common questions
- [Migration guide](python_migration.md) — from PySpark or Sparkless 3.x

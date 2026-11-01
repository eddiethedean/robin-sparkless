# 4.19.0 candidate support contract

Reviewed against branch `codex/release-4.19` on 2026-11-01. **4.19.0 is an
unpublished release candidate; this page describes only the behavior and
platforms covered by the retained candidate evidence.** See [versions and
installation](INSTALLATION.md).
“Implemented” describes a bounded API, not every Spark semantic or workload.
“Behavior-tested” means linked regression/example coverage exists. “Unsupported”
is outside the published contract; “unknown” requires workload validation.

| Capability | Local Python | Rust embedding | HTTP service | Spark Connect |
| --- | --- | --- | --- | --- |
| DataFrame filter/project/aggregate/join/window | Implemented, behavior-tested | Implemented, feature/context dependent | SQL allowlist only | Range/local relation and the documented read-only source subset below; general Connect SQL remains unsupported |
| SQL queries | SQL enabled in official wheel | Requires `sql` | Bounded SELECT on service catalog | Unsupported SQL commands |
| Scalar Python UDF | Behavior-tested in withColumn/select/scalar comparison filters | Native expression callbacks, separate API | Unsupported | Unsupported |
| pandas/vectorized/grouped Python UDF, UDTF | Unsupported public API | No equivalent public Python contract | Unsupported | Unsupported |
| CSV/JSON/Parquet | Implemented | Implemented | No general file IO | Unsupported file sources |
| Delta | Compiled in default wheel | Requires `delta` | Unsupported | Unsupported |
| JDBC/SQLite | Requires native feature build | Driver-specific Cargo features | Unsupported | Unsupported |
| CREATE TABLE / INSERT / database DDL | SQL subset | SQL subset | Bounded mutations with identity | Unsupported commands |
| Warehouse persistence | Parquet with explicit warehouse config | Same engine mechanism | In-memory service catalog only | In-memory session/operation state |
| External catalog provider | Local callback contract | Core provider trait | No general provider configuration contract | Unsupported |
| Optional DuckDB table source | Not applicable | `duckdb-source` feature; read-only standalone file or shared Quack owner | Trusted server configuration only; no client-selected paths or credentials | Candidate-tested with `pyspark-client==4.0.5`: named-table schema/read, filter, project, limit, global/column count, same-owner inner equijoin, grouped count and sum |
| Distributed scheduling / Structured Streaming | Unsupported | Unsupported | Unsupported | Unsupported |
| Repartition/cache/broadcast hints | Local compatibility behavior; no Spark cluster semantics | Local only | Unsupported | Unsupported |

Do not interpret a name being importable as a behavior guarantee. Runtime stubs
and no-op hints are called out in [PySpark differences](PYSPARK_DIFFERENCES.md).
Null ordering, schema inference, decimal/timestamp conversions, exceptions, and
query plans can differ. Run representative pipelines against the actual Spark
version used in production, including nulls, empty inputs, invalid data, and types.

Evidence and narrower contracts:

- The 4.19 DuckDB source is opt-in through the `duckdb-source` server feature and
  `SPARKLESS_DUCKDB_SOURCES`. Public smoke and reference cases cover BOOLEAN,
  SMALLINT, INT, BIGINT, finite DOUBLE, STRING, DATE, UTC microsecond TIMESTAMP,
  and DECIMAL(12, 2); the source contract permits the documented wider DECIMAL
  range but does not certify every precision/scale boundary. Nested/unsigned
  values, non-finite DOUBLEs, writes, cross-owner
  joins, and arbitrary Connect SQL are rejected or unsupported. See the
  [4.19 source contract](plans/sparkless-server/RELEASE_4_19.md#duckdb-source-contract)
  and [candidate evidence](plans/sparkless-server/RELEASE_4_19_EVIDENCE.md).
- Public DuckDB Connect behavior is verified on macOS arm64. The candidate
  server wheels now carry the platform-matched DuckDB 2.0.0 Quack extension; the
  exact-commit Linux x86_64 and macOS arm64 install/source smokes still need to
  pass before those platform results are accepted. A source-built server can
  load a trusted local extension path. Installed wheels resolve the bundled
  extension beside the server executable when `extension_path` is omitted,
  without a runtime download.

- [Generated compatibility inventory](PARITY_STATUS.md): counts and oracle provenance, not a parity percentage.
- [Executable examples](https://github.com/eddiethedean/robin-sparkless/tree/main/examples/python): backend identity, pipeline, UDF.
- [UDF contexts](UDF_GUIDE.md), [catalog lifetime](PERSISTENCE_GUIDE.md), [external providers](EXTERNAL_CATALOGS.md).
- [HTTP reference](HTTP_API.md) and [Connect type/lifecycle matrix](SPARK_CONNECT.md).
- [Distribution features](INSTALLATION.md), [release evidence](RELEASE_EVIDENCE.md).

Unlisted APIs and combinations have **unknown** coverage; do not infer support
from older plans or PySpark documentation. Open a minimal compatibility issue or
add an oracle regression before relying on them.

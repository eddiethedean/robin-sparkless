# 4.20.0 support contract

This published contract defines the supported 4.20.0 behavior. “Implemented”
describes a bounded API, not all PySpark semantics. “Behavior-tested” means
regression or reference coverage exists. “Unsupported” is outside the contract;
“unknown” requires workload validation. See the [package and platform matrix](INSTALLATION.md)
and [release evidence](RELEASE_EVIDENCE.md).

| Capability | Local Python | Rust embedding | Base server | DuckDB server | Spark Connect |
| --- | --- | --- | --- | --- | --- |
| DataFrame filter/project/aggregate/join/window | Implemented, behavior-tested | Implemented, feature/context dependent | Bounded SQL service only | Polars mode: same as base plus configured sources. DuckDB mode: only the plan subset below | Range, bounded inline data, managed tables, and configured read-only source tables; DuckDB mode adds the plans in [DuckDB execution](DUCKDB_EXECUTION.md) |
| SQL queries | SQL enabled in the official wheel | Requires `sql` | Bounded SELECT on the managed catalog | Polars mode: bounded SELECT on managed and source catalogs. DuckDB mode: Connect SQL subset; HTTP SQL is disabled | DuckDB mode: one read-only SELECT relation with the documented clauses and functions |
| CREATE TABLE / INSERT | Local SQL subset | Local SQL subset | Bounded managed-table mutations with operation identity | Polars mode: bounded service mutations. DuckDB mode: typed CREATE TABLE and literal INSERT VALUES through Connect | Mutations are unsupported by default; DuckDB mode accepts the declared DDL/INSERT subset with durable receipts |
| Scalar Python UDF | Behavior-tested in documented local contexts | Native expression callbacks, separate API | Unsupported | Unsupported | Unsupported |
| pandas/vectorized/grouped Python UDF, UDTF | Unsupported public API | No equivalent public Python contract | Unsupported | Unsupported | Unsupported |
| CSV/JSON/Parquet | Implemented | Implemented | No general file IO | No general file IO | Unsupported file sources |
| Delta | Compiled in default wheel | Requires `delta` | Unsupported | Unsupported | Unsupported |
| JDBC/SQLite | Requires native feature build | Driver-specific Cargo features | Unsupported | Unsupported | Unsupported |
| DuckDB table source | Not applicable | Optional source crate | Rejects source configuration | Polars mode reads configured tables; DuckDB mode keeps source aliases read-only and separate from its execution catalog | Named reads from the configured local or shared owner; no source writes |
| DuckDB execution backend | Not a local-engine mode | Server adapter only | Unsupported | Opt-in; Polars remains the default | Optional, fixed for the server process, with the supported SQL/plan/type subset below |
| Distributed scheduling / Structured Streaming | Unsupported | Unsupported | Unsupported | Unsupported | Unsupported |
| Repartition/cache/broadcast hints | Local compatibility behavior; no Spark cluster semantics | Local only | Unsupported | Unsupported | Unsupported |

## DuckDB Connect execution limits

Enable this backend only in `sparkless-server-duckdb`, with
`SPARKLESS_EXECUTION_BACKEND=duckdb` and one configured
`SPARKLESS_DUCKDB_EXECUTION_OWNER`. The server reports the active backend in
readiness and capabilities. An unsupported operation fails explicitly; it is
never retried in Polars. The base product has no DuckDB/Quack payload.

The declared Connect subset includes `range`, bounded scalar `LocalRelation`,
configured tables, aliases, projection, supported scalar filters, `limit`,
inner equality joins (including `USING`), `CROSS JOIN` in SQL, grouped
`count`/`sum`, and the documented read-only SQL expressions and clauses. Writes
are limited to explicitly typed `CREATE TABLE` and literal
`INSERT INTO ... VALUES`. Timestamp and decimal string values may use the
documented explicit casts. CTEs, ordering, windows, `DISTINCT`, CTAS,
insert-from-select, DataFrame writers, temp views, parameterized SQL, UDFs,
nested result types, and general Spark SQL are unsupported. See the full
[request and type matrix](DUCKDB_EXECUTION.md#supported-request-subset).

The backend is single-node. The local owner permits one writable server process
per database file. Shared access uses the matching Quack extension and a
configured shared owner. A local database file must not be opened for writes by
another process while Sparkless owns it.

| Enforced or accepted limit | Value |
| --- | ---: |
| Write/receipt connections | 1 serialized connection |
| Concurrent read connections / queued reads | 4 / 16 |
| DuckDB execution threads / memory limit | 1 thread / 256 MiB per connection |
| `range` rows | 1,000,000 |
| LocalRelation rows / encoded IPC / decoded bytes | 10,000 / 1 MiB / 8 MiB |
| Arrow output rows / IPC bytes per batch | 16,384 / 1 MiB |
| Durable operation receipts | 1,000,000 |
| Accepted 8-client load with one paced reader | 60 seconds, 1,000,000 rows, active limit 16, 5-second queue timeout |
| Server process RSS / shared Quack owner RSS under that workload | 512 MiB / 256 MiB |
| Correct throughput / short-query p95 / health p95 under that workload | at least 5 ops/s / 5 seconds / 1 second |

The RSS and latency values are acceptance ceilings for the measured workload,
not universal capacity promises. Server and owner RSS are measured separately
for Quack; local-owner memory is included in server RSS. Raise instance limits
only after testing your workload and keeping health, queue, cancellation, and
shutdown behavior within these ceilings.

## Compatibility and evidence

The Connect adapter targets the pinned Spark Connect 4.0.5 protocol. Result
values and schemas were compared with an unmodified Spark 4.0.5 client and
reference server for the published matrix. Null ordering, casts, timestamps,
decimals, exceptions, and query plans can differ outside that matrix. Compare
representative values and schemas with the Spark version used in production.

The optional DuckDB **source** mode remains read-only and feeds supported
transformations to Polars. It is distinct from the DuckDB **execution** mode,
which runs declared plans inside DuckDB and accepts the bounded writes above.
Read [the 4.19 source contract](DUCKDB_SOURCE.md) and [4.20 execution guide](DUCKDB_EXECUTION.md)
before selecting a mode.

Other boundaries:

- [PySpark differences](PYSPARK_DIFFERENCES.md) lists local API gaps and
  compatibility behavior; an importable symbol is not a behavior guarantee.
- [Generated compatibility inventory](PARITY_STATUS.md) records counts and
  oracle provenance, not a parity percentage.
- [UDF contexts](UDF_GUIDE.md), [catalog lifetime](PERSISTENCE_GUIDE.md),
  [external providers](EXTERNAL_CATALOGS.md), [HTTP reference](HTTP_API.md),
  and [Connect lifecycle/type limits](SPARK_CONNECT.md) define narrower APIs.
- Unlisted APIs and combinations have **unknown** coverage. Add an oracle
  regression or a reproducible issue before relying on them.

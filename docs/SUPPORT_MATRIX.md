# 4.19.0 support contract

This page describes the published 4.19.0 products. See the [package and platform matrix](INSTALLATION.md) and [release evidence](RELEASE_EVIDENCE.md).

“Implemented” describes a bounded API, not every Spark semantic or workload.
“Behavior-tested” means linked regression/example coverage exists. “Unsupported”
is outside the published contract; “unknown” requires workload validation.

| Capability | Local Python | Rust embedding | Base server | DuckDB server | Spark Connect |
| --- | --- | --- | --- | --- | --- |
| DataFrame filter/project/aggregate/join/window | Implemented, behavior-tested | Implemented, feature/context dependent | SQL allowlist only | Same Polars SQL service surface | [Supported Connect plan subset](SPARK_CONNECT.md#supported-request-surface) for range, inline DataFrames, and configured table sources |
| SQL queries | SQL enabled in official wheel | Requires `sql` | Bounded SELECT on managed catalog | Bounded SELECT on managed and configured source catalogs | Unsupported SQL commands |
| Scalar Python UDF | Behavior-tested in withColumn/select/scalar comparison filters | Native expression callbacks, separate API | Unsupported | Unsupported | Unsupported |
| pandas/vectorized/grouped Python UDF, UDTF | Unsupported public API | No equivalent public Python contract | Unsupported | Unsupported | Unsupported |
| CSV/JSON/Parquet | Implemented | Implemented | No general file IO | No general file IO | Unsupported file sources |
| Delta | Compiled in default wheel | Requires `delta` | Unsupported | Unsupported | Unsupported |
| JDBC/SQLite | Requires native feature build | Driver-specific Cargo features | Unsupported | Unsupported | Unsupported |
| CREATE TABLE / INSERT / database DDL | SQL subset | SQL subset | Bounded mutations with identity | Same bounded mutations | Unsupported commands |
| Warehouse persistence | Parquet with explicit warehouse config | Same engine mechanism | In-memory service catalog only | In-memory service catalog only | In-memory session/operation state |
| External catalog provider | Local callback contract | Core provider trait | No general provider configuration contract | No general provider configuration contract | Unsupported |
| Optional DuckDB table source | Not applicable | `robin-sparkless-duckdb`, optional source feature | Rejects DuckDB source configuration | [Read-only local file or shared Quack owner](DUCKDB_SOURCE.md) configured by the operator | Available through the DuckDB server product |
| Distributed scheduling / Structured Streaming | Unsupported | Unsupported | Unsupported | Unsupported | Unsupported |
| Repartition/cache/broadcast hints | Local compatibility behavior; no Spark cluster semantics | Local only | Unsupported | Unsupported | Unsupported |

Do not interpret a name being importable as a behavior guarantee. Runtime stubs
and no-op hints are called out in [PySpark differences](PYSPARK_DIFFERENCES.md).
Null ordering, schema inference, decimal/timestamp conversions, exceptions, and
query plans can differ. Run representative pipelines against the actual Spark
version used in production, including nulls, empty inputs, invalid data, and types.

Evidence and narrower contracts:

- The 4.19 DuckDB source is packaged by `sparkless-server-duckdb` and enabled by
  trusted `SPARKLESS_DUCKDB_SOURCES` configuration. The `duckdb-source` feature
  belongs to the Polars adapter crate and is not exposed by the base product.
  Public smoke and reference cases cover BOOLEAN,
  SMALLINT, INT, BIGINT (including integer extrema and non-null schema), finite
  DOUBLE, STRING, DATE, UTC microsecond TIMESTAMP,
  and exact decimal values at `DECIMAL(1,0)`, `(9,0)`, `(18,6)`, `(38,0)`, and
  `(38,38)`. These exercise low, intermediate, maximum precision, and maximum
  scale boundaries; they do not compare every valid precision/scale pair.
  Nested/unsigned values, non-finite DOUBLEs, writes, cross-owner joins, and
  arbitrary Connect SQL are rejected or unsupported. See the
  [4.19 source contract](plans/sparkless-server/RELEASE_4_19.md#duckdb-source-contract)
  and [candidate evidence](plans/sparkless-server/RELEASE_4_19_EVIDENCE.md).
- See [release evidence](RELEASE_EVIDENCE.md) for published packages and
  [4.19 development evidence](plans/sparkless-server/RELEASE_4_19_EVIDENCE.md)
  for the scope and revisions of cross-platform and resource measurements.
  Installed DuckDB wheels resolve the bundled Quack extension beside the server
  executable when `extension_path` is omitted.

- [Generated compatibility inventory](PARITY_STATUS.md): counts and oracle provenance, not a parity percentage.
- [Executable examples](https://github.com/eddiethedean/robin-sparkless/tree/main/examples/python): backend identity, pipeline, UDF.
- [UDF contexts](UDF_GUIDE.md), [catalog lifetime](PERSISTENCE_GUIDE.md), [external providers](EXTERNAL_CATALOGS.md).
- [HTTP reference](HTTP_API.md) and [Connect type/lifecycle matrix](SPARK_CONNECT.md).
- [Distribution features](INSTALLATION.md), [release evidence](RELEASE_EVIDENCE.md).

Unlisted APIs and combinations have **unknown** coverage; do not infer support
from older plans or PySpark documentation. Open a minimal compatibility issue or
add an oracle regression before relying on them.

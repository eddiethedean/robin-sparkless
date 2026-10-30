# 4.18.0 support contract

Reviewed against candidate source on 2026-10-29. **4.18.0 is a release candidate
until the package indexes publish it.** See [versions and installation](INSTALLATION.md).
“Implemented” describes a bounded API, not every Spark semantic or workload.
“Behavior-tested” means linked regression/example coverage exists. “Unsupported”
is outside the published contract; “unknown” requires workload validation.

| Capability | Local Python | Rust embedding | HTTP service | Spark Connect |
| --- | --- | --- | --- | --- |
| DataFrame filter/project/aggregate/join/window | Implemented, behavior-tested | Implemented, feature/context dependent | SQL allowlist only | Range/local relation, filter/project/limit/global count only |
| SQL queries | SQL enabled in official wheel | Requires `sql` | Bounded SELECT on service catalog | Unsupported SQL commands |
| Scalar Python UDF | Behavior-tested in withColumn/select/scalar comparison filters | Native expression callbacks, separate API | Unsupported | Unsupported |
| pandas/vectorized/grouped Python UDF, UDTF | Unsupported public API | No equivalent public Python contract | Unsupported | Unsupported |
| CSV/JSON/Parquet | Implemented | Implemented | No general file IO | Unsupported file sources |
| Delta | Compiled in default wheel | Requires `delta` | Unsupported | Unsupported |
| JDBC/SQLite | Requires native feature build | Driver-specific Cargo features | Unsupported | Unsupported |
| CREATE TABLE / INSERT / database DDL | SQL subset | SQL subset | Bounded mutations with identity | Unsupported commands |
| Warehouse persistence | Parquet with explicit warehouse config | Same engine mechanism | In-memory service catalog only | In-memory session/operation state |
| External catalog provider | Local callback contract | Core provider trait | No general provider configuration contract | Unsupported |
| Distributed scheduling / Structured Streaming | Unsupported | Unsupported | Unsupported | Unsupported |
| Repartition/cache/broadcast hints | Local compatibility behavior; no Spark cluster semantics | Local only | Unsupported | Unsupported |

Do not interpret a name being importable as a behavior guarantee. Runtime stubs
and no-op hints are called out in [PySpark differences](PYSPARK_DIFFERENCES.md).
Null ordering, schema inference, decimal/timestamp conversions, exceptions, and
query plans can differ. Run representative pipelines against the actual Spark
version used in production, including nulls, empty inputs, invalid data, and types.

Evidence and narrower contracts:

- [Generated compatibility inventory](PARITY_STATUS.md): counts and oracle provenance, not a parity percentage.
- [Executable examples](https://github.com/eddiethedean/robin-sparkless/tree/main/examples/python): backend identity, pipeline, UDF.
- [UDF contexts](UDF_GUIDE.md), [catalog lifetime](PERSISTENCE_GUIDE.md), [external providers](EXTERNAL_CATALOGS.md).
- [HTTP reference](HTTP_API.md) and [Connect type/lifecycle matrix](SPARK_CONNECT.md).
- [Distribution features](INSTALLATION.md), [release evidence](RELEASE_EVIDENCE.md).

Unlisted APIs and combinations have **unknown** coverage; do not infer support
from older plans or PySpark documentation. Open a minimal compatibility issue or
add an oracle regression before relying on them.

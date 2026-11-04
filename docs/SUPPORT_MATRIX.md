# 4.19.0 candidate support contract

Reviewed against branch `codex/release-4.19` on 2026-11-02. **4.19.0 is an
unpublished release candidate; this page describes only the behavior and
platforms covered by the retained candidate evidence.** See [versions and
installation](INSTALLATION.md).
“Implemented” describes a bounded API, not every Spark semantic or workload.
“Behavior-tested” means linked regression/example coverage exists. “Unsupported”
is outside the published contract; “unknown” requires workload validation.

The branch contains separate local, base-server, and DuckDB-server package
boundaries and a shared lightweight Python launcher. The
[Phase 7 record](plans/solid-refactor-4.19.md#phase-7-implementation-record-2026-11-03)
identifies the split-product revision that passed acceptance. The subsequent
[review fixes](plans/sparkless-server/RELEASE_4_19_EVIDENCE.md#local-review-verification)
have local regression coverage. Both 4.19 server products execute through Polars.

| Capability | Local Python | Rust embedding | Base server | DuckDB server | Spark Connect |
| --- | --- | --- | --- | --- | --- |
| DataFrame filter/project/aggregate/join/window | Implemented, behavior-tested | Implemented, feature/context dependent | SQL allowlist only | Same Polars SQL service surface | [Supported Connect plan subset](SPARK_CONNECT.md#phase-419-branch-additions-pre-release) for range, inline DataFrames, and configured table sources |
| SQL queries | SQL enabled in official wheel | Requires `sql` | Bounded SELECT on managed catalog | Bounded SELECT on managed and configured source catalogs | Unsupported SQL commands |
| Scalar Python UDF | Behavior-tested in withColumn/select/scalar comparison filters | Native expression callbacks, separate API | Unsupported | Unsupported | Unsupported |
| pandas/vectorized/grouped Python UDF, UDTF | Unsupported public API | No equivalent public Python contract | Unsupported | Unsupported | Unsupported |
| CSV/JSON/Parquet | Implemented | Implemented | No general file IO | No general file IO | Unsupported file sources |
| Delta | Compiled in default wheel | Requires `delta` | Unsupported | Unsupported | Unsupported |
| JDBC/SQLite | Requires native feature build | Driver-specific Cargo features | Unsupported | Unsupported | Unsupported |
| CREATE TABLE / INSERT / database DDL | SQL subset | SQL subset | Bounded mutations with identity | Same bounded mutations | Unsupported commands |
| Warehouse persistence | Parquet with explicit warehouse config | Same engine mechanism | In-memory service catalog only | In-memory service catalog only | In-memory session/operation state |
| External catalog provider | Local callback contract | Core provider trait | No general provider configuration contract | No general provider configuration contract | Unsupported |
| Optional DuckDB table source | Not applicable | `robin-sparkless-duckdb`; optional source feature | Unsupported; rejects DuckDB source configuration | Trusted read-only standalone or shared Quack owner; no client-selected paths or credentials | Available only through the DuckDB server product; tested source subset below |
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
- The retained local macOS wheel and public standalone/Quack source smokes
  validate the **pre-separation** DuckDB-enabled candidate only. They do not
  certify the new base or DuckDB product wheels, whose exact-candidate checks
  remain pending under V59. Historical wheel contents, tags, extension hashes,
  and smoke results are recorded in the
  [candidate evidence](plans/sparkless-server/RELEASE_4_19_EVIDENCE.md); do not
  reuse those hashes as evidence for the split products. Execution on an actual
  macOS 14 host also remains open.
- The earlier exact-commit
  [release workflow run 41351811918](https://github.com/eddiethedean/robin-sparkless/actions/runs/41351811918)
  passed installed Linux x86_64 and macOS arm64 server-extra source smokes and a
  four-reader, 100k-row slow-consumer run on each platform. This is historical
  context only for current implementation work. Every measured reader returned
  all rows; observed server-child RSS was 192.9 MiB on Linux and 137.8 MiB on
  macOS. These are single-run observations, not a hard or broad cross-platform
  RSS guarantee. Retained raw reports and limits are in the
  [4.19 resource evidence](plans/sparkless-server/RELEASE_4_19_EVIDENCE.md#exact-commit-linux-and-macos-source-extra-resource-checks-2026-11-01).
  A current packaged-wheel release build on macOS arm64 completed three
  measured rounds of the 100k- and 350k-row four-reader slow-consumer cohorts;
  all 24 measured reader actions completed with zero result-cap rejections and
  peak server RSS of 136.6 MiB and 209.8 MiB respectively. See the
  [current local reports](plans/sparkless-server/RELEASE_4_19_EVIDENCE.md#current-packaged-source-100k350k-slow-reader-rerun-2026-11-02).
  Repeated current-source four-reader 1M-row macOS scans returned all rows at
  active limits 1/2/4. Twenty measured actions per setting across three fresh
  server processes peaked at 257.8/281.6/325.5 MiB, respectively, with zero
  result-cap rejections. A provisional 384 MiB guardrail covers this specific
  Mac workload; it is not enforced or portable. See the
  [repeated 1M-row reports](plans/sparkless-server/RELEASE_4_19_EVIDENCE.md#repeated-current-source-one-million-row-slow-reader-runs-2026-11-01).
  The exact current packaged candidate also completed one local 1M-row round
  at active limits 1/2/4, with all twelve reader actions returning every row
  and zero cap rejections; peak RSS was 257.4/290.1/320.8 MiB. These are
  single-round macOS results, not portable RSS guarantees. See the
  [current boundary reports](plans/sparkless-server/RELEASE_4_19_EVIDENCE.md#current-packaged-source-one-million-row-boundary-rerun-2026-11-02).
  Current-source Linux x86_64 standalone smoke, generic `linux_x86_64` wheel
  verifier, and one resource round pass under Docker emulation. These checks do
  not certify the `manylinux_2_28_x86_64` wheel. Native Linux, Linux Quack, and
  broader resource acceptance remain open.
  A source-built server can load a trusted local extension path. Installed
  wheels resolve the bundled extension beside the server executable when
  `extension_path` is omitted, without a runtime download.

- [Generated compatibility inventory](PARITY_STATUS.md): counts and oracle provenance, not a parity percentage.
- [Executable examples](https://github.com/eddiethedean/robin-sparkless/tree/main/examples/python): backend identity, pipeline, UDF.
- [UDF contexts](UDF_GUIDE.md), [catalog lifetime](PERSISTENCE_GUIDE.md), [external providers](EXTERNAL_CATALOGS.md).
- [HTTP reference](HTTP_API.md) and [Connect type/lifecycle matrix](SPARK_CONNECT.md).
- [Distribution features](INSTALLATION.md), [release evidence](RELEASE_EVIDENCE.md).

Unlisted APIs and combinations have **unknown** coverage; do not infer support
from older plans or PySpark documentation. Open a minimal compatibility issue or
add an oracle regression before relying on them.

# SS-17: Connect sources, artifacts, caching, and checkpoints

> **Status: planned; owner unassigned.** Phase: 8; target 4.24.0; connectors and storage access explicitly enabled.
> [Index](README.md) · [Roadmap](ROADMAP.md) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome and scope

Support public Connect readers/writers for CSV, JSON, and Parquet, plus an optional JDBC reader/writer connector. Define exact schema/inference, partition, mode, option, path/URI, and transaction behavior per source/sink. Delta/UC remains governed by SS-13; a Delta-enabled deployment must also test its public Connect load/save path here. Implement named table/catalog and temporary-view API paths needed by these operations on each advertised backend; persistent metadata must have an explicit owner and restart contract.

Implement bounded `AddArtifacts`, `ArtifactStatus`, and `CachedLocalRelation`, including the public client's normal cache-threshold crossing. Add DataFrame `cache`/`persist`/`unpersist`, truthful storage status, and durable/local DataFrame checkpoints with separately documented lifetimes. A checkpoint materializes data; it does not by itself preserve operation replay across restart.

## Dependencies and boundaries

Use SS-16's expanded plan/type matrix and SS-04/07/10 lifecycle/accounting. Gate each optional source independently; native file support must not require DuckDB, Delta, Python workers, or JDBC. Keep the core native server/image Python/JVM-free. Actual JDBC drivers may use a separately packaged, opt-in JVM connector process; freeze that RPC, driver distribution, trust, resource, and process-tree contract before enabling JDBC. A native database driver must not be mislabeled as JDBC.

Sources, destinations, driver artifacts, credentials, and extension loading require explicit server policy. Do not grant arbitrary filesystem/network access merely because a client can submit an option or artifact. Never execute uploaded Python/JAR/native code inside the core process. Reuse SS-12 worker isolation requirements for optional connector processes without making Python a JDBC dependency.

| Slice | Deliverable | Exit evidence |
| --- | --- | --- |
| 424-01 | Freeze D17 source/sink/artifact/cache/checkpoint contracts | Public API fixtures; source/platform matrix; paths, credentials, limits, atomicity, lifetime, and optional connector design |
| 424-02 | Implement CSV/JSON/Parquet and optional JDBC reads/writes | Typed empty/nonempty round trips, inference/options, append/overwrite/error modes, independent readers and database clients |
| 424-03 | Implement artifact transfer and cached local relations | Real client cache-threshold crossing, chunk/digest/status correctness, bounded staging and decoded expansion |
| 424-04 | Implement cache/persistence/checkpoint ownership and APIs | Observable reuse, invalidation/eviction, storage levels, disk quotas, session close and restart behavior |
| 424-05 | Complete V41–V43 and optional/default packaged installs | Coordinated 4.24.0 artifacts and disabled-feature regressions |

## Acceptance

- **V41:** unmodified-client load/save/table APIs match the declared reference semantics. Validate file contents with independent readers and JDBC effects with an independent database client. Cover malformed data, schema drift, partition discovery, permissions, missing files, retries, partial writes, commit/cancel/disconnect races, credentials, and connector crash/cleanup. No successful partial writes or invented atomicity guarantees.
- **V42:** real artifact-backed `createDataFrame` succeeds without changing client defaults. Cover duplicate/partial uploads, hash mismatch, rejected content, concurrent sessions, expiry, quota exhaustion, cancellation, and disconnect. Bound uploaded, staged, decoded, and retained bytes independently; reject before unaccounted allocation.
- **V43:** demonstrate cache reuse and correct recomputation/invalidation; test cache pressure, unpersist, checkpoints, source changes, restart, access isolation, and orphan cleanup. Measure all disk/memory retention, including checkpoints and artifact-backed caches. Expose supported storage semantics honestly.
- Reuse earlier operation identity and outcome rules for writes. Until SS-18 passes, crash-unknown writes and cross-restart retries remain explicitly unsupported even when data/checkpoints survive.

Structured Streaming checkpoints belong to SS-19. This phase enables batch I/O and DataFrame checkpoints only.

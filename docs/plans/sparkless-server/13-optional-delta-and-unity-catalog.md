# SS-13: Optional native Delta Lake and Unity Catalog

> **Status: planned; owner unassigned.** Phase: 4; optional.
> [Index](README.md) · [Epic contract](EPIC.md#acceptance-and-release-gates) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome

Add explicitly enabled native storage/catalog interoperability with independently verified transactions.

## Dependencies and delivery slices

SS-07 catalog/mutation contracts, SS-08 exact types, SS-11 reproducible integration infrastructure. Python workers are not a dependency.

Plan numbers are workstream identifiers, not a strict execution order. Only the applicable phase slice blocks a milestone; later extensions must not delay an earlier gate.

## Implementation sequence

1. Select native Delta/UC dependencies and compatible protocol/storage versions; document feature flags and exact tested combinations.

2. Map identifiers, schemas, decimal/timestamp types, credentials, and catalog ownership through the explicit service catalog.

3. Define supported DDL/DML and atomic commit/conflict semantics; reject unsupported writes before execution.

4. Verify data files and _delta_log using an independent reader. Validate schema, precision, transaction visibility, and restart recovery.

5. Exercise concurrent writers, failed commits, cancellation near commit, external catalog failures, and reader consistency.

6. Document supported storage/network policy and enabled-image dependencies while preserving the default lightweight native service.

7. Implement the [SS-14 MiniLake adapter](14-minilake-interoperability.md#catalog-and-storage-contract) in Rust using existing `ExternalCatalogProvider` and UC HTTP endpoints. Start with EXTERNAL DELTA tables on a shared local `/data` volume, not Java connectors, managed DuckDB files, or assumed cloud storage support. Verify provider installation per session, exact schemas/snapshot identity, missing/error distinctions, and drop/recreate behavior.

8. Validate path write → separate SDK registration → MiniLake/DuckDB read → native named-table read. Define committed-but-unregistered recovery; provider hooks alone do not implement remote writer commands or creation/DDL. MiniLake-generated classic Delta DML remains unsupported unless a structured native delegation hook is agreed and V36 passes.

## Acceptance criteria

- [ ] Independent readers validate schema and transactions, including decimals and time types.
- [ ] Unsupported DDL/writes cannot return success; conflicts and partial failures have truthful outcomes.
- [ ] Core releases remain usable without enabling Delta/UC or Python workers.
- [ ] If advertising MiniLake shared data/catalogs, V34–V35 independently prove interoperability without MiniLake catalog changes or Java/JVM. V36 gates optional MiniLake SQL Delta DML; no silent Spark fallback.

## Decisions and implementation touchpoints

Resolve or explicitly defer D09, D10, D13 in the [decision register](DECISIONS.md). Record evidence and immutable revisions in the [validation matrix](VALIDATION.md); writing this plan does not complete a gate.

- [src/lib.rs](https://github.com/eddiethedean/robin-sparkless/blob/main/src/lib.rs)
- [docs/EXTERNAL_CATALOGS.md](../../EXTERNAL_CATALOGS.md)
- [docs/PERSISTENCE_GUIDE.md](../../PERSISTENCE_GUIDE.md)

## Scope notes

Phase numbering describes the roadmap; Delta/UC does not inherently require the optional Python worker phase to ship first.

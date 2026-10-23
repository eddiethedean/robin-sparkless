# SS-01: Compatibility contracts and emulator results

> **Status: planned; owner unassigned.** Phase: 0A, with integrated SDK gate in 0B.
> [Index](README.md) · [Epic contract](EPIC.md#phase-0a-contracts-and-feasibility) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome

Own the version/semantics manifest and the external emulator prerequisite. The SDK must receive exact typed results through its public result objects.

## Dependencies and delivery slices

No implementation prerequisite for the contract inventory; integrated validation needs SS-05.

Plan numbers are workstream identifiers, not a strict execution order. Only the applicable phase slice blocks a milestone; later extensions must not delay an earlier gate.

## Implementation sequence

1. Inventory one exact pyspark-client release in the Spark 4.0 family and its matching proto tag/commit, Python range, gRPC/protobuf/PyArrow dependencies, and reference-server version. Record all selections in DECISIONS.md with lockfile locations and evidence.

2. Specify SQL semantic profile independently of the wire version: ANSI, timezone, case sensitivity, decimals, and allowed configuration keys. Keep launcher, agent-envelope, package, protocol, and engine versions distinct.

3. In the emulator, preserve the typed agent envelope using typed decoding, json.Number, or raw JSON. Map positional rows and schema to result.data_array and manifest.schema; do not route numbers through float64.

4. Track the upstream issue/PR, owner, release/revision, and image digest. The current repo cannot implement that external change by adding a Rust endpoint.

5. Build public SDK decoder fixtures for ordered columns, duplicate labels, empty typed results, nulls, BIGINT 9007199254740993, and high-precision decimals. Keep non-null JSON_ARRAY cells in the SDK's documented string form.

6. Capture the emulator's forwarded/re-written SQL and warehouse-session behavior. Run integrated fixtures against the corrected pinned emulator once SS-05 is ready.

## Acceptance criteria

- [ ] An immutable compatibility manifest and SDK decoder fixtures exist; no invented pins or claimed support.
- [ ] The external fix has a tracked owner and implementation. Its corrected build is required for Phase 0B completion.
- [ ] The unmodified SDK reads actual values and schema using result.data_array and manifest.schema, without custom result.text decoding.

## Decisions and implementation touchpoints

Resolve or explicitly defer D01, D02, D03, D04 in the [decision register](DECISIONS.md). Record evidence and immutable revisions in the [validation matrix](VALIDATION.md); writing this plan does not complete a gate.

- [python/pyproject.toml](https://github.com/eddiethedean/robin-sparkless/blob/main/python/pyproject.toml)
- [crates/robin-sparkless-core/src/runtime_config.rs](https://github.com/eddiethedean/robin-sparkless/blob/main/crates/robin-sparkless-core/src/runtime_config.rs)

## Scope notes

Do not create upstream issues or send external messages as a side effect of writing these documents. Assign the external work during implementation.

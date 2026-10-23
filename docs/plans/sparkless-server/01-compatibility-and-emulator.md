# SS-01: Compatibility contracts and emulator results

> **Status: partially prepared; optional external certification pending.** Phase: 0A integration evaluation; does not block 4.16 native HTTP publication.
> [Index](README.md) · [Epic contract](EPIC.md#phase-0a-contracts-and-feasibility) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome

Own the native version/semantics manifest and optional external emulator certification. A certified SDK route must deliver exact typed results through public result objects; upstream changes and certification do not block native HTTP release readiness.

## Dependencies and delivery slices

No implementation prerequisite for the contract inventory; integrated validation needs SS-05.

Plan numbers are workstream identifiers, not a strict execution order. Only the applicable phase slice blocks a milestone; later extensions must not delay an earlier gate.

## Implementation sequence

1. The candidate compatibility selection is recorded in [COMPATIBILITY_4_16.json](COMPATIBILITY_4_16.json): `pyspark-client==4.0.4`, Apache Spark protocol/reference commit `c7d67e3f5d4c9d88a480367b44fc54d26adf99ab`, and a pinned CPython 3.11 integration environment. The package lock is [requirements-connect-4.0.4.lock](../../../scripts/server-tests/requirements-connect-4.0.4.lock). It is install-tested; real Connect and oracle traces are still required.

2. Specify SQL semantic profile independently of the wire version: ANSI, timezone, case sensitivity, decimals, and allowed configuration keys. Keep launcher, agent-envelope, package, protocol, and engine versions distinct.

3. In the emulator, preserve the typed agent envelope using typed decoding, json.Number, or raw JSON. Map positional rows and schema to result.data_array and manifest.schema; do not route numbers through float64.

4. The inspected emulator revision is recorded in the manifest, but it still converts the typed agent's `application/json` payload into `result.text`. The corrected upstream revision, maintainer owner, issue/PR, and image digest remain missing. Track these items for optional route certification; none is a native publication dependency.

5. Build public SDK decoder fixtures for ordered columns, duplicate labels, empty typed results, nulls, BIGINT 9007199254740993, and high-precision decimals. Keep non-null JSON_ARRAY cells in the SDK's documented string form.

6. Capture the emulator's forwarded/re-written SQL and warehouse-session behavior. Run integrated fixtures against the corrected pinned emulator once SS-05 is ready.

## Acceptance criteria

- [x] An immutable candidate compatibility manifest, exact dependency versions, and public SDK model fixtures exist; direct model fixtures are not integrated emulator evidence.
- [ ] Before certifying the external route, the fix has a tracked owner, implementation, and immutable corrected build. This is independent of Phase 0B publication.
- [ ] The unmodified SDK reads actual values and schema using result.data_array and manifest.schema, without custom result.text decoding.

## Decisions and implementation touchpoints

Resolve or explicitly defer D01, D02, D03, D04 in the [decision register](DECISIONS.md). Record evidence and immutable revisions in the [validation matrix](VALIDATION.md); writing this plan does not complete a gate.

- [python/pyproject.toml](https://github.com/eddiethedean/robin-sparkless/blob/main/python/pyproject.toml)
- [crates/robin-sparkless-core/src/runtime_config.rs](https://github.com/eddiethedean/robin-sparkless/blob/main/crates/robin-sparkless-core/src/runtime_config.rs)

## Scope notes

Do not create upstream issues or send external messages as a side effect of writing these documents. Assign the external work during implementation.

## MiniLake contract inventory

Include [SS-14](14-minilake-interoperability.md) as a separate integration target: pin its wheel/source, runner image/client dependencies, SDK version, and native capabilities before route certification. The reviewed MiniLake revision is a source baseline only, not tested evidence. Its existing Jobs/environment/catalog seams are the preferred contract; target no source changes for initial Jobs and external-table reads.

The optional Go numeric-decoding/`result.text` integration work above belongs specifically to `calvinchengx/databricks-emulator`. MiniLake SQL remains on DuckDB and cannot satisfy optional HTTP-agent SDK certification. MiniLake Jobs evidence also does not certify an emulator proxy. Track its V31–V36 separately; no MiniLake feature is a new 0B release requirement.

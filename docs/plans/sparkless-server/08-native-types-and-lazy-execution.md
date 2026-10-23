# SS-08: Logical types and lazy native execution

> **Status: planned; owner unassigned.** Phase: Scalar/schema fixes in 0B; expanded types/lazy paths in 1–2.
> [Index](README.md) · [Epic contract](EPIC.md#native-engine-and-type-prerequisites) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome

Make the existing engine's schema and execution behavior sufficient for truthful protocol adapters.

## Dependencies and delivery slices

SS-01 supported semantics; SS-03 observed Connect requirements. Coordinate with SS-05/09/10 before advertising types.

Plan numbers are workstream identifiers, not a strict execution order. Only the applicable phase slice blocks a milestone; later extensions must not delay an earlier gate.

## Implementation sequence

1. Inventory losses in the core schema and Polars conversion: FLOAT32 widening, missing BYTE/NTZ, field/nested nullability, metadata, map representation, and default string fallbacks.

2. Repair the minimum scalar schema path required by HTTP conformance in 0B. Reject unrepresented types rather than delaying necessary repairs until Phase 1.

3. Preserve richer logical schema metadata through plan transformations and map it consistently to analysis, HTTP schema, and Arrow IPC.

4. Audit range, count, input decoding, schema inference, and each exposed transform for eager allocation/materialization. Put expensive planning under SS-04 budgets.

5. Implement lazy range and aggregate count paths or documented bounded alternatives; preserve overflow/step/error semantics and verify execution placement.

6. Test the chosen Polars/Arrow IPC boundary, encoded-size limits, decimals, timestamp units/timezones, nested nullability, and oversized individual values.

7. Measure intermediate memory and allocations, not only serialized output size. Record unsupported behavior in capabilities.

## Acceptance criteria

- [ ] HTTP scalar values/schema agree before Phase 0B completion.
- [ ] Enabled types round-trip without silent widening/stringification or lost nullability; unsupported types fail explicitly.
- [ ] Large range/count tests demonstrate the chosen lazy/bounded behavior and planning stays off I/O workers.
- [ ] Schema analysis and executed payloads agree for empty and nonempty results.

## Decisions and implementation touchpoints

Resolve or explicitly defer D02, D04, D11 in the [decision register](DECISIONS.md). Record evidence and immutable revisions in the [validation matrix](VALIDATION.md); writing this plan does not complete a gate.

- [crates/robin-sparkless-core/src/schema.rs](https://github.com/eddiethedean/robin-sparkless/blob/main/crates/robin-sparkless-core/src/schema.rs)
- [crates/robin-sparkless-polars/src/schema_conv.rs](https://github.com/eddiethedean/robin-sparkless/blob/main/crates/robin-sparkless-polars/src/schema_conv.rs)
- [crates/robin-sparkless-polars/src/session/mod.rs](https://github.com/eddiethedean/robin-sparkless/blob/main/crates/robin-sparkless-polars/src/session/mod.rs)
- [crates/robin-sparkless-polars/src/dataframe/mod.rs](https://github.com/eddiethedean/robin-sparkless/blob/main/crates/robin-sparkless-polars/src/dataframe/mod.rs)

## Scope notes

Existing JSON execute_plan and collect_batches are useful components, not proof of full Connect plan coverage or hard intermediate-memory bounds.

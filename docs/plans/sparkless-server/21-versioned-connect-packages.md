---
search:
  exclude: true
---

> **Status: planned; owner unassigned.** Phases 11–14; targets 4.27.0–4.30.0. These are scope milestones, not release dates or current compatibility claims.
> Use the [current support contract](../../SUPPORT_MATRIX.md) for released behavior.

# SS-21: Versioned Sparkless Connect server compatibility packages

[Plan index](README.md) · [Roadmap](ROADMAP.md) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome and placement

Publish `sparkless-connect40` only after all planned server features emulating the declared PySpark 4.0 surface are complete. Phases 3–10, their resolved feature matrices in [SS-20](20-connect-feature-scope.md), and applicable native/HTTP/Connect, optional-backend/worker/storage, recovery and streaming gates must pass first. Proposed function lists and unresolved phase decisions must be finalized and implemented before certification; shipping a wrapper around the existing limited server does not satisfy this milestone.

Then complete one separate phase for each additional requested upstream line: `sparkless-connect35`, `sparkless-connect41`, and `sparkless-connect42`. The 4.0 baseline ships first because the existing client/protocol/reference work targets that line. The older 3.5 profile follows as a compatibility backport. There are no released Spark 3.6–3.9 lines; do not create packages or phases for those nonexistent upstream targets. Apache's [documentation inventory](https://spark.apache.org/documentation.html) and [release history](https://spark.apache.org/releases/) establish the upstream lines.

Package suffixes identify the emulated Spark major/minor, while distribution versions follow Sparkless's coordinated release number. `sparkless-connect40==4.27.0` means a Sparkless 4.27.0 distribution certified against a selected exact Spark 4.0 patch. It does not mean Spark 4.27 or complete Apache Spark parity. Existing exclusions, including RDD/SparkContext and distributed execution, remain explicit.

## One phase per upstream version

| Phase | Sparkless target | New distribution | Required implementation and evidence | Gates |
| --- | --- | --- | --- | --- |
| **11** | **4.27.0** | `sparkless-connect40` | Finish and certify the entire planned 4.0 server matrix from Phases 3–10. Freeze an exact 4.0 client/protocol/oracle patch, implement explicit server profile selection and package/runtime checks, package SparklessConnect with exact matching server dependencies, and pass the full feature and lifecycle corpus through the real client. | V48/V49 |
| **12** | **4.28.0** | `sparkless-connect35` | Audit 3.5 versus the certified 4.0 baseline: client encoders/RPCs, types, configuration, SQL/ANSI semantics, Arrow, UDF payloads, errors, replay, and streaming. Implement an isolated 3.5 server profile and its applicable feature matrix; identify later-only APIs explicitly rather than pretending 3.5 exposes them. Certify the actual 3.5 client distribution and Python/dependency range. | V50/V51 |
| **13** | **4.29.0** | `sparkless-connect41` | Diff the exact 4.1 client/protocol/reference against 4.0. Enumerate every changed/new API, function, SQL construct, type, setting and lifecycle behavior. Implement all deltas admitted to the planned single-node feature scope; explicitly classify remaining upstream features as excluded or separately scheduled. Certify the resulting 4.1 profile and rerun earlier profiles. | V52/V53 |
| **14** | **4.30.0** | `sparkless-connect42` | Repeat the complete source/API/protocol/semantic delta audit for 4.2 versus 4.1, implement the admitted feature deltas, freeze exact pins and certify the 4.2 profile. Regress 3.5, 4.0 and 4.1 independently before publication. | V54/V55 |

The client/protocol differences for 3.5, 4.1 and 4.2 are research deliverables of their phases, not presumed equivalent to 4.0. Each audit produces a machine-readable disposition inventory with upstream source references, implementation owner, fixture and status for every item. No unclassified difference may remain at a phase exit. Later phases retain support for previously certified profiles; they do not widen an earlier profile's semantics silently.

## Packaging and runtime contract

- Maintain one native server codebase and a shared `SparklessConnect` implementation. Version packages select server compatibility profiles and carry only genuinely necessary adapters; no server fork per upstream version.
- Pin the distribution, native binary and common launcher to the same exact Sparkless release. Pin the upstream client to one exact tested patch; hash-lock full dependency and oracle environments in CI. Upstream patches retain their own version numbers. Select a maintained, tested patch at each release, rather than permanently freezing the current 4.0.5 research pin.
- Use `pyspark-client` for certified 4.x profiles where available. Spark 3.5's documented client installation is `pyspark[connect]`; investigate and pin that actual distribution rather than inventing `pyspark-client==3.5.*`. Its larger dependency footprint and supported Python platforms must be documented. See [Apache 3.5 installation](https://spark.apache.org/docs/3.5.7/api/python/getting_started/install.html).
- Set Python requirements to the tested intersection of the selected upstream client and Sparkless requirements. Certify each line in a separate environment; multiple upstream clients sharing the `pyspark` namespace are not a supported way to run multiple profiles in one Python environment. Define separate profile metadata without overlapping installed files and provide actionable conflict/version errors.
- `SparklessConnect` starts the exact owned server with the requested compatibility profile, verifies the binary version and profile capability handshake, returns a real upstream Connect SparkSession, and stops session then server on normal/exceptional exit. Missing or mismatched profile support fails before session execution; do not guess the profile from an incoming protobuf message.
- Bind each session and operation to its immutable compatibility profile. Include it in analysis/configuration snapshots, identity, replay and durable-recovery compatibility. Unsupported cross-profile reuse and incompatible restored state fail explicitly.
- Publish a per-profile feature matrix for default Polars and enabled DuckDB/Delta/worker/JDBC/streaming combinations. Optional features must be implemented and certified for the phase baseline but remain opt-in installation/runtime capabilities. The default compatibility package must not silently install every worker or connector.
- Join the coordinated publication inventory on introduction. Each later release republishes existing first-party compatibility distributions with exact matching internal requirements. Build/verify binaries and common packages before publishing the profile packages; verify fresh public installs afterwards. A dependency pin alone is not a server compatibility gate.

## Delivery sequence within every version phase

1. Freeze D20's exact patch/source hashes, Python/dependency/platform matrix, upstream API disposition inventory, profile semantics and installation model.
2. Capture unmodified-client fixtures for all admitted APIs/RPCs and reference results/errors/schema/configuration from the matching Apache Spark oracle. Record semantic differences from earlier profiles.
3. Implement profile-specific protocol, analysis, execution, type/Arrow, UDF, storage, recovery and streaming changes. Reuse implementation only where the source audit and differential tests demonstrate equivalent behavior.
4. Run the complete certified feature matrix, optional deployment combinations, resource budgets and lifecycle/fault corpus. Include unsupported descendants, null/NaN/ANSI/decimal/time cases, identity and isolation, cross-profile rejection, crash recovery, and server/process cleanup.
5. Build all coordinated artifacts; perform clean-environment resolver/install/uninstall, outside-checkout SparklessConnect usage and platform checks. Publish only after the candidate gate passes, then verify fresh exact-version public artifacts and record the evidence.

## Acceptance

V48/V50/V52/V54 certify the named server profile and all admitted version-specific features against that version's client and reference engine. V49/V51/V53/V55 certify packaging, exact internal/client pins, fresh candidate/public installation, lifecycle/profile handshake and regressions of every earlier profile. Failed optional combinations remain uncertified and cannot be advertised as supported. No wrapper-only smoke test or successful dependency resolution completes a version phase.

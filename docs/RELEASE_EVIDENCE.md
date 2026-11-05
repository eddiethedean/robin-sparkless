# Release evidence

## 4.19.0 public artifacts — 2026-11-05 UTC

Source tag [v4.19.0](https://github.com/eddiethedean/robin-sparkless/tree/v4.19.0)
identifies commit [43b0fbff](https://github.com/eddiethedean/robin-sparkless/commit/43b0fbff3c1b19e67774be5230d4c059ea1c3cb0).
[CI run 41921768435](https://github.com/eddiethedean/robin-sparkless/actions/runs/41921768435)
passed on that exact SHA. The tag-triggered
[Release run 41930107216](https://github.com/eddiethedean/robin-sparkless/actions/runs/41930107216)
passed release acceptance and published the Rust crates, testing helpers,
launcher, native engine, server wheels, and container images. The `sparkless`
API upload step also succeeded, but the run failed during its post-upload
candidate inventory check: PyPI returned `.publish.attestation` sidecars with
the wheel and source archive, and the verifier treated those extra files as an
inventory mismatch. Coordinated public hash verification therefore did not
complete in that run. The PyPI file pages below show the currently listed
artifacts; do not treat the failed final check as a successful hash audit.

| Distribution | Files in the public index |
| --- | --- |
| [sparkless](https://pypi.org/project/sparkless/4.19.0/#files) | API wheel and source archive |
| [sparkless-native](https://pypi.org/project/sparkless-native/4.19.0/#files) | Eight native wheels and source archive |
| [sparkless-connect](https://pypi.org/project/sparkless-connect/4.19.0/#files) | Launcher wheel and source archive |
| [sparkless-testing](https://pypi.org/project/sparkless-testing/4.19.0/#files) | Testing wheel and source archive |
| [sparkless-server](https://pypi.org/project/sparkless-server/4.19.0/#files) | Linux x86_64 and macOS arm64 wheels |
| [sparkless-server-duckdb](https://pypi.org/project/sparkless-server-duckdb/4.19.0/#files) | Linux x86_64 and macOS arm64 wheels |
| [`ghcr.io/eddiethedean/sparkless-server:v4.19.0`](https://github.com/eddiethedean/robin-sparkless/pkgs/container/sparkless-server) | Published and smoke-tested; digest `sha256:bd872a2951e823cf97f571a58ddd4962e6c8e908212c786e893a81f485ee4103` |
| [`ghcr.io/eddiethedean/sparkless-server-duckdb:v4.19.0`](https://github.com/eddiethedean/robin-sparkless/pkgs/container/sparkless-server-duckdb) | Published and smoke-tested; digest `sha256:6a86395161d77c05a8a3927b8d252f7fbde8528e38e89d8fb6edafe0b0285c09` |
| [robin-sparkless](https://crates.io/crates/robin-sparkless/4.19.0) | Rust facade crate |
| [spark-sql-parser](https://crates.io/crates/spark-sql-parser/4.19.0), [robin-sparkless-core](https://crates.io/crates/robin-sparkless-core/4.19.0), [robin-sparkless-polars](https://crates.io/crates/robin-sparkless-polars/4.19.0), [robin-sparkless-duckdb](https://crates.io/crates/robin-sparkless-duckdb/4.19.0) | Parser, engine contracts, Polars backend, and DuckDB source crates |
| [robin-sparkless-plan](https://crates.io/crates/robin-sparkless-plan/4.19.0), [robin-sparkless-sql](https://crates.io/crates/robin-sparkless-sql/4.19.0), [robin-sparkless-delta](https://crates.io/crates/robin-sparkless-delta/4.19.0), [robin-sparkless-jdbc](https://crates.io/crates/robin-sparkless-jdbc/4.19.0), [robin-sparkless-jdbc-polars](https://crates.io/crates/robin-sparkless-jdbc-polars/4.19.0) | Plan, SQL, Delta, and JDBC crates |
| [sparkless-server](https://crates.io/crates/sparkless-server/4.19.0), [sparkless-server-duckdb](https://crates.io/crates/sparkless-server-duckdb/4.19.0) | Rust server products |
| [sparkless-server-core](https://crates.io/crates/sparkless-server-core/4.19.0), [sparkless-server-runtime](https://crates.io/crates/sparkless-server-runtime/4.19.0), [sparkless-server-http](https://crates.io/crates/sparkless-server-http/4.19.0), [sparkless-server-connect](https://crates.io/crates/sparkless-server-connect/4.19.0), [sparkless-server-polars](https://crates.io/crates/sparkless-server-polars/4.19.0) | Server contracts, runtime, transports, and adapter crates |

The PyPI file pages show checksums and per-file provenance when available. The
[development evidence](plans/sparkless-server/RELEASE_4_19_EVIDENCE.md) records
build, differential, and resource results for the exact revisions listed
there. Use the [support matrix](SUPPORT_MATRIX.md) to interpret that coverage.
The [Rust API guide](RUST_API_REFERENCE.md) gives a version-pinned local
documentation command while the 4.19.0 docs.rs API page is unavailable.

## 4.18.1 publication outcome — 2026-10-31

Tag [`v4.18.1`](https://github.com/eddiethedean/robin-sparkless/tree/v4.18.1)
points to commit [`0ad225677ec47b20c9c6225fa9f4b73b00e54aba`](https://github.com/eddiethedean/robin-sparkless/commit/0ad225677ec47b20c9c6225fa9f4b73b00e54aba).
[CI run 41168209494](https://github.com/eddiethedean/robin-sparkless/actions/runs/41168209494)
and the tag-triggered [Release run 41170498256](https://github.com/eddiethedean/robin-sparkless/actions/runs/41170498256)
passed on that exact SHA. The release acceptance gate, all platform builds and
smokes, and post-publication file/hash verification succeeded.

The initial tag run 41162761208 stopped before Python publication because Cargo
reported already-published 4.18.0 crates as “already exists on crates.io index.”
[PR #1734](https://github.com/eddiethedean/robin-sparkless/pull/1734) taught the
publisher to recognize that duplicate response while still failing other Cargo
errors. Since that first attempt had not uploaded Python packages, the existing
`v4.18.1` tag was moved to the corrected, merged, CI-verified commit above before
the successful release rerun.

| Component | Published state |
| --- | --- |
| [`sparkless==4.18.1`](https://pypi.org/project/sparkless/4.18.1/) | Universal wheel and source archive published; public filenames and SHA-256 hashes verified |
| [`sparkless-native==4.18.0`](https://pypi.org/project/sparkless-native/4.18.0/) | Native wheel matrix and source archive published; public inventory verified |
| [`sparkless-server-bin==4.18.0`](https://pypi.org/project/sparkless-server-bin/4.18.0/) | Existing supported wheels reused; public filenames and SHA-256 hashes verified |
| Five first-party crates, 4.18.0 | Already present on crates.io; duplicate skips and final version verification passed |
| `ghcr.io/eddiethedean/sparkless-server:v4.18.1` | Published and smoke-tested; digest `sha256:0eece842823e308c727fa793472b992d5f05fa76bc34213bf040f8b3956e3736` |

The exact-SHA size preflight passed PyPI's 100 MiB per-file and 10 GiB per-project
default limits:

| PyPI project | Current storage | Added | Projected storage |
| --- | ---: | ---: | ---: |
| `sparkless` | 9.987 GiB | 0.13 MiB | 9.987 GiB |
| `sparkless-native` | 0.000 GiB | 324.61 MiB | 0.317 GiB |
| `sparkless-server-bin` | 0.283 GiB | 0.00 MiB | 0.283 GiB |

The 4.18.1 API package is a small universal distribution that pins the separate
4.18.0 native engine and optional server binary. The 4.18.0 release remains
partially published as originally recorded below; no historical files were
removed. See [the 4.18.1 PyPI page](https://pypi.org/project/sparkless/4.18.1/)
and [release workflow](https://github.com/eddiethedean/robin-sparkless/actions/runs/41170498256)
for the public record.

## 4.18.0 publication outcome — 2026-10-30

Tag [`v4.18.0`](https://github.com/eddiethedean/robin-sparkless/tree/v4.18.0)
points to commit [`101c6f5a05912a293bf8719c61972bce9f1616d6`](https://github.com/eddiethedean/robin-sparkless/commit/101c6f5a05912a293bf8719c61972bce9f1616d6).
[CI run 41115170468](https://github.com/eddiethedean/robin-sparkless/actions/runs/41115170468)
passed on that exact commit. [PR #1732](https://github.com/eddiethedean/robin-sparkless/pull/1732)
moved release jobs from the unavailable macOS 14 runners to macOS 15 before this cut.

The tagged [Release run 41119833550](https://github.com/eddiethedean/robin-sparkless/actions/runs/41119833550)
passed every build, wheel smoke test, server consumer check, and release validation
gate. Publication then stopped at the final `sparkless` PyPI upload:

| Component | Observed publication state |
| --- | --- |
| Five first-party Rust crates, 4.18.0 | Published; Rust publisher succeeded |
| `sparkless-server-bin==4.18.0` | Both supported server wheels published |
| `ghcr.io/eddiethedean/sparkless-server:v4.18.0` | Published from the tested server wheel |
| `sparkless==4.18.0` | Six of eight extension wheels published; Windows x64/ARM64 wheels and source archive missing |
| Coordinated public inventory verification / V34 | Incomplete; final publisher failed before public verification |

PyPI rejected the Windows x64 wheel with HTTP 400:
`Project size too large. Limit for project 'sparkless' total size is 10 GB.`
The Windows ARM64 wheel and source archive were not attempted.

A [public PyPI metadata](https://pypi.org/pypi/sparkless/json) snapshot on
2026-10-30 counted 73 earlier versions and 391 files, totaling 10,470,383,176
bytes (9.75 GiB) before the partial 4.18.0 upload. The six new wheels added
253,340,956 bytes, bringing usage to about 9.99 GiB. The quota counts all retained
release files, not just one release; see [PyPI storage limits](https://docs.pypi.org/project-management/storage-limits/).
No historical files were deleted. Published 4.18.0 files remain unchanged; the
packaging recovery uses a new API patch version.

## Earlier evidence snapshots

| Scope | Commit | Evidence |
| --- | --- | --- |
| Short README/linked-guide baseline; normal CI | `3e604079bfd0cc824ce94c88acd93ce6f84be84f` | [CI40990790680](https://github.com/eddiethedean/robin-sparkless/actions/runs/40990790680), passed |
| Same source, cross-platform release validation | `3e604079bfd0cc824ce94c88acd93ce6f84be84f` | [Release40990790490](https://github.com/eddiethedean/robin-sparkless/actions/runs/40990790490), passed |
| Previous full candidate validation | `e61b924a` | [CI40986568452](https://github.com/eddiethedean/robin-sparkless/actions/runs/40986568452), [Release40986569038](https://github.com/eddiethedean/robin-sparkless/actions/runs/40986569038), passed |

The rows above are historical pre-release snapshots; the exact-SHA CI and Release
runs in the publication outcome establish the 4.18.1 cut. Any later source change
needs new CI and Release validation on that exact commit before it is tagged.
Source-bound diagnostics from earlier commits may be reused only with an explicit
source-scope comparison. GitHub artifacts have retention limits;
workflow metadata and source remain inspectable, but an expired artifact is not
downloadable evidence.

## Collect and retain

```bash
gh run view RUN_ID --repo eddiethedean/robin-sparkless --json headSha,event,status,conclusion,jobs
gh api repos/eddiethedean/robin-sparkless/actions/runs/RUN_ID/artifacts
gh run download RUN_ID --repo eddiethedean/robin-sparkless --dir release-evidence
```

Replace RUN_ID with the reviewed run. Preserve exact commit, job results,
artifact names/digests, platform inventory, audit graphs/exceptions, oracle
versions and benchmark baseline/candidate reports with the release's evidence.
Local `target/...` paths in phase notes are reproduction breadcrumbs, not public
artifact links. See [the 4.18 implementation record](plans/sparkless-server/RELEASE_4_18.md)
for historical diagnostics and [the security notice](security/zstd-4.18-exception.md)
for approval/expiry. Publication requires the exact-SHA successful CI evidence
specified in RELEASING, not a green check from a different candidate.

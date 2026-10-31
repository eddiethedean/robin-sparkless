# Release evidence — 4.18 Python packaging recovery

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

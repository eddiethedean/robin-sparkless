# Release evidence — 4.18 Python packaging recovery

## Current 4.18.1 candidate

The Python packaging recovery separates the universal `sparkless==4.18.1` API
from `sparkless-native==4.18.0`. The server extra pins the already published
`sparkless-server-bin==4.18.0`; the five Rust crates also remain at 4.18.0.
The release tag follows the API version. Exact dependency pins and separate
inventories keep the component versions explicit.

The native project contains the platform wheels and native source archive. The
API contains one `py3-none-any` wheel and one small Python source archive; it does
not bundle a native binary. This stops large extension wheels accumulating in the
nearly full `sparkless` PyPI project and permits Python-only patches to reuse
published component versions. Each PyPI project still has its own storage quota.

This candidate requires exact-commit CI and Release validation before tagging.
The new `sparkless-native` PyPI project also needs a pending Trusted Publisher for
`eddiethedean/robin-sparkless`, workflow `release.yml`, environment `pypi`.
[RELEASING](RELEASING.md) defines the build, reuse and publication checks.

PyPI's default limits are 100 MiB per file and 10 GiB per project. The locally
built API wheel and source archive total 131,862 bytes (69,492 and 62,370 bytes);
the API project therefore adds about 129 KiB to its roughly 9.99 GiB current
inventory. The local macOS ARM64 native wheel is 44,091,761 bytes and its source
archive is 2,991,226 bytes. The new native project has room for all eight wheels;
the release gate checks each actual platform artifact and the combined project
inventory before publication.

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

The exact candidate-head runs above cover the source revision shown. Any later
source change needs new CI and Release validation on that exact commit before it
is tagged. Source-bound diagnostics from earlier commits may be reused only with
an explicit source-scope comparison. GitHub artifacts have retention limits;
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

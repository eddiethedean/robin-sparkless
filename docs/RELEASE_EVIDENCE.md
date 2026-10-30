# Release evidence — 4.18.0 candidate

Publication state is separate from validation. A direct registry API check on
2026-10-30 found 4.17.0 latest across the coordinated PyPI/crates.io inventory;
no 4.18 publication is asserted here.
[RELEASING](RELEASING.md) is the authority for current gates; phase plans are
historical evidence, and optional diagnostics are not silently promoted to requirements.

## Current 4.18.0 candidate evidence

On 2026-10-30, both required candidate workflows passed on exact branch head
`525ffe564585a4349890aa36a458f8530ea7ee7c` (`codex/release-4.18-readiness`):

- [CI run 40996635814](https://github.com/eddiethedean/robin-sparkless/actions/runs/40996635814)
  passed, including the CI acceptance gate, server HTTP/Connect suite, Python
  suites, audit, strict documentation build, and documentation examples.
- [Release run 40996636369](https://github.com/eddiethedean/robin-sparkless/actions/runs/40996636369)
  passed the cross-platform candidate build and smoke checks, server wheels and
  container, and candidate inventory/acceptance checks. The retained
  `verified-sparkless-candidate` artifact has SHA-256
  `f5920a37e4ceef0eedf1c5eacc5080579aac9dca5e56da9d40b2be3e138b33dd` and is
  available through 2027-01-28.

This is candidate validation only: no `v4.18.0` tag or public package/image
publication is asserted. The normal release procedure still requires merging the
reviewed candidate to `main` and successful CI on the exact commit to be tagged.
V34 remains a post-publication check.

The registry check found `4.17.0` latest for [sparkless on PyPI](https://pypi.org/pypi/sparkless/json),
[sparkless-server-bin on PyPI](https://pypi.org/pypi/sparkless-server-bin/json),
and the five first-party crates ([robin-sparkless](https://crates.io/api/v1/crates/robin-sparkless),
[robin-sparkless-core](https://crates.io/api/v1/crates/robin-sparkless-core),
[robin-sparkless-polars](https://crates.io/api/v1/crates/robin-sparkless-polars),
[spark-sql-parser](https://crates.io/api/v1/crates/spark-sql-parser),
[sparkless-server](https://crates.io/api/v1/crates/sparkless-server)). Each `4.18.0`
version lookup returned HTTP 404. GitHub lookups for the `v4.18.0` tag and release
also returned HTTP 404, so no partial 4.18 publication was present at review time.

The evidence above covers the committed source revision shown. The documentation
reconciliation currently in this checkout is later than those runs; its release
metadata and strict documentation checks pass locally, and the resulting commit
must receive exact-commit CI and Release validation before tagging.

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

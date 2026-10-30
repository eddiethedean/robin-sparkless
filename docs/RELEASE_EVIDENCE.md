# Release evidence — 4.18.0 candidate

Publication state is separate from validation. At the 2026-10-29 documentation
review, PyPI/crates.io stable was 4.17.0. No 4.18 publication is asserted here.
[RELEASING](RELEASING.md) is the authority for current gates; phase plans are
historical evidence, and optional diagnostics are not silently promoted to requirements.

## Durable evidence snapshots

| Scope | Commit | Evidence |
| --- | --- | --- |
| Short README/linked-guide baseline; normal CI | `3e604079bfd0cc824ce94c88acd93ce6f84be84f` | [CI40990790680](https://github.com/eddiethedean/robin-sparkless/actions/runs/40990790680), passed |
| Same source, cross-platform release validation | `3e604079bfd0cc824ce94c88acd93ce6f84be84f` | [Release40990790490](https://github.com/eddiethedean/robin-sparkless/actions/runs/40990790490), passed |
| Previous full candidate validation | `e61b924a` | [CI40986568452](https://github.com/eddiethedean/robin-sparkless/actions/runs/40986568452), [Release40986569038](https://github.com/eddiethedean/robin-sparkless/actions/runs/40986569038), passed |

These snapshots do not certify later documentation/code edits. For the exact
candidate head, inspect the [CI workflow](https://github.com/eddiethedean/robin-sparkless/actions/workflows/ci.yml)
and [Release workflow](https://github.com/eddiethedean/robin-sparkless/actions/workflows/release.yml),
confirm `headSha`, event, successful conclusion, and retained reports/artifacts.
Source-bound diagnostics from earlier commits may be reused only with an explicit
source-scope comparison. GitHub artifacts have retention limits; workflow metadata
and source remain inspectable, but an expired artifact is not downloadable evidence.

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

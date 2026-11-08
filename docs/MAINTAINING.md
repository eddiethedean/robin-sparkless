# Maintainer responsibilities and triage

Repository maintainers review and merge changes and coordinate releases. Feature
authors maintain the behavior contracts and examples affected by their work.
The release maintainer verifies the exact-source release evidence and public
documentation. These are responsibility areas, not a promise of paid support,
response times or a named on-call rotation. See [support](SUPPORT.md).

## Route a report

| Report | First evidence | Owning area |
| --- | --- | --- |
| Installation/import | Python/OS/architecture, wheel name, package/native import paths, exact error | Packaging/native ABI |
| Wrong result/schema | Minimal input and explicit schema, actual backend, expected reference Spark result | Local engine or selected server adapter |
| Unsupported API/option | Exact public method/option and surface support matrix | API/compatibility contract |
| Timeout/duplicate write | Saved request identity, safe receipt/status information, owner/instance history | Runtime/provider and storage owner |
| Broken guide | Page/version, copied command, expected/actual result | Feature author and documentation reviewer |
| Sensitive security report | Private reporting route, affected versions/features | Repository security contact process |

Remove credentials and private datasets before sharing reproductions. Follow
[security reporting](SECURITY.md#reporting) rather than posting exploit details.

## Decide and verify

Classify a report as a supported-behavior defect, documentation defect,
unsupported feature request, environment issue or unverified combination.
Cross-check the versioned support matrix; historical plans and inventory counts
are not evidence that a capability is supported. Request missing evidence once
and explain what it will distinguish. Prioritize incorrect results, ambiguous
writes, installation blockers and regressions in supported workflows.

A behavior fix needs a regression that observes values/schema/effects, the
affected API/transport tests, and updated docs. A documentation fix needs exact
links, runnable complete examples or explicit fragment labels, and reconciled
claims on every affected surface. State any environment-dependent checks that
could not run. Do not mark a contract behavior-tested on a syntax check alone.

## Documentation release gate

Use the [maintenance checklist](DOCUMENTATION.md#release-documentation-checklist)
and [executable example manifest](EXAMPLES.md). The documentation reviewer checks
the support matrix against differences, deferred scope, SQL, operations and
migration pages. The release maintainer verifies published wheels, `latest` and
`stable` site versions, and the browsable Rust API route independently. Record
exact version/source and date of checks, and retain any unavailable-hosting
limitation instead of asserting publication.

No LTS branch, backport schedule or stable minor-version API is promised. Record
breaking changes, affected packages, replacements and migration guidance in the
changelog. Follow [releasing](RELEASING.md) for artifact validation and credentials;
follow [contributing](https://github.com/eddiethedean/robin-sparkless/blob/main/CONTRIBUTING.md)
for normal PR checks.

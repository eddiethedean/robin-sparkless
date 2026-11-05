# Documentation maintenance

Owner: repository maintainers; feature authors own the accuracy of the pages
and examples their changes affect. Release maintainer reviews the coordinated
version contract before a cut. Reviewed version/date are recorded in
`docs/reference/release.json` and shown on every built page. This metadata is a
review record, not automatic evidence that every old page was checked.

## Sources of truth

| Information | Authority |
| --- | --- |
| Package version / native features / extras | Cargo manifests, python/pyproject.toml, checked build workflows |
| Supported behavior by surface | SUPPORT_MATRIX plus executable regressions/oracle examples |
| Signatures | Installed-wheel generated Python symbol inventory; pinned rustdoc |
| Current release requirements | RELEASING |
| Known security exception | Versioned security notice and checked security policy files |
| Plans / past counts / experiments | Archive, never current capability authority |

Keep repository/PyPI READMEs brief. Put setup detail, matrices, troubleshooting,
API contracts and operations in the docs. Link one authoritative explanation
rather than copying conflicting versions across FAQ/migration/marketing pages.
A new feature needs purpose, prerequisites, runnable example, context/type limits,
errors and a navigation entry. A named unsupported/no-op API must not be described
as fully implemented merely because it imports.

## Checks

```bash
uv run --no-project --with-requirements docs/requirements.txt python scripts/check_documentation.py
uv run --no-project --with-requirements docs/requirements.txt mkdocs build --strict
.venv/bin/python scripts/generate_documentation.py --api
.venv/bin/python scripts/generate_documentation.py --api --check
```

Review generated changes; do not blindly bless them. CI checks links/anchors,
Python fence syntax and complete TOML snippets, zero MkDocs warnings, the
signature/compatibility inventories and installed-wheel executable examples.
Examples run outside the checkout, with identity checks on Sparkless, PySpark 3.5
and 4.0.5. Rustdoc is generated with the pinned toolchain and retained as an artifact.
Static syntax checks do not establish that arbitrary prose snippets execute;
promote adoption-critical examples into the executable example set.

## Review and deprecation rules

Update version/date only after reviewing the release surfaces. Record API
removals/behavior changes in release notes with migration advice and planned
removal versions where practical; security/correctness exceptions must state why.
Historical plans stay searchable through the archive index but are excluded from
the normal search corpus. Preserve old URLs where possible; fix incoming links
or provide a redirect/signpost when renaming a current page.

## Release documentation checklist

- Match package/crate/server versions and supported Python/platform floors.
- Reconcile support matrix, FAQ, migration, UDF/catalog and transport claims.
- Regenerate runtime symbols/compatibility inventory and run executable examples.
- Verify stable/candidate availability and public install commands against indexes.
- Verify the actual versioned docs.rs route; retain explicit fallback if it fails.
- Review security exception scope/expiry and all Rust install entry points.
- Check exact-SHA CI/release conclusions, artifacts and evidence retention.
- Review public package metadata/URLs and rendered README; keep README brief.
- Build docs strictly, validate all links, and inspect navigation/version banner.
- At publication, change release status from candidate to published and link the
  immutable tag and release evidence. Do not assert publication ahead of the indexes.

## After publication

1. Confirm the source tag and public PyPI and crates.io records. Link the
   version-specific file lists and record any missing hosted artifacts in
   [release evidence](RELEASE_EVIDENCE.md).
2. Update `docs/reference/release.json` and use the public install commands.
   The **Published documentation examples** workflow runs the local, pytest,
   HTTP, Connect, and DuckDB guides in separate PyPI environments. Run it after
   the publication update lands. The normal docs example job also checks the
   built wheel and the two pinned PySpark reference runtimes.
3. Verify the rendered [Read the Docs latest site](https://robin-sparkless.readthedocs.io/en/latest/)
   shows the expected version and package instructions. Check the stable
   version separately if the host exposes it. Source tags record the docs at
   the cut. Fixes merged later appear in the main branch's manual after the
   host rebuilds it.
4. Open the versioned docs.rs API route and record its availability. Keep the
   [pinned rustdoc build](RUST_API_REFERENCE.md) available for offline use.

Keep historical plans in the archive, including the status and results as
recorded at each checkpoint. Use the support matrix and release evidence for
current product claims.

# Executable documentation examples

Complete programs are registered in
[examples.json](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/reference/examples.json)
with source, runtime profile, dependencies, expected result and verification
method. Fragments remain labeled as fragments; Python syntax checks alone do
not establish that their API calls work.

| Profile | What runs | Environment |
| --- | --- | --- |
| `local` | READMEs, Python first run, API examples, UDF, cancellation, migration, independent warehouse processes | Installed Sparkless wheel, PyArrow |
| `provider` | Complete Delta provider, receipt validation and rejected-prefix check | Installed default Delta-enabled native wheel, PyArrow |
| `native-sqlite` | Native SQLite read/write checked independently with sqlite3 | Extension built with `sqlite` |
| `http` | First query, same-instance replay, old-instance rejection after restart | Base server wheel |
| `connect` | First query, file pipeline, shared pytest workers | Base Connect product, pytest and xdist |
| `duckdb` | Source read plus execution write/independent read/restart | DuckDB Connect product, duckdb 1.5.1 |
| `rust` | Complete Rust quickstart and Column/SQL user guide | Repository Rust 1.93.1 |

After installing the matching dependencies, run from a checkout:

```bash
python scripts/run_documentation_examples.py local
python scripts/run_documentation_examples.py provider
python scripts/run_documentation_examples.py native-sqlite
python scripts/run_documentation_examples.py rust
```

Python programs run in fresh temporary directories. Local distribution checks
reject checkout imports; `--allow-checkout` explicitly selects source validation
and is not evidence that a published wheel works. Keep PYTHONPATH absolute when
using that source-only mode, since child processes use another working directory.
Rust programs run as named Cargo examples; the static checker verifies their
complete guide fences match the compiled sources.

The **Published documentation examples** workflow installs PyPI packages in
separate environments. Normal CI checks built-wheel examples, provider behavior,
the SQLite feature example, and Rust examples. The protocol recovery fault
exercise and container commands have separate dependencies and explicit run
instructions in [write recovery](WRITE_RECOVERY.md) and [containers](CONTAINERS.md);
they are not silently counted as covered by a successful local profile.

When adding a complete example, register its profile and evidence here and in the
manifest. Add the required CI environment before calling it continuously checked.
Run install commands in a clean environment; validate actual values, schemas,
side effects and cleanup. A new source file cannot substitute for executing the
corresponding guide. See [documentation maintenance](DOCUMENTATION.md).

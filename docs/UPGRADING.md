# Choose an upgrade path

Pin and test the complete package set used by your application. Sparkless 4.x
minor releases may change interfaces. Compatibility profiles change selected
semantics; they do not install a Spark runtime or guarantee full parity.

| Starting point | Read before changing dependencies |
| --- | --- |
| PySpark application | [Import and behavior migration](python_migration.md#from-pyspark), then [differences](PYSPARK_DIFFERENCES.md) |
| Sparkless 3.x | [Python/native migration](python_migration.md#from-sparkless-3); Python 3.11+ required |
| Sparkless 4.18.x | [Package/import split](python_migration.md#from-sparkless-418-to-419) and [Rust/provider changes](MIGRATION_4_19.md), then the 4.20 notes below |
| Sparkless 4.19.x | [4.20 migration](MIGRATION_4_20.md); DuckDB execution is opt-in |
| Any version using testing helpers | [Comparison contract](TESTING_PYTHON_API.md#comparison-semantics); 4.19.0 has the known schema-comparison defect fixed in 4.20 |

Historical 4.19 instructions deliberately pin 4.19 for that transition. For a
new deployment choose the current [4.20 installation matrix](INSTALLATION.md)
after applying the import changes. Do not mix launcher/product/native component
versions arbitrarily. Keep JVM PySpark and standalone `pyspark-client` in
separate environments because they occupy the same Python namespace.

## Verify and roll back

1. Save the existing dependency lock file and environment identity.
2. Create a fresh environment with the target package set; run `python -m pip check`.
3. Run a representative pipeline with explicit schema, nulls, empty data, invalid
   input and storage round trips. Check backend identity and exact Spark version.
4. For servers, verify selected product/backend, readiness, restart and write
   recovery against disposable data before routing callers to the new process.
5. Restore the old lock file and recreate its environment to roll back code.
   Database/schema changes require a separate storage rollback plan; restoring
   Python packages does not undo committed data.

See [release notes](https://github.com/eddiethedean/robin-sparkless/blob/main/CHANGELOG.md),
[support policy](SUPPORT.md), [security by version](SECURITY.md), and
[write recovery](WRITE_RECOVERY.md).

# Spark Connect 4.18 contract fixtures

This corpus is pinned to **`pyspark-client==4.0.5`**, with the matching Apache Spark **4.0.5** protocol source. It captures protobuf requests from unmodified public Python APIs and the client's own reattachment/release encoder. The recording stub stops before a server executes a plan. These files establish client wire shapes; they do not claim Sparkless interoperability or server behavior.

The native `connect` feature compiles Apache Spark's ten unchanged protocol sources vendored in
`crates/sparkless-server/proto/spark/connect/`. Their reviewed source hashes and selected client
behavior are recorded in `docs/plans/sparkless-server/CONNECT_PROTOCOL_RESEARCH.md`. The Rust
service tests decode the checked-in `execute-range-filter-count.pb` fixture and cover malformed
session IDs, duplicate operation IDs, session-generation/tombstone behavior, structural protobuf
limits, and a real gRPC listener's inbound-size rejection. Run them with:

```sh
cargo test -p sparkless-server --no-default-features --features connect connect::tests --lib
```

These are slice 2 transport and identity tests. They do not exercise native plan translation or
successful Spark Connect results.

Slice 4's focused public-client smoke starts a development gRPC listener and runs the locked,
unmodified client against it. In one terminal, start the listener:

```sh
cargo run -p sparkless-server --example connect-dev-server --no-default-features --features connect
```

In another terminal, using the environment created by the fixture setup above, run:

```sh
/tmp/sparkless-connect-client-py311/bin/python tests/connect/smoke_public_client.py
```

The smoke checks the range/filter/count witness, scalar local relations and their Arrow physical
types, schema-only empty input, multi-batch offsets, and duplicate aliases through both
`collect()` and `toArrow()`. It is development evidence only; it does not complete V18/V20 or test
reattachment, replay, the reference engine, or the response-byte/total-output failure boundaries.
Stop the listener with Ctrl-C when finished.

The interpreter-specific dependency snapshots are hash-locked for CPython 3.11:

- `client-requirements.in` and `client-py311.lock` install only the standalone client package.
- `oracle-requirements.in` and `oracle-py311.lock` install the matching Python reference client and Connect dependencies (`pyspark[connect]==4.0.5`). Both locks constrain pandas to `>=2,<3`, matching the [Spark 4.0.5 documented Connect dependency range](https://spark.apache.org/docs/4.0.5/api/python/getting_started/install.html#spark-connect). The reference server is a separate Apache binary distribution: [`spark-4.0.5-bin-hadoop3-connect.tgz`](https://downloads.apache.org/spark/spark-4.0.5/spark-4.0.5-bin-hadoop3-connect.tgz), SHA-512 `4cc2e43959b5e0a67b7d895e1aa1fb3ab05fa6ad4a5912de76236bb9b70f9557b20837b05ee63474798e927130e72ed6d1fadcd273a71e65c70eac18771c0d6b`. Verify against the [Apache checksum](https://downloads.apache.org/spark/spark-4.0.5/spark-4.0.5-bin-hadoop3-connect.tgz.sha512) and signature, then run it with Java 17. Keep the reference server/client outside the request-fixture environment.

The reference starts with Apache's `sbin/start-connect-server.sh`, with UTC timezone, ANSI enabled, case sensitivity disabled, and `spark.sql.execution.arrow.useLargeVarTypes=false`. The client's local-relation cache threshold remains at its real 64 MiB default. The matching Python distribution is pinned in `oracle-py311.lock`; this is the client-side dependency lock, not the server archive checksum.

Recreate and verify the client fixtures in an isolated environment:

```sh
uv venv /tmp/sparkless-connect-client-py311 --python 3.11
uv pip sync --python /tmp/sparkless-connect-client-py311/bin/python tests/connect/client-py311.lock
/tmp/sparkless-connect-client-py311/bin/python tests/connect/capture_public_client_fixtures.py --check
```

Regenerate a dependency snapshot after an intentional pin change with:

```sh
uv pip compile --python-version 3.11 --generate-hashes tests/connect/client-requirements.in -o tests/connect/client-py311.lock
uv pip compile --python-version 3.11 --generate-hashes tests/connect/oracle-requirements.in -o tests/connect/oracle-py311.lock
```

Each fixture has a readable `.textproto` and deterministic protobuf `.pb` form. `manifest.json` records the RPC/type and SHA-256 for both encodings. The recorder fixes user/session/operation identities, resets plan IDs, and clears the client runtime string so OS and Python patch version do not create irrelevant diffs. It verifies that parsing either fixture produces the same protobuf bytes.

The captured public paths cover range/filter/count, Column comparisons/arithmetic/aliasing, projection/limit, `take`, `head`, `toLocalIterator`, normal and empty local relations, DDL schema parsing, schema analysis, SQL command encoding, the Python `RuntimeConfig` operations (`Get`, `GetWithDefault`, `GetAll`, `IsModifiable`, `Set`, and `Unset`), and the client's ExecutePlan reattach, prefix/all release, interrupt, and session-release messages. The protobuf-defined `GetOption` branch is captured separately through the client's low-level Config request builder; the pinned Python `RuntimeConfig` has no public method that emits it. The cached-local-relation boundary fixture forces the threshold to one byte and substitutes a stable artifact ID at the upload seam; it proves only that the real API then emits `CachedLocalRelation`. It does not capture `AddArtifacts` or imply artifact support. Spark 4.0.5's actual `spark.sql.session.localRelationCacheThreshold` default is 67,108,864 bytes (64 MiB); normal client defaults are left intact in the other captures.

The fixture harness supplies Config responses only to let the public client construct requests. In particular, a successful client-side `set` fixture is not permission for Sparkless to accept unsupported settings. Implementations must preserve optional/default lookup semantics and reject unsupported changes explicitly.

# Vendored Spark Connect protocol sources

The ten `.proto` files under `spark/connect/` are copied without modification from Apache Spark
tag [`v4.0.5`](https://github.com/apache/spark/tree/b472a3aa3c4b71b5a6ff068ef396f757bd662e59/sql/connect/common/src/main/protobuf/spark/connect/)
at commit `b472a3aa3c4b71b5a6ff068ef396f757bd662e59`. `SHA256SUMS` records the source hashes;
verify them from this directory with `shasum -a 256 -c SHA256SUMS`. The hashes are also in the
[4.18 protocol research note](../../../docs/plans/sparkless-server/CONNECT_PROTOCOL_RESEARCH.md#frozen-418-client-and-protocol-baseline).
The matching Apache-2.0 license is included as `LICENSE-Apache-2.0`.

`build.rs` compiles the checked-in sources only when the server's `connect` feature is enabled.
It uses the pinned vendored `protoc`, Tonic/Prost generator, and descriptor output; generated
Rust bindings stay in Cargo's `OUT_DIR` and are not hand-edited. The generated service uses a
descriptor-aware wire preflight before Prost allocates request messages, then rechecks decoded
requests. Sparkless applies a 4 MiB per-message cap, 64 message levels, 10,000 message nodes,
and 256 schema fields. These are Sparkless resource policies, not protocol limits defined by
Apache Spark.

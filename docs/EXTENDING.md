# Extend Sparkless

Choose the boundary before adding code:

| Need | Extension path | Contract |
| --- | --- | --- |
| Local row transformation | Python typed scalar `F.udf` | [UDF contexts](UDF_GUIDE.md); eager/materializing |
| Rust expression | Native `register_udf` / expression APIs | [Rust reference](RUST_API_REFERENCE.md) |
| External table storage | Python/Core external catalog provider | [descriptors, writes, receipts, cancellation](EXTERNAL_CATALOGS.md) |
| New engine backend | Core engine traits + facade implementation | [engine architecture](ENGINE_ARCHITECTURE.md) |
| New remote plan/SQL surface | Server translation, analysis and admission contracts | [HTTP](HTTP_API.md), [Connect](SPARK_CONNECT.md) |

There is no general pip-installable plugin discovery or remote Python worker
contract. Register providers explicitly with
`spark.set_external_catalog_provider(provider)` using the provider guide's
complete examples. Preserve schema/type/nullability and receipt validation;
never report cancellation after a write may have committed.

Add engine behavior tests plus Python/transport tests for any exposed surface.
Record unsupported combinations and oracle provenance. Follow
[the regression walkthrough](TEST_CREATION_GUIDE.md) and [documentation checks](DOCUMENTATION.md).

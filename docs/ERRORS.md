# Error codes and recovery

Use structured fields where present; exact exception wording is diagnostic.
Python exposes `SparklessError` and its cancellation/provider subclasses.
Validation may also raise TypeError, ValueError or FileExistsError. Rust
`EngineError` exposes `category()`, `code()`, `retryability()` and `detail()`;
Column APIs may still return PolarsError. HTTP execution errors can use HTTP 200
with JSON `status="error"`. Connect reports gRPC status and the engine code.

## Common stable engine codes

| Code | Category / typical cause | Action |
| --- | --- | --- |
| `INVALID_INPUT` | Invalid input or configuration | Correct the request; unchanged retries are terminal |
| `NOT_FOUND` | Missing relation or resource | Check session, namespace and storage; unchanged lookup is terminal at this boundary |
| `CANCELLED` | Action reached a cancellation safe point | Wait for termination; reconcile writes before resubmitting |
| `IO_ERROR` | Filesystem/database IO | Inspect cause and permissions; retryability may be unspecified |
| `SQL_ERROR` | SQL planning/execution failure | Reduce to the surface's documented dialect; do not assume rollback |
| `INTERNAL` | Unexpected engine failure | Preserve versions, minimal input and structured detail; report a bug |
| `OTHER` | Unclassified engine error | Inspect operation/cause and reproduce before deciding to retry |
| `WRITE_CAPABILITY_DENIED` | Owner has no usable write capability | Choose a writable provider or supported destination; unchanged retry is terminal |

Providers can define additional stable codes; this table is not an exhaustive
enum. Inspect `operation`, `metadata` and `cause` when present. Do not put secrets
in provider categories/codes or log full SQL/input by default.

## Retryability

| Value | Meaning |
| --- | --- |
| `terminal` | The unchanged operation should not be retried without correcting its cause |
| `retryable` | Retry may be appropriate under the operation's identity contract; use bounded backoff |
| `outcome_unknown` | An effect may have committed; reconcile before another submission |
| `unspecified` | No automatic retry decision is supplied |

Python provider errors also expose provider-specific category/code and
`provider_retryability` metadata. A write callback exception is conservatively
classified as unknown outcome even if the provider raised after a pre-commit
validation failure. The [provider guide](EXTERNAL_CATALOGS.md) defines sanitization
and the distinction between provider retry intent and actual write outcome.

## Transport failures

| Signal | Interpretation / next step |
| --- | --- |
| HTTP 400/413/415 | Invalid protocol, excessive body or content type; fix request |
| HTTP 409 / `OperationIdentityConflict` | Wrong instance or changed content under an ID; retain original request and investigate |
| HTTP 404 on operation status | Unknown/expired record; does not prove rollback |
| HTTP 503 or gRPC RESOURCE_EXHAUSTED | Draining/admission/resource exhaustion; inspect effective limits before bounded retry |
| gRPC INVALID_ARGUMENT | Missing or invalid supported request fields |
| gRPC UNIMPLEMENTED | Operation outside the selected backend's contract |
| Client timeout/disconnect | Client lost observation; native work or commit may still complete |

Follow [write recovery](WRITE_RECOVERY.md),
[troubleshooting](TROUBLESHOOTING_PYTHON.md) and [support](SUPPORT.md). Include
backend, versions, input schema and safe structured error fields in a report.

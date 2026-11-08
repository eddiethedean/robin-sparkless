# Build a small external catalog provider

Goal: create a real Delta table, resolve its owner, append bounded batches,
validate a commit receipt, and reject excess input without committing a prefix.
Use the local API with Python 3.11+; no server or Java is required.

```bash
python -m pip install "sparkless==4.20.0" pyarrow
```

Download [external_provider.py](https://github.com/eddiethedean/robin-sparkless/blob/main/examples/python/external_provider.py)
and run `python external_provider.py`. It creates and removes its own fixture.
Expect `provider read, commit, receipt and budget rejection passed`.

## Follow the write

1. The script creates a Delta table containing `id=1` with an explicit long schema.
2. The provider owns `example.analytics.*`; an unknown table in that namespace
   remains an external miss and cannot fall through to local storage.
3. `saveAsTable` supplies an operation ID, descriptor, save mode and bounded
   Arrow batches to `write_table_batches`.
4. The provider validates **all** batches in a small in-memory stage. It caps
   rows at 10,000 and decoded bytes at 64 MiB before storing each batch.
5. After complete consumption, one native Delta write appends `2` and `3`.
   The provider reads the committed log version and returns a validated receipt.
6. A second request exceeds a one-row budget. Its staged first batch is discarded;
   reading storage again still returns exactly `1, 2, 3` and version 1.

The descriptor keeps table identity, location, schema and provider stable. Its
version changes only after a storage commit. Receipt validation checks identity
and version; it cannot undo a write that already happened.

## Adapt the example

This is a **single-writer fixture provider**. Its in-memory stage and receipt map
are suitable for bounded examples, not a durable production operation journal.
It supports the append/overwrite modes delivered by the provider hook and relies
on Delta for the storage transaction. It does not implement concurrent descriptor
updates or cross-process replay. Empty schema-bearing batches carry no new rows.

For a production adapter, stage to bounded temporary storage, enforce schema,
options and partition policy, and atomically record operation identity with the
commit in your storage system. Reject unsupported writer options explicitly.
Fingerprint requests before implementing receipt replay so an ID cannot be reused
with different content. Reconcile commit exceptions using that durable record;
the Python boundary conservatively reports raised write callbacks as unknown
outcomes even when your adapter knows it rejected input before commit.

Read the [complete provider contract](EXTERNAL_CATALOGS.md) for cancellation,
ownership, errors, schema boundaries and unsupported operations. Add a regression
for failure before commit, failure after commit, and changed-content retries.

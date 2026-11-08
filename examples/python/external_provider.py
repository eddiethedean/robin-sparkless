"""A disposable, single-writer Delta provider with bounded pre-commit staging."""

from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa

from sparkless import SparklessExternalCatalogError
from sparkless.external_catalog import (
    ExternalCatalogOwnership,
    ExternalTableDescriptor,
    ExternalTableWriteResult,
)
from sparkless.sql import SparkSession, types as T

IDENTIFIER = "example.analytics.orders"


class Catalog:
    """For small local fixtures; production providers need durable reconciliation."""

    def __init__(self, spark, path, schema):
        self.spark = spark
        self.path = path
        self.row_limit = 10_000
        self.byte_limit = 64 * 1024 * 1024
        self.table = ExternalTableDescriptor(
            table_id="example-orders",
            canonical_location=str(path),
            schema=schema,
            provider="tutorial",
            version=0,
        )
        self.receipts = {}

    def resolve_ownership(self, identifier):
        if identifier.startswith("example.analytics."):
            return ExternalCatalogOwnership(
                "example.analytics", read=True, write=True, batch_write=True
            )
        return None

    def resolve_table(self, identifier):
        return self.table if identifier == IDENTIFIER else None

    def write_table_batches(self, request):
        staged = []
        rows = size = 0
        try:
            for batch in request.batches:
                if rows + batch.num_rows > self.row_limit:
                    raise ValueError("provider row budget exceeded")
                if size + batch.nbytes > self.byte_limit:
                    raise ValueError("provider byte budget exceeded")
                staged.append(batch)
                rows += batch.num_rows
                size += batch.nbytes
        finally:
            request.batches.close()
        # Nothing has touched storage before complete input validation.
        if not staged:
            raise ValueError("this tutorial requires a schema-bearing batch")
        table = pa.Table.from_batches(staged)
        self.spark.createDataFrame(table).write.format("delta").mode(request.mode).save(
            str(self.path)
        )
        version = max(int(p.stem) for p in (self.path / "_delta_log").glob("*.json"))
        self.table = replace(request.table, version=version)
        receipt = ExternalTableWriteResult(self.table, committed_version=version)
        receipt.validate_against(request.table)
        self.receipts[request.operation_id] = receipt
        return receipt


def main():
    spark = SparkSession.builder.app_name("provider-tutorial").get_or_create()
    try:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "orders"
            schema = T.StructType([T.StructField("id", T.LongType(), True)])
            spark.createDataFrame([(1,)], schema).write.format("delta").mode(
                "overwrite"
            ).save(str(path))
            provider = Catalog(spark, path, schema)
            spark.set_external_catalog_provider(provider)
            assert spark.table(IDENTIFIER).collect()[0]["id"] == 1
            spark.createDataFrame([(2,), (3,)], schema).write.mode("append").option(
                "sparkless.writer.maxRowsPerBatch", "1"
            ).saveAsTable(IDENTIFIER)
            assert sorted(r["id"] for r in spark.table(IDENTIFIER).collect()) == [
                1,
                2,
                3,
            ]
            assert provider.table.version == 1
            assert len(provider.receipts) == 1
            provider.row_limit = 1
            try:
                spark.createDataFrame([(4,), (5,)], schema).write.mode("append").option(
                    "sparkless.writer.maxRowsPerBatch", "1"
                ).saveAsTable(IDENTIFIER)
            except SparklessExternalCatalogError as error:
                assert error.operation == "write_table_batches"
            else:
                raise AssertionError("oversized write must fail")
            # Reconcile through storage: the rejected prefix was never committed.
            assert sorted(r["id"] for r in spark.table(IDENTIFIER).collect()) == [
                1,
                2,
                3,
            ]
            assert provider.table.version == 1
            assert len(provider.receipts) == 1
            # Bypass the pinned descriptor and inspect the latest storage state.
            assert sorted(
                r["id"] for r in spark.read.format("delta").load(str(path)).collect()
            ) == [1, 2, 3]
            assert max(int(p.stem) for p in (path / "_delta_log").glob("*.json")) == 1
            spark.set_external_catalog_provider(None)
        print("provider read, commit, receipt and budget rejection passed")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

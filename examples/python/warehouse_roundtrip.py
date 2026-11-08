"""Verify warehouse persistence in separate writer and reader processes."""

import argparse
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory

from sparkless.sql import SparkSession, types as T


def worker(mode, warehouse):
    spark = (
        SparkSession.builder.app_name("warehouse-" + mode)
        .config("spark.sql.warehouse.dir", str(warehouse))
        .get_or_create()
    )
    try:
        if mode == "write":
            schema = T.StructType([T.StructField("id", T.LongType(), True)])
            spark.createDataFrame([(1,), (2,)], schema).write.mode("error").saveAsTable(
                "orders"
            )
        else:
            rows = spark.table("orders").collect()
            assert sorted(row["id"] for row in rows) == [1, 2]
            assert isinstance(spark.table("orders").schema["id"].dataType, T.LongType)
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["write", "read"], nargs="?")
    parser.add_argument("warehouse", type=Path, nargs="?")
    args = parser.parse_args()
    if args.mode:
        if args.warehouse is None:
            parser.error("a warehouse path is required with write/read")
        worker(args.mode, args.warehouse.resolve())
        return
    with TemporaryDirectory() as directory:
        for mode in ("write", "read"):
            subprocess.run(
                [sys.executable, str(Path(__file__).resolve()), mode, directory],
                check=True,
            )
    print("warehouse values and schema survived independent process restart")


if __name__ == "__main__":
    main()

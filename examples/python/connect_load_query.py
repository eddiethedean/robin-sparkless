#!/usr/bin/env python3
"""Load a CSV through Spark Connect, query it, and export the returned rows.

Requires Sparkless 4.18 with a Connect-enabled server and pyspark-client==4.0.5.
CSV parsing and JSON export happen in Python; DataFrame queries run on the server.
"""

import argparse
import csv
import json
from pathlib import Path

from pyspark.sql.connect.session import SparkSession
from pyspark.sql.functions import col
from sparkless import SparklessServer


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-bin", help="Explicit native server executable")
    parser.add_argument(
        "--output-dir", type=Path, default=Path("sparkless-connect-output")
    )
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Create a small, reproducible input file. Amounts use integer cents.
    csv_path = args.output_dir / "orders.csv"
    csv_path.write_text(
        "order_id,customer,total_cents\n"
        "1001,Alice,1200\n"
        "1002,Bob,7500\n"
        "1003,Alice,2500\n"
        "1004,Carol,12000\n"
        "1005,Bob,5000\n"
        "1006,Dan,900\n",
        encoding="utf-8",
    )
    with csv_path.open(newline="", encoding="utf-8") as source:
        rows = [
            (int(row["order_id"]), row["customer"], int(row["total_cents"]))
            for row in csv.DictReader(source)
        ]
    print(f"Read {len(rows)} orders from {csv_path.name}")

    with SparklessServer(executable_path=args.server_bin) as server:
        spark = SparkSession.builder.remote(server.spark_connect_url).getOrCreate()
        try:
            # The Connect client sends these typed rows as Arrow IPC.
            orders = spark.createDataFrame(
                rows, "order_id INT, customer STRING, total_cents BIGINT"
            )
            print(f"Schema: {orders.schema.simpleString()}")
            total = orders.count()
            print(f"Server row count: {total}")

            result = orders.filter(col("total_cents") >= 5000).select(
                "order_id", col("customer").alias("customer_name"), "total_cents"
            )
            matched = result.count()
            print(f"Orders worth at least $50.00: {matched}")
            # Sort returned rows locally for deterministic presentation.
            records = sorted(
                (row.asDict() for row in result.collect()),
                key=lambda row: row["order_id"],
            )
            print(json.dumps(records, indent=2))

            output = args.output_dir / "high_value_orders.json"
            output.write_text(json.dumps(records, indent=2) + "\n", encoding="utf-8")
            expected = [
                {"order_id": 1002, "customer_name": "Bob", "total_cents": 7500},
                {"order_id": 1004, "customer_name": "Carol", "total_cents": 12000},
                {"order_id": 1005, "customer_name": "Bob", "total_cents": 5000},
            ]
            assert total == 6 and matched == 3
            assert records == expected
            assert json.loads(output.read_text(encoding="utf-8")) == expected
            print(f"Saved and verified {output.name}")
        finally:
            spark.stop()

    assert not server.is_running
    print("Session closed; server stopped.")


if __name__ == "__main__":
    main()

"""Check public-client error behavior for unsupported plans and output boundaries."""

import sys
import os

from pyspark.sql.connect.session import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructField, StructType, StringType


def expect_failure(label: str, action, markers: tuple[str, ...]) -> None:
    try:
        action()
    except Exception as error:
        message = str(error).lower()
        if not any(marker in message for marker in markers):
            raise AssertionError(
                f"{label} failed without an expected diagnostic: {error}"
            ) from error
        print(
            f"{label} returned a visible client error: {type(error).__name__}",
            flush=True,
        )
    else:
        raise AssertionError(f"{label} unexpectedly succeeded")


def main() -> None:
    connect_url = os.environ.get("SPARKLESS_CONNECT_URL")
    if connect_url is None:
        if len(sys.argv) != 2:
            raise ValueError(
                "pass the Connect URL as the only argument or in SPARKLESS_CONNECT_URL"
            )
        connect_url = sys.argv[1]
    spark = SparkSession.builder.remote(connect_url).getOrCreate()
    try:
        expect_failure(
            "unsupported multiplication expression",
            lambda: spark.range(5).selectExpr("id * 2").collect(),
            ("unsupported", "not implemented", "unimplemented"),
        )

        expect_failure(
            "oversized local scalar",
            lambda: spark.createDataFrame(
                [("x" * (512 * 1024 + 1),)], "payload STRING"
            ).collect(),
            ("limit", "exceed", "resource_exhausted", "resource exhausted"),
        )

        wide_schema = StructType(
            [StructField(f"f{index}", StringType(), True) for index in range(257)]
        )
        expect_failure(
            "oversized local schema",
            lambda: spark.createDataFrame([], wide_schema).collect(),
            ("schema", "field", "limit", "exceed", "resource_exhausted"),
        )

        duplicated_columns = [col("id").alias(f"c{index}") for index in range(10)]
        expect_failure(
            "total result byte limit",
            lambda: spark.range(1_000_000).select(*duplicated_columns).toArrow(),
            ("byte", "resource_exhausted", "resource exhausted", "limit"),
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

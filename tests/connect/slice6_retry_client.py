"""Exercise Spark's stock reattach behavior through the fault-injection proxy."""

import sys

from pyspark.sql.connect.session import SparkSession


def main() -> None:
    connect_url = sys.argv[1]
    spark = SparkSession.builder.remote(connect_url).getOrCreate()
    try:
        values = [row.id for row in spark.range(50_000).collect()]
        assert len(values) == 50_000
        assert values[0] == 0 and values[-1] == 49_999
        assert sum(values) == 1_249_975_000
        print("default client retry and response reattachment passed")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

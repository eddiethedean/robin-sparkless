#!/usr/bin/env python3
"""SQL and temp views with Sparkless — run after: pip install "sparkless>=4,<5"."""

from sparkless.sql import SparkSession, functions as F

def main() -> None:
    spark = SparkSession.builder.app_name("sql_example").get_or_create()

    df = spark.createDataFrame(
        [
            {"region": "US", "product": "A", "units": 10},
            {"region": "US", "product": "B", "units": 5},
            {"region": "EU", "product": "A", "units": 8},
        ]
    )

    df.createOrReplaceTempView("sales")

    result = spark.sql(
        """
        SELECT region, SUM(units) AS total_units
        FROM sales
        GROUP BY region
        ORDER BY total_units DESC
        """
    )

    result.show()

    # DataFrame API equivalent
    api_result = df.groupBy("region").agg(F.sum("units").alias("total_units"))
    api_result.show()

    spark.stop()


if __name__ == "__main__":
    main()

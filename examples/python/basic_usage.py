#!/usr/bin/env python3
"""Basic Sparkless usage — run after: pip install "sparkless>=4,<5"."""

from sparkless.sql import SparkSession, functions as F

def main() -> None:
    spark = SparkSession.builder.app_name("basic_example").get_or_create()

    df = spark.createDataFrame(
        [
            {"name": "Alice", "dept": "Eng", "salary": 90000},
            {"name": "Bob", "dept": "Eng", "salary": 85000},
            {"name": "Carol", "dept": "Sales", "salary": 70000},
        ]
    )

    high_earners = (
        df.filter(F.col("salary") >= 85000)
        .select("name", "dept", "salary")
        .orderBy(F.desc("salary"))
    )

    high_earners.show()
    print("Count:", high_earners.count())

    spark.stop()


if __name__ == "__main__":
    main()

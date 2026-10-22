"""SQL COUNT must preserve Spark's BIGINT schema and Arrow int64 output (#1723)."""

import pytest


pytestmark = [
    pytest.mark.parametrize("grouped", [False, True], ids=["scalar", "grouped"]),
    pytest.mark.parametrize(
        "count_arg",
        ["*", "n", "o.n", "1"],
        ids=["star", "column", "qualified", "literal"],
    ),
    pytest.mark.parametrize("empty", [False, True], ids=["populated", "empty"]),
]


@pytest.fixture
def count_results(spark, spark_imports, table_prefix, grouped, count_arg, empty):
    data = [] if empty else [("a", 1), ("a", 2), ("a", None), ("b", None)]
    orders = spark.createDataFrame(data, "g string, n long")
    orders.createOrReplaceTempView(table_prefix)

    F = spark_imports.F
    non_null_count = count_arg in ("n", "o.n")
    frame_arg = "n" if non_null_count else F.lit(1) if count_arg == "1" else "*"
    count = F.count(frame_arg).alias("n")
    source = f"{table_prefix} AS o" if count_arg == "o.n" else table_prefix
    group_col = "o.g" if count_arg == "o.n" else "g"

    if grouped:
        sql_result = spark.sql(
            f"SELECT {group_col} AS g, COUNT({count_arg}) AS n FROM {source} "
            f"GROUP BY {group_col} ORDER BY g"
        )
        frame_result = orders.groupBy("g").agg(count).orderBy("g")
        expected = (
            []
            if empty
            else [
                {"g": "a", "n": 2 if non_null_count else 3},
                {"g": "b", "n": 0 if non_null_count else 1},
            ]
        )
    else:
        sql_result = spark.sql(f"SELECT COUNT({count_arg}) AS n FROM {source}")
        frame_result = orders.agg(count)
        expected = [{"n": 0 if empty else 2 if non_null_count else 4}]

    return sql_result, frame_result, expected


def test_sql_count_long_schema_and_values(count_results, spark_imports):
    sql_result, frame_result, expected = count_results

    for result in (sql_result, frame_result):
        assert isinstance(result.schema["n"].dataType, spark_imports.LongType)
        assert [row.asDict() for row in result.collect()] == expected


def test_sql_count_arrow_int64_schema_and_values(count_results):
    pa = pytest.importorskip("pyarrow")
    sql_result, frame_result, expected = count_results
    if not hasattr(type(sql_result), "toArrow"):
        pytest.skip("DataFrame.toArrow() requires PySpark 4.0 or newer")

    for result in (sql_result, frame_result):
        arrow = result.toArrow()
        assert arrow.schema.field("n").type == pa.int64()
        assert arrow.to_pylist() == expected

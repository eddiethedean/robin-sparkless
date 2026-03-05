"""
Window API parity tests (#187). Expected outputs are from a prior PySpark 3.5 run;
tests compare backend results to these recorded expectations.
Uses shared spark fixture and get_spark_imports().
"""

from __future__ import annotations

from tests.fixtures.spark_imports import get_spark_imports
from tests.utils import _row_to_dict, assert_rows_equal

_imports = get_spark_imports()
F = _imports.F
Window = _imports.Window


# Shared data: id, salary, dept (dept "a" has 100, 90; dept "b" has 80)
_WINDOW_DATA = [(1, 100, "a"), (2, 90, "a"), (3, 80, "b")]
_WINDOW_COLUMNS = ["id", "salary", "dept"]


# Expected outputs (from prior PySpark 3.5 run; orderBy id so row order is 1,2,3)
EXPECTED_ROW_NUMBER = [
    {"id": 1, "salary": 100, "dept": "a", "rn": 1},
    {"id": 2, "salary": 90, "dept": "a", "rn": 2},
    {"id": 3, "salary": 80, "dept": "b", "rn": 1},
]

EXPECTED_RANK = [
    {"id": 1, "salary": 100, "dept": "a", "rk": 1},
    {"id": 2, "salary": 90, "dept": "a", "rk": 2},
    {"id": 3, "salary": 80, "dept": "b", "rk": 1},
]

EXPECTED_DENSE_RANK = [
    {"id": 1, "salary": 100, "dept": "a", "dr": 1},
    {"id": 2, "salary": 90, "dept": "a", "dr": 2},
    {"id": 3, "salary": 80, "dept": "b", "dr": 1},
]

# lag/lead over dept orderBy id: a has (1,100) then (2,90) -> prev null,100; nxt 90,null. b has (3,80) -> prev null, nxt null
EXPECTED_LAG_LEAD = [
    {"id": 1, "salary": 100, "dept": "a", "prev": None, "nxt": 90},
    {"id": 2, "salary": 90, "dept": "a", "prev": 100, "nxt": None},
    {"id": 3, "salary": 80, "dept": "b", "prev": None, "nxt": None},
]

EXPECTED_SUM_OVER = [
    {"id": 1, "salary": 100, "dept": "a", "total_by_dept": 190},
    {"id": 2, "salary": 90, "dept": "a", "total_by_dept": 190},
    {"id": 3, "salary": 80, "dept": "b", "total_by_dept": 80},
]

# first/last over dept orderBy id: a first=100 last=90; b first=80 last=80
EXPECTED_FIRST_LAST = [
    {"id": 1, "salary": 100, "dept": "a", "first_sal": 100, "last_sal": 90},
    {"id": 2, "salary": 90, "dept": "a", "first_sal": 100, "last_sal": 90},
    {"id": 3, "salary": 80, "dept": "b", "first_sal": 80, "last_sal": 80},
]

EXPECTED_COUNT_OVER = [
    {"id": 1, "salary": 100, "dept": "a", "cnt": 2},
    {"id": 2, "salary": 90, "dept": "a", "cnt": 2},
    {"id": 3, "salary": 80, "dept": "b", "cnt": 1},
]


def test_window_row_number_pyspark_parity(spark) -> None:
    """row_number().over(partition) matches PySpark (#187)."""
    df = spark.createDataFrame(_WINDOW_DATA, _WINDOW_COLUMNS)
    win = Window.partitionBy("dept").orderBy(F.col("salary").desc())
    df = df.withColumn("rn", F.row_number().over(win))
    df = df.orderBy(["id"])
    actual = [_row_to_dict(r) for r in df.collect()]
    assert_rows_equal(actual, EXPECTED_ROW_NUMBER, order_matters=True)


def test_window_rank_pyspark_parity(spark) -> None:
    """rank().over(partition) matches PySpark (#187)."""
    df = spark.createDataFrame(_WINDOW_DATA, _WINDOW_COLUMNS)
    win = Window.partitionBy("dept").orderBy(F.col("salary").desc())
    df = df.withColumn("rk", F.rank().over(win))
    df = df.orderBy(["id"])
    actual = [_row_to_dict(r) for r in df.collect()]
    assert_rows_equal(actual, EXPECTED_RANK, order_matters=True)


def test_window_dense_rank_pyspark_parity(spark) -> None:
    """dense_rank().over(partition) matches PySpark (#187)."""
    df = spark.createDataFrame(_WINDOW_DATA, _WINDOW_COLUMNS)
    win = Window.partitionBy("dept").orderBy(F.col("salary").desc())
    df = df.withColumn("dr", F.dense_rank().over(win))
    df = df.orderBy(["id"])
    actual = [_row_to_dict(r) for r in df.collect()]
    assert_rows_equal(actual, EXPECTED_DENSE_RANK, order_matters=True)


def test_window_lag_lead_pyspark_parity(spark) -> None:
    """lag(1) and lead(1).over(partition) match PySpark (#187)."""
    df = spark.createDataFrame(_WINDOW_DATA, _WINDOW_COLUMNS)
    win = Window.partitionBy("dept").orderBy("id")
    df = df.withColumn("prev", F.lag(F.col("salary"), 1).over(win))
    df = df.withColumn("nxt", F.lead(F.col("salary"), 1).over(win))
    df = df.orderBy(["id"])
    actual = [_row_to_dict(r) for r in df.collect()]
    assert_rows_equal(actual, EXPECTED_LAG_LEAD, order_matters=True)


def test_window_sum_over_pyspark_parity(spark) -> None:
    """sum(col).over(partition) matches PySpark (#187)."""
    df = spark.createDataFrame(_WINDOW_DATA, _WINDOW_COLUMNS)
    win = Window.partitionBy("dept")
    df = df.withColumn(
        "total_by_dept",
        F.sum(F.col("salary")).over(win),
    )
    df = df.orderBy(["id"])
    actual = [_row_to_dict(r) for r in df.collect()]
    assert_rows_equal(actual, EXPECTED_SUM_OVER, order_matters=True)


def test_window_first_last_pyspark_parity(spark) -> None:
    """first_value() and last_value().over(partition) match PySpark (#187)."""
    df = spark.createDataFrame(_WINDOW_DATA, _WINDOW_COLUMNS)
    win = (
        Window.partitionBy("dept")
        .orderBy("id")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    df = df.withColumn(
        "first_sal",
        F.first(F.col("salary")).over(win),
    )
    df = df.withColumn(
        "last_sal",
        F.last(F.col("salary")).over(win),
    )
    df = df.orderBy(["id"])
    actual = [_row_to_dict(r) for r in df.collect()]
    assert_rows_equal(actual, EXPECTED_FIRST_LAST, order_matters=True)


def test_window_count_over_pyspark_parity(spark) -> None:
    """count(col).over(partition) matches PySpark (#187)."""
    df = spark.createDataFrame(_WINDOW_DATA, _WINDOW_COLUMNS)
    win = Window.partitionBy("dept")
    df = df.withColumn(
        "cnt",
        F.count(F.col("salary")).over(win),
    )
    df = df.orderBy(["id"])
    actual = [_row_to_dict(r) for r in df.collect()]
    assert_rows_equal(actual, EXPECTED_COUNT_OVER, order_matters=True)


def test_window_rank_with_ties_pyspark_parity(spark) -> None:
    """rank() with ties: same salary in partition gets same rank (expected from prior PySpark run)."""
    data_tie = [(1, 100, "a"), (2, 100, "a"), (3, 80, "b")]
    expected = [
        {"id": 1, "salary": 100, "dept": "a", "rk": 1},
        {"id": 2, "salary": 100, "dept": "a", "rk": 1},
        {"id": 3, "salary": 80, "dept": "b", "rk": 1},
    ]
    df = spark.createDataFrame(data_tie, _WINDOW_COLUMNS)
    win = Window.partitionBy("dept").orderBy(F.col("salary").desc())
    df = df.withColumn("rk", F.rank().over(win))
    df = df.orderBy(["id"])
    actual = [_row_to_dict(r) for r in df.collect()]
    assert_rows_equal(actual, expected, order_matters=True)

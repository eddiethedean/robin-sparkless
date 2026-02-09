"""
Window API parity tests (#187). Expected outputs are from a prior PySpark 3.5 run;
tests do not execute PySpark at runtime. We compare robin-sparkless results to
these recorded expectations.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils import assert_rows_equal, get_session


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


def _robin_df():
    spark = get_session()
    return spark.create_dataframe(_WINDOW_DATA, _WINDOW_COLUMNS)


# --- Parity tests ---


def test_window_row_number_pyspark_parity() -> None:
    """row_number().over(partition) matches PySpark (#187)."""
    import robin_sparkless as rs

    df = _robin_df()
    df = df.with_column(
        "rn",
        rs.col("salary").row_number(descending=True).over(["dept"]),
    )
    df = df.order_by(["id"])
    actual = df.collect()
    assert_rows_equal(actual, EXPECTED_ROW_NUMBER, order_matters=True)


def test_window_rank_pyspark_parity() -> None:
    """rank().over(partition) matches PySpark (#187)."""
    import robin_sparkless as rs

    df = _robin_df()
    df = df.with_column(
        "rk",
        rs.col("salary").rank(descending=True).over(["dept"]),
    )
    df = df.order_by(["id"])
    actual = df.collect()
    assert_rows_equal(actual, EXPECTED_RANK, order_matters=True)


def test_window_dense_rank_pyspark_parity() -> None:
    """dense_rank().over(partition) matches PySpark (#187)."""
    import robin_sparkless as rs

    df = _robin_df()
    df = df.with_column(
        "dr",
        rs.col("salary").dense_rank(descending=True).over(["dept"]),
    )
    df = df.order_by(["id"])
    actual = df.collect()
    assert_rows_equal(actual, EXPECTED_DENSE_RANK, order_matters=True)


def test_window_lag_lead_pyspark_parity() -> None:
    """lag(1) and lead(1).over(partition) match PySpark (#187)."""
    import robin_sparkless as rs

    df = _robin_df()
    df = df.with_column("prev", rs.col("salary").lag(1).over(["dept"]))
    df = df.with_column("nxt", rs.col("salary").lead(1).over(["dept"]))
    df = df.order_by(["id"])
    actual = df.collect()
    assert_rows_equal(actual, EXPECTED_LAG_LEAD, order_matters=True)


def test_window_sum_over_pyspark_parity() -> None:
    """sum(col).over(partition) matches PySpark (#187)."""
    import robin_sparkless as rs

    df = _robin_df()
    df = df.with_column(
        "total_by_dept",
        rs.sum(rs.col("salary")).over(["dept"]),
    )
    df = df.order_by(["id"])
    actual = df.collect()
    assert_rows_equal(actual, EXPECTED_SUM_OVER, order_matters=True)


def test_window_first_last_pyspark_parity() -> None:
    """first_value() and last_value().over(partition) match PySpark (#187)."""
    import robin_sparkless as rs

    df = _robin_df()
    df = df.with_column(
        "first_sal",
        rs.col("salary").first_value().over(["dept"]),
    )
    df = df.with_column(
        "last_sal",
        rs.col("salary").last_value().over(["dept"]),
    )
    df = df.order_by(["id"])
    actual = df.collect()
    expected = EXPECTED_FIRST_LAST
    assert_rows_equal(actual, expected, order_matters=True)


EXPECTED_COUNT_OVER = [
    {"id": 1, "salary": 100, "dept": "a", "cnt": 2},
    {"id": 2, "salary": 90, "dept": "a", "cnt": 2},
    {"id": 3, "salary": 80, "dept": "b", "cnt": 1},
]


def test_window_count_over_pyspark_parity() -> None:
    """count(col).over(partition) matches PySpark (#187)."""
    import robin_sparkless as rs

    df = _robin_df()
    df = df.with_column(
        "cnt",
        rs.count(rs.col("salary")).over(["dept"]),
    )
    df = df.order_by(["id"])
    actual = df.collect()
    assert_rows_equal(actual, EXPECTED_COUNT_OVER, order_matters=True)


def test_window_rank_with_ties_pyspark_parity() -> None:
    """rank() with ties: same salary in partition gets same rank (expected from prior PySpark run)."""
    import robin_sparkless as rs

    # Data with tie: two 100 in dept "a", one 80 in "b"
    data_tie = [(1, 100, "a"), (2, 100, "a"), (3, 80, "b")]
    expected = [
        {"id": 1, "salary": 100, "dept": "a", "rk": 1},
        {"id": 2, "salary": 100, "dept": "a", "rk": 1},
        {"id": 3, "salary": 80, "dept": "b", "rk": 1},
    ]

    spark = get_session()
    df = spark.create_dataframe(data_tie, _WINDOW_COLUMNS)
    df = df.with_column(
        "rk",
        rs.col("salary").rank(descending=True).over(["dept"]),
    )
    df = df.order_by(["id"])
    actual = df.collect()
    assert_rows_equal(actual, expected, order_matters=True)

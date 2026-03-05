"""
Tests for issue #415: DataFrame.orderBy(list of column names) treats list as single column.

PySpark allows df.orderBy(["col1", "col2"]) - equivalent to orderBy(*["a", "b"]).
Sparkless must unpack a single list argument to multiple columns.

Uses spark fixture - runs with both sparkless and PySpark
(MOCK_SPARK_TEST_BACKEND=pyspark).
"""


def test_orderby_with_list_of_column_names(spark):
    """df.orderBy(["a", "b"]) should order by a then b, matching PySpark."""
    df = spark.createDataFrame(
        [(1, 2, 3), (1, 1, 4), (2, 0, 5)],
        ["a", "b", "c"],
    )
    result = df.orderBy(["a", "b"]).collect()
    assert len(result) == 3
    # Sorted by (a, b): (1,1), (1,2), (2,0)
    assert result[0]["a"] == 1 and result[0]["b"] == 1 and result[0]["c"] == 4
    assert result[1]["a"] == 1 and result[1]["b"] == 2 and result[1]["c"] == 3
    assert result[2]["a"] == 2 and result[2]["b"] == 0 and result[2]["c"] == 5


def test_orderby_with_single_column_list(spark):
    """df.orderBy(["a"]) with single-element list should work."""
    df = spark.createDataFrame(
        [(2,), (1,), (3,)],
        ["a"],
    )
    result = df.orderBy(["a"]).collect()
    assert len(result) == 3
    assert result[0]["a"] == 1
    assert result[1]["a"] == 2
    assert result[2]["a"] == 3


def test_orderby_desc_with_list(spark):
    """df.orderBy(["a", "b"], ascending=False) should work."""
    df = spark.createDataFrame(
        [(1, 2, 3), (1, 1, 4), (2, 0, 5)],
        ["a", "b", "c"],
    )
    result = df.orderBy(["a", "b"], ascending=False).collect()
    assert len(result) == 3
    # Descending: (2,0), (1,2), (1,1)
    assert result[0]["a"] == 2 and result[0]["b"] == 0
    assert result[1]["a"] == 1 and result[1]["b"] == 2
    assert result[2]["a"] == 1 and result[2]["b"] == 1


def test_orderby_with_df_columns(spark):
    """df.orderBy(df.columns) should work - df.columns returns a list."""
    df = spark.createDataFrame(
        [
            {"name": "Charlie", "dept": "IT", "salary": 70000},
            {"name": "Alice", "dept": "IT", "salary": 50000},
            {"name": "Bob", "dept": "HR", "salary": 60000},
        ]
    )
    result = df.orderBy(df.columns).collect()
    assert len(result) == 3
    # Sorted by dept, name, salary (column order)
    assert result[0]["dept"] == "HR"
    assert result[0]["name"] == "Bob"
    assert result[1]["dept"] == "IT"
    assert result[1]["name"] == "Alice"
    assert result[2]["dept"] == "IT"
    assert result[2]["name"] == "Charlie"


def test_orderby_with_string_columns(spark):
    """df.orderBy with list of string column names."""
    df = spark.createDataFrame(
        [
            {"dept": "Z", "name": "Zara"},
            {"dept": "A", "name": "Alice"},
            {"dept": "A", "name": "Bob"},
        ]
    )
    result = df.orderBy(["dept", "name"]).collect()
    assert len(result) == 3
    assert result[0]["dept"] == "A" and result[0]["name"] == "Alice"
    assert result[1]["dept"] == "A" and result[1]["name"] == "Bob"
    assert result[2]["dept"] == "Z" and result[2]["name"] == "Zara"


def test_orderby_with_three_columns(spark):
    """df.orderBy with list of 3+ columns."""
    df = spark.createDataFrame(
        [
            (3, 2, 1),
            (1, 2, 3),
            (1, 1, 2),
            (1, 1, 1),
        ],
        ["a", "b", "c"],
    )
    result = df.orderBy(["a", "b", "c"]).collect()
    assert len(result) == 4
    assert (result[0]["a"], result[0]["b"], result[0]["c"]) == (1, 1, 1)
    assert (result[1]["a"], result[1]["b"], result[1]["c"]) == (1, 1, 2)
    assert (result[2]["a"], result[2]["b"], result[2]["c"]) == (1, 2, 3)
    assert (result[3]["a"], result[3]["b"], result[3]["c"]) == (3, 2, 1)


def test_orderby_then_limit(spark):
    """df.orderBy([...]).limit(n) chained."""
    df = spark.createDataFrame(
        [(3,), (1,), (2,), (4,)],
        ["x"],
    )
    result = df.orderBy(["x"]).limit(2).collect()
    assert len(result) == 2
    assert result[0]["x"] == 1
    assert result[1]["x"] == 2


def test_filter_then_orderby_list(spark):
    """df.filter(...).orderBy([...]) chained."""
    df = spark.createDataFrame(
        [(1, "a"), (2, "b"), (3, "a"), (4, "b")],
        ["id", "grp"],
    )
    result = df.filter("grp = 'a'").orderBy(["id"]).collect()
    assert len(result) == 2
    assert result[0]["id"] == 1 and result[0]["grp"] == "a"
    assert result[1]["id"] == 3 and result[1]["grp"] == "a"


def test_orderby_then_select(spark):
    """df.orderBy([...]).select(...) chained."""
    df = spark.createDataFrame(
        [(3, 30), (1, 10), (2, 20)],
        ["a", "b"],
    )
    result = df.orderBy(["a"]).select("a", "b").collect()
    assert len(result) == 3
    assert result[0]["a"] == 1 and result[0]["b"] == 10
    assert result[1]["a"] == 2 and result[1]["b"] == 20
    assert result[2]["a"] == 3 and result[2]["b"] == 30


def test_orderby_with_explicit_list_variable(spark):
    """orderBy(columns_to_sort) where variable is a list."""
    df = spark.createDataFrame(
        [(2, 2), (1, 1), (3, 3)],
        ["x", "y"],
    )
    columns_to_sort = ["x"]
    result = df.orderBy(columns_to_sort).collect()
    assert len(result) == 3
    assert result[0]["x"] == 1
    assert result[1]["x"] == 2
    assert result[2]["x"] == 3


def test_orderby_list_empty_dataframe(spark):
    """orderBy([...]) on empty DataFrame should not raise."""
    df = spark.createDataFrame([], "a int, b int")
    result = df.orderBy(["a", "b"]).collect()
    assert len(result) == 0

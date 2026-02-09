"""
Robust tests for issue #182: select() must evaluate Column expressions, not resolve them by name.

Previously select() tried extract::<String>() before extract::<PyColumn>(), so expressions like
lit(2) + col("x") were treated as a column named "(2 + x)" and raised RuntimeError: not found.

These tests assert that select() accepts Column expressions (and mixes of names + expressions)
and produces the correct computed columns.
"""

from __future__ import annotations


def test_select_expression_literal_plus_column() -> None:
    """select(lit(2) + col('x')) evaluates expression; plus_two = [12, 22] for x=[10, 20]."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows([{"x": 10}, {"x": 20}], [("x", "int")])

    # Without alias: one column, values 12 and 22 (column name is engine-defined)
    result = df.select(rs.lit(2) + rs.col("x")).collect()
    assert len(result) == 2
    vals = [list(r.values())[0] for r in result]
    assert vals == [12, 22]

    result_with_alias = df.select((rs.lit(2) + rs.col("x")).alias("plus_two")).collect()
    assert result_with_alias == [{"plus_two": 12}, {"plus_two": 22}]


def test_select_expression_literal_times_column() -> None:
    """select(lit(3) * col('x')) evaluates expression; tripled = [30, 60] for x=[10, 20]."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows([{"x": 10}, {"x": 20}], [("x", "int")])

    result = df.select((rs.lit(3) * rs.col("x")).alias("tripled")).collect()
    assert result == [{"tripled": 30}, {"tripled": 60}]


def test_select_expression_aliased() -> None:
    """select((col('a') * 2).alias('doubled')) evaluates expression; doubled = [2, 4, 6] for a=[1,2,3]."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [{"a": 1}, {"a": 2}, {"a": 3}], [("a", "int")]
    )

    result = df.select((rs.col("a") * 2).alias("doubled")).collect()
    assert result == [{"doubled": 2}, {"doubled": 4}, {"doubled": 6}]


def test_select_mixed_column_names_and_expressions() -> None:
    """select('x', lit(2) + col('x')) returns original column and computed column."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows([{"x": 10}, {"x": 20}], [("x", "int")])

    result = df.select("x", (rs.lit(2) + rs.col("x")).alias("plus_two")).collect()
    assert result == [{"x": 10, "plus_two": 12}, {"x": 20, "plus_two": 22}]


def test_select_list_of_expressions() -> None:
    """select([expr1, expr2]) with list of Column expressions works."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [{"a": 1, "b": 10}, {"a": 2, "b": 20}],
        [("a", "int"), ("b", "int")],
    )

    result = df.select(
        [
            rs.col("a"),
            (rs.col("a") + rs.col("b")).alias("sum_ab"),
        ]
    ).collect()
    assert result == [
        {"a": 1, "sum_ab": 11},
        {"a": 2, "sum_ab": 22},
    ]


def test_select_varargs_expressions() -> None:
    """select(expr1, expr2) with varargs Column expressions works."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows([{"x": 5}, {"x": 7}], [("x", "int")])

    result = df.select(
        rs.col("x"),
        (rs.col("x") * 2).alias("doubled"),
        (rs.lit(1) + rs.col("x")).alias("plus_one"),
    ).collect()
    assert result == [
        {"x": 5, "doubled": 10, "plus_one": 6},
        {"x": 7, "doubled": 14, "plus_one": 8},
    ]


def test_select_string_column_names_still_work() -> None:
    """select('a', 'b') and select(['a', 'b']) still resolve by name (backward compatibility)."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}],
        [("a", "int"), ("b", "int"), ("c", "int")],
    )

    result_list = df.select(["a", "b"]).collect()
    assert result_list == [{"a": 1, "b": 2}, {"a": 4, "b": 5}]

    result_varargs = df.select("a", "b").collect()
    assert result_varargs == [{"a": 1, "b": 2}, {"a": 4, "b": 5}]


def test_select_mixed_strings_and_expressions() -> None:
    """select('a', col('b') * 2, 'c') mixes column names and expressions."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [{"a": 1, "b": 10, "c": 100}, {"a": 2, "b": 20, "c": 200}],
        [("a", "int"), ("b", "int"), ("c", "int")],
    )

    result = df.select(
        "a",
        (rs.col("b") * 2).alias("b_twice"),
        "c",
    ).collect()
    assert result == [
        {"a": 1, "b_twice": 20, "c": 100},
        {"a": 2, "b_twice": 40, "c": 200},
    ]


def test_select_expression_with_multiply_literal_left() -> None:
    """select(3 * col('x')) - literal on left (issue #182 repro)."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows([{"x": 10}, {"x": 20}], [("x", "int")])

    result = df.select((3 * rs.col("x")).alias("tripled")).collect()
    assert result == [{"tripled": 30}, {"tripled": 60}]


def test_select_complex_expression() -> None:
    """select((col('a') + col('b')) * col('c')) - chained expression."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}],
        [("a", "int"), ("b", "int"), ("c", "int")],
    )

    result = df.select(
        ((rs.col("a") + rs.col("b")) * rs.col("c")).alias("product")
    ).collect()
    assert result == [{"product": 9}, {"product": 54}]


def test_select_single_expression_no_alias() -> None:
    """select(expr) with one expression and no alias gets default name (e.g. 'literal')."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows([{"x": 1}], [("x", "int")])

    result = df.select(rs.col("x") + rs.lit(10)).collect()
    assert len(result) == 1
    row = result[0]
    assert len(row) == 1
    # Polars may use "literal" or similar for unnamed expr
    val = list(row.values())[0]
    assert val == 11

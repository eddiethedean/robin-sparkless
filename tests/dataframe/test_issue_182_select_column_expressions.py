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
    from tests.utils import get_functions, get_spark, _row_to_dict

    F = get_functions()

    spark = get_spark("issue_182")
    df = spark.createDataFrame([{"x": 10}, {"x": 20}], ["x"])

    # Without alias: one column, values 12 and 22 (column name is engine-defined)
    result = df.select(F.lit(2) + F.col("x")).collect()
    assert len(result) == 2
    vals = [list(_row_to_dict(r).values())[0] for r in result]
    assert vals == [12, 22]

    result_with_alias = df.select((F.lit(2) + F.col("x")).alias("plus_two")).collect()
    as_dicts = [_row_to_dict(r) for r in result_with_alias]
    assert as_dicts == [{"plus_two": 12}, {"plus_two": 22}]


def test_select_expression_literal_times_column() -> None:
    """select(lit(3) * col('x')) evaluates expression; tripled = [30, 60] for x=[10, 20]."""
    from tests.utils import get_functions, get_spark, _row_to_dict

    F = get_functions()

    spark = get_spark("issue_182")
    df = spark.createDataFrame([{"x": 10}, {"x": 20}], ["x"])

    result = df.select((F.lit(3) * F.col("x")).alias("tripled")).collect()
    as_dicts = [_row_to_dict(r) for r in result]
    assert as_dicts == [{"tripled": 30}, {"tripled": 60}]


def test_select_expression_aliased() -> None:
    """select((col('a') * 2).alias('doubled')) evaluates expression; doubled = [2, 4, 6] for a=[1,2,3]."""
    from tests.utils import get_functions, get_spark, _row_to_dict

    F = get_functions()

    spark = get_spark("issue_182")
    df = spark.createDataFrame([{"a": 1}, {"a": 2}, {"a": 3}], ["a"])

    result = df.select((F.col("a") * 2).alias("doubled")).collect()
    as_dicts = [_row_to_dict(r) for r in result]
    assert as_dicts == [{"doubled": 2}, {"doubled": 4}, {"doubled": 6}]


def test_select_mixed_column_names_and_expressions() -> None:
    """select('x', lit(2) + col('x')) returns original column and computed column."""
    from tests.utils import get_functions, get_spark, _row_to_dict

    F = get_functions()

    spark = get_spark("issue_182")
    df = spark.createDataFrame([{"x": 10}, {"x": 20}], ["x"])

    result = df.select("x", (F.lit(2) + F.col("x")).alias("plus_two")).collect()
    as_dicts = [_row_to_dict(r) for r in result]
    assert as_dicts == [{"x": 10, "plus_two": 12}, {"x": 20, "plus_two": 22}]


def test_select_list_of_expressions() -> None:
    """select([expr1, expr2]) with list of Column expressions works."""
    from tests.utils import get_functions, get_spark, _row_to_dict

    F = get_functions()

    spark = get_spark("issue_182")
    df = spark.createDataFrame(
        [{"a": 1, "b": 10}, {"a": 2, "b": 20}],
        ["a", "b"],
    )

    result = df.select(
        [
            F.col("a"),
            (F.col("a") + F.col("b")).alias("sum_ab"),
        ]
    ).collect()
    as_dicts = [_row_to_dict(r) for r in result]
    assert as_dicts == [
        {"a": 1, "sum_ab": 11},
        {"a": 2, "sum_ab": 22},
    ]


def test_select_varargs_expressions() -> None:
    """select(expr1, expr2) with varargs Column expressions works."""
    from tests.utils import get_functions, get_spark, _row_to_dict

    F = get_functions()

    spark = get_spark("issue_182")
    df = spark.createDataFrame([{"x": 5}, {"x": 7}], ["x"])

    result = df.select(
        F.col("x"),
        (F.col("x") * 2).alias("doubled"),
        (F.lit(1) + F.col("x")).alias("plus_one"),
    ).collect()
    as_dicts = [_row_to_dict(r) for r in result]
    assert as_dicts == [
        {"x": 5, "doubled": 10, "plus_one": 6},
        {"x": 7, "doubled": 14, "plus_one": 8},
    ]


def test_select_string_column_names_still_work() -> None:
    """select('a', 'b') and select(['a', 'b']) still resolve by name (backward compatibility)."""
    from tests.utils import get_spark, _row_to_dict

    spark = get_spark("issue_182")
    df = spark.createDataFrame(
        [{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}],
        ["a", "b", "c"],
    )

    result_list = df.select(["a", "b"]).collect()
    as_dicts_list = [_row_to_dict(r) for r in result_list]
    assert as_dicts_list == [{"a": 1, "b": 2}, {"a": 4, "b": 5}]

    result_varargs = df.select("a", "b").collect()
    as_dicts_varargs = [_row_to_dict(r) for r in result_varargs]
    assert as_dicts_varargs == [{"a": 1, "b": 2}, {"a": 4, "b": 5}]


def test_select_mixed_strings_and_expressions() -> None:
    """select('a', col('b') * 2, 'c') mixes column names and expressions."""
    from tests.utils import get_functions, get_spark, _row_to_dict

    F = get_functions()

    spark = get_spark("issue_182")
    df = spark.createDataFrame(
        [{"a": 1, "b": 10, "c": 100}, {"a": 2, "b": 20, "c": 200}],
        ["a", "b", "c"],
    )

    result = df.select(
        "a",
        (F.col("b") * 2).alias("b_twice"),
        "c",
    ).collect()
    as_dicts = [_row_to_dict(r) for r in result]
    assert as_dicts == [
        {"a": 1, "b_twice": 20, "c": 100},
        {"a": 2, "b_twice": 40, "c": 200},
    ]


def test_select_expression_with_multiply_literal_left() -> None:
    """select(3 * col('x')) - literal on left (issue #182 repro)."""
    from tests.utils import get_functions, get_spark, _row_to_dict

    F = get_functions()

    spark = get_spark("issue_182")
    df = spark.createDataFrame([{"x": 10}, {"x": 20}], ["x"])

    result = df.select((3 * F.col("x")).alias("tripled")).collect()
    as_dicts = [_row_to_dict(r) for r in result]
    assert as_dicts == [{"tripled": 30}, {"tripled": 60}]


def test_select_complex_expression() -> None:
    """select((col('a') + col('b')) * col('c')) - chained expression."""
    from tests.utils import get_functions, get_spark, _row_to_dict

    F = get_functions()

    spark = get_spark("issue_182")
    df = spark.createDataFrame(
        [{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}],
        ["a", "b", "c"],
    )

    result = df.select(
        ((F.col("a") + F.col("b")) * F.col("c")).alias("product")
    ).collect()
    as_dicts = [_row_to_dict(r) for r in result]
    assert as_dicts == [{"product": 9}, {"product": 54}]


def test_select_single_expression_no_alias() -> None:
    """select(expr) with one expression and no alias gets default name (e.g. 'literal')."""
    from tests.utils import get_functions, get_spark, _row_to_dict

    F = get_functions()

    spark = get_spark("issue_182")
    df = spark.createDataFrame([{"x": 1}], ["x"])

    result = df.select(F.col("x") + F.lit(10)).collect()
    assert len(result) == 1
    row = _row_to_dict(result[0])
    assert len(row) == 1
    # Polars may use "literal" or similar for unnamed expr
    val = list(row.values())[0]
    assert val == 11

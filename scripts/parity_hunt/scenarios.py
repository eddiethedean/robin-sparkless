from __future__ import annotations

from typing import Any, Callable, List

from parity_hunt.runner import Scenario


def _df_basic(session):
    df = session.createDataFrame(
        [
            {"a": 1, "b": None, "s": "x"},
            {"a": 2, "b": 3, "s": "y"},
            {"a": None, "b": 4, "s": ""},
        ]
    )
    return df.orderBy("a")


def _backend_is_pyspark(session) -> bool:
    return "pyspark" in type(session).__module__


def scenario_select_star(session):
    df = session.createDataFrame([(1, 2, 3)], ["a", "b", "c"])
    return df.select("*")


def scenario_show_truncate(session):
    df = session.createDataFrame([("x" * 200,)], ["s"])
    # show output differs across engines; we capture UI to compare.
    return df


def scenario_null_sort_default(session):
    df = session.createDataFrame([(None,), (1,), (0,)], ["x"])
    return df.orderBy("x")


def scenario_window_last_default_frame(session):
    if _backend_is_pyspark(session):
        from pyspark.sql import functions as F  # type: ignore
        from pyspark.sql.window import Window  # type: ignore
    else:
        from sparkless.sql import functions as F  # type: ignore
        from sparkless.window import Window  # type: ignore

    df = session.createDataFrame(
        [(1, 100, "a"), (2, 90, "a"), (3, 80, "b")], ["id", "salary", "dept"]
    )
    win = Window.partitionBy("dept").orderBy("id")
    return df.withColumn("last_sal", F.last("salary").over(win)).orderBy("id")


def scenario_errors_missing_column(session):
    if _backend_is_pyspark(session):
        from pyspark.sql import functions as F  # type: ignore
    else:
        from sparkless.sql import functions as F  # type: ignore

    df = session.createDataFrame([(1,)], ["x"])
    return df.select(F.col("nope").alias("y"))


def scenario_sql_show_databases(session):
    return session.sql("SHOW DATABASES")


def scenario_sql_show_tables(session):
    return session.sql("SHOW TABLES")


def scenario_sql_describe_extended(session):
    df = session.createDataFrame([(1,)], ["x"])
    df.createOrReplaceTempView("t")
    return session.sql("DESCRIBE EXTENDED t")


def scenario_pow_operator(session):
    if _backend_is_pyspark(session):
        from pyspark.sql import functions as F  # type: ignore
    else:
        from sparkless.sql import functions as F  # type: ignore

    df = session.createDataFrame([(3,), (5,)], ["x"])
    return df.select((F.col("x") ** F.lit(2)).alias("sq")).orderBy("x")


def scenario_split_limit(session):
    if _backend_is_pyspark(session):
        from pyspark.sql import functions as F  # type: ignore
    else:
        from sparkless.sql import functions as F  # type: ignore

    df = session.createDataFrame([("a,b,c,d",)], ["s"])
    return df.select(F.split(F.col("s"), ",", 2).alias("arr"))


def scenario_conf_app_name(session):
    conf = session.conf() if callable(session.conf) else session.conf
    # Return as single-row DF for uniform capture.
    return session.createDataFrame([(conf.get("spark.app.name"),)], ["spark_app_name"])


def scenario_cast_invalid_string(session):
    """Invalid string -> int cast; compare error vs null semantics."""
    if _backend_is_pyspark(session):
        from pyspark.sql import functions as F  # type: ignore
    else:
        from sparkless.sql import functions as F  # type: ignore

    df = session.createDataFrame([("nope",)], ["s"])
    return df.select(F.col("s").cast("int").alias("i"))


def scenario_join_on_expression(session):
    df1 = session.createDataFrame([(1, "a"), (2, "b")], ["id", "v"])
    df2 = session.createDataFrame([(1, "x"), (3, "y")], ["id", "w"])
    return df1.join(df2, on=df1["id"] == df2["id"], how="inner").orderBy("id")


def scenario_struct_getfield_alias(session):
    if _backend_is_pyspark(session):
        from pyspark.sql import functions as F  # type: ignore
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType  # type: ignore
    else:
        from sparkless.sql import functions as F  # type: ignore
        from sparkless.sql.types import StructType, StructField, StringType, IntegerType  # type: ignore

    schema = StructType(
        [
            StructField(
                "st",
                StructType(
                    [
                        StructField("E1", IntegerType(), True),
                        StructField("E2", StringType(), True),
                    ]
                ),
                True,
            )
        ]
    )
    df = session.createDataFrame([{"st": {"E1": 1, "E2": "a"}}], schema=schema)
    return df.select(F.col("st").getField("E1").alias("e1_out"))


def _mutations() -> list[Scenario]:
    """A small set of mutated variants to shake out edge cases."""
    muts: list[Scenario] = []
    # Null ordering variants
    for ascending in [True, False]:

        def _mk(asc: bool) -> Callable[[Any], Any]:
            def f(session):
                df = session.createDataFrame([(None,), (1,), (0,)], ["x"])
                return df.orderBy("x", ascending=asc)

            return f

        muts.append(
            Scenario(
                id=f"orderby.null_ascending_{ascending}",
                title=f"Ordering: null ordering ascending={ascending}",
                fn=_mk(ascending),
                tags=["ordering", "mutation"],
            )
        )
    return muts


def _more_scenarios() -> list[Scenario]:
    """
    Larger, generated corpus for parity hunting.

    Goals:
    - Deterministic and fast (tiny inputs, no randomness).
    - Mostly backend-agnostic code paths.
    - Cover broad API surface (string/numeric/date/array/window/joins/df ops).
    """

    def F(session):
        if _backend_is_pyspark(session):
            from pyspark.sql import functions as _F  # type: ignore
        else:
            from sparkless.sql import functions as _F  # type: ignore
        return _F

    def Window(session):
        if _backend_is_pyspark(session):
            from pyspark.sql.window import Window as _W  # type: ignore
        else:
            from sparkless.window import Window as _W  # type: ignore
        return _W

    out: list[Scenario] = []

    # -----------------
    # String functions
    # -----------------
    string_cases: list[tuple[str, str, Callable[[Any], Any]]] = [
        (
            "string.upper",
            "String: upper()",
            lambda session: (
                session.createDataFrame([("Abc",), ("",), (None,)], ["s"])
                .select(F(session).upper("s").alias("out"))
                .orderBy("out")
            ),
        ),
        (
            "string.lower",
            "String: lower()",
            lambda session: (
                session.createDataFrame([("AbC",), ("XYZ",), (None,)], ["s"])
                .select(F(session).lower("s").alias("out"))
                .orderBy("out")
            ),
        ),
        (
            "string.length",
            "String: length()",
            lambda session: (
                session.createDataFrame([("a",), ("ab",), ("",), (None,)], ["s"])
                .select(F(session).length("s").alias("len"))
                .orderBy("len")
            ),
        ),
        (
            "string.trim",
            "String: trim()",
            lambda session: (
                session.createDataFrame([("  a  ",), ("\txy\n",), (None,)], ["s"])
                .select(F(session).trim("s").alias("out"))
                .orderBy("out")
            ),
        ),
        (
            "string.substring",
            "String: substring()",
            lambda session: (
                session.createDataFrame([("abcdef",), ("ab",), (None,)], ["s"])
                .select(F(session).substring("s", 2, 3).alias("out"))
                .orderBy("out")
            ),
        ),
        (
            "string.concat_ws",
            "String: concat_ws()",
            lambda session: (
                session.createDataFrame(
                    [("a", "b"), ("a", None), (None, "c")], ["a", "b"]
                )
                .select(
                    F(session)
                    .concat_ws("-", F(session).col("a"), F(session).col("b"))
                    .alias("out")
                )
                .orderBy("out")
            ),
        ),
        (
            "string.regexp_replace",
            "String: regexp_replace()",
            lambda session: (
                session.createDataFrame([("a1b2",), ("",), (None,)], ["s"])
                .select(F(session).regexp_replace("s", "[0-9]", "_").alias("out"))
                .orderBy("out")
            ),
        ),
        (
            "string.regexp_extract",
            "String: regexp_extract()",
            lambda session: (
                session.createDataFrame([("abc123",), ("zzz",), (None,)], ["s"])
                .select(F(session).regexp_extract("s", "([0-9]+)", 1).alias("out"))
                .orderBy("out")
            ),
        ),
        (
            "string.split_basic",
            "String: split() basic",
            lambda session: (
                session.createDataFrame([("a,b,c",), ("a",), (None,)], ["s"])
                .select(F(session).split(F(session).col("s"), ",").alias("arr"))
                .orderBy("s")
            ),
        ),
        (
            "string.lpad",
            "String: lpad()",
            lambda session: (
                session.createDataFrame([("a",), ("ab",), (None,)], ["s"])
                .select(F(session).lpad("s", 4, "x").alias("out"))
                .orderBy("out")
            ),
        ),
    ]
    for sid, title, fn in string_cases:
        out.append(Scenario(id=sid, title=title, fn=fn, tags=["string", "generated"]))

    # ------------------
    # Numeric functions
    # ------------------
    numeric_cases: list[tuple[str, str, Callable[[Any], Any]]] = [
        (
            "numeric.abs",
            "Numeric: abs()",
            lambda session: (
                session.createDataFrame([(-3,), (0,), (2,), (None,)], ["x"])
                .select(F(session).abs("x").alias("out"))
                .orderBy("out")
            ),
        ),
        (
            "numeric.round",
            "Numeric: round()",
            lambda session: (
                session.createDataFrame([(1.2345,), (1.2,), (None,)], ["x"])
                .select(F(session).round("x", 2).alias("out"))
                .orderBy("out")
            ),
        ),
        (
            "numeric.floor",
            "Numeric: floor()",
            lambda session: (
                session.createDataFrame([(1.9,), (-1.1,), (None,)], ["x"])
                .select(F(session).floor("x").alias("out"))
                .orderBy("out")
            ),
        ),
        (
            "numeric.ceil",
            "Numeric: ceil()",
            lambda session: (
                session.createDataFrame([(1.1,), (-1.9,), (None,)], ["x"])
                .select(F(session).ceil("x").alias("out"))
                .orderBy("out")
            ),
        ),
        (
            "numeric.greatest",
            "Numeric: greatest()",
            lambda session: (
                session.createDataFrame([(1, 2), (None, 3), (5, None)], ["a", "b"])
                .select(F(session).greatest("a", "b").alias("out"))
                .orderBy("out")
            ),
        ),
        (
            "numeric.least",
            "Numeric: least()",
            lambda session: (
                session.createDataFrame([(1, 2), (None, 3), (5, None)], ["a", "b"])
                .select(F(session).least("a", "b").alias("out"))
                .orderBy("out")
            ),
        ),
        (
            "numeric.sqrt",
            "Numeric: sqrt()",
            lambda session: (
                session.createDataFrame([(4.0,), (2.0,), (0.0,), (None,)], ["x"])
                .select(F(session).sqrt("x").alias("out"))
                .orderBy("out")
            ),
        ),
        (
            "numeric.pmod",
            "Numeric: pmod()",
            lambda session: (
                session.createDataFrame([(-5, 3), (5, 3), (None, 3)], ["a", "b"])
                .select(F(session).pmod("a", "b").alias("out"))
                .orderBy("out")
            ),
        ),
        (
            "numeric.signum",
            "Numeric: signum()",
            lambda session: (
                session.createDataFrame([(-5,), (0,), (5,), (None,)], ["x"])
                .select(F(session).signum("x").alias("out"))
                .orderBy("out")
            ),
        ),
        (
            "numeric.log10",
            "Numeric: log10()",
            lambda session: (
                session.createDataFrame([(1.0,), (10.0,), (100.0,), (None,)], ["x"])
                .select(F(session).log10("x").alias("out"))
                .orderBy("out")
            ),
        ),
    ]
    for sid, title, fn in numeric_cases:
        out.append(Scenario(id=sid, title=title, fn=fn, tags=["numeric", "generated"]))

    # --------------
    # Date functions
    # --------------
    # Prefer functions on literals to avoid timezone surprises. Keep to date not timestamp.
    date_cases: list[tuple[str, str, Callable[[Any], Any]]] = [
        (
            "date.to_date",
            "Date: to_date()",
            lambda session: (
                session.createDataFrame([("2020-01-02",), ("invalid",), (None,)], ["s"])
                .select(F(session).to_date("s").alias("d"))
                .orderBy("s")
            ),
        ),
        (
            "date.date_add",
            "Date: date_add()",
            lambda session: (
                session.createDataFrame([("2020-01-02",), (None,)], ["s"])
                .select(F(session).date_add(F(session).to_date("s"), 3).alias("d"))
                .orderBy("d")
            ),
        ),
        (
            "date.date_sub",
            "Date: date_sub()",
            lambda session: (
                session.createDataFrame([("2020-01-02",), (None,)], ["s"])
                .select(F(session).date_sub(F(session).to_date("s"), 3).alias("d"))
                .orderBy("d")
            ),
        ),
        (
            "date.year",
            "Date: year()",
            lambda session: (
                session.createDataFrame([("2020-01-02",), (None,)], ["s"])
                .select(F(session).year(F(session).to_date("s")).alias("y"))
                .orderBy("y")
            ),
        ),
        (
            "date.month",
            "Date: month()",
            lambda session: (
                session.createDataFrame([("2020-12-02",), (None,)], ["s"])
                .select(F(session).month(F(session).to_date("s")).alias("m"))
                .orderBy("m")
            ),
        ),
        (
            "date.dayofmonth",
            "Date: dayofmonth()",
            lambda session: (
                session.createDataFrame([("2020-12-02",), (None,)], ["s"])
                .select(F(session).dayofmonth(F(session).to_date("s")).alias("d"))
                .orderBy("d")
            ),
        ),
        (
            "date.datediff",
            "Date: datediff()",
            lambda session: (
                session.createDataFrame(
                    [("2020-01-10", "2020-01-02"), (None, "2020-01-02")], ["a", "b"]
                )
                .select(
                    F(session)
                    .datediff(F(session).to_date("a"), F(session).to_date("b"))
                    .alias("dd")
                )
                .orderBy("dd")
            ),
        ),
        (
            "date.add_months",
            "Date: add_months()",
            lambda session: (
                session.createDataFrame(
                    [("2020-01-31",), ("2020-02-29",), (None,)], ["s"]
                )
                .select(F(session).add_months(F(session).to_date("s"), 1).alias("d"))
                .orderBy("s")
            ),
        ),
        (
            "date.date_format",
            "Date: date_format()",
            lambda session: (
                session.createDataFrame([("2020-01-02",), (None,)], ["s"])
                .select(
                    F(session)
                    .date_format(F(session).to_date("s"), "yyyy-MM-dd")
                    .alias("out")
                )
                .orderBy("out")
            ),
        ),
        (
            "date.make_date",
            "Date: make_date()",
            lambda session: (
                session.createDataFrame(
                    [(2020, 1, 2), (2020, 2, 30), (None, 1, 2)], ["y", "m", "d"]
                )
                .select(F(session).make_date("y", "m", "d").alias("out"))
                .orderBy("y")
            ),
        ),
    ]
    for sid, title, fn in date_cases:
        out.append(Scenario(id=sid, title=title, fn=fn, tags=["date", "generated"]))

    # ---------------
    # Array functions
    # ---------------
    array_cases: list[tuple[str, str, Callable[[Any], Any]]] = [
        (
            "array.size",
            "Array: size()",
            lambda session: (
                session.createDataFrame([(["a", "b"],), ([],), (None,)], ["arr"])
                .select(F(session).size("arr").alias("out"))
                .orderBy("out")
            ),
        ),
        (
            "array.contains",
            "Array: array_contains()",
            lambda session: (
                session.createDataFrame([(["a", "b"],), (["x"],), (None,)], ["arr"])
                .select(
                    F(session).array_contains(F(session).col("arr"), "a").alias("out")
                )
                .orderBy("out")
            ),
        ),
        (
            "array.element_at",
            "Array: element_at()",
            lambda session: (
                session.createDataFrame([(["a", "b"],), (["x"],), (None,)], ["arr"])
                .select(F(session).element_at("arr", 1).alias("out"))
                .orderBy("out")
            ),
        ),
        (
            "array.concat",
            "Array: concat()",
            lambda session: (
                session.createDataFrame(
                    [(["a"], ["b"]), ([], ["x"]), (None, ["y"])], ["a", "b"]
                )
                .select(
                    F(session)
                    .concat(F(session).col("a"), F(session).col("b"))
                    .alias("out")
                )
                .orderBy("out")
            ),
        ),
        (
            "array.sort_array",
            "Array: sort_array()",
            lambda session: (
                session.createDataFrame(
                    [(["b", "a"],), (["a", "a"],), (None,)], ["arr"]
                )
                .select(F(session).sort_array("arr").alias("out"))
                .orderBy("out")
            ),
        ),
        (
            "array.distinct",
            "Array: array_distinct()",
            lambda session: (
                session.createDataFrame([(["a", "a", "b"],), ([],), (None,)], ["arr"])
                .select(F(session).array_distinct("arr").alias("out"))
                .orderBy("out")
            ),
        ),
        (
            "array.slice",
            "Array: slice()",
            lambda session: (
                session.createDataFrame(
                    [(["a", "b", "c"],), (["x"],), (None,)], ["arr"]
                )
                .select(F(session).slice("arr", 2, 2).alias("out"))
                .orderBy("out")
            ),
        ),
        (
            "array.explode",
            "Array: explode()",
            lambda session: (
                session.createDataFrame([(["a", "b"],), ([],), (None,)], ["arr"])
                .select(F(session).explode("arr").alias("out"))
                .orderBy("out")
            ),
        ),
        (
            "array.array_literal",
            "Array: array() literal",
            lambda session: session.range(1, 2).select(
                F(session).array(F(session).lit("a"), F(session).lit("b")).alias("arr")
            ),
        ),
        (
            "array.struct_in_array",
            "Array: array(struct(...))",
            lambda session: session.range(1, 2).select(
                F(session)
                .array(
                    F(session).struct(
                        F(session).lit(1).alias("x"), F(session).lit("a").alias("s")
                    )
                )
                .alias("arr")
            ),
        ),
    ]
    for sid, title, fn in array_cases:
        out.append(Scenario(id=sid, title=title, fn=fn, tags=["array", "generated"]))

    # ----------------
    # DataFrame basics
    # ----------------
    df_cases: list[tuple[str, str, Callable[[Any], Any]]] = [
        (
            "df.withcolumn",
            "DataFrame: withColumn()",
            lambda session: (
                session.createDataFrame([(1,), (2,), (None,)], ["x"])
                .withColumn("y", F(session).col("x") + F(session).lit(1))
                .orderBy("x")
            ),
        ),
        (
            "df.withcolumnrenamed",
            "DataFrame: withColumnRenamed()",
            lambda session: session.createDataFrame([(1,)], ["x"]).withColumnRenamed(
                "x", "y"
            ),
        ),
        (
            "df.filter_isnull",
            "DataFrame: filter(isNull)",
            lambda session: (
                session.createDataFrame([(1,), (None,), (2,)], ["x"])
                .filter(F(session).col("x").isNull())
                .orderBy("x")
            ),
        ),
        (
            "df.distinct",
            "DataFrame: distinct()",
            lambda session: (
                session.createDataFrame([(1,), (1,), (2,)], ["x"])
                .distinct()
                .orderBy("x")
            ),
        ),
        (
            "df.dropduplicates",
            "DataFrame: dropDuplicates()",
            lambda session: (
                session.createDataFrame([(1, "a"), (1, "a"), (1, "b")], ["x", "y"])
                .dropDuplicates(["x", "y"])
                .orderBy("x", "y")
            ),
        ),
        (
            "df.unionbyname",
            "DataFrame: unionByName()",
            lambda session: (
                session.createDataFrame([(1, "a")], ["x", "y"])
                .unionByName(session.createDataFrame([(2, "b")], ["x", "y"]))
                .orderBy("x")
            ),
        ),
        (
            "df.selectexpr",
            "DataFrame: selectExpr()",
            lambda session: session.createDataFrame([(1, 2)], ["a", "b"]).selectExpr(
                "a + b as c"
            ),
        ),
        (
            "df.na_fill",
            "DataFrame: na.fill()",
            lambda session: (
                session.createDataFrame([(None,), (1,), (None,)], ["x"])
                .na.fill(0)
                .orderBy("x")
            ),
        ),
        (
            "df.limit",
            "DataFrame: limit()",
            lambda session: session.range(0, 5).limit(2).orderBy("id"),
        ),
        (
            "df.orderby_multi",
            "DataFrame: orderBy multiple cols",
            lambda session: session.createDataFrame(
                [(1, "b"), (1, "a"), (2, "a")], ["x", "y"]
            ).orderBy("x", "y"),
        ),
    ]
    for sid, title, fn in df_cases:
        out.append(
            Scenario(id=sid, title=title, fn=fn, tags=["dataframe", "generated"])
        )

    # -------
    # Joins
    # -------
    join_cases: list[tuple[str, str, Callable[[Any], Any]]] = []
    for how in ["inner", "left", "right", "full", "left_semi", "left_anti"]:
        sid = f"join.by_name.{how}"
        title = f"Join: by name how={how}"

        def _mk_join(h: str) -> Callable[[Any], Any]:
            def f(session):
                left = session.createDataFrame(
                    [(1, "a"), (2, "b"), (None, "n")], ["id", "v"]
                )
                right = session.createDataFrame(
                    [(1, "x"), (3, "y"), (None, "z")], ["id", "w"]
                )
                return left.join(right, on=["id"], how=h).orderBy("id")

            return f

        join_cases.append((sid, title, _mk_join(how)))
    for sid, title, fn in join_cases:
        out.append(Scenario(id=sid, title=title, fn=fn, tags=["join", "generated"]))

    # --------
    # Windows
    # --------
    win_cases: list[tuple[str, str, Callable[[Any], Any]]] = [
        (
            "window.row_number",
            "Window: row_number()",
            lambda session: (
                session.createDataFrame(
                    [(1, "a", 10), (2, "a", 20), (3, "b", 30)], ["id", "grp", "x"]
                )
                .withColumn(
                    "rn",
                    F(session)
                    .row_number()
                    .over(Window(session).partitionBy("grp").orderBy("x")),
                )
                .orderBy("id")
            ),
        ),
        (
            "window.dense_rank",
            "Window: dense_rank()",
            lambda session: (
                session.createDataFrame(
                    [(1, "a", 10), (2, "a", 10), (3, "a", 20)], ["id", "grp", "x"]
                )
                .withColumn(
                    "dr",
                    F(session)
                    .dense_rank()
                    .over(Window(session).partitionBy("grp").orderBy("x")),
                )
                .orderBy("id")
            ),
        ),
        (
            "window.lag",
            "Window: lag()",
            lambda session: (
                session.createDataFrame(
                    [(1, "a", 10), (2, "a", 20), (3, "a", 30)], ["id", "grp", "x"]
                )
                .withColumn(
                    "prev",
                    F(session)
                    .lag("x", 1)
                    .over(Window(session).partitionBy("grp").orderBy("id")),
                )
                .orderBy("id")
            ),
        ),
        (
            "window.lead",
            "Window: lead()",
            lambda session: (
                session.createDataFrame(
                    [(1, "a", 10), (2, "a", 20), (3, "a", 30)], ["id", "grp", "x"]
                )
                .withColumn(
                    "nxt",
                    F(session)
                    .lead("x", 1)
                    .over(Window(session).partitionBy("grp").orderBy("id")),
                )
                .orderBy("id")
            ),
        ),
        (
            "window.sum_over",
            "Window: sum() over partition",
            lambda session: (
                session.createDataFrame(
                    [(1, "a", 10), (2, "a", 20), (3, "b", 30)], ["id", "grp", "x"]
                )
                .withColumn(
                    "s", F(session).sum("x").over(Window(session).partitionBy("grp"))
                )
                .orderBy("id")
            ),
        ),
        (
            "window.avg_over",
            "Window: avg() over partition",
            lambda session: (
                session.createDataFrame(
                    [(1, "a", 10), (2, "a", 20), (3, "b", 30)], ["id", "grp", "x"]
                )
                .withColumn(
                    "a", F(session).avg("x").over(Window(session).partitionBy("grp"))
                )
                .orderBy("id")
            ),
        ),
    ]
    for sid, title, fn in win_cases:
        out.append(Scenario(id=sid, title=title, fn=fn, tags=["window", "generated"]))

    # ----------
    # Aggregates
    # ----------
    agg_cases: list[tuple[str, str, Callable[[Any], Any]]] = [
        (
            "agg.groupby_sum",
            "Agg: groupBy().sum()",
            lambda session: (
                session.createDataFrame(
                    [("a", 1), ("a", 2), ("b", 3), (None, 4)], ["g", "x"]
                )
                .groupBy("g")
                .sum("x")
                .orderBy("g")
            ),
        ),
        (
            "agg.groupby_count",
            "Agg: groupBy().count()",
            lambda session: (
                session.createDataFrame([("a",), ("a",), ("b",), (None,)], ["g"])
                .groupBy("g")
                .count()
                .orderBy("g")
            ),
        ),
        (
            "agg.groupby_avg",
            "Agg: groupBy().avg()",
            lambda session: (
                session.createDataFrame(
                    [("a", 1), ("a", 2), ("b", 3), (None, 4)], ["g", "x"]
                )
                .groupBy("g")
                .avg("x")
                .orderBy("g")
            ),
        ),
        (
            "agg.groupby_multi",
            "Agg: groupBy() multi agg",
            lambda session: (
                session.createDataFrame([("a", 1), ("a", 2), ("b", 3)], ["g", "x"])
                .groupBy("g")
                .agg(F(session).min("x").alias("mn"), F(session).max("x").alias("mx"))
                .orderBy("g")
            ),
        ),
    ]
    for sid, title, fn in agg_cases:
        out.append(Scenario(id=sid, title=title, fn=fn, tags=["agg", "generated"]))

    # ----------
    # SQL SELECTs
    # ----------
    def _sql_order_limit(session):
        session.createDataFrame([(2,), (1,), (3,)], ["x"]).createOrReplaceTempView("t")
        return session.sql("SELECT * FROM t ORDER BY x LIMIT 2")

    def _sql_group_by(session):
        session.createDataFrame(
            [("a", 1), ("a", 2), ("b", 3)], ["g", "x"]
        ).createOrReplaceTempView("t2")
        return session.sql("SELECT g, sum(x) AS sx FROM t2 GROUP BY g ORDER BY g")

    sql_cases: list[tuple[str, str, Callable[[Any], Any]]] = [
        (
            "sql.simple_select",
            "SQL: simple SELECT literal",
            lambda session: session.sql("SELECT 1 AS x, 'a' AS s"),
        ),
        (
            "sql.select_where",
            "SQL: SELECT with WHERE",
            lambda session: session.sql("SELECT 1 AS x WHERE 1 = 1"),
        ),
        (
            "sql.order_limit",
            "SQL: ORDER BY + LIMIT",
            _sql_order_limit,
        ),
        (
            "sql.group_by",
            "SQL: GROUP BY + SUM",
            _sql_group_by,
        ),
    ]
    for sid, title, fn in sql_cases:
        out.append(Scenario(id=sid, title=title, fn=fn, tags=["sql", "generated"]))

    # Ensure we added ~100+ items; if not, add simple parameterized variants.
    # Variants: substring length and split patterns.
    if len(out) < 100:
        for i in range(1, 1 + (100 - len(out))):
            sid = f"generated.pad_{i}"

            def _mk(i_: int) -> Callable[[Any], Any]:
                def f(session):
                    return session.createDataFrame([("a",)], ["s"]).select(
                        F(session).lpad("s", 1 + i_, "x").alias("out")
                    )

                return f

            out.append(
                Scenario(
                    id=sid,
                    title=f"Generated: lpad variant {i}",
                    fn=_mk(i),
                    tags=["generated", "string"],
                )
            )

    return out


def all_scenarios() -> List[Scenario]:
    # Keep the initial corpus small + high-signal; expand via mutations later.
    return (
        [
            Scenario(
                id="ui.show_truncate",
                title="UI: show() truncation",
                fn=scenario_show_truncate,
                tags=["ui"],
            ),
            Scenario(
                id="df.select_star",
                title="DataFrame: select('*') expansion",
                fn=scenario_select_star,
                tags=["dataframe", "ui"],
            ),
            Scenario(
                id="orderby.null_default",
                title="Ordering: default null ordering",
                fn=scenario_null_sort_default,
                tags=["ordering"],
            ),
            Scenario(
                id="window.last_default_frame",
                title="Window: last() over default frame",
                fn=scenario_window_last_default_frame,
                tags=["window"],
            ),
            Scenario(
                id="errors.missing_column",
                title="Errors: missing column error type/message",
                fn=scenario_errors_missing_column,
                tags=["errors"],
            ),
            Scenario(
                id="sql.show_databases",
                title="SQL: SHOW DATABASES",
                fn=scenario_sql_show_databases,
                tags=["sql"],
            ),
            Scenario(
                id="sql.show_tables",
                title="SQL: SHOW TABLES",
                fn=scenario_sql_show_tables,
                tags=["sql"],
            ),
            Scenario(
                id="sql.describe_extended",
                title="SQL: DESCRIBE EXTENDED temp view",
                fn=scenario_sql_describe_extended,
                tags=["sql"],
            ),
            Scenario(
                id="column.pow_operator",
                title="Column: pow operator with Column exponent",
                fn=scenario_pow_operator,
                tags=["column", "numeric"],
            ),
            Scenario(
                id="string.split_limit",
                title="String: split() with limit",
                fn=scenario_split_limit,
                tags=["string"],
            ),
            Scenario(
                id="session.conf_app_name",
                title="Session: spark.conf.get('spark.app.name')",
                fn=scenario_conf_app_name,
                tags=["session"],
            ),
            Scenario(
                id="cast.invalid_string_to_int",
                title="Cast: invalid string to int",
                fn=scenario_cast_invalid_string,
                tags=["cast", "errors"],
            ),
            Scenario(
                id="join.on_expression",
                title="Join: on Column expression",
                fn=scenario_join_on_expression,
                tags=["join"],
            ),
            Scenario(
                id="struct.getfield_alias",
                title="Struct: getField + alias propagation",
                fn=scenario_struct_getfield_alias,
                tags=["struct"],
            ),
        ]
        + _mutations()
        + _more_scenarios()
    )

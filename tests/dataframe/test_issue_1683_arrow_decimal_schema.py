"""Regression coverage for preserving PyArrow decimal schemas (#1683)."""

from __future__ import annotations

from decimal import Decimal

import pytest


def test_pyarrow_decimal_schema_precision_values_and_nulls(spark) -> None:
    pa = pytest.importorskip("pyarrow")
    arrow_schema = pa.schema([("amount", pa.decimal128(12, 2))])
    table = pa.Table.from_pylist(
        [{"amount": Decimal("10.50")}, {"amount": None}],
        schema=arrow_schema,
    )

    df = spark.createDataFrame(table)

    assert df.schema["amount"].dataType.simpleString() == "decimal(12,2)"
    rows = df.collect()
    assert rows[0]["amount"] == Decimal("10.50")
    assert rows[1]["amount"] is None


def test_pyarrow_decimal_values_with_exponent_notation(spark) -> None:
    pa = pytest.importorskip("pyarrow")
    table = pa.Table.from_pylist(
        [{"amount": Decimal("1E-7")}, {"amount": Decimal("1E+3")}],
        schema=pa.schema([("amount", pa.decimal128(12, 7))]),
    )

    rows = spark.createDataFrame(table).collect()

    assert [row["amount"] for row in rows] == [Decimal("0.0000001"), Decimal("1000.0000000")]


def test_python_decimal_inference_and_decimal_string_schema(spark) -> None:
    df = spark.createDataFrame(
        [{"amount": Decimal("0.10")}, {"amount": Decimal("10.50")}]
    )

    assert df.schema["amount"].dataType.simpleString() == "decimal(4,2)"
    assert [row["amount"] for row in df.collect()] == [Decimal("0.10"), Decimal("10.50")]

    as_string = spark.createDataFrame(
        [{"amount": Decimal("1.50")}], schema=[("amount", "string")]
    )
    assert as_string.collect()[0]["amount"] == "1.50"


def test_decimal_overflow_raises_instead_of_becoming_null(spark) -> None:
    with pytest.raises(Exception, match="decimal|precision|overflow"):
        spark.createDataFrame(
            [{"amount": Decimal("1E+39")}], schema="amount decimal(38,0)"
        )


def test_to_arrow_preserves_decimal_schema(spark) -> None:
    pa = pytest.importorskip("pyarrow")
    df = spark.createDataFrame(
        [{"amount": Decimal("10.50")}], schema="amount decimal(12,2)"
    )

    table = df.toArrow()

    assert table.schema.field("amount").type == pa.decimal128(12, 2)
    assert table.to_pylist() == [{"amount": Decimal("10.50")}]


def test_decimal_filter_and_aggregation_remain_exact(spark) -> None:
    pa = pytest.importorskip("pyarrow")
    from sparkless.testing import get_imports

    table = pa.Table.from_pylist(
        [
            {"kind": "g", "amount": Decimal("10.50")},
            {"kind": "g", "amount": Decimal("2.25")},
            {"kind": "g", "amount": Decimal("0.10")},
        ],
        schema=pa.schema(
            [
                ("kind", pa.string()),
                ("amount", pa.decimal128(12, 2)),
            ]
        ),
    )
    F = get_imports().F

    row = (
        spark.createDataFrame(table)
        .filter("amount >= 2.00")
        .groupBy("kind")
        .agg(F.sum("amount").alias("total"))
        .collect()[0]
    )

    assert row["total"] == Decimal("12.75")


def test_decimal_filter_keeps_large_integral_values_exact(spark) -> None:
    pa = pytest.importorskip("pyarrow")
    table = pa.Table.from_pylist(
        [
            {"amount": Decimal("9007199254740992")},
            {"amount": Decimal("9007199254740993")},
        ],
        schema=pa.schema([("amount", pa.decimal128(20, 0))]),
    )

    rows = spark.createDataFrame(table).filter(
        "amount = 9007199254740993"
    ).collect()

    assert [row["amount"] for row in rows] == [Decimal("9007199254740993")]

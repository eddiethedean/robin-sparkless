"""Identical expressions and exact result checks on both engines."""
from datetime import date, datetime
from decimal import Decimal

NAMES = [
    'ingest_10', 'ingest_100', 'ingest_1000', 'ingest_10000',
    'orders_pipeline', 'nullable_orders', 'scalar_roundtrip',
    'wide_projection', 'string_payload', 'empty_result',
    'selectivity_none', 'selectivity_one', 'selectivity_half', 'selectivity_all',
    'arithmetic_pipeline', 'preview_limit', 'collect_10000', 'collect_100000',
    'schema_100_columns',
]


def workload(spark, name):
    from pyspark.sql import functions as F
    if name.startswith('ingest_'):
        n = int(name.split('_')[1])
        df = spark.createDataFrame([(i, i % 200) for i in range(n)],
                                   'order_id LONG, total LONG')
        assert df.filter(df.total >= 100).count() == sum(i % 200 >= 100 for i in range(n))
    elif name == 'orders_pipeline':
        df = spark.createDataFrame([(i, i % 200) for i in range(1000)],
                                   'order_id LONG, total LONG')
        rows = (df.filter(df.total >= 100)
                .select(df.order_id.alias('order'), (df.total + 5).alias('adjusted'))
                .limit(25).collect())
        assert [tuple(r) for r in rows] == [(i, i % 200 + 5) for i in range(100,125)]
    elif name == 'nullable_orders':
        values = [(i, None if i % 3 == 0 else i % 200) for i in range(1000)]
        df = spark.createDataFrame(values, 'order_id LONG, total LONG')
        rows = df.filter(df.total >= 100).select('order_id').collect()
        assert [r.order_id for r in rows] == [i for i, v in values if v is not None and v >= 100]
    elif name == 'scalar_roundtrip':
        values = [(i, i / 4, i % 2 == 0, None if i % 3 == 0 else f'item-{i}',
                   date(2026,1,1), datetime(2026,1,1,12,0,0), Decimal('12.34')) for i in range(256)]
        df = spark.createDataFrame(values,
            'id LONG, value DOUBLE, flag BOOLEAN, label STRING, day DATE, at TIMESTAMP, price DECIMAL(10,2)')
        assert [tuple(r) for r in df.collect()] == values
    elif name == 'wide_projection':
        values = [tuple(i+j for j in range(12)) for i in range(1000)]
        df = spark.createDataFrame(values, ', '.join(f'c{j} LONG' for j in range(12)))
        rows = df.select(*[(df[f'c{j}'] + j).alias(f'v{j}') for j in range(12)]).collect()
        assert [tuple(r) for r in rows] == [tuple(i+2*j for j in range(12)) for i in range(1000)]
    elif name == 'string_payload':
        values = [(i, f'{i:04d}:' + 'x'*251) for i in range(1000)]
        df = spark.createDataFrame(values, 'id LONG, payload STRING')
        assert [tuple(r) for r in df.collect()] == values
    elif name == 'empty_result':
        rows = spark.range(1000).filter('id > 1000').collect()
        assert rows == []
    elif name.startswith('selectivity_'):
        cutoff = {'none':1000000, 'one':999999, 'half':500000, 'all':0}[name.split('_')[1]]
        df = spark.range(1000000)
        assert df.filter(df.id >= cutoff).count() == 1000000 - cutoff
    elif name == 'arithmetic_pipeline':
        df = spark.range(1000000)
        projected = df.select((df.id + 1).alias('value'))
        assert projected.filter(projected.value >= 500000).count() == 500001
    elif name == 'preview_limit':
        rows = spark.range(1000000).select('id').limit(25).collect()
        assert [r.id for r in rows] == list(range(25))
    elif name.startswith('collect_'):
        n = int(name.split('_')[1])
        rows = spark.range(n).select('id').collect()
        assert [r.id for r in rows] == list(range(n))
    elif name == 'schema_100_columns':
        df = spark.range(1).select(*[F.col('id').alias(f'field_{i}') for i in range(100)])
        assert df.schema.fieldNames() == [f'field_{i}' for i in range(100)]
    else:
        raise ValueError(name)

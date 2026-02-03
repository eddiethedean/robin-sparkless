//! Benchmarks: robin-sparkless vs plain Polars for filter → select → groupBy+count.
//! Run with: cargo bench

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use polars::prelude::{col, len, DataFrame as PlDataFrame, IntoLazy, NamedFrom, Series};
use robin_sparkless::{DataFrame as RobinDataFrame, SparkSession};

fn sample_data_robin(n: usize) -> (Vec<(i64, i64, String)>, Vec<&'static str>) {
    let mut data = Vec::with_capacity(n);
    for i in 0..n {
        data.push((
            i as i64,
            (i % 80) as i64, // age 0..79
            format!("user_{}", i),
        ));
    }
    (data, vec!["id", "age", "name"])
}

fn sample_data_polars(n: usize) -> PlDataFrame {
    let id: Vec<i64> = (0..n as i64).collect();
    let age: Vec<i64> = (0..n).map(|i| (i % 80) as i64).collect();
    let name: Vec<String> = (0..n).map(|i| format!("user_{}", i)).collect();
    PlDataFrame::new(vec![
        Series::new("id".into(), id).into(),
        Series::new("age".into(), age).into(),
        Series::new("name".into(), name).into(),
    ])
    .expect("polars df")
}

fn bench_robin_filter_select_groupby(c: &mut Criterion, n: usize) {
    let (data, cols) = sample_data_robin(n);
    let spark = SparkSession::builder().app_name("bench").get_or_create();
    c.bench_function(&format!("robin_filter_select_groupby_{}", n), |b| {
        b.iter(|| {
            let df: RobinDataFrame = spark
                .create_dataframe(black_box(data.clone()), cols.clone())
                .expect("create_dataframe");
            let filtered = df
                .filter(polars::prelude::col("age").gt(polars::prelude::lit(30)))
                .expect("filter");
            let selected = filtered.select(vec!["name", "age"]).expect("select");
            let grouped = selected.group_by(vec!["age"]).expect("group_by");
            let result = grouped.count().expect("count");
            black_box(result)
        })
    });
}

fn bench_polars_filter_select_groupby(c: &mut Criterion, n: usize) {
    let df = sample_data_polars(n);
    c.bench_function(&format!("polars_filter_select_groupby_{}", n), |b| {
        b.iter(|| {
            let lf = df
                .clone()
                .lazy()
                .filter(col("age").gt(polars::prelude::lit(30)))
                .select([col("name"), col("age")])
                .group_by([col("age")])
                .agg([len().alias("count")]);
            let result = lf.collect().expect("collect");
            black_box(result)
        })
    });
}

fn bench_filter_select_groupby(c: &mut Criterion) {
    bench_robin_filter_select_groupby(c, 10_000);
    bench_polars_filter_select_groupby(c, 10_000);
    bench_robin_filter_select_groupby(c, 100_000);
    bench_polars_filter_select_groupby(c, 100_000);
}

criterion_group!(benches, bench_filter_select_groupby);
criterion_main!(benches);

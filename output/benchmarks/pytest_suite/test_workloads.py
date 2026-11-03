"""Actual pytest tests: two passes over 19 correctness-checked workloads."""
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from diverse_workloads import NAMES, workload


@pytest.fixture(scope='session')
def spark():
    from pyspark.sql import SparkSession
    if os.environ['BENCH_ENGINE']=='connect':
        session=SparkSession.builder.remote(os.environ['BENCH_CONNECT_URL']).create()
    else:
        cores=os.environ.get('BENCH_WORKER_CORES','4')
        session=(SparkSession.builder.master(f'local[{cores}]').appName('pytest-benchmark')
                 .config('spark.ui.enabled','false')
                 .config('spark.driver.host','127.0.0.1')
                 .config('spark.sql.session.timeZone','UTC')
                 .config('spark.sql.shuffle.partitions',cores)
                 .config('spark.default.parallelism',cores).getOrCreate())
        session.sparkContext.setLogLevel('ERROR')
    try:
        yield session
    finally:
        session.stop()


@pytest.mark.parametrize('repeat',range(2))
@pytest.mark.parametrize('name',NAMES)
def test_workload(spark,name,repeat):
    workload(spark,name)

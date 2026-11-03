import argparse
import concurrent.futures
import json
import os
import time
from contextlib import ExitStack
from importlib.metadata import version
from pathlib import Path

from compare_servers import summary, timed
from diverse_workloads import NAMES, workload


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--engine', choices=['local','connect'], required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--probe-only', action='store_true')
    args = p.parse_args()
    result = {'engine':args.engine, 'pyspark':version('pyspark' if args.engine=='local' else 'pyspark-client'),
              'active_operations':os.environ.get('SPARKLESS_MAX_ACTIVE_OPERATIONS','2 (default)'),
              'workloads':{}, 'mixed_concurrency':{}, 'unsupported_probes':{}}
    def save():
        args.output.write_text(json.dumps(result, indent=2)+'\n')
    with ExitStack() as stack:
        from pyspark.sql import SparkSession, functions as F
        if args.engine == 'connect':
            from sparkless import SparklessServer
            server = stack.enter_context(SparklessServer())
            spark = SparkSession.builder.remote(server.spark_connect_url).create()
        else:
            spark = (SparkSession.builder.master('local[4]').appName('diverse-benchmarks')
                     .config('spark.ui.enabled','false')
                     .config('spark.sql.session.timeZone','UTC')
                     .config('spark.sql.shuffle.partitions','4')
                     .config('spark.default.parallelism','4')
                     .config('spark.driver.host','127.0.0.1').getOrCreate())
            spark.sparkContext.setLogLevel('ERROR')
        stack.callback(spark.stop)
        valid = []
        for name in NAMES:
            try:
                workload(spark,name)
                valid.append(name)
                result['workloads'][name] = {'status':'pass', 'samples_seconds':[]}
            except Exception as e:
                result['workloads'][name] = {'status':'error','error':f'{type(e).__name__}: {str(e)[:700]}'}
            print(name,result['workloads'][name]['status'],flush=True)
            save()
        if not args.probe_only:
            for iteration in range(8):
                rotated = valid[iteration % len(valid):] + valid[:iteration % len(valid)]
                for name in rotated:
                    elapsed,_ = timed(lambda:workload(spark,name))
                    result['workloads'][name]['samples_seconds'].append(elapsed)
            for name in valid:
                result['workloads'][name].update(summary(result['workloads'][name]['samples_seconds']))
            save()
            for workers in [1,4,8]:
                sessions = [(SparkSession.builder.remote(server.spark_connect_url).create()
                             if args.engine=='connect' else spark.newSession()) for _ in range(workers)]
                if args.engine=='connect':
                    for s in sessions:
                        stack.callback(s.stop)
                names = ['ingest_1000','nullable_orders','preview_limit','collect_10000']
                for s in sessions:
                    workload(s,'preview_limit')
                def client(item):
                    index,session = item
                    order = names[index % len(names):] + names[:index % len(names)]
                    samples = []
                    for name in order:
                        elapsed,_ = timed(lambda:workload(session,name))
                        samples.append({'name':name,'seconds':elapsed})
                    return samples
                rounds,samples = [],[]
                with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
                    for _ in range(3):
                        start=time.perf_counter()
                        batches=list(pool.map(client,enumerate(sessions)))
                        rounds.append(time.perf_counter()-start)
                        samples.extend(v for batch in batches for v in batch)
                result['mixed_concurrency'][str(workers)] = {
                    'jobs_per_round': workers*4, 'rounds_seconds':rounds,'samples':samples}
                print('mixed clients',workers,'pass',flush=True)
                save()
        probes = {
            'grouped_sum':lambda:spark.range(10).groupBy('id').agg(F.sum('id')).collect(),
            'join':lambda:spark.range(10).join(spark.range(10),'id').collect(),
            'sort':lambda:spark.range(10).orderBy(F.col('id').desc()).collect(),
            'sql':lambda:spark.sql('SELECT 1 AS value').collect(),
        }
        for name,fn in probes.items():
            try:
                fn()
                result['unsupported_probes'][name] = {'status':'pass'}
            except Exception as e:
                result['unsupported_probes'][name] = {'status':'error','error':f'{type(e).__name__}: {str(e)[:700]}'}
        save()


if __name__=='__main__':
    main()

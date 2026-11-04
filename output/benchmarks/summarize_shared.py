import ast
import json
import statistics
from pathlib import Path
from diverse_workloads import NAMES

ROOT=Path(__file__).resolve().parent
LABELS=ast.literal_eval(next(node.value for node in ast.parse(
    (ROOT/'summarize_diverse.py').read_text()).body
    if isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='LABELS' for t in node.targets)))
runs=json.loads((ROOT/'shared-results.json').read_text())
assert len(runs)==6
micro={engine:[json.loads((ROOT/f'shared-{engine}-{i}-micro.json').read_text()) for i in range(3)]
       for engine in ['sparkless','jvm']}
result={'suites':{},'workloads':{},'mixed_concurrency':{}}
lines=['# Shared Spark Connect server comparison','',
    'Measured 2026-10-31 on the same Apple M4 Pro, 12 logical CPUs, 24 GiB RAM, macOS 26.5.2.',
    'Python 3.11.15; identical pyspark-client==4.0.5 environment for both engines. Sparkless API 4.18.1 / native and server 4.18.0 versus Apache Spark JVM Connect server 4.0.5 with Java 17.','',
    '## Matched topology and configuration','',
    '- Both engines expose one shared loopback Spark Connect server. Four pytest-xdist workers each own an independent session on that same server. No classic JVM is launched in a test worker.',
    '- JVM: local[4], four default and shuffle partitions, FAIR scheduling, UTC, Spark UI off, 2 GiB driver heap, -XX:ActiveProcessorCount=4. Sparkless: four active operations, POLARS_MAX_THREADS=4, RAYON_NUM_THREADS=4, OMP_NUM_THREADS=4. These approximate equal compute configurations, not hard CPU/RSS quotas; thread, partition and operation semantics differ between engines.',
    '- Both use the same 38 correctness-checked tests: two copies of 19 supported workloads. Both use the same PySpark client, serialization, pytest plugins and test fixture. All pytest suites start fresh client processes.',
    '- Three fresh server runs per engine, alternating engine order. One engine runs at a time. Filesystem caches remain warm; package installation is excluded. No explicit DataFrame caching.', '',
    '## Actual four-worker pytest suite','',
    '| Measurement | Sparkless Connect | JVM Spark Connect | JVM / Sparkless time |',
    '|---|---:|---:|---:|']
metrics={'startup_first_action_seconds':'Server startup → first successful remote count',
         'startup_and_cold_pytest_seconds':'Server startup + readiness count + first 38-test suite',
         'warm_pytest_seconds':'38-test suite against already running server'}
for key,label in metrics.items():
    times={engine:statistics.median([r[key] for r in runs if r['engine']==engine])
           for engine in micro}
    result['suites'][key]=times
    lines.append(f"| {label} | {times['sparkless']:.3f} s | {times['jvm']:.3f} s | {times['jvm']/times['sparkless']:.2f}× |")
lines+=['',
    'Each cell is the median of three independent server runs. Startup includes native/JVM server launch and a successful remote range(1).count(), followed by session close. The first suite immediately follows that readiness witness. Warm-suite time includes pytest launch, four client-worker imports and session setup/cleanup; server startup and server shutdown are excluded. Each server is stopped after the subsequent microbenchmarks. This avoids attributing server startup gains to warm execution.','',
    '## Warm workloads using the identical remote client','',
    'Medians pool 30 actions per workload across three independent servers, after the two pytest suites and one explicit workload warmup per server. Each action includes input/plan creation and exact output validation. This is a warm-runtime comparison.','',
    '| Workload | Sparkless Connect | JVM Spark Connect | JVM / Sparkless time |',
    '|---|---:|---:|---:|']
names=NAMES+['orders_string_1000','filtered_range_1000000','filtered_range_10000000']
labels={**LABELS,'orders_string_1000':'Graphic test: ingest/filter/count 1,000 orders (string predicate)',
        'filtered_range_1000000':'Range/filter/count 1 million (string predicate)',
        'filtered_range_10000000':'Range/filter/count 10 million (string predicate)'}
for name in names:
    times={engine:statistics.median([v for run in values
            for v in run['workloads'][name]['samples_seconds']])*1000
            for engine,values in micro.items()}
    result['workloads'][name]=times
    lines.append(f"| {labels[name]} | {times['sparkless']:.2f} ms | {times['jvm']:.2f} ms | {times['jvm']/times['sparkless']:.2f}× |")
lines+=['','Ratios above one favor Sparkless; ratios below one favor JVM Spark.','',
        '## Warm mixed-client throughput','',
        'Same four-job mix per client as before: ingest/filter/count, nullable-data collection, 25-row preview, and 10k-row collection. Nine rounds across three servers per engine. Jobs/s = jobs per round / median round time. Independent sessions run in Python threads for this measurement; actual pytest above uses processes.','',
        '| Clients | Sparkless jobs/s | JVM Spark jobs/s |',
        '|---|---:|---:|']
for workers in ['1','4','8']:
    rates={engine:int(workers)*4/statistics.median([v for run in values
           for v in run['mixed_concurrency'][workers]['rounds_seconds']])
           for engine,values in micro.items()}
    result['mixed_concurrency'][workers]=rates
    lines.append(f"| {workers} | {rates['sparkless']:.1f} | {rates['jvm']:.1f} |")
lines+=['','## Scope and previous results','',
    'This comparison replaces the older 2.2× suite claim for a shared-server comparison. The older claim used four independent local[1] JVMs, and remains valid only for that architecture. The older 4.7× order-test claim compared Sparkless Connect with classic local[4] PySpark; use the new remote-client measurement when comparing shared servers.',
    'Only the released Sparkless Connect subset was measured. This is a single-machine supported-workload comparison, not a distributed-cluster or production-capacity claim. JVM Spark supports operations outside this suite, including joins and grouped aggregates, which this Sparkless Connect release rejects. Large result collections and small jobs are both retained in the report.', '',
    '## Reproduce','',
    'Use the environments described in DIVERSE_REPORT.md. Run run_shared_comparison.py with the pyspark-client/Sparkless interpreter, then summarize_shared.py. Adapt Spark installation and interpreter paths in shared_runtime.py and run_diverse.py. shared_runtime.py records the exact JVM launch flags. Raw action samples, server/client logs, and JUnit XML are stored alongside this report.']
(ROOT/'SHARED_SERVER_REPORT.md').write_text('\n'.join(lines)+'\n')
(ROOT/'shared-summary.json').write_text(json.dumps(result,indent=2)+'\n')
print('\n'.join(lines[:29]))

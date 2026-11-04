import json
import subprocess
import time
import xml.etree.ElementTree as ET

from pyspark.sql import SparkSession
from shared_runtime import shared_server
from run_diverse import ROOT,env,interpreters

CLIENT=interpreters['connect']


def pytest_suite(url,name):
    run_env={**env,'BENCH_ENGINE':'connect','BENCH_CONNECT_URL':url,
             'PYTEST_DISABLE_PLUGIN_AUTOLOAD':'1'}
    command=[CLIENT,'-m','pytest','-q','-p','xdist.plugin',
        '--rootdir',str(ROOT/'pytest_suite'),'-c','/dev/null',
        str(ROOT/'pytest_suite'),'-n','4','--dist','load',f'--junitxml={ROOT/name}.xml']
    t=time.perf_counter()
    with (ROOT/f'{name}.log').open('w') as log:
        subprocess.run(command,env=run_env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=180)
    elapsed=time.perf_counter()-t
    xml=ET.parse(ROOT/f'{name}.xml').getroot()
    assert len(list(xml.iter('testcase')))==38
    assert not list(xml.iter('failure')) and not list(xml.iter('error'))
    return elapsed


def main():
    results=[]
    for repetition in range(3):
        for engine in (['sparkless','jvm'] if repetition%2==0 else ['jvm','sparkless']):
            name=f'shared-{engine}-{repetition}'
            print('RUN',name,flush=True)
            started=time.perf_counter()
            with shared_server(engine,name) as url:
                # First successful remote action is the common readiness witness.
                s=SparkSession.builder.remote(url).create()
                try:
                    assert s.range(1).count()==1
                finally:
                    s.stop()
                startup=time.perf_counter()-started
                cold_pytest=pytest_suite(url,name+'-cold-pytest')
                cold_total=time.perf_counter()-started
                warm_pytest=pytest_suite(url,name+'-warm-pytest')
                result={'engine':engine,'repetition':repetition,
                    'startup_first_action_seconds':startup,'cold_pytest_seconds':cold_pytest,
                    'startup_and_cold_pytest_seconds':cold_total,
                    'warm_pytest_seconds':warm_pytest,'testcases_per_suite':38}
                results.append(result)
                (ROOT/'shared-results.json').write_text(json.dumps(results,indent=2)+'\n')
                print(result,flush=True)
                with (ROOT/f'{name}-micro.log').open('w') as log:
                    subprocess.run([CLIENT,str(ROOT/'shared_client.py'),'--url',url,
                        '--output',str(ROOT/f'{name}-micro.json')],
                        env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
            print('PASS',name,round(time.perf_counter()-started,2),'seconds',flush=True)


if __name__=='__main__':
    main()

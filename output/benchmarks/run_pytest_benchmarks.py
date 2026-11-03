import json
import subprocess
import time
import xml.etree.ElementTree as ET
from contextlib import ExitStack
from pathlib import Path

from run_diverse import ROOT,env,interpreters


def main():
    results=[]
    for repetition in range(3):
        configs=[('connect',0),('local',0),('connect',4),('local',4)]
        if repetition%2:
            configs.reverse()
        for engine,workers in configs:
            name=f'pytest-{engine}-workers{workers}-{repetition}'
            print('RUN',name,flush=True)
            run_env=env.copy()
            run_env.update(BENCH_ENGINE=engine,PYTEST_DISABLE_PLUGIN_AUTOLOAD='1',
                           BENCH_WORKER_CORES='1' if workers else '4')
            start=time.perf_counter()
            with ExitStack() as stack:
                if engine=='connect':
                    # env must be set before the server process is launched.
                    import os
                    os.environ.update(env)
                    from sparkless import SparklessServer
                    server=stack.enter_context(SparklessServer())
                    run_env['BENCH_CONNECT_URL']=server.spark_connect_url
                command=[interpreters[engine],'-m','pytest','-q','-p','xdist.plugin',
                         '--rootdir',str(ROOT/'pytest_suite'),'-c','/dev/null',
                         str(ROOT/'pytest_suite'),f'--junitxml={ROOT/name}.xml']
                if workers:
                    command+=['-n',str(workers),'--dist','load']
                with (ROOT/f'{name}.log').open('w') as log:
                    completed=subprocess.run(command,env=run_env,stdout=log,stderr=subprocess.STDOUT,
                                             timeout=180)
            seconds=time.perf_counter()-start
            suite=ET.parse(ROOT/f'{name}.xml').getroot()
            cases=list(suite.iter('testcase'))
            failures=len(list(suite.iter('failure')))+len(list(suite.iter('error')))
            result={'engine':engine,'xdist_workers':workers,'repetition':repetition,
                    'wall_seconds':seconds,'testcases':len(cases),'failures':failures,
                    'exit_code':completed.returncode}
            results.append(result)
            (ROOT/'pytest-results.json').write_text(json.dumps(results,indent=2)+'\n')
            print(result,flush=True)
            if completed.returncode or failures or len(cases)!=38:
                raise RuntimeError(f'pytest benchmark failed: {name}')


if __name__=='__main__':
    main()

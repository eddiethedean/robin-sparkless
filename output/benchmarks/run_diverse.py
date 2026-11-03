import json
import os
import subprocess
import time
from pathlib import Path

ROOT=Path(__file__).resolve().parent
env=os.environ.copy()
for key in list(env):
    if key.startswith('SPARKLESS_') or key in ['SPARK_HOME','SPARK_CONF_DIR','PYSPARK_PYTHON',
        'PYSPARK_DRIVER_PYTHON','PYSPARK_SUBMIT_ARGS','PYTHONPATH','SPARK_REMOTE']:
        env.pop(key,None)
env.update(JAVA_HOME='/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home',
           SPARK_LOCAL_IP='127.0.0.1',POLARS_MAX_THREADS='4',RAYON_NUM_THREADS='4',
           OMP_NUM_THREADS='4',PYTHONHASHSEED='0',TZ='UTC')
interpreters={'local':'/tmp/sparkless-benchmark-local/bin/python',
              'connect':'/tmp/sparkless-linkedin-verify/bin/python'}
if __name__=='__main__':
    configs=['connect-default','local','connect-active4']
    for repetition in range(3):
        order=configs[repetition:]+configs[:repetition]
        for config in order:
            engine='local' if config=='local' else 'connect'
            run_env=env.copy()
            if config=='connect-active4':
                run_env['SPARKLESS_MAX_ACTIVE_OPERATIONS']='4'
            name=f'diverse-{config}-{repetition}'
            print('RUN',name,flush=True)
            started=time.perf_counter()
            with (ROOT/f'{name}.log').open('w') as log:
                subprocess.run([interpreters[engine],str(ROOT/'compare_diverse.py'),
                    '--engine',engine,'--output',str(ROOT/f'{name}.json')],
                    env=run_env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
            print('PASS',name,round(time.perf_counter()-started,2),'seconds',flush=True)

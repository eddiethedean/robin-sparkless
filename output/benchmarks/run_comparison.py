import json
import os
import platform
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
env = os.environ.copy()
for key in [
    "SPARK_HOME",
    "SPARK_CONF_DIR",
    "PYSPARK_PYTHON",
    "PYSPARK_DRIVER_PYTHON",
    "PYSPARK_SUBMIT_ARGS",
    "PYTHONPATH",
    "SPARK_REMOTE",
]:
    env.pop(key, None)
env.update(
    JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
    SPARK_LOCAL_IP="127.0.0.1",
    POLARS_MAX_THREADS="4",
    RAYON_NUM_THREADS="4",
    OMP_NUM_THREADS="4",
    PYTHONHASHSEED="0",
)
interpreters = {
    "local": "/tmp/sparkless-benchmark-local/bin/python",
    "connect": "/tmp/sparkless-linkedin-verify/bin/python",
}
metadata = {
    "platform": platform.platform(),
    "processor": subprocess.check_output(
        ["sysctl", "-n", "machdep.cpu.brand_string"], text=True
    ).strip(),
    "logical_cpus": os.cpu_count(),
    "memory_bytes": int(
        subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True)
    ),
    "note": "Sequential engine runs; fresh processes, warm filesystem cache; no installation time.",
}
(ROOT / "machine.json").write_text(json.dumps(metadata, indent=2) + "\n")
for phase, repetition in [
    (phase, i) for phase in ["startup", "warm"] for i in range(3)
]:
    for engine in ["connect", "local"] if repetition % 2 == 0 else ["local", "connect"]:
        name = f"{engine}-{phase}-{repetition}"
        print("RUN", name, flush=True)
        started = time.perf_counter()
        with (ROOT / f"{name}.log").open("w") as log:
            subprocess.run(
                [
                    interpreters[engine],
                    str(ROOT / "compare_servers.py"),
                    "--engine",
                    engine,
                    "--phase",
                    phase,
                    "--output",
                    str(ROOT / f"{name}.json"),
                ],
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                check=True,
                timeout=300,
            )
        print(
            "PASS", name, round(time.perf_counter() - started, 2), "seconds", flush=True
        )

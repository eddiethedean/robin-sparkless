"""One shared server per engine; no classic Spark JVMs in pytest workers."""

import contextlib
import socket
import subprocess
import time
from pathlib import Path

from run_diverse import env, ROOT

SPARK_HOME_PATH = Path(
    "/tmp/sparkless-benchmark-local/lib/python3.11/site-packages/pyspark"
)


@contextlib.contextmanager
def shared_server(engine, log_name):
    if engine == "sparkless":
        from sparkless import SparklessServer

        with SparklessServer(
            env={**env, "SPARKLESS_MAX_ACTIVE_OPERATIONS": "4"}
        ) as server:
            yield server.spark_connect_url
        return
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    command = [
        str(SPARK_HOME_PATH / "bin/spark-submit"),
        "--class",
        "org.apache.spark.sql.connect.service.SparkConnectServer",
        "--master",
        "local[4]",
        "--name",
        "shared-server-benchmark",
        "--driver-memory",
        "2g",
        "--conf",
        "spark.driver.extraJavaOptions=-XX:ActiveProcessorCount=4",
        "--conf",
        "spark.ui.enabled=false",
        "--conf",
        "spark.driver.host=127.0.0.1",
        "--conf",
        "spark.sql.session.timeZone=UTC",
        "--conf",
        "spark.default.parallelism=4",
        "--conf",
        "spark.sql.shuffle.partitions=4",
        "--conf",
        "spark.scheduler.mode=FAIR",
        "--conf",
        "spark.connect.grpc.binding.address=127.0.0.1",
        "--conf",
        f"spark.connect.grpc.binding.port={port}",
        str(SPARK_HOME_PATH / "jars/spark-connect_2.13-4.0.5.jar"),
    ]
    with (ROOT / f"{log_name}-jvm-server.log").open("w") as log:
        process = subprocess.Popen(
            command, env=env, stdout=log, stderr=subprocess.STDOUT
        )
        try:
            deadline = time.monotonic() + 45
            while True:
                if process.poll() is not None:
                    raise RuntimeError(f"JVM Connect server exited: {log.name}")
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                        break
                except OSError:
                    if time.monotonic() > deadline:
                        raise TimeoutError(f"JVM Connect server not ready: {log.name}")
                    time.sleep(0.05)
            yield f"sc://127.0.0.1:{port}"
        finally:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()

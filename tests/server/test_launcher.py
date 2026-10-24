import asyncio
import sys
import textwrap

import pytest

import sparkless
from sparkless.server import (
    AsyncSparklessServer,
    SparklessServer,
    SparklessServerError,
    _validate_ready,
)


def _write_fake_server(path, *, delay=0.0, stderr_bytes=0):
    source = textwrap.dedent(
        """\
        #!{python}
        import json
        import sys
        import time
        import uuid

        time.sleep({delay})
        sys.stderr.write('x' * {stderr_bytes})
        sys.stderr.flush()
        identity = str(uuid.uuid4())
        print(json.dumps({{
            'type': 'ready',
            'control_protocol_version': 1,
            'server_version': {version!r},
            'agent_protocol_version': '1',
            'server_instance_id': identity,
            'http_url': 'http://127.0.0.1:45678',
            'capabilities': {{
                'transport': 'http',
                'server_instance_id': identity,
                'spark_connect_supported': False,
                'agent_protocol_version': '1',
                'mutation_protocol_version': '1'
            }}
        }}), flush=True)
        for line in sys.stdin:
            try:
                command = json.loads(line)
            except ValueError:
                continue
            if command == {{'control_protocol_version': 1, 'command': 'shutdown'}}:
                print(json.dumps({{
                    'type': 'stopped',
                    'control_protocol_version': 1
                }}), flush=True)
                break
        """
    ).format(
        python=sys.executable,
        delay=delay,
        stderr_bytes=stderr_bytes,
        version=sparkless.__version__,
    )
    path.write_text(source)
    path.chmod(0o755)
    return str(path)


def test_sync_launcher_owns_child_restart_and_bounds_diagnostics(tmp_path):
    executable = _write_fake_server(tmp_path / "sparkless-server", stderr_bytes=100_000)
    server = SparklessServer(
        executable_path=executable,
        startup_timeout=2,
        shutdown_timeout=2,
        kill_timeout=1,
    )
    server.start()
    first_identity = server.server_instance_id
    assert server.start() is server
    assert server.server_instance_id == first_identity
    assert server.http_url == "http://127.0.0.1:45678"
    with pytest.raises(SparklessServerError, match="Spark Connect is not supported"):
        _ = server.spark_connect_url
    assert len(server.diagnostics.encode("utf-8")) <= 64 * 1024

    server.stop()
    with pytest.raises(SparklessServerError, match="only while the server is ready"):
        _ = server.http_url
    server.start()
    assert server.server_instance_id != first_identity
    server.stop()


def test_same_sync_context_owner_rejects_nested_entry(tmp_path):
    executable = _write_fake_server(tmp_path / "sparkless-server")
    server = SparklessServer(executable_path=executable, startup_timeout=2)
    with server:
        with pytest.raises(SparklessServerError, match="nested entry"):
            server.__enter__()


def test_launcher_rejects_wrong_control_protocol(tmp_path):
    record = {
        "type": "ready",
        "control_protocol_version": True,
        "server_version": sparkless.__version__,
        "agent_protocol_version": "1",
        "server_instance_id": "instance",
        "http_url": "http://127.0.0.1:1234",
        "capabilities": {
            "transport": "http",
            "server_instance_id": "instance",
            "spark_connect_supported": False,
            "agent_protocol_version": "1",
            "mutation_protocol_version": "1",
        },
    }
    with pytest.raises(SparklessServerError, match="control protocol"):
        _validate_ready(record)


def test_async_launcher_reaps_child_on_startup_cancellation(tmp_path):
    executable = _write_fake_server(tmp_path / "sparkless-server", delay=10)
    server = AsyncSparklessServer(
        executable_path=executable,
        startup_timeout=30,
        shutdown_timeout=1,
        kill_timeout=1,
    )

    async def run():
        task = asyncio.create_task(server.start())
        for _ in range(100):
            if server._process is not None:
                break
            await asyncio.sleep(0.01)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert server._process is None

    asyncio.run(run())


def test_async_launcher_context_stops_owned_child(tmp_path):
    executable = _write_fake_server(tmp_path / "sparkless-server")
    server = AsyncSparklessServer(
        executable_path=executable,
        startup_timeout=2,
        shutdown_timeout=2,
        kill_timeout=1,
    )

    async def run():
        async with server:
            identity = server.server_instance_id
            assert identity
            assert server.http_url == "http://127.0.0.1:45678"
            with pytest.raises(SparklessServerError, match="nested entry"):
                await server.__aenter__()
        assert not server.is_running

    asyncio.run(run())

import asyncio
import sys
import textwrap
from types import ModuleType

import pytest

import sparkless
import sparkless.server as server_module
from sparkless.server import (
    AsyncSparklessServer,
    SparklessServer,
    SparklessServerError,
    UnsupportedSparkConnectError,
    _validate_ready,
)


def _write_fake_server(path, *, delay=0.0, stderr_bytes=0, version=None):
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
            'control_protocol_version': 2,
            'capabilities_protocol_version': 1,
            'server_version': {version!r},
            'agent_protocol_version': '1',
            'server_instance_id': identity,
            'http_url': 'http://127.0.0.1:45678',
            'spark_connect_url': None if '--disable-connect' in sys.argv else 'sc://127.0.0.1:45679',
            'capabilities': {{
                'capabilities_protocol_version': 1,
                'transport': 'http',
                'http_url': 'http://127.0.0.1:45678',
                'transports': ['http'] if '--disable-connect' in sys.argv else ['http', 'spark_connect'],
                'server_instance_id': identity,
                'spark_connect_supported': '--disable-connect' not in sys.argv,
                'spark_connect_url': None if '--disable-connect' in sys.argv else 'sc://127.0.0.1:45679',
                'spark_connect_protocol_version': None if '--disable-connect' in sys.argv else '4.0.5',
                'agent_protocol_version': '1',
                'mutation_protocol_version': '1'
            }}
        }}), flush=True)
        for line in sys.stdin:
            try:
                command = json.loads(line)
            except ValueError:
                continue
            if command == {{'control_protocol_version': 2, 'command': 'shutdown'}}:
                print(json.dumps({{
                    'type': 'stopped',
                    'control_protocol_version': 2
                }}), flush=True)
                break
        """
    ).format(
        python=sys.executable,
        delay=delay,
        stderr_bytes=stderr_bytes,
        version=version
        or server_module._installed_server_binary_version()
        or sparkless.__version__,
    )
    path.write_text(source)
    path.chmod(0o755)
    return str(path)


def _use_python_fake_server(monkeypatch):
    # The test child is a Python script, not a native executable. Windows cannot
    # launch a shebang script directly, so route it through the current interpreter.
    monkeypatch.setattr(
        server_module,
        "_command",
        lambda executable, _host, _port: (sys.executable, executable),
    )


def test_sync_launcher_owns_child_restart_and_bounds_diagnostics(tmp_path, monkeypatch):
    executable = _write_fake_server(tmp_path / "sparkless-server", stderr_bytes=100_000)
    _use_python_fake_server(monkeypatch)
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
    assert server.spark_connect_url == "sc://127.0.0.1:45679"
    assert len(server.diagnostics.encode("utf-8")) <= 64 * 1024

    server.stop()
    with pytest.raises(SparklessServerError, match="only while the server is ready"):
        _ = server.http_url
    server.start()
    assert server.server_instance_id != first_identity
    server.stop()

    http_only = SparklessServer(executable_path=executable, connect_port=None)
    http_only.start()
    try:
        assert http_only._ready_record["spark_connect_url"] is None
        assert http_only._ready_record["capabilities"]["transports"] == ["http"]
        with pytest.raises(
            UnsupportedSparkConnectError, match="disabled or unavailable"
        ):
            _ = http_only.spark_connect_url
    finally:
        http_only.stop()


def test_same_sync_context_owner_rejects_nested_entry(tmp_path, monkeypatch):
    executable = _write_fake_server(tmp_path / "sparkless-server")
    _use_python_fake_server(monkeypatch)
    server = SparklessServer(executable_path=executable, startup_timeout=2)
    with server:
        with pytest.raises(SparklessServerError, match="nested entry"):
            server.__enter__()


def test_packaged_server_version_can_differ_from_api_patch(tmp_path, monkeypatch):
    executable = _write_fake_server(tmp_path / "sparkless-server", version="4.18.0")
    package = ModuleType("sparkless_server_bin")
    package.executable_path = lambda: executable
    monkeypatch.setitem(sys.modules, "sparkless_server_bin", package)
    monkeypatch.setattr(
        server_module, "_installed_server_binary_version", lambda: "4.18.0"
    )
    _use_python_fake_server(monkeypatch)

    server = SparklessServer(startup_timeout=2)
    server.start()
    try:
        assert sparkless.__version__ != "4.18.0"
        assert server._ready_record["server_version"] == "4.18.0"
    finally:
        server.stop()


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


@pytest.mark.parametrize("phase", ["spawn", "diagnostics"])
def test_async_launcher_reaps_child_on_startup_cancellation(
    tmp_path, monkeypatch, phase
):
    executable = _write_fake_server(tmp_path / "sparkless-server", delay=10)
    _use_python_fake_server(monkeypatch)
    server = AsyncSparklessServer(
        executable_path=executable,
        startup_timeout=30,
        shutdown_timeout=1,
        kill_timeout=1,
    )

    async def run():
        diagnostics_started = asyncio.Event()
        inspect_binary = server._binary_lacks_spark_connect

        async def inspect_after_failure():
            diagnostics_started.set()
            return await inspect_binary()

        monkeypatch.setattr(
            server, "_binary_lacks_spark_connect", inspect_after_failure
        )
        task = asyncio.create_task(server.start())
        for _ in range(100):
            if (
                server._process is not None
                if phase == "spawn"
                else server._stderr_task is not None
            ):
                break
            await asyncio.sleep(0.01)
        child = server._process
        task.cancel()
        if phase == "diagnostics":
            await asyncio.wait_for(diagnostics_started.wait(), 2)
        else:
            await asyncio.sleep(0)
        task.cancel()
        try:
            with pytest.raises(asyncio.CancelledError):
                await task
            assert server._process is None
            assert child is not None and child.returncode is not None
        finally:
            await server.stop()

    asyncio.run(run())


def test_async_launcher_context_stops_owned_child(tmp_path, monkeypatch):
    executable = _write_fake_server(tmp_path / "sparkless-server")
    _use_python_fake_server(monkeypatch)
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

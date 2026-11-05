"""Synchronous owned-process launcher."""

from __future__ import annotations

from ._common import (
    Any,
    Dict,
    Optional,
    SparklessServerError,
    UnsupportedSparkConnectError,
    _ByteTail,
    _binary_lacks_spark_connect,
    _command_with_connect,
    _drain_sync_stderr,
    _parse_record,
    _product_package,
    _read_sync_frames,
    _resolve_executable,
    _validate_ready,
    _validate_timeouts,
    os,
    queue,
    subprocess,
    threading,
)


class SparklessServer:
    """A synchronous owner for one ``sparkless-server`` child process."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 0,
        executable_path: Optional[str] = None,
        startup_timeout: float = 30.0,
        shutdown_timeout: float = 10.0,
        kill_timeout: float = 5.0,
        env: Optional[Dict[str, str]] = None,
        connect_host: str = "127.0.0.1",
        connect_port: Optional[int] = 0,
        product: str = "base",
    ) -> None:
        self.host = host
        self.port = port
        self._executable_path = executable_path
        self.startup_timeout = startup_timeout
        self.shutdown_timeout = shutdown_timeout
        self.kill_timeout = kill_timeout
        _validate_timeouts(startup_timeout, shutdown_timeout, kill_timeout)
        self.env = None if env is None else dict(env)
        self.connect_host = connect_host
        self.connect_port = connect_port
        _product_package(product)
        self.product = product
        self._process: Optional[subprocess.Popen] = None
        self._records: Optional["queue.Queue[Any]"] = None
        self._stderr_tail = _ByteTail()
        self._threads = []  # type: list[threading.Thread]
        self._owner_pid = os.getpid()
        self._http_url: Optional[str] = None
        self._connect_url: Optional[str] = None
        self._instance_id: Optional[str] = None
        self._ready_record: Optional[Dict[str, Any]] = None
        self._entered = False

    def _check_owner(self) -> None:
        if os.getpid() != self._owner_pid:
            raise SparklessServerError(
                "server process ownership changed after fork; create a new launcher in the child"
            )

    @property
    def is_running(self) -> bool:
        self._check_owner()
        return self._process is not None and self._process.poll() is None

    @property
    def http_url(self) -> str:
        self._check_owner()
        if not self.is_running or self._http_url is None:
            raise SparklessServerError(
                "HTTP endpoint is available only while the server is ready"
            )
        return self._http_url

    @property
    def server_instance_id(self) -> str:
        self._check_owner()
        if not self.is_running or self._instance_id is None:
            raise SparklessServerError(
                "server identity is available only while the server is ready"
            )
        return self._instance_id

    @property
    def spark_connect_url(self) -> str:
        self._check_owner()
        if not self.is_running:
            raise SparklessServerError(
                "Spark Connect endpoint is available only while the server is ready"
            )
        if self.connect_port is None or self._connect_url is None:
            raise UnsupportedSparkConnectError(
                "Spark Connect is disabled or unavailable in this server binary"
            )
        return self._connect_url

    @property
    def diagnostics(self) -> str:
        return self._stderr_tail.text()

    def start(self) -> "SparklessServer":
        self._check_owner()
        if self.is_running and self._http_url is not None:
            return self
        if self._process is not None:
            self._stop_owned(force=False)
        executable = _resolve_executable(self._executable_path, self.product)
        command = _command_with_connect(
            executable, self.host, self.port, self.connect_host, self.connect_port
        )
        child_env = None
        if self.env is not None:
            child_env = os.environ.copy()
            child_env.update(self.env)
        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=child_env,
            bufsize=0,
        )
        self._process = process
        self._records = queue.Queue(maxsize=4)
        self._stderr_tail = _ByteTail()
        stdout_thread = threading.Thread(
            target=_read_sync_frames, args=(process.stdout, self._records), daemon=True
        )
        stderr_thread = threading.Thread(
            target=_drain_sync_stderr,
            args=(process.stderr, self._stderr_tail),
            daemon=True,
        )
        self._threads = [stdout_thread, stderr_thread]
        for thread in self._threads:
            thread.start()
        try:
            frame = self._records.get(timeout=self.startup_timeout)
            if frame is None:
                raise SparklessServerError("server exited before readiness")
            if isinstance(frame, BaseException):
                raise frame
            record = _parse_record(frame)
            url, connect_url = _validate_ready(
                record,
                require_spark_connect=self.connect_port is not None,
                expected_product=self.product,
                expected_backend=(self.env or {}).get(
                    "SPARKLESS_EXECUTION_BACKEND",
                    os.environ.get("SPARKLESS_EXECUTION_BACKEND", "polars"),
                ),
            )
        except BaseException as error:
            if self.connect_port is not None and self._process is not None:
                try:
                    self._process.wait(timeout=0.25)
                except subprocess.TimeoutExpired:
                    pass
                for thread in self._threads:
                    thread.join(timeout=0.25)
            connect_unavailable = (
                self.connect_port is not None
                and _binary_lacks_spark_connect(self.diagnostics)
            )
            self._stop_owned(force=True)
            if connect_unavailable:
                raise UnsupportedSparkConnectError(
                    "server binary does not provide Spark Connect"
                ) from error
            raise
        self._ready_record = record
        self._http_url = url
        self._connect_url = connect_url
        self._instance_id = record["server_instance_id"]
        return self

    def stop(self) -> None:
        self._check_owner()
        self._stop_owned(force=False)

    def _stop_owned(self, force: bool) -> None:
        process = self._process
        if process is None:
            return
        if process.poll() is None and not force:
            try:
                if process.stdin is not None:
                    process.stdin.write(
                        b'{"control_protocol_version":2,"command":"shutdown"}\n'
                    )
                    process.stdin.flush()
                    process.stdin.close()
            except (BrokenPipeError, OSError):
                pass
            try:
                process.wait(timeout=self.shutdown_timeout)
            except subprocess.TimeoutExpired:
                process.terminate()
                try:
                    process.wait(timeout=self.kill_timeout)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        elif process.poll() is None:
            process.kill()
            try:
                process.wait(timeout=self.kill_timeout)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        if process.stdout is not None:
            try:
                process.stdout.close()
            except OSError:
                pass
        if process.stderr is not None:
            try:
                process.stderr.close()
            except OSError:
                pass
        self._process = None
        self._records = None
        self._http_url = None
        self._connect_url = None
        self._instance_id = None
        self._ready_record = None

    def restart(self) -> "SparklessServer":
        self.stop()
        return self.start()

    def __enter__(self) -> "SparklessServer":
        if self._entered:
            raise SparklessServerError(
                "nested entry of the same server owner is not supported"
            )
        self._entered = True
        try:
            return self.start()
        except BaseException:
            self._entered = False
            raise

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        try:
            self.stop()
        finally:
            self._entered = False

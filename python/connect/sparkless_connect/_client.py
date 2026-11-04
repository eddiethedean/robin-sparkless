"""Spark Connect client and server lifecycle composition."""

from __future__ import annotations

from typing import Literal

from ._common import (
    Any,
    Mapping,
    Optional,
    SparklessServerError,
    os,
)
from ._sync import SparklessServer


class SparklessConnect:
    """Own a Spark Connect server and one real pyspark-client session."""

    server_class = SparklessServer

    def __init__(self, *, server_options: Optional[Mapping[str, Any]] = None) -> None:
        if server_options is not None and not isinstance(server_options, Mapping):
            raise TypeError(
                "server_options must be a mapping of SparklessServer options"
            )
        self._options = {} if server_options is None else dict(server_options)
        if self._options.get("connect_port", 0) is None:
            raise ValueError("SparklessConnect requires the Spark Connect endpoint")
        self._server: Optional[SparklessServer] = None
        self._session: Optional[Any] = None
        self._active = False
        self._owner_pid = os.getpid()

    @staticmethod
    def _attach_cleanup_errors(
        primary: BaseException, cleanup_errors: list[BaseException]
    ) -> None:
        for error in cleanup_errors:
            primary.add_note(f"SparklessConnect cleanup also failed: {error!r}")

    def _ensure_owner_process(self) -> None:
        if os.getpid() != self._owner_pid:
            raise SparklessServerError(
                "SparklessConnect ownership changed after fork; create a new helper in the child"
            )

    def __enter__(self) -> Any:
        self._ensure_owner_process()
        if self._active:
            raise SparklessServerError(
                "nested entry of the same SparklessConnect is not supported"
            )
        # Fail before creating a child process when the optional client is missing.
        try:
            from pyspark.sql import SparkSession
        except ImportError as error:
            raise ImportError(
                "SparklessConnect requires pyspark-client; install sparkless-connect[client]"
            ) from error

        self._active = True
        try:
            # Retain the launcher before start: it owns any child created before a
            # readiness/startup error is raised, and the failure path must still
            # attempt to stop it.
            server = self.server_class(**self._options)
            self._server = server
            server.start()
            self._session = SparkSession.builder.remote(
                server.spark_connect_url
            ).create()
            return self._session
        except BaseException as primary:
            cleanup_errors: list[BaseException] = []
            if self._session is not None:
                try:
                    self._session.stop()
                    self._session = None
                except BaseException as error:
                    cleanup_errors.append(error)
            if self._server is not None:
                try:
                    self._server.stop()
                    self._server = None
                except BaseException as error:
                    cleanup_errors.append(error)
            self._active = self._session is not None or self._server is not None
            self._attach_cleanup_errors(primary, cleanup_errors)
            raise

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> Literal[False]:
        self._ensure_owner_process()
        cleanup_errors: list[BaseException] = []
        session, server = self._session, self._server
        if session is not None:
            try:
                session.stop()
                self._session = None
            except BaseException as error:
                cleanup_errors.append(error)
        if server is not None:
            try:
                server.stop()
                self._server = None
            except BaseException as error:
                cleanup_errors.append(error)
        # Keep whichever resource failed cleanup so another __exit__ can retry;
        # don't allow sequential reuse while this instance still owns either one.
        self._active = self._session is not None or self._server is not None

        if exc is not None:
            self._attach_cleanup_errors(exc, cleanup_errors)
            return False
        if cleanup_errors:
            primary = cleanup_errors[0]
            self._attach_cleanup_errors(primary, cleanup_errors[1:])
            raise primary
        return False

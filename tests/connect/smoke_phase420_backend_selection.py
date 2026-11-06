"""Probe product/backend startup identity and pre-readiness rejection boundaries."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import tempfile
import urllib.request

from sparkless_connect import SparklessServer as BaseServer
from sparkless_connect import SparklessServerError
from sparkless_server_duckdb import SparklessServer as DuckDbServer


ROOT = Path(__file__).resolve().parents[2]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def owner_config(database_path: Path) -> str:
    return json.dumps({"mode": "local", "database_path": str(database_path)})


def verify_live_identity(
    server: BaseServer, *, expected_product: str, expected_backend: str
) -> dict[str, object]:
    server.start()
    record = server._ready_record
    assert isinstance(record, dict), "launcher should retain the readiness record"
    with urllib.request.urlopen(
        server.http_url + "/capabilities", timeout=10
    ) as response:
        capabilities = json.load(response)

    assert record["server_product"] == expected_product
    assert record["execution_backend"] == expected_backend
    assert capabilities == record["capabilities"]
    assert capabilities["server_product"] == expected_product
    assert capabilities["execution_backend"] == expected_backend
    assert capabilities["server_instance_id"] == record["server_instance_id"]
    assert capabilities["spark_connect_supported"] is True
    assert "spark_connect" in capabilities["transports"]
    assert capabilities["spark_connect_url"] == record["spark_connect_url"]
    return {
        "server_product": expected_product,
        "execution_backend": expected_backend,
        "server_instance_id_matches": True,
        "readiness_capabilities_match": True,
        "spark_connect_supported": True,
    }


def expect_startup_rejection(
    server: BaseServer, *, expected_message: str
) -> dict[str, object]:
    try:
        server.start()
    except SparklessServerError as error:
        diagnostic = f"{error}\n{server.diagnostics}".lower()
        assert expected_message.lower() in diagnostic, diagnostic
        assert not server.is_running, "invalid startup must not expose a live process"
        return {"rejected_before_readiness": True, "diagnostic": expected_message}
    finally:
        server.stop()
    raise AssertionError(f"server unexpectedly started; expected {expected_message!r}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-server-bin", type=Path, required=True)
    parser.add_argument("--duckdb-server-bin", type=Path, required=True)
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT
        / "docs/plans/sparkless-server/evidence/phase-420-backend-startup-local.json",
    )
    args = parser.parse_args()
    for binary in (args.base_server_bin, args.duckdb_server_bin):
        if not binary.is_file():
            parser.error(f"server binary does not exist: {binary}")

    with tempfile.TemporaryDirectory(prefix="sparkless-phase420-startup-") as directory:
        root = Path(directory)
        base_binary = args.base_server_bin.resolve()
        duckdb_binary = args.duckdb_server_bin.resolve()

        base_default = BaseServer(
            executable_path=str(base_binary), connect_port=0, startup_timeout=30
        )
        try:
            base_default_result = verify_live_identity(
                base_default, expected_product="base", expected_backend="polars"
            )
        finally:
            base_default.stop()

        duckdb_default = DuckDbServer(
            executable_path=str(duckdb_binary), connect_port=0, startup_timeout=30
        )
        try:
            duckdb_default_result = verify_live_identity(
                duckdb_default, expected_product="duckdb", expected_backend="polars"
            )
        finally:
            duckdb_default.stop()

        selected_owner = root / "selected.duckdb"
        duckdb_selected = DuckDbServer(
            executable_path=str(duckdb_binary),
            connect_port=0,
            startup_timeout=30,
            env={
                "SPARKLESS_EXECUTION_BACKEND": "duckdb",
                "SPARKLESS_DUCKDB_EXECUTION_OWNER": owner_config(selected_owner),
            },
        )
        try:
            duckdb_selected_result = verify_live_identity(
                duckdb_selected, expected_product="duckdb", expected_backend="duckdb"
            )
            assert (
                duckdb_selected._ready_record["capabilities"][
                    "duckdb_execution_owner_mode"
                ]
                == "local"
            )
        finally:
            duckdb_selected.stop()

        base_wrong_product = BaseServer(
            executable_path=str(base_binary),
            connect_port=0,
            startup_timeout=10,
            env={
                "SPARKLESS_EXECUTION_BACKEND": "duckdb",
                "SPARKLESS_DUCKDB_EXECUTION_OWNER": owner_config(
                    root / "wrong-product.duckdb"
                ),
            },
        )
        wrong_product_result = expect_startup_rejection(
            base_wrong_product,
            expected_message="DuckDB execution requires the sparkless-server-duckdb product",
        )

        duckdb_missing_owner = DuckDbServer(
            executable_path=str(duckdb_binary),
            connect_port=0,
            startup_timeout=10,
            env={"SPARKLESS_EXECUTION_BACKEND": "duckdb"},
        )
        missing_owner_result = expect_startup_rejection(
            duckdb_missing_owner,
            expected_message="SPARKLESS_DUCKDB_EXECUTION_OWNER is required",
        )

        duckdb_missing_connect = DuckDbServer(
            executable_path=str(duckdb_binary),
            connect_port=None,
            startup_timeout=10,
            env={
                "SPARKLESS_EXECUTION_BACKEND": "duckdb",
                "SPARKLESS_DUCKDB_EXECUTION_OWNER": owner_config(
                    root / "missing-connect.duckdb"
                ),
            },
        )
        missing_connect_result = expect_startup_rejection(
            duckdb_missing_connect,
            expected_message="DuckDB execution requires a Spark Connect listener",
        )

    report = {
        "format_version": 1,
        "phase": "4.20",
        "slice": "420-01",
        "result": "passed",
        "platform": platform.platform(),
        "base_server": {
            "path": str(base_binary),
            "sha256": sha256(base_binary),
            "version": subprocess.run(
                [str(base_binary), "--version"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip(),
        },
        "duckdb_server": {
            "path": str(duckdb_binary),
            "sha256": sha256(duckdb_binary),
            "version": subprocess.run(
                [str(duckdb_binary), "--version"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip(),
        },
        "runner_sha256": sha256(Path(__file__).resolve()),
        "probes": {
            "base_default_polars": base_default_result,
            "duckdb_product_default_polars": duckdb_default_result,
            "duckdb_product_selected_local_owner": duckdb_selected_result,
            "base_product_rejects_duckdb_backend": wrong_product_result,
            "duckdb_backend_requires_owner": missing_owner_result,
            "duckdb_backend_requires_connect": missing_connect_result,
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Phase 4.20 backend startup probes passed; report: {args.output}")


if __name__ == "__main__":
    main()

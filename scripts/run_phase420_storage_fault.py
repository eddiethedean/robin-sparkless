"""Run the DuckDB COMMIT storage-failure witness under an OS file-size limit."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import platform
import resource
import signal
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
TEST_NAME = "execution::tests::storage_write_failure_during_commit_returns_unknown"
FILE_LIMIT_BYTES = 4 * 1024 * 1024


def test_binary_from_cargo_output(stdout: str) -> Path:
    candidates: list[Path] = []
    for line in stdout.splitlines():
        if not line.startswith("{"):
            continue
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        if message.get("reason") != "compiler-artifact":
            continue
        target = message.get("target", {})
        profile = message.get("profile", {})
        executable = message.get("executable")
        if (
            target.get("name") == "robin_sparkless_duckdb"
            and profile.get("test") is True
            and executable
        ):
            candidates.append(Path(executable))
    if len(candidates) != 1:
        raise RuntimeError(
            "expected one robin-sparkless-duckdb unit-test binary, found "
            f"{len(candidates)}"
        )
    return candidates[0]


def ignore_file_size_signal_and_limit() -> None:
    signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
    resource.setrlimit(resource.RLIMIT_FSIZE, (FILE_LIMIT_BYTES, FILE_LIMIT_BYTES))


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def run(output: Path) -> dict[str, object]:
    if sys.platform not in ("darwin", "linux"):
        raise RuntimeError("the RLIMIT_FSIZE witness requires Linux or macOS")
    build_command = [
        "cargo",
        "test",
        "--release",
        "--locked",
        "-p",
        "robin-sparkless-duckdb",
        "--no-run",
        "--message-format=json",
    ]
    build = subprocess.run(
        build_command,
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    test_binary = test_binary_from_cargo_output(build.stdout)
    command = [
        str(test_binary),
        TEST_NAME,
        "--exact",
        "--ignored",
        "--nocapture",
    ]
    test = subprocess.run(
        command,
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        preexec_fn=ignore_file_size_signal_and_limit,
    )
    output_text = test.stdout + test.stderr
    required_witness = (
        "test execution::tests::storage_write_failure_during_commit_returns_unknown ... ok",
        "transaction outcome is unknown after commit was requested",
        "post-fault recovery: rows=0 receipts=0 payload_bytes=0",
    )
    report: dict[str, object] = {
        "schema_version": 1,
        "phase": "4.20",
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "source_revision": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "build_command": build_command,
        "command": command,
        "test_binary_sha256": sha256(test_binary),
        "rlimit_fsize_bytes": FILE_LIMIT_BYTES,
        "sigxfsz_ignored": True,
        "exit_code": test.returncode,
        "required_witnesses_present": {
            witness: witness in output_text for witness in required_witness
        },
        "output": output_text,
        "scope": "one real local WAL file-size failure during COMMIT; not arbitrary partial-I/O coverage",
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    if test.returncode != 0 or not all(report["required_witnesses_present"].values()):
        raise RuntimeError(
            f"storage-failure witness failed; report written to {output}"
        )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = run(args.output.resolve())
    print(
        json.dumps(
            {
                key: report[key]
                for key in ("platform", "test_binary_sha256", "exit_code")
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Run and validate isolated V17 process-RSS witnesses for release candidates."""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any


DEFAULT_ROWS = (10_000, 1_000_000, 10_000_000)
RSS_PATTERN = re.compile(r"Maximum resident set size \(kbytes\):\s*(\d+)")
BSD_RSS_PATTERN = re.compile(r"^\s*(\d+)\s+maximum resident set size\s*$", re.MULTILINE)
PASSED_PATTERN = re.compile(r"test result: ok\.\s+1 passed;\s+0 failed;")


def parse_max_rss_kib(report: str) -> int:
    """Parse GNU time's KiB or BSD time's byte maximum-RSS field."""
    matches = RSS_PATTERN.findall(report)
    if len(matches) == 1:
        return int(matches[0])
    bsd_matches = BSD_RSS_PATTERN.findall(report)
    if len(bsd_matches) == 1:
        return (int(bsd_matches[0]) + 1023) // 1024
    raise ValueError("time report must contain one maximum RSS value")


def parse_measurement(
    output: str, marker: str, expected_rows: int
) -> dict[str, int | str]:
    matches = re.findall(rf"^{re.escape(marker)}\s+([^\r\n]+)$", output, re.MULTILINE)
    if len(matches) != 1:
        raise ValueError(
            f"expected one {marker} measurement line, found {len(matches)}"
        )

    fields: dict[str, int | str] = {}
    for token in matches[0].split():
        key, separator, value = token.partition("=")
        if not separator:
            continue
        fields[key] = int(value) if value.isdecimal() else value
    fields.setdefault("rows", expected_rows)
    if fields.get("rows") != expected_rows:
        raise ValueError(
            f"{marker} reported rows={fields.get('rows')!r}, expected {expected_rows}"
        )
    return fields


def validate_catalog_measurement(fields: dict[str, int | str], rows: int) -> None:
    expected = {
        "original_bytes": rows * 8,
        "candidate_bytes": rows * 8,
        "staged_accounted_bytes": rows * 16,
        "retained_pinned_bytes": rows * 8,
        "read_rows": rows,
        "read_bytes": rows * 8,
    }
    for key, value in expected.items():
        if fields.get(key) != value:
            raise ValueError(
                f"catalog measurement {key}={fields.get(key)!r}; expected {value}"
            )


def validate_range_measurement(fields: dict[str, int | str], rows: int) -> None:
    expected = {"strategy": "lazy_scan", "row_count_hint": rows, "columns": 1}
    for key, value in expected.items():
        if fields.get(key) != value:
            raise ValueError(
                f"range measurement {key}={fields.get(key)!r}; expected {value}"
            )


def validate_range_benchmark_measurement(
    fields: dict[str, int | str], rows: int
) -> None:
    expected: dict[str, int | str] = {
        "rows": rows,
        "schema_columns": 1,
        "schema_type": "Long",
        "schema_nullable": "false",
        "count": rows - 6,
        "expected_count": rows - 6,
        "limit_rows": 10,
        "output_bytes": 80,
        "strategy": "lazy_range_bounded_batches",
        "projected_strategy": "bounded_batches",
        "projected_columns": 1,
        "projected_schema_type": "Long",
        "projected_nullable": "true",
        "projected_rows": rows,
        "projected_batch_count": (rows + 16_383) // 16_384,
        "projected_largest_batch_rows": min(rows, 16_384),
        "projected_first_value": 0,
        "projected_last_value": (rows - 1) * 2,
    }
    for key, value in expected.items():
        if fields.get(key) != value:
            raise ValueError(
                f"range benchmark {key}={fields.get(key)!r}; expected {value}"
            )
    for key in (
        "construction_us",
        "construction_allocated_bytes",
        "construction_live_bytes",
        "construction_peak_live_bytes",
        "count_us",
        "count_allocated_bytes",
        "count_live_bytes",
        "count_peak_live_bytes",
        "limit_us",
        "limit_allocated_bytes",
        "limit_live_bytes",
        "limit_peak_live_bytes",
        "projected_plan_us",
        "projected_plan_allocated_bytes",
        "projected_plan_live_bytes",
        "projected_plan_peak_live_bytes",
        "projected_scan_us",
        "projected_scan_allocated_bytes",
        "projected_scan_live_bytes",
        "projected_scan_peak_live_bytes",
    ):
        if not isinstance(fields.get(key), int) or fields[key] < 0:
            raise ValueError(f"range benchmark is missing a valid {key} measurement")


def parse_rows(raw: str) -> tuple[int, ...]:
    try:
        rows = tuple(int(value) for value in raw.split(","))
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "rows must be comma-separated positive integers"
        ) from error
    if (
        not rows
        or any(value <= 0 for value in rows)
        or tuple(sorted(set(rows))) != rows
    ):
        raise argparse.ArgumentTypeError(
            "rows must be unique, positive, and increasing"
        )
    return rows


def host_facts() -> dict[str, Any]:
    cpu_model = None
    memory_kib = None
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.lower().startswith("model name"):
                cpu_model = line.split(":", 1)[1].strip()
                break
    except OSError:
        pass
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal:"):
                memory_kib = int(line.split()[1])
                break
    except (OSError, ValueError, IndexError):
        pass
    return {
        "runner_os": os.environ.get("RUNNER_OS", platform.system()),
        "runner_arch": os.environ.get("RUNNER_ARCH", platform.machine()),
        "platform": platform.platform(),
        "cpu_model": cpu_model or platform.processor() or platform.machine(),
        "memory_kib": memory_kib,
    }


def stream_process(
    command: list[str],
    *,
    environment: dict[str, str] | None = None,
    log_path: Path,
) -> subprocess.CompletedProcess[str]:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    output: list[str] = []
    with log_path.open("w", encoding="utf-8", errors="replace") as log:
        process = subprocess.Popen(
            command,
            env=environment,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
        )
        assert process.stdout is not None
        for line in process.stdout:
            print(line, end="", flush=True)
            log.write(line)
            log.flush()
            output.append(line)
        returncode = process.wait()
    return subprocess.CompletedProcess(command, returncode, "".join(output), "")


def run_build(command: list[str], log_path: Path) -> None:
    print("$ " + " ".join(command), flush=True)
    result = stream_process(command, log_path=log_path)
    if result.returncode:
        raise RuntimeError(
            f"test binary build failed with exit code {result.returncode}; log: {log_path}"
        )


def timed_test(
    *,
    label: str,
    command: list[str],
    environment: dict[str, str],
    rss_cap_mib: int,
    marker: str | None = None,
    rows: int | None = None,
    expect_test: bool = True,
    log_path: Path,
) -> dict[str, Any]:
    time_binary = Path("/usr/bin/time")
    if not time_binary.is_file():
        raise RuntimeError("the V17 witness requires GNU time at /usr/bin/time")

    with tempfile.TemporaryDirectory(prefix="sparkless-v17-time-") as directory:
        report_path = Path(directory) / "time.txt"
        is_bsd_time = platform.system() == "Darwin"
        timed_command = (
            [str(time_binary), "-l", *command]
            if is_bsd_time
            else [str(time_binary), "-v", "-o", str(report_path), *command]
        )
        started = time.perf_counter()
        result = stream_process(
            timed_command, environment=environment, log_path=log_path
        )
        elapsed_seconds = time.perf_counter() - started
        combined = result.stdout + "\n" + result.stderr
        if result.returncode:
            raise RuntimeError(
                f"{label} failed with exit code {result.returncode}; log: {log_path}"
            )
        if expect_test and not PASSED_PATTERN.search(combined):
            raise RuntimeError(f"{label} did not report exactly one passing Rust test")

        report = combined if is_bsd_time else report_path.read_text()
        peak_rss_kib = parse_max_rss_kib(report)
        rss_cap_kib = rss_cap_mib * 1024
        if peak_rss_kib > rss_cap_kib:
            raise RuntimeError(
                f"{label} peak RSS {peak_rss_kib} KiB exceeded its {rss_cap_mib} MiB cap"
            )
        measurement = (
            parse_measurement(combined, marker, rows)
            if marker is not None and rows is not None
            else None
        )
        result_record = {
            "label": label,
            "rows": rows,
            "peak_rss_kib": peak_rss_kib,
            "rss_cap_mib": rss_cap_mib,
            "elapsed_seconds": round(elapsed_seconds, 3),
            "test_passed": True,
        }
        if measurement is not None:
            result_record["measurement"] = measurement
        print(
            f"V17_RESOURCE label={label} rows={rows or 'n/a'} "
            f"peak_rss_kib={peak_rss_kib} cap_mib={rss_cap_mib} "
            f"elapsed_seconds={elapsed_seconds:.3f}",
            flush=True,
        )
        return result_record


def validate_report(report: dict[str, Any], expected_sha: str | None = None) -> None:
    if report.get("schema_version") != 1:
        raise ValueError("unsupported V17 resource report schema")
    if expected_sha and report.get("candidate_sha") != expected_sha:
        raise ValueError("V17 resource report SHA does not match the candidate")
    rows = report.get("rows")
    repetitions = report.get("repetitions")
    caps = report.get("caps_mib")
    if (
        not isinstance(rows, list)
        or not rows
        or any(not isinstance(row, int) or row <= 0 for row in rows)
        or rows != sorted(set(rows))
        or not isinstance(repetitions, int)
        or repetitions < 1
    ):
        raise ValueError("V17 report is missing workload sizes or repetitions")
    if not isinstance(report.get("candidate_sha"), str) or not report["candidate_sha"]:
        raise ValueError("V17 report is missing its candidate SHA")
    if not isinstance(caps, dict) or any(
        not isinstance(caps.get(key), int) or caps[key] < 1
        for key in (
            "lazy_range_construction",
            "range_workload",
            "catalog_retention_staging",
            "sustained_http",
        )
    ):
        raise ValueError("V17 report is missing positive per-workload RSS caps")

    for key in (
        "lazy_range_construction",
        "range_workload",
        "catalog_retention_staging",
    ):
        runs = report.get(key)
        if not isinstance(runs, list) or len(runs) != len(rows) * repetitions:
            raise ValueError(f"V17 report has incomplete {key} repetitions")
        cap_kib = caps[key] * 1024
        for row_count in rows:
            row_runs = [entry for entry in runs if entry.get("rows") == row_count]
            if len(row_runs) != repetitions:
                raise ValueError(f"V17 report has incomplete {key} rows={row_count}")
            if {entry.get("repeat") for entry in row_runs} != set(
                range(1, repetitions + 1)
            ):
                raise ValueError(
                    f"V17 report has duplicate or missing {key} run numbers"
                )
        for entry in runs:
            if (
                entry.get("test_passed") is not True
                or entry.get("peak_rss_kib", cap_kib + 1) > cap_kib
            ):
                raise ValueError(
                    f"V17 report contains a failed or over-budget {key} run"
                )
            measured_rows = entry.get("rows")
            measurement = entry.get("measurement")
            if not isinstance(measured_rows, int) or not isinstance(measurement, dict):
                raise ValueError(f"V17 report has malformed {key} measurement metadata")
            if key == "lazy_range_construction":
                validate_range_measurement(measurement, measured_rows)
            elif key == "range_workload":
                validate_range_benchmark_measurement(measurement, measured_rows)
            else:
                validate_catalog_measurement(measurement, measured_rows)

    http_runs = report.get("sustained_http")
    if not isinstance(http_runs, list) or len(http_runs) != repetitions:
        raise ValueError("V17 report has incomplete sustained HTTP repetitions")
    http_cap_kib = caps["sustained_http"] * 1024
    if {entry.get("repeat") for entry in http_runs} != set(range(1, repetitions + 1)):
        raise ValueError(
            "V17 report has duplicate or missing sustained HTTP run numbers"
        )
    for entry in http_runs:
        if (
            entry.get("test_passed") is not True
            or entry.get("peak_rss_kib", http_cap_kib + 1) > http_cap_kib
        ):
            raise ValueError(
                "V17 report contains a failed or over-budget sustained HTTP run"
            )

    range_counts = report.get("bounded_range_count")
    shared_cap_kib = caps["catalog_retention_staging"] * 1024
    if (
        not isinstance(range_counts, dict)
        or range_counts.get("test_passed") is not True
        or range_counts.get("peak_rss_kib", shared_cap_kib + 1) > shared_cap_kib
    ):
        raise ValueError(
            "V17 report is missing the bounded filtered-range correctness witness"
        )


def write_step_summary(report: dict[str, Any]) -> None:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return

    lines = [
        "## V17 resource witness",
        "",
        f"Candidate: `{report['candidate_sha']}`; runner: "
        f"`{report['host']['runner_os']}/{report['host']['runner_arch']}`; "
        f"CPU: `{report['host']['cpu_model']}`; cap: "
        f"range `{report['caps_mib']['lazy_range_construction']} MiB`, "
        f"catalog/HTTP `{report['caps_mib']['catalog_retention_staging']} MiB`.",
        "",
        "| Workload | Rows | Run | Peak RSS (MiB) | Wall (s) |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for key, name in (
        ("lazy_range_construction", "Lazy range construction + schema"),
        ("range_workload", "Range construction + filtered count + LIMIT 10"),
        ("catalog_retention_staging", "Pinned snapshot + staged replacement"),
    ):
        for entry in report[key]:
            lines.append(
                f"| {name} | {entry['rows']:,} | {entry['repeat']} | "
                f"{entry['peak_rss_kib'] / 1024:.1f} | {entry['elapsed_seconds']:.3f} |"
            )
    for entry in report["sustained_http"]:
        lines.append(
            f"| 1,024 filtered counts / 4 clients | — | {entry['repeat']} | "
            f"{entry['peak_rss_kib'] / 1024:.1f} | {entry['elapsed_seconds']:.3f} |"
        )
    with Path(summary_path).open("a") as stream:
        stream.write("\n".join(lines) + "\n")


def run_witness(args: argparse.Namespace) -> dict[str, Any]:
    log_directory = (
        Path(args.output).parent / "v17-logs" if args.output else Path("v17-logs")
    )
    log_directory.mkdir(parents=True, exist_ok=True)
    args.diagnostic_state = {
        "schema_version": 1,
        "status": "running",
        "candidate_sha": os.environ.get("GITHUB_SHA", "local"),
        "run_id": os.environ.get("GITHUB_RUN_ID"),
        "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
        "workflow": os.environ.get("GITHUB_WORKFLOW"),
        "phase": "initializing",
        "completed_steps": [],
        "log_directory": str(log_directory),
    }
    _write_diagnostic(args)

    build_commands = [
        [
            "cargo",
            "test",
            "--release",
            "--locked",
            "-p",
            "robin-sparkless-polars",
            "--features",
            "sql",
            "--lib",
            "--no-run",
        ],
        [
            "cargo",
            "test",
            "--release",
            "--locked",
            "-p",
            "sparkless-server",
            "--lib",
            "--no-run",
        ],
        [
            "cargo",
            "build",
            "--release",
            "--locked",
            "-p",
            "robin-sparkless-polars",
            "--features",
            "sql",
            "--example",
            "phase1_range_benchmark",
        ],
    ]
    for index, command in enumerate(build_commands, start=1):
        args.diagnostic_state["phase"] = "build-{}".format(index)
        _write_diagnostic(args)
        run_build(command, log_directory / "build-{}.log".format(index))
        args.diagnostic_state["completed_steps"].append("build-{}".format(index))
        _write_diagnostic(args)

    metadata = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"],
        text=True,
        capture_output=True,
        check=True,
    )
    target_directory = Path(json.loads(metadata.stdout)["target_directory"])
    benchmark_name = (
        "phase1_range_benchmark.exe" if os.name == "nt" else "phase1_range_benchmark"
    )
    benchmark_binary = target_directory / "release" / "examples" / benchmark_name
    if not benchmark_binary.is_file():
        raise RuntimeError(f"range benchmark binary was not built: {benchmark_binary}")

    base_environment = os.environ.copy()
    base_environment["LC_ALL"] = "C"
    base_environment["CARGO_TERM_COLOR"] = "never"
    range_cap = args.range_rss_cap_mib
    shared_cap = args.rss_cap_mib
    catalog_runs: list[dict[str, Any]] = []
    range_runs: list[dict[str, Any]] = []
    range_workload_runs: list[dict[str, Any]] = []

    for rows in args.rows:
        for repeat in range(1, args.repetitions + 1):
            environment = base_environment.copy()
            environment["SPARKLESS_RANGE_BENCH_ROWS"] = str(rows)
            entry = timed_test(
                label="lazy-range-construction",
                command=[
                    "cargo",
                    "test",
                    "--release",
                    "--locked",
                    "-p",
                    "robin-sparkless-polars",
                    "--features",
                    "sql",
                    "--lib",
                    "lazy_range_construction_memory_measurement",
                    "--",
                    "--ignored",
                    "--nocapture",
                ],
                environment=environment,
                rss_cap_mib=range_cap,
                marker="range_memory",
                rows=rows,
                log_path=log_directory / "lazy-range-{}-{}.log".format(rows, repeat),
            )
            validate_range_measurement(entry["measurement"], rows)
            entry["repeat"] = repeat
            range_runs.append(entry)
            _record_diagnostic_step(args, "lazy-range-{}-{}".format(rows, repeat))

            environment = base_environment.copy()
            environment["SPARKLESS_MAX_RANGE_ROWS"] = str(max(args.rows))
            entry = timed_test(
                label="range-count-limit-allocation-benchmark",
                command=[str(benchmark_binary), str(rows)],
                environment=environment,
                rss_cap_mib=range_cap,
                marker=f"rows={rows}",
                rows=rows,
                expect_test=False,
                log_path=log_directory
                / "range-workload-{}-{}.log".format(rows, repeat),
            )
            validate_range_benchmark_measurement(entry["measurement"], rows)
            entry["repeat"] = repeat
            range_workload_runs.append(entry)
            _record_diagnostic_step(args, "range-workload-{}-{}".format(rows, repeat))

            environment = base_environment.copy()
            environment["SPARKLESS_CATALOG_BENCH_ROWS"] = str(rows)
            entry = timed_test(
                label="catalog-retention-staging",
                command=[
                    "cargo",
                    "test",
                    "--release",
                    "--locked",
                    "-p",
                    "robin-sparkless-polars",
                    "--features",
                    "sql",
                    "--lib",
                    "shared_catalog_retention_and_staging_memory_measurement",
                    "--",
                    "--ignored",
                    "--nocapture",
                ],
                environment=environment,
                rss_cap_mib=shared_cap,
                marker="catalog_memory",
                rows=rows,
                log_path=log_directory / "catalog-{}-{}.log".format(rows, repeat),
            )
            validate_catalog_measurement(entry["measurement"], rows)
            entry["repeat"] = repeat
            catalog_runs.append(entry)
            _record_diagnostic_step(args, "catalog-{}-{}".format(rows, repeat))

    range_count = timed_test(
        label="bounded-filtered-range-count",
        command=[
            "cargo",
            "test",
            "--release",
            "--locked",
            "-p",
            "robin-sparkless-polars",
            "--features",
            "sql",
            "--lib",
            "filtered_large_range_count_uses_bounded_batches",
            "--",
            "--nocapture",
        ],
        environment=base_environment,
        rss_cap_mib=shared_cap,
        log_path=log_directory / "bounded-filtered-range-count.log",
    )
    _record_diagnostic_step(args, "bounded-filtered-range-count")

    http_runs: list[dict[str, Any]] = []
    for repeat in range(1, args.repetitions + 1):
        entry = timed_test(
            label="sustained-multi-client-http",
            command=[
                "cargo",
                "test",
                "--release",
                "--locked",
                "-p",
                "sparkless-server",
                "--lib",
                "sustained_multi_client_lazy_counts_keep_health_progressing",
                "--",
                "--nocapture",
            ],
            environment=base_environment,
            rss_cap_mib=shared_cap,
            log_path=log_directory / "sustained-http-{}.log".format(repeat),
        )
        entry["repeat"] = repeat
        http_runs.append(entry)
        _record_diagnostic_step(args, "sustained-http-{}".format(repeat))

    report = {
        "schema_version": 1,
        "candidate_sha": os.environ.get("GITHUB_SHA", "local"),
        "run_id": os.environ.get("GITHUB_RUN_ID"),
        "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
        "workflow": os.environ.get("GITHUB_WORKFLOW"),
        "status": "success",
        "diagnostic_logs": sorted(path.name for path in log_directory.glob("*.log")),
        "host": host_facts(),
        "rustc": subprocess.run(
            ["rustc", "--version"], text=True, capture_output=True, check=True
        ).stdout.strip(),
        "cargo": subprocess.run(
            ["cargo", "--version"], text=True, capture_output=True, check=True
        ).stdout.strip(),
        "rows": list(args.rows),
        "repetitions": args.repetitions,
        "caps_mib": {
            "lazy_range_construction": range_cap,
            "range_workload": range_cap,
            "catalog_retention_staging": shared_cap,
            "sustained_http": shared_cap,
        },
        "lazy_range_construction": range_runs,
        "range_workload": range_workload_runs,
        "catalog_retention_staging": catalog_runs,
        "bounded_range_count": range_count,
        "sustained_http": http_runs,
    }
    validate_report(report)
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    args.diagnostic_state["status"] = "success"
    args.diagnostic_state["phase"] = "complete"
    _write_diagnostic(args, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    write_step_summary(report)
    return report


def _write_diagnostic(
    args: argparse.Namespace, report: dict[str, Any] | None = None
) -> None:
    if not args.output:
        return
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    diagnostic = report if report is not None else args.diagnostic_state
    output.write_text(json.dumps(diagnostic, indent=2, sort_keys=True) + "\n")


def _record_diagnostic_step(args: argparse.Namespace, name: str) -> None:
    args.diagnostic_state["phase"] = name
    args.diagnostic_state["completed_steps"].append(name)
    _write_diagnostic(args)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=parse_rows, default=DEFAULT_ROWS)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--rss-cap-mib", type=int, default=384)
    parser.add_argument("--range-rss-cap-mib", type=int, default=256)
    parser.add_argument("--output", help="write the full JSON evidence report here")
    parser.add_argument(
        "--verify-report",
        help="validate an existing evidence report without running tests",
    )
    parser.add_argument(
        "--expected-sha", help="candidate SHA required while validating a report"
    )
    args = parser.parse_args()
    if args.repetitions < 1 or args.rss_cap_mib < 1 or args.range_rss_cap_mib < 1:
        parser.error("repetitions and RSS caps must be positive")
    try:
        if args.verify_report:
            report = json.loads(Path(args.verify_report).read_text())
            validate_report(report, args.expected_sha)
            print(f"verified V17 resource evidence for {report['candidate_sha']}")
        else:
            run_witness(args)
    except (
        OSError,
        ValueError,
        RuntimeError,
        subprocess.CalledProcessError,
        KeyError,
        TypeError,
    ) as error:
        if not args.verify_report and args.output and hasattr(args, "diagnostic_state"):
            args.diagnostic_state["status"] = "failed"
            args.diagnostic_state["failure"] = str(error)
            _write_diagnostic(args)
        print(f"V17 resource verification failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

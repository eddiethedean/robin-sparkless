"""Compare public HTTP SQL reads against a published baseline on one host."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import platform
import subprocess
import sys
import time
from typing import Any
from urllib.request import Request, urlopen

from benchmark_lock import benchmark_lock
from run_slice8_benchmarks import (
    ProcessSampler,
    ProgressMonitor,
    binary_sha256,
    percentile,
    process_metrics,
    repository_dirty_digest,
    repository_dirty_paths,
    repository_revision,
    server_configuration,
    start_server,
    stop_server,
)


def statement_request(url: str, sql: str, **identity: str) -> Request:
    return Request(
        f"{url}/statements",
        data=json.dumps(
            {
                "session": "phase420-http-benchmark",
                "kind": "sql",
                "code": sql,
                **identity,
            }
        ).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )


def fetch(request: Request) -> bytes:
    with urlopen(request, timeout=15) as response:
        return response.read()


def check_result(
    body: bytes, fields: list[str], expected: list[list[Any]], max_result_bytes: int
) -> dict[str, Any]:
    result = json.loads(body)
    if result["status"] != "ok":
        raise RuntimeError(f"HTTP SQL read failed: {result}")
    table = result["data"]["application/json"]
    actual_fields = [field["name"] for field in table["schema"]["fields"]]
    rows = table["data"]
    if (
        actual_fields != fields
        or rows != expected
        or any(
            type(actual) is not type(value)
            for row, expected_row in zip(rows, expected)
            for actual, value in zip(row, expected_row)
        )
    ):
        raise AssertionError(f"HTTP SQL read changed schema or data for {fields}")
    if len(body) > max_result_bytes:
        raise AssertionError("HTTP response exceeded the configured byte limit")
    return table["schema"]


def measure(
    name: str,
    url: str,
    sql: str,
    fields: list[str],
    expected: list[list[Any]],
    pid: int,
    iterations: int,
    max_result_bytes: int,
) -> dict[str, Any]:
    request = statement_request(url, sql)
    warmup = fetch(request)
    schema = check_result(warmup, fields, expected, max_result_bytes)
    body_bytes = len(warmup)
    if name == "near_result_limit" and body_bytes < max_result_bytes * 0.90:
        raise AssertionError("near-limit case did not exercise the response byte limit")

    samples_ms = []
    cpu_before, rss_before = process_metrics(pid)
    sampler = ProcessSampler(pid)
    sampler.start()
    try:
        for _ in range(iterations):
            started = time.perf_counter()
            body = fetch(request)
            samples_ms.append((time.perf_counter() - started) * 1_000)
            if (
                check_result(body, fields, expected, max_result_bytes) != schema
                or len(body) != body_bytes
            ):
                raise AssertionError(
                    "the response schema or size changed between reads"
                )
    finally:
        sampled = sampler.stop()
    if sampled["server_sampling_errors"]:
        raise RuntimeError(str(sampled["server_sampling_errors"]))
    cpu_after, rss_after = process_metrics(pid)
    return {
        "workload": name,
        "sql": sql,
        "schema": schema,
        "response_bytes": body_bytes,
        "samples_ms": samples_ms,
        "sample_count": len(samples_ms),
        "p50_ms": percentile(samples_ms, 0.50),
        "p95_ms": percentile(samples_ms, 0.95),
        "p99_ms": percentile(samples_ms, 0.99),
        "server_cpu_seconds": cpu_after - cpu_before,
        "server_peak_rss_bytes_observed": max(
            rss_before, rss_after, sampled["server_peak_rss_bytes"] or 0
        ),
        "server_samples": sampled["server_samples"],
    }


def prepare_payload_table(url: str, instance_id: str) -> list[list[Any]]:
    rows = [[index, str(index % 10) * 200_000] for index in range(45)]
    setup = ["CREATE TABLE phase420_payload (id BIGINT, payload STRING)"]
    setup.extend(
        f"INSERT INTO phase420_payload VALUES ({index}, '{payload}')"
        for index, payload in rows
    )
    for step, sql in enumerate(setup):
        result = json.loads(
            fetch(
                statement_request(
                    url,
                    sql,
                    server_instance_id=instance_id,
                    operation_id=f"setup-{step}",
                )
            )
        )
        if result.get("status") != "ok":
            raise RuntimeError(f"HTTP fixture setup failed at step {step}: {result}")
    return rows


def run_side(binary: Path, log: Path, iterations: int) -> dict[str, Any]:
    configuration = server_configuration(1_000_000, 2, 5_000)
    max_result_bytes = int(configuration["SPARKLESS_MAX_RESULT_BYTES"])
    table_select = "SELECT id, payload FROM phase420_payload"
    wide_fields = [f"value_{index}" for index in range(32)]
    wide_fields[1] = wide_fields[0]
    wide_values = [1, None, True, "界"] * 8
    wide_exprs = ["CAST(1 AS BIGINT)", "CAST(NULL AS STRING)", "TRUE", "'界'"] * 8
    wide_sql = "SELECT " + ", ".join(
        f"{expr} AS {name}" for expr, name in zip(wide_exprs, wide_fields, strict=True)
    )
    process, ready, _ = start_server(binary, log, 1_000_000, 2, 5_000, 1024, 4 << 20)
    url = ready["http_url"]
    progress = ProgressMonitor(url)
    try:
        small = measure(
            "small_read",
            url,
            "SELECT 1 AS result",
            ["result"],
            [[1]],
            process.pid,
            iterations,
            max_result_bytes,
        )
        wide = measure(
            "wide_mixed_read",
            url,
            wide_sql,
            wide_fields,
            [wide_values],
            process.pid,
            iterations,
            max_result_bytes,
        )
        payload_rows = prepare_payload_table(url, ready["server_instance_id"])
        progress.start()
        try:
            near_limit = measure(
                "near_result_limit",
                url,
                table_select + " WHERE id < 40 ORDER BY id",
                ["id", "payload"],
                payload_rows[:40],
                process.pid,
                iterations,
                max_result_bytes,
            )
        finally:
            progress_result = progress.stop()
        if (
            progress_result["probe_errors"]
            or not progress_result["health_requests"]
            or not progress_result["http_statements"]
        ):
            raise RuntimeError(f"HTTP progress check failed: {progress_result}")
        over_limit = json.loads(
            fetch(statement_request(url, table_select + " ORDER BY id"))
        )
        if over_limit.get(
            "status"
        ) != "error" or "exceeds the configured byte limit" not in over_limit.get(
            "evalue", ""
        ):
            raise AssertionError("HTTP SQL did not reject the over-limit result")
    finally:
        shutdown = stop_server(process)
    if not shutdown["graceful"]:
        raise RuntimeError(f"HTTP benchmark server did not stop cleanly: {shutdown}")
    return {
        "server_command": ready["_command"],
        "server_configuration": configuration,
        "measurements": [small, wide, near_limit],
        "progress_during_near_limit": progress_result,
        "over_limit_error": {key: over_limit[key] for key in ("ename", "evalue")},
        "server_shutdown": shutdown,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-bin", type=Path, required=True)
    parser.add_argument("--server-bin", type=Path, required=True)
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.iterations < 1:
        parser.error("--iterations must be positive")
    binaries = {
        "baseline": args.baseline_bin.resolve(),
        "candidate": args.server_bin.resolve(),
    }
    with benchmark_lock():
        return run_comparison(args, binaries)


def run_comparison(args: argparse.Namespace, binaries: dict[str, Path]) -> int:
    artifacts = {
        side: {
            "path": str(binary),
            "sha256": binary_sha256(binary),
            "version": subprocess.check_output(
                [str(binary), "--version"], text=True
            ).strip(),
        }
        for side, binary in binaries.items()
    }
    if artifacts["baseline"]["sha256"] == artifacts["candidate"]["sha256"]:
        raise ValueError("baseline and candidate must be distinct binaries")
    args.output.mkdir(parents=True)
    path = args.output / "comparison.json"
    dirty_paths = repository_dirty_paths()
    report = {
        "schema_version": 1,
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "command": [sys.executable, *sys.argv],
        "platform": platform.platform(),
        "python": platform.python_version(),
        "source_revision": repository_revision(),
        "source_dirty_sha256": repository_dirty_digest(dirty_paths),
        "artifacts": artifacts,
        "iterations_per_workload": args.iterations,
        "warmups_per_workload": 1,
        "regression_budget": {
            "p50": {"relative": 0.05, "absolute_ms": 1},
            "p95": {"relative": 0.10, "absolute_ms": 2},
        },
        "pairs": [],
        "workload_assessment": [],
    }
    for pair in range(1, 4):
        order = ["baseline", "candidate"] if pair % 2 else ["candidate", "baseline"]
        runs = {}
        for side in order:
            print(f"pair={pair} side={side}", flush=True)
            runs[side] = run_side(
                binaries[side], args.output / f"pair-{pair}-{side}.log", args.iterations
            )
        for base, cand in zip(
            runs["baseline"]["measurements"],
            runs["candidate"]["measurements"],
            strict=True,
        ):
            if (
                base["schema"] != cand["schema"]
                or base["response_bytes"] != cand["response_bytes"]
            ):
                raise AssertionError(
                    "paired HTTP schemas or response byte lengths differ"
                )
            assessment = next(
                (
                    item
                    for item in report["workload_assessment"]
                    if item["workload"] == base["workload"]
                ),
                None,
            )
            if assessment is None:
                assessment = {"workload": base["workload"], "pairs": []}
                report["workload_assessment"].append(assessment)
            pair_metrics = {"pair": pair}
            for quantile, budget in report["regression_budget"].items():
                baseline_ms = base[f"{quantile}_ms"]
                candidate_ms = cand[f"{quantile}_ms"]
                limit_ms = max(baseline_ms * budget["relative"], budget["absolute_ms"])
                pair_metrics[f"{quantile}_change_ms"] = candidate_ms - baseline_ms
                pair_metrics[f"{quantile}_limit_ms"] = limit_ms
                pair_metrics[f"{quantile}_over_budget"] = (
                    candidate_ms - baseline_ms > limit_ms
                )
            assessment["pairs"].append(pair_metrics)
        if (
            runs["baseline"]["over_limit_error"]
            != runs["candidate"]["over_limit_error"]
        ):
            raise AssertionError("paired HTTP over-limit error responses differ")
        report["pairs"].append({"pair": pair, "order": order, **runs})
        path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"report={path}", flush=True)
    failed = False
    for assessment in report["workload_assessment"]:
        for quantile in report["regression_budget"]:
            changes = [pair[f"{quantile}_change_ms"] for pair in assessment["pairs"]]
            assessment[f"median_{quantile}_change_ms"] = sorted(changes)[1]
            breaches = sum(
                pair[f"{quantile}_over_budget"] for pair in assessment["pairs"]
            )
            assessment[f"{quantile}_breached_pairs"] = breaches
            failed = failed or breaches > 1
    path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())

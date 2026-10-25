import unittest

from verify_v17_memory import (
    parse_max_rss_kib,
    parse_measurement,
    validate_catalog_measurement,
    validate_range_benchmark_measurement,
    validate_range_measurement,
    validate_report,
)


class V17MemoryVerifierTests(unittest.TestCase):
    def test_gnu_time_peak_rss_is_parsed_in_kibibytes(self) -> None:
        self.assertEqual(
            parse_max_rss_kib("Maximum resident set size (kbytes): 12345\n"),
            12_345,
        )

    def test_bsd_time_peak_rss_bytes_are_rounded_up_to_kibibytes(self) -> None:
        self.assertEqual(parse_max_rss_kib(" 12345 maximum resident set size\n"), 13)

    def test_catalog_measurement_checks_retained_and_staged_bytes(self) -> None:
        rows = 10_000
        output = (
            "catalog_memory rows=10000 strategy=eager_i64_table_replacement "
            "original_bytes=80000 candidate_bytes=80000 staged_accounted_bytes=160000 "
            "retained_pinned_bytes=80000 read_rows=10000 read_bytes=80000"
        )
        fields = parse_measurement(output, "catalog_memory", rows)
        validate_catalog_measurement(fields, rows)
        fields["staged_accounted_bytes"] = 159_999
        with self.assertRaisesRegex(ValueError, "staged_accounted_bytes"):
            validate_catalog_measurement(fields, rows)

    def test_range_measurement_requires_lazy_single_column_schema(self) -> None:
        output = (
            "range_memory rows=1000000 strategy=lazy_scan construction_us=17 "
            "schema_us=2 row_count_hint=1000000 columns=1"
        )
        validate_range_measurement(
            parse_measurement(output, "range_memory", 1_000_000), 1_000_000
        )
        with self.assertRaisesRegex(ValueError, "row_count_hint"):
            validate_range_measurement(
                {
                    "rows": 1_000_000,
                    "strategy": "lazy_scan",
                    "row_count_hint": 9,
                    "columns": 1,
                },
                1_000_000,
            )

    def test_range_benchmark_checks_count_limit_and_allocation_counters(self) -> None:
        rows = 10_000
        output = (
            "rows=10000 schema_columns=1 schema_type=Long schema_nullable=false "
            "predicate=id>5 count=9994 expected_count=9994 limit_rows=10 "
            "limit_values=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9] output_bytes=80 "
            "strategy=lazy_range_bounded_batches construction_us=50 "
            "construction_allocated_bytes=80422 construction_live_bytes=0 "
            "construction_peak_live_bytes=15970 count_us=242 "
            "count_allocated_bytes=215960 count_live_bytes=0 "
            "count_peak_live_bytes=160904 limit_us=50 limit_allocated_bytes=100 "
            "limit_live_bytes=0 limit_peak_live_bytes=8473"
        )
        fields = parse_measurement(output, "rows=10000", rows)
        validate_range_benchmark_measurement(fields, rows)
        fields["count"] = 9_993
        with self.assertRaisesRegex(ValueError, "count="):
            validate_range_benchmark_measurement(fields, rows)

    def test_candidate_report_requires_complete_repeated_workloads(self) -> None:
        rows = 10_000
        range_measurement = {
            "rows": rows,
            "strategy": "lazy_scan",
            "row_count_hint": rows,
            "columns": 1,
            "construction_us": 5,
            "schema_us": 1,
        }
        catalog_measurement = {
            "rows": rows,
            "original_bytes": rows * 8,
            "candidate_bytes": rows * 8,
            "staged_accounted_bytes": rows * 16,
            "retained_pinned_bytes": rows * 8,
            "read_rows": rows,
            "read_bytes": rows * 8,
        }
        range_benchmark_measurement = {
            "rows": rows,
            "schema_columns": 1,
            "schema_type": "Long",
            "schema_nullable": "false",
            "count": rows - 6,
            "expected_count": rows - 6,
            "limit_rows": 10,
            "output_bytes": 80,
            "strategy": "lazy_range_bounded_batches",
            "construction_us": 10,
            "construction_allocated_bytes": 100,
            "construction_live_bytes": 10,
            "construction_peak_live_bytes": 20,
            "count_us": 10,
            "count_allocated_bytes": 100,
            "count_live_bytes": 10,
            "count_peak_live_bytes": 20,
            "limit_us": 10,
            "limit_allocated_bytes": 100,
            "limit_live_bytes": 10,
            "limit_peak_live_bytes": 20,
        }
        measured = {
            "rows": rows,
            "repeat": 1,
            "test_passed": True,
            "peak_rss_kib": 10_000,
            "elapsed_seconds": 0.01,
        }
        report = {
            "schema_version": 1,
            "candidate_sha": "abc123",
            "rows": [rows],
            "repetitions": 1,
            "caps_mib": {
                "lazy_range_construction": 256,
                "range_workload": 256,
                "catalog_retention_staging": 384,
                "sustained_http": 384,
            },
            "lazy_range_construction": [{**measured, "measurement": range_measurement}],
            "range_workload": [
                {**measured, "measurement": range_benchmark_measurement}
            ],
            "catalog_retention_staging": [
                {**measured, "measurement": catalog_measurement}
            ],
            "sustained_http": [{**measured, "rows": None}],
            "bounded_range_count": {"test_passed": True, "peak_rss_kib": 10_000},
        }
        validate_report(report, "abc123")
        with self.assertRaisesRegex(ValueError, "SHA"):
            validate_report(report, "different")
        report["sustained_http"][0]["peak_rss_kib"] = 384 * 1024 + 1
        with self.assertRaisesRegex(ValueError, "sustained HTTP"):
            validate_report(report, "abc123")


if __name__ == "__main__":
    unittest.main()

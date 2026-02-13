.PHONY: build test test-rust test-python sparkless-parity pyspark-parity extract-pyspark-tests extract-pyspark-tests-expanded batch-regenerate-extracted test-parity-phase-a test-parity-phase-b test-parity-phase-c test-parity-phase-d test-parity-phase-e test-parity-phase-f test-parity-phase-g test-parity-phases bench-python clean check check-full fmt fmt-check clippy audit outdated deny lint-python all gap-analysis gap-analysis-quick gap-analysis-runtime

# Use stable toolchain when no default is configured (override with RUSTUP_TOOLCHAIN=nightly etc.)
export RUSTUP_TOOLCHAIN ?= stable
# Use real Cargo home to avoid sandbox cache corruption (e.g. Cursor IDE).
# Force override (:=) so Cursor/sandbox CARGO_HOME does not take precedence.
export CARGO_HOME := $(HOME)/.cargo

# Build
build:
	cargo build

build-release:
	cargo build --release

# Run Rust tests only
test-rust:
	cargo test

# Run Python linters and type checker (ruff format, ruff check, mypy). Uses same .venv as test-python.
lint-python:
	@if [ ! -d .venv ]; then python3 -m venv .venv; fi
	. .venv/bin/activate && pip install -q ruff 'mypy>=1.4,<1.10' && ruff format --check . && ruff check . && mypy .

# Run Python tests (creates .venv, installs extension with sql+delta, runs pytest)
# SQL and Delta features enable create_or_replace_temp_view, table(), sql(), read_delta, write_delta.
test-python:
	@if [ ! -d .venv ]; then python3 -m venv .venv; fi
	. .venv/bin/activate && pip install -q maturin pytest && maturin develop --features "pyo3,sql,delta"
	. .venv/bin/activate && pytest tests/python/ -v

# Run all tests (Rust + Python)
test: test-rust test-python

# Compare robin-sparkless vs sparkless performance. Use .venv-sparkless for both backends (pip install sparkless + maturin develop).
# Quick run (smaller sizes): . .venv-sparkless/bin/activate && python scripts/bench_robin_vs_sparkless.py --quick
bench-python:
	@if [ ! -d .venv-sparkless ]; then python3 -m venv .venv-sparkless; fi
	. .venv-sparkless/bin/activate && pip install -q maturin sparkless && maturin develop --features pyo3
	. .venv-sparkless/bin/activate && python scripts/bench_robin_vs_sparkless.py

# Run parity tests (optionally convert from Sparkless expected_outputs, regenerate expected from PySpark, then run).
# Set SPARKLESS_EXPECTED_OUTPUTS=/path/to/sparkless/tests/expected_outputs to run converter + regeneration.
sparkless-parity:
	@if [ -n "$$SPARKLESS_EXPECTED_OUTPUTS" ]; then \
		mkdir -p tests/fixtures/converted; \
		python3 tests/convert_sparkless_fixtures.py --batch "$$SPARKLESS_EXPECTED_OUTPUTS" tests/fixtures --output-subdir converted --dedupe; \
		 python3 tests/regenerate_expected_from_pyspark.py tests/fixtures/converted --include-skipped 2>/dev/null || true; \
	fi
	cargo test pyspark_parity_fixtures

# Run parity on tests/fixtures/, tests/fixtures/converted/, tests/fixtures/pyspark_extracted/
pyspark-parity:
	cargo test pyspark_parity_fixtures

# Extract PySpark SQL tests to fixtures and pytest stubs. Requires SPARK_REPO_PATH or use --clone.
# Uses expanded TARGET_FILES (18+ files: test_functions, test_dataframe, test_column, test_catalog, etc.).
# Example: make extract-pyspark-tests
# Example: SPARK_REPO_PATH=/path/to/spark make extract-pyspark-tests
extract-pyspark-tests:
	@if [ -n "$$SPARK_REPO_PATH" ]; then \
		python3 scripts/extract_pyspark_tests.py --spark-repo "$$SPARK_REPO_PATH"; \
	else \
		python3 scripts/extract_pyspark_tests.py --clone --branch v3.5.0; \
	fi
	@echo "Extracted fixtures -> tests/fixtures/pyspark_extracted. Run: make batch-regenerate-extracted"

# Alias for extract-pyspark-tests (uses expanded target files)
extract-pyspark-tests-expanded: extract-pyspark-tests

# Regenerate expected sections for all pyspark_extracted fixtures. Requires PySpark + Java 17+.
batch-regenerate-extracted:
	python3 scripts/batch_regenerate_extracted.py

# Run parity tests for a specific phase (A–G). Uses tests/fixtures/phase_manifest.json.
test-parity-phase-a: ; PARITY_PHASE=a cargo test pyspark_parity_fixtures --
test-parity-phase-b: ; PARITY_PHASE=b cargo test pyspark_parity_fixtures --
test-parity-phase-c: ; PARITY_PHASE=c cargo test pyspark_parity_fixtures --
test-parity-phase-d: ; PARITY_PHASE=d cargo test pyspark_parity_fixtures --
test-parity-phase-e: ; PARITY_PHASE=e cargo test pyspark_parity_fixtures --
test-parity-phase-f: ; PARITY_PHASE=f cargo test pyspark_parity_fixtures --
test-parity-phase-g: ; PARITY_PHASE=g cargo test pyspark_parity_fixtures --

# Run all phase-specific parity tests
test-parity-phases: test-parity-phase-a test-parity-phase-b test-parity-phase-c test-parity-phase-d test-parity-phase-e test-parity-phase-f test-parity-phase-g

# Run all Rust checks (format check, clippy, audit, deny, Rust tests). Completes without Python build.
# Run with -j5 to run the five jobs in parallel: make -j5 check
check: fmt-check clippy audit deny test-rust
	@echo "All checks passed"

# Run full check: Rust checks + Python lint (ruff, mypy) + Python tests (builds PyO3 extension).
# Run with -j3 to run Rust checks, lint-python, and test-python in parallel: make -j3 check-full
check-full: check lint-python test-python
	@echo "All checks including Python passed"

# Format code
fmt:
	cargo fmt
	@echo "Formatted"

# Check format without modifying
fmt-check:
	cargo fmt --check

# Lint with Clippy
clippy:
	cargo clippy -- -D warnings

# Security: scan for known vulnerabilities
audit:
	cargo audit

# List outdated dependencies
outdated:
	cargo outdated

# Advisory, bans, sources checks (skip licenses - needs per-crate clarifications)
deny:
	cargo deny check advisories bans sources

# Run everything: format, lint, security, deny, tests
all: check
	@echo "All updates and checks complete"

# Gap analysis: PySpark vs robin-sparkless (from Apache Spark repo)
# Full pipeline: clone Spark, extract APIs, produce docs/GAP_ANALYSIS_PYSPARK_REPO.{json,md}
gap-analysis:
	python3 scripts/extract_pyspark_api_from_repo.py --clone --branch v3.5.0 --output docs/pyspark_api_from_repo.json
	python3 scripts/extract_robin_api_from_source.py --output docs/robin_api_from_source.json
	python3 scripts/gap_analysis_pyspark_repo.py --pyspark docs/pyspark_api_from_repo.json --robin docs/robin_api_from_source.json --write-md docs/GAP_ANALYSIS_PYSPARK_REPO.md
	@echo "Gap analysis complete. See docs/GAP_ANALYSIS_PYSPARK_REPO.md"

# Quick gap analysis (no clone): use existing pyspark_api_from_repo.json and robin signatures
gap-analysis-quick:
	@test -f docs/pyspark_api_from_repo.json || (echo "Run 'make gap-analysis' first to create docs/pyspark_api_from_repo.json" && exit 1)
	python3 scripts/extract_robin_api_from_source.py --output docs/robin_api_from_source.json
	python3 scripts/gap_analysis_pyspark_repo.py --pyspark docs/pyspark_api_from_repo.json --robin docs/robin_api_from_source.json --write-md docs/GAP_ANALYSIS_PYSPARK_REPO.md
	@echo "Gap analysis complete. See docs/GAP_ANALYSIS_PYSPARK_REPO.md"

# Gap analysis using runtime introspection (requires maturin develop; accurate param names)
gap-analysis-runtime:
	@test -f docs/pyspark_api_from_repo.json || (echo "Run 'make gap-analysis' first to create docs/pyspark_api_from_repo.json" && exit 1)
	. .venv/bin/activate && pip install -q maturin && maturin develop --features "pyo3,sql,delta"
	. .venv/bin/activate && python scripts/export_robin_signatures.py --output docs/signatures_robin_sparkless.json
	python3 scripts/gap_analysis_pyspark_repo.py --pyspark docs/pyspark_api_from_repo.json --robin docs/signatures_robin_sparkless.json --write-md docs/GAP_ANALYSIS_PYSPARK_REPO.md
	@echo "Gap analysis (runtime) complete. See docs/GAP_ANALYSIS_PYSPARK_REPO.md"

# Clean
clean:
	cargo clean
	rm -rf target/

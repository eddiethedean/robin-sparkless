.PHONY: build test test-rust test-python sparkless-parity bench-python clean check check-full fmt clippy audit outdated deny lint-python all gap-analysis gap-analysis-quick gap-analysis-runtime

# Use stable toolchain when no default is configured (override with RUSTUP_TOOLCHAIN=nightly etc.)
export RUSTUP_TOOLCHAIN ?= stable
# Use real Cargo home to avoid sandbox cache corruption (e.g. Cursor IDE)
export CARGO_HOME ?= $(HOME)/.cargo

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
		 python3 tests/regenerate_expected_from_pyspark.py tests/fixtures/converted 2>/dev/null || true; \
	fi
	cargo test pyspark_parity_fixtures

# Run all Rust checks (format, clippy, audit, deny, Rust tests). Completes without Python build.
check: fmt clippy audit deny test-rust
	@echo "All checks passed"

# Run full check: Rust checks + Python lint (ruff, mypy) + Python tests (builds PyO3 extension).
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
